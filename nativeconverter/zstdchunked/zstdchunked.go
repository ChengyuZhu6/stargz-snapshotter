/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package zstdchunked

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/images/converter"
	"github.com/containerd/containerd/v2/core/images/converter/uncompress"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/containerd/containerd/v2/pkg/labels"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/containerd/stargz-snapshotter/estargz"
	"github.com/containerd/stargz-snapshotter/estargz/zstdchunked"
	"github.com/containerd/stargz-snapshotter/util/ioutils"
	"github.com/klauspost/compress/zstd"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type zstdCompression struct {
	*zstdchunked.Decompressor
	*zstdchunked.Compressor
}

// LayerConvertWithLayerOptsFunc converts legacy tar.gz layers into zstd:chunked layers.
//
// This changes Docker MediaType to OCI MediaType so this should be used in
// conjunction with WithDockerToOCI().
// See LayerConvertFunc for more details. The difference between this function and
// LayerConvertFunc is that this allows to specify additional eStargz options per layer.
func LayerConvertWithLayerOptsFunc(opts map[digest.Digest][]estargz.Option) converter.ConvertFunc {
	return LayerConvertWithLayerOptsFuncWithCompressionLevel(zstd.SpeedDefault, opts)
}

// LayerConvertFunc converts legacy tar.gz layers into zstd:chunked layers.
//
// This changes Docker MediaType to OCI MediaType so this should be used in
// conjunction with WithDockerToOCI().
//
// Otherwise "io.containers.zstd-chunked.manifest-checksum" annotation will be lost,
// because the Docker media type does not support layer annotations.
//
// SpeedDefault (level 3) is used for the compression level.
// See also: https://pkg.go.dev/github.com/klauspost/compress/zstd#EncoderLevel
func LayerConvertFunc(opts ...estargz.Option) converter.ConvertFunc {
	return LayerConvertFuncWithCompressionLevel(zstd.SpeedDefault, opts...)
}

// LayerConvertWithLayerOptsFuncWithCompressionLevel converts legacy tar.gz layers into zstd:chunked layers.
// This function allows to specify the compression level of zstd.
//
// This changes Docker MediaType to OCI MediaType so this should be used in
// conjunction with WithDockerToOCI().
// See LayerConvertFunc for more details. The difference between this function and
// LayerConvertFunc is that this allows to specify additional eStargz options per layer and
// allows to specify the compression level.
func LayerConvertWithLayerOptsFuncWithCompressionLevel(compressionLevel zstd.EncoderLevel, opts map[digest.Digest][]estargz.Option) converter.ConvertFunc {
	if opts == nil {
		return LayerConvertFuncWithCompressionLevel(compressionLevel)
	}
	return func(ctx context.Context, cs content.Store, desc ocispec.Descriptor) (*ocispec.Descriptor, error) {
		// TODO: enable to speciy option per layer "index" because it's possible that there are
		//       two layers having same digest in an image (but this should be rare case)
		return LayerConvertFuncWithCompressionLevel(compressionLevel, opts[desc.Digest]...)(ctx, cs, desc)
	}
}

// LayerConvertFuncWithCompressionLevel converts legacy tar.gz layers into zstd:chunked layers with
// the specified compression level.
//
// This changes Docker MediaType to OCI MediaType so this should be used in
// conjunction with WithDockerToOCI().
// See LayerConvertFunc for more details. The difference between this function and
// LayerConvertFunc is that this allows configuring the compression level.
func LayerConvertFuncWithCompressionLevel(compressionLevel zstd.EncoderLevel, opts ...estargz.Option) converter.ConvertFunc {
	return func(ctx context.Context, cs content.Store, desc ocispec.Descriptor) (*ocispec.Descriptor, error) {
		totalStart := time.Now()

		// Performance tracking structure
		stages := make(map[string]float64)
		perfData := map[string]interface{}{
			"digest":            desc.Digest.String(),
			"original_size":     desc.Size,
			"timestamp":         totalStart.Format(time.RFC3339),
			"converter":         "zstdchunked",
			"compression_level": int(compressionLevel),
			"stages":            stages,
		}

		// Step 1: Check if it's a layer type
		stepStart := time.Now()
		if !images.IsLayerType(desc.MediaType) {
			stages["layer_type_check"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["reason"] = "not_layer_type"
			fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
			return nil, nil
		}
		stages["layer_type_check"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 2: Handle uncompression if needed
		stepStart = time.Now()
		uncompressedDesc := &desc
		// We need to uncompress the archive first
		if !uncompress.IsUncompressedType(desc.MediaType) {
			var err error
			uncompressedDesc, err = uncompress.LayerConvertFunc(ctx, cs, desc)
			if err != nil {
				stages["uncompress_layer"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
				perfData["success"] = false
				perfData["error"] = err.Error()
				fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
				return nil, err
			}
			if uncompressedDesc == nil {
				stages["uncompress_layer"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
				perfData["success"] = false
				perfData["error"] = "unexpectedly got the same blob after compression"
				fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
				return nil, fmt.Errorf("unexpectedly got the same blob after compression (%s, %q)", desc.Digest, desc.MediaType)
			}
			log.G(ctx).Debugf("zstdchunked: uncompressed %s into %s", desc.Digest, uncompressedDesc.Digest)
		}
		stages["uncompress_layer"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 3: Get content info
		stepStart = time.Now()
		info, err := cs.Info(ctx, desc.Digest)
		if err != nil {
			stages["get_content_info"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
			return nil, err
		}
		labelz := info.Labels
		if labelz == nil {
			labelz = make(map[string]string)
		}
		stages["get_content_info"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 4: Create reader
		stepStart = time.Now()
		uncompressedReaderAt, err := cs.ReaderAt(ctx, *uncompressedDesc)
		if err != nil {
			stages["create_reader"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
			return nil, err
		}
		defer uncompressedReaderAt.Close()
		uncompressedSR := io.NewSectionReader(uncompressedReaderAt, 0, uncompressedDesc.Size)
		stages["create_reader"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 5: Build zstdchunked
		stepStart = time.Now()
		metadata := make(map[string]string)
		opts = append(opts, estargz.WithCompression(&zstdCompression{
			new(zstdchunked.Decompressor),
			&zstdchunked.Compressor{
				CompressionLevel: compressionLevel,
				Metadata:         metadata,
			},
		}))
		blob, err := estargz.Build(uncompressedSR, append(opts, estargz.WithContext(ctx))...)
		if err != nil {
			stages["build_zstdchunked"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
			return nil, err
		}
		defer blob.Close()
		stages["build_zstdchunked"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 6: Prepare writer
		stepStart = time.Now()
		ref := fmt.Sprintf("convert-zstdchunked-from-%s", desc.Digest)
		w, err := content.OpenWriter(ctx, cs, content.WithRef(ref))
		if err != nil {
			stages["prepare_writer"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
			return nil, err
		}
		defer w.Close()

		// Reset the writing position
		if err := w.Truncate(0); err != nil {
			stages["prepare_writer"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
			return nil, err
		}
		stages["prepare_writer"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 7: Data copy and decompression
		stepStart = time.Now()
		pr, pw := io.Pipe()
		c := new(ioutils.CountWriter)
		doneCount := make(chan struct{})
		go func() {
			defer close(doneCount)
			defer pr.Close()
			decompressR, err := compression.DecompressStream(pr)
			if err != nil {
				pr.CloseWithError(err)
				return
			}
			defer decompressR.Close()
			if _, err := io.Copy(c, decompressR); err != nil {
				pr.CloseWithError(err)
				return
			}
		}()

		copyStart := time.Now()
		n, err := io.Copy(w, io.TeeReader(blob, pw))
		if err != nil {
			stages["data_copy"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
			return nil, err
		}
		copyDuration := time.Since(copyStart)

		if err := blob.Close(); err != nil {
			return nil, err
		}
		if err := pw.Close(); err != nil {
			return nil, err
		}
		<-doneCount

		stages["data_copy"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
		perfData["pure_copy_ms"] = float64(copyDuration.Nanoseconds()) / 1e6

		// Step 8: Update metadata and commit
		stepStart = time.Now()
		// update diffID label
		labelz[labels.LabelUncompressed] = blob.DiffID().String()
		if err = w.Commit(ctx, n, "", content.WithLabels(labelz)); err != nil && !errdefs.IsAlreadyExists(err) {
			stages["update_commit"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
			return nil, err
		}
		if err := w.Close(); err != nil {
			stages["update_commit"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
			return nil, err
		}
		stages["update_commit"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 9: Build new descriptor
		stepStart = time.Now()
		newDesc := desc
		newDesc.MediaType, err = convertMediaTypeToZstd(newDesc.MediaType)
		if err != nil {
			stages["build_descriptor"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))
			return nil, err
		}
		newDesc.Digest = w.Digest()
		newDesc.Size = n
		if newDesc.Annotations == nil {
			newDesc.Annotations = make(map[string]string, 1)
		}
		tocDgst := blob.TOCDigest().String()
		newDesc.Annotations[estargz.TOCJSONDigestAnnotation] = tocDgst
		newDesc.Annotations[estargz.StoreUncompressedSizeAnnotation] = fmt.Sprintf("%d", c.Size())
		if p, ok := metadata[zstdchunked.ManifestChecksumAnnotation]; ok {
			newDesc.Annotations[zstdchunked.ManifestChecksumAnnotation] = p
		}
		if p, ok := metadata[zstdchunked.ManifestPositionAnnotation]; ok {
			newDesc.Annotations[zstdchunked.ManifestPositionAnnotation] = p
		}
		stages["build_descriptor"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Final metrics
		totalDuration := time.Since(totalStart)
		perfData["total_duration_ms"] = float64(totalDuration.Nanoseconds()) / 1e6
		perfData["compressed_size"] = n
		perfData["compression_ratio"] = float64(n) / float64(desc.Size) * 100
		perfData["success"] = true

		// Output performance data as JSON
		fmt.Println("PERF_DATA:", mustMarshalJSON(perfData))

		return &newDesc, nil
	}
}

// NOTE: this converts docker mediatype to OCI mediatype
func convertMediaTypeToZstd(mt string) (string, error) {
	ociMediaType := converter.ConvertDockerMediaTypeToOCI(mt)
	switch ociMediaType {
	case ocispec.MediaTypeImageLayer, ocispec.MediaTypeImageLayerGzip, ocispec.MediaTypeImageLayerZstd:
		return ocispec.MediaTypeImageLayerZstd, nil
	case ocispec.MediaTypeImageLayerNonDistributable, ocispec.MediaTypeImageLayerNonDistributableGzip, ocispec.MediaTypeImageLayerNonDistributableZstd: //nolint:staticcheck // deprecated
		return ocispec.MediaTypeImageLayerNonDistributableZstd, nil //nolint:staticcheck // deprecated
	default:
		return "", fmt.Errorf("unknown mediatype %q", mt)
	}
}

// mustMarshalJSON marshals data to JSON, returns empty object on error
func mustMarshalJSON(data interface{}) string {
	bytes, err := json.Marshal(data)
	if err != nil {
		return "{}"
	}
	return string(bytes)
}
