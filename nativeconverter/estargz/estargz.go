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

package estargz

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
	"github.com/containerd/stargz-snapshotter/util/ioutils"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// LayerConvertWithLayerAndCommonOptsFunc converts legacy tar.gz layers into eStargz tar.gz
// layers. Media type is unchanged. Should be used in conjunction with WithDockerToOCI(). See
// LayerConvertFunc for more details. The difference between this function and
// LayerConvertFunc is that this allows to specify additional eStargz options per layer.
func LayerConvertWithLayerAndCommonOptsFunc(opts map[digest.Digest][]estargz.Option, commonOpts ...estargz.Option) converter.ConvertFunc {
	if opts == nil {
		return LayerConvertFunc(commonOpts...)
	}
	return func(ctx context.Context, cs content.Store, desc ocispec.Descriptor) (*ocispec.Descriptor, error) {
		// TODO: enable to speciy option per layer "index" because it's possible that there are
		//       two layers having same digest in an image (but this should be rare case)
		return LayerConvertFunc(append(commonOpts, opts[desc.Digest]...)...)(ctx, cs, desc)
	}
}

// LayerConvertFunc converts legacy tar.gz layers into eStargz tar.gz layers.
// Media type is unchanged.
//
// Should be used in conjunction with WithDockerToOCI().
//
// Otherwise "containerd.io/snapshot/stargz/toc.digest" annotation will be lost,
// because the Docker media type does not support layer annotations.
func LayerConvertFunc(opts ...estargz.Option) converter.ConvertFunc {
	return func(ctx context.Context, cs content.Store, desc ocispec.Descriptor) (*ocispec.Descriptor, error) {
		totalStart := time.Now()

		// Performance tracking structure
		stages := make(map[string]float64)
		perfData := map[string]interface{}{
			"digest":        desc.Digest.String(),
			"original_size": desc.Size,
			"timestamp":     totalStart.Format(time.RFC3339),
			"stages":        stages,
		}

		// Step 1: Check if it's a layer type
		stepStart := time.Now()
		if !images.IsLayerType(desc.MediaType) {
			stages["layer_type_check"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6 // milliseconds
			perfData["success"] = false
			perfData["reason"] = "not_layer_type"
			log.G(ctx).Infof("PERF_DATA: %s", mustMarshalJSON(perfData))
			return nil, nil
		}
		stages["layer_type_check"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 2: Get content info
		stepStart = time.Now()
		info, err := cs.Info(ctx, desc.Digest)
		if err != nil {
			stages["get_content_info"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			log.G(ctx).Infof("PERF_DATA: %s", mustMarshalJSON(perfData))
			return nil, err
		}
		labelz := info.Labels
		if labelz == nil {
			labelz = make(map[string]string)
		}
		stages["get_content_info"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 3: Create reader
		stepStart = time.Now()
		ra, err := cs.ReaderAt(ctx, desc)
		if err != nil {
			stages["create_reader"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			log.G(ctx).Infof("PERF_DATA: %s", mustMarshalJSON(perfData))
			return nil, err
		}
		defer ra.Close()
		sr := io.NewSectionReader(ra, 0, desc.Size)
		stages["create_reader"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 4: Build eStargz
		stepStart = time.Now()
		blob, err := estargz.Build(sr, append(opts, estargz.WithContext(ctx))...)
		if err != nil {
			stages["build_estargz"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			log.G(ctx).Infof("PERF_DATA: %s", mustMarshalJSON(perfData))
			return nil, err
		}
		defer blob.Close()
		stages["build_estargz"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 5: Prepare writer
		stepStart = time.Now()
		ref := fmt.Sprintf("convert-estargz-from-%s", desc.Digest)
		w, err := content.OpenWriter(ctx, cs, content.WithRef(ref))
		if err != nil {
			stages["prepare_writer"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			log.G(ctx).Infof("PERF_DATA: %s", mustMarshalJSON(perfData))
			return nil, err
		}
		defer w.Close()

		// Reset the writing position
		if err := w.Truncate(0); err != nil {
			stages["prepare_writer"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			log.G(ctx).Infof("PERF_DATA: %s", mustMarshalJSON(perfData))
			return nil, err
		}
		stages["prepare_writer"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 6: Data copy and decompression
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
			log.G(ctx).Infof("PERF_DATA: %s", mustMarshalJSON(perfData))
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

		// Step 7: Update metadata and commit
		stepStart = time.Now()
		labelz[labels.LabelUncompressed] = blob.DiffID().String()
		if err = w.Commit(ctx, n, "", content.WithLabels(labelz)); err != nil && !errdefs.IsAlreadyExists(err) {
			stages["update_commit"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			log.G(ctx).Infof("PERF_DATA: %s", mustMarshalJSON(perfData))
			return nil, err
		}
		if err := w.Close(); err != nil {
			stages["update_commit"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6
			perfData["success"] = false
			perfData["error"] = err.Error()
			log.G(ctx).Infof("PERF_DATA: %s", mustMarshalJSON(perfData))
			return nil, err
		}
		stages["update_commit"] = float64(time.Since(stepStart).Nanoseconds()) / 1e6

		// Step 8: Build new descriptor
		stepStart = time.Now()
		newDesc := desc
		if uncompress.IsUncompressedType(newDesc.MediaType) {
			if images.IsDockerType(newDesc.MediaType) {
				newDesc.MediaType += ".gzip"
			} else {
				newDesc.MediaType += "+gzip"
			}
		}
		newDesc.Digest = w.Digest()
		newDesc.Size = n
		if newDesc.Annotations == nil {
			newDesc.Annotations = make(map[string]string, 1)
		}
		newDesc.Annotations[estargz.TOCJSONDigestAnnotation] = blob.TOCDigest().String()
		newDesc.Annotations[estargz.StoreUncompressedSizeAnnotation] = fmt.Sprintf("%d", c.Size())
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

// mustMarshalJSON marshals data to JSON, returns empty object on error
func mustMarshalJSON(data interface{}) string {
	bytes, err := json.Marshal(data)
	if err != nil {
		return "{}"
	}
	return string(bytes)
}
