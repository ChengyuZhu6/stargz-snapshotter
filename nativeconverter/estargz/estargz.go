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
		log.G(ctx).Infof("Starting layer conversion for digest: %s", desc.Digest)

		// Step 1: Check if it's a layer type
		stepStart := time.Now()
		if !images.IsLayerType(desc.MediaType) {
			log.G(ctx).Infof("Step 1 (Layer type check): %v - Not a layer type, skipping conversion", time.Since(stepStart))
			return nil, nil
		}
		log.G(ctx).Infof("Step 1 (Layer type check): %v - Layer type confirmed", time.Since(stepStart))

		// Step 2: Get content info
		stepStart = time.Now()
		info, err := cs.Info(ctx, desc.Digest)
		if err != nil {
			return nil, err
		}
		labelz := info.Labels
		if labelz == nil {
			labelz = make(map[string]string)
		}
		log.G(ctx).Infof("Step 2 (Get content info): %v", time.Since(stepStart))

		// Step 3: Create reader
		stepStart = time.Now()
		ra, err := cs.ReaderAt(ctx, desc)
		if err != nil {
			return nil, err
		}
		defer ra.Close()
		sr := io.NewSectionReader(ra, 0, desc.Size)
		log.G(ctx).Infof("Step 3 (Create reader): %v", time.Since(stepStart))

		// Step 4: Build eStargz
		stepStart = time.Now()
		blob, err := estargz.Build(sr, append(opts, estargz.WithContext(ctx))...)
		if err != nil {
			return nil, err
		}
		defer blob.Close()
		log.G(ctx).Infof("Step 4 (Build eStargz): %v", time.Since(stepStart))

		// Step 5: Prepare writer
		stepStart = time.Now()
		ref := fmt.Sprintf("convert-estargz-from-%s", desc.Digest)
		w, err := content.OpenWriter(ctx, cs, content.WithRef(ref))
		if err != nil {
			return nil, err
		}
		defer w.Close()

		// Reset the writing position
		// Old writer possibly remains without aborted
		// (e.g. conversion interrupted by a signal)
		if err := w.Truncate(0); err != nil {
			return nil, err
		}
		log.G(ctx).Infof("Step 5 (Prepare writer): %v", time.Since(stepStart))

		// Step 6: Setup concurrent processing for copy and decompression counting
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

		// Copy data and measure
		copyStart := time.Now()
		n, err := io.Copy(w, io.TeeReader(blob, pw))
		if err != nil {
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
		log.G(ctx).Infof("Step 6 (Data copy and decompression): %v (pure copy: %v)", time.Since(stepStart), copyDuration)

		// Step 7: Update metadata and commit
		stepStart = time.Now()
		labelz[labels.LabelUncompressed] = blob.DiffID().String()
		if err = w.Commit(ctx, n, "", content.WithLabels(labelz)); err != nil && !errdefs.IsAlreadyExists(err) {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		log.G(ctx).Infof("Step 7 (Update metadata and commit): %v", time.Since(stepStart))

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
		log.G(ctx).Infof("Step 8 (Build new descriptor): %v", time.Since(stepStart))

		totalDuration := time.Since(totalStart)
		log.G(ctx).Infof("Layer conversion completed in %v. Original size: %d, New size: %d, Compression ratio: %.2f%%",
			totalDuration, desc.Size, n, float64(n)/float64(desc.Size)*100)

		return &newDesc, nil
	}
}
