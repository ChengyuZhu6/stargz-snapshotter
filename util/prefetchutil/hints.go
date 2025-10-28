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

package prefetchutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/log"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// PrefetchHint represents a stored prefetch hint for an image type
type PrefetchHint struct {
	FeatureKey     string        `json:"feature_key"`
	RecordDigest   digest.Digest `json:"record_digest"`
	ManifestDigest digest.Digest `json:"manifest_digest"`
	ImageRef       string        `json:"image_ref"`
	CreatedAt      int64         `json:"created_at"`
}

// HintsIndex manages the storage and retrieval of prefetch hints
type HintsIndex struct {
	indexPath string
	mu        sync.RWMutex
	hints     map[string]*PrefetchHint
}

// NewHintsIndex creates a new hints index with the specified storage path
func NewHintsIndex(indexPath string) *HintsIndex {
	if indexPath == "" {
		homeDir, _ := os.UserHomeDir()
		indexPath = filepath.Join(homeDir, ".stargz-prefetch", "hints.json")
	}

	return &HintsIndex{
		indexPath: indexPath,
		hints:     make(map[string]*PrefetchHint),
	}
}

// Load reads the hints index from disk
func (h *HintsIndex) Load() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(h.indexPath), 0755); err != nil {
		return fmt.Errorf("failed to create hints directory: %w", err)
	}

	file, err := os.Open(h.indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, start with empty index
			return nil
		}
		return fmt.Errorf("failed to open hints index: %w", err)
	}
	defer file.Close()

	var hints map[string]*PrefetchHint
	if err := json.NewDecoder(file).Decode(&hints); err != nil {
		return fmt.Errorf("failed to decode hints index: %w", err)
	}

	h.hints = hints
	return nil
}

// Save writes the hints index to disk
func (h *HintsIndex) Save() error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(h.indexPath), 0755); err != nil {
		return fmt.Errorf("failed to create hints directory: %w", err)
	}

	file, err := os.Create(h.indexPath)
	if err != nil {
		return fmt.Errorf("failed to create hints index file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(h.hints); err != nil {
		return fmt.Errorf("failed to encode hints index: %w", err)
	}

	return nil
}

// Get retrieves a prefetch hint by feature key
func (h *HintsIndex) Get(featureKey string) *PrefetchHint {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.hints[featureKey]
}

// Put stores a prefetch hint
func (h *HintsIndex) Put(featureKey string, hint *PrefetchHint) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.hints[featureKey] = hint
}

// Delete removes a prefetch hint
func (h *HintsIndex) Delete(featureKey string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.hints, featureKey)
}

// List returns all stored hints
func (h *HintsIndex) List() map[string]*PrefetchHint {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make(map[string]*PrefetchHint)
	for k, v := range h.hints {
		result[k] = v
	}
	return result
}

// ExtractPrefetchListFromRecord extracts file paths from a recorder digest
func ExtractPrefetchListFromRecord(ctx context.Context, cs content.Store, recordDigest digest.Digest) ([]string, error) {
	ra, err := cs.ReaderAt(ctx, ocispec.Descriptor{Digest: recordDigest})
	if err != nil {
		return nil, fmt.Errorf("failed to get reader for record: %w", err)
	}
	defer ra.Close()

	var paths []string
	dec := json.NewDecoder(io.NewSectionReader(ra, 0, ra.Size()))

	for dec.More() {
		var entry struct {
			Path string `json:"path"`
		}
		if err := dec.Decode(&entry); err != nil {
			return nil, fmt.Errorf("failed to decode record entry: %w", err)
		}
		if entry.Path != "" {
			paths = append(paths, entry.Path)
		}
	}

	return paths, nil
}

// SavePrefetchListToFile saves a list of prefetch paths to a file
func SavePrefetchListToFile(paths []string, filePath string) error {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create prefetch list file: %w", err)
	}
	defer file.Close()

	for _, path := range paths {
		if _, err := fmt.Fprintln(file, path); err != nil {
			return fmt.Errorf("failed to write path to file: %w", err)
		}
	}

	return nil
}

// GetTempPrefetchListPath generates a temporary file path for prefetch list
func GetTempPrefetchListPath(featureKey string) string {
	tempDir := os.TempDir()
	return filepath.Join(tempDir, fmt.Sprintf("stargz-prefetch-%s.txt", featureKey[:16]))
}

// LogHintUsage logs information about hint usage
func LogHintUsage(ctx context.Context, action string, featureKey string, imageRef string) {
	log.G(ctx).WithFields(map[string]interface{}{
		"action":      action,
		"feature_key": featureKey[:16] + "...",
		"image_ref":   imageRef,
	}).Info("prefetch hint operation")
}
