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
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images/converter/uncompress"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/containerd/platforms"
	"github.com/containerd/stargz-snapshotter/util/containerdutil"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
)

// ImageFeatures represents the characteristics of an image for similarity detection
type ImageFeatures struct {
	OS           string   `json:"os"`
	Architecture string   `json:"architecture"`
	Entrypoint   []string `json:"entrypoint"`
	Cmd          []string `json:"cmd"`
	Env          []string `json:"env"`

	// File system characteristics
	PackageManagers []string `json:"package_managers"`
	Runtimes        []string `json:"runtimes"`
	Frameworks      []string `json:"frameworks"`
	OSRelease       string   `json:"os_release"`
}

// FeatureKey generates a stable key for image similarity matching
func (f *ImageFeatures) FeatureKey() string {
	// Normalize and sort all fields for consistent hashing
	normalized := map[string]interface{}{
		"os":               f.OS,
		"arch":             f.Architecture,
		"entrypoint":       normalizeCommand(f.Entrypoint),
		"cmd":              normalizeCommand(f.Cmd),
		"env_keys":         extractEnvKeys(f.Env),
		"package_managers": sortedUnique(f.PackageManagers),
		"runtimes":         sortedUnique(f.Runtimes),
		"frameworks":       sortedUnique(f.Frameworks),
		"os_release":       f.OSRelease,
	}

	data, _ := json.Marshal(normalized)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

// ExtractImageFeatures analyzes an image and extracts its characteristics
func ExtractImageFeatures(ctx context.Context, client *containerd.Client, srcRef string, platformMC platforms.MatchComparer) (*ImageFeatures, error) {
	cs := client.ContentStore()
	is := client.ImageService()

	// Get image and manifest
	srcImg, err := is.Get(ctx, srcRef)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	manifestDesc, err := containerdutil.ManifestDesc(ctx, cs, srcImg.Target, platformMC)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}

	manifestBlob, err := content.ReadBlob(ctx, cs, manifestDesc)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBlob, &manifest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest: %w", err)
	}

	// Get image config
	configBlob, err := content.ReadBlob(ctx, cs, manifest.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var config ocispec.Image
	if err := json.Unmarshal(configBlob, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	features := &ImageFeatures{
		OS:           config.OS,
		Architecture: config.Architecture,
		Entrypoint:   config.Config.Entrypoint,
		Cmd:          config.Config.Cmd,
		Env:          config.Config.Env,
	}

	// Extract filesystem characteristics
	if err := extractFilesystemFeatures(ctx, cs, manifest.Layers, features); err != nil {
		return nil, fmt.Errorf("failed to extract filesystem features: %w", err)
	}

	return features, nil
}

// extractFilesystemFeatures scans layers for package managers, runtimes, and frameworks
func extractFilesystemFeatures(ctx context.Context, cs content.Store, layers []ocispec.Descriptor, features *ImageFeatures) error {
	// Define patterns to look for
	packageManagerPaths := map[string]string{
		"apk":    "/sbin/apk",
		"apt":    "/usr/bin/apt",
		"yum":    "/usr/bin/yum",
		"dnf":    "/usr/bin/dnf",
		"pacman": "/usr/bin/pacman",
		"zypper": "/usr/bin/zypper",
	}

	runtimePaths := map[string][]string{
		"python": {"/usr/bin/python", "/usr/bin/python3", "/usr/local/bin/python"},
		"node":   {"/usr/bin/node", "/usr/local/bin/node"},
		"java":   {"/usr/bin/java", "/usr/lib/jvm"},
		"dotnet": {"/usr/share/dotnet", "/usr/bin/dotnet"},
		"ruby":   {"/usr/bin/ruby", "/usr/local/bin/ruby"},
		"go":     {"/usr/local/go/bin/go", "/usr/bin/go"},
		"php":    {"/usr/bin/php", "/usr/local/bin/php"},
	}

	frameworkFiles := map[string]string{
		"nodejs":   "package.json",
		"python":   "requirements.txt",
		"ruby":     "Gemfile",
		"java":     "pom.xml",
		"gradle":   "build.gradle",
		"dotnet":   "*.csproj",
		"composer": "composer.json",
	}

	// Collect all files from all layers
	allFiles := make(map[string]struct{})
	var osReleaseContent string

	var eg errgroup.Group
	for _, desc := range layers {
		desc := desc
		eg.Go(func() error {
			readerAt, err := cs.ReaderAt(ctx, desc)
			if err != nil {
				return fmt.Errorf("failed to get reader for layer %v: %w", desc.Digest, err)
			}
			defer readerAt.Close()

			r := io.Reader(io.NewSectionReader(readerAt, 0, desc.Size))
			if !uncompress.IsUncompressedType(desc.MediaType) {
				r, err = compression.DecompressStream(r)
				if err != nil {
					return fmt.Errorf("failed to decompress layer %v: %w", desc.Digest, err)
				}
			}

			tr := tar.NewReader(r)
			for {
				h, err := tr.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}

				cleanPath := cleanEntryName(h.Name)
				allFiles[cleanPath] = struct{}{}

				// Extract os-release content if found
				if cleanPath == "etc/os-release" && h.Typeflag == tar.TypeReg {
					content, err := io.ReadAll(tr)
					if err == nil {
						osReleaseContent = string(content)
					}
				}
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	// Check for package managers
	for name, path := range packageManagerPaths {
		if _, exists := allFiles[strings.TrimPrefix(path, "/")]; exists {
			features.PackageManagers = append(features.PackageManagers, name)
		}
	}

	// Check for runtimes
	for name, paths := range runtimePaths {
		for _, path := range paths {
			if _, exists := allFiles[strings.TrimPrefix(path, "/")]; exists {
				features.Runtimes = append(features.Runtimes, name)
				break
			}
		}
	}

	// Check for frameworks
	for name, file := range frameworkFiles {
		if strings.Contains(file, "*") {
			// Simple glob matching for extensions
			ext := strings.TrimPrefix(file, "*")
			for filePath := range allFiles {
				if strings.HasSuffix(filePath, ext) {
					features.Frameworks = append(features.Frameworks, name)
					break
				}
			}
		} else {
			if _, exists := allFiles[file]; exists {
				features.Frameworks = append(features.Frameworks, name)
			}
		}
	}

	// Parse os-release for more specific OS identification
	if osReleaseContent != "" {
		features.OSRelease = parseOSRelease(osReleaseContent)
	}

	return nil
}

// Helper functions
func normalizeCommand(cmd []string) string {
	if len(cmd) == 0 {
		return ""
	}
	// Extract just the executable name, ignore arguments
	executable := path.Base(cmd[0])
	return executable
}

func extractEnvKeys(env []string) []string {
	var keys []string
	for _, e := range env {
		if idx := strings.Index(e, "="); idx > 0 {
			keys = append(keys, e[:idx])
		}
	}
	sort.Strings(keys)
	return keys
}

func sortedUnique(slice []string) []string {
	if len(slice) == 0 {
		return slice
	}

	sort.Strings(slice)
	result := make([]string, 0, len(slice))
	prev := ""
	for _, s := range slice {
		if s != prev {
			result = append(result, s)
			prev = s
		}
	}
	return result
}

func parseOSRelease(content string) string {
	lines := strings.Split(content, "\n")
	var id, version string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ID=") {
			id = strings.Trim(strings.TrimPrefix(line, "ID="), "\"")
		} else if strings.HasPrefix(line, "VERSION_ID=") {
			version = strings.Trim(strings.TrimPrefix(line, "VERSION_ID="), "\"")
		}
	}

	if id != "" && version != "" {
		return fmt.Sprintf("%s:%s", id, version)
	} else if id != "" {
		return id
	}
	return ""
}

func cleanEntryName(name string) string {
	return strings.TrimPrefix(path.Clean("/"+name), "/")
}
