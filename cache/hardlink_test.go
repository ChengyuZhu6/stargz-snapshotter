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

package cache

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Common test setup helper
func setupTestEnvironment(t *testing.T) (string, string, string, *HardlinkManager) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0700); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}

	sourceFile := filepath.Join(sourceDir, "test.txt")
	content := []byte("test content")
	if err := os.WriteFile(sourceFile, content, 0600); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	hlm, err := NewHardlinkManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create hardlink manager: %v", err)
	}

	t.Cleanup(func() {
		if err := hlm.Close(); err != nil {
			t.Errorf("failed to close hardlink manager: %v", err)
		}
	})

	return tmpDir, sourceDir, sourceFile, hlm
}

// Helper function to generate a digest for testing
func generateTestDigest(content string) string {
	hash := sha256.Sum256([]byte(content))
	return fmt.Sprintf("sha256:%x", hash)
}

// Test RegisterDigestFile and GetLinkByDigest functionality
func TestHardlinkManager_RegisterAndGetDigest(t *testing.T) {
	_, _, sourceFile, hlm := setupTestEnvironment(t)
	testDigest := generateTestDigest("test content")

	t.Run("RegisterAndRetrieveDigest", func(t *testing.T) {
		// Register the file with a digest
		if err := hlm.RegisterDigestFile(testDigest, sourceFile); err != nil {
			t.Fatalf("RegisterDigestFile failed: %v", err)
		}

		// Verify digest is registered
		filePath, exists := hlm.GetLinkByDigest(testDigest)
		if !exists {
			t.Fatal("digest should exist")
		}
		if filePath != sourceFile {
			t.Fatalf("expected file path %q, got %q", sourceFile, filePath)
		}
	})

	t.Run("NonExistentFile", func(t *testing.T) {
		nonExistentFile := filepath.Join(t.TempDir(), "nonexistent.txt")
		err := hlm.RegisterDigestFile("sha256:nonexistent", nonExistentFile)
		if err == nil {
			t.Fatal("should fail with nonexistent file")
		}
	})

	t.Run("NonExistentDigest", func(t *testing.T) {
		_, exists := hlm.GetLinkByDigest("sha256:nonexistent")
		if exists {
			t.Fatal("should not find nonexistent digest")
		}
	})
}

// Test MapKeyToDigest and GetLink functionality
func TestHardlinkManager_MapKeyToDigest(t *testing.T) {
	_, _, sourceFile, hlm := setupTestEnvironment(t)
	testDigest := generateTestDigest("test content")

	t.Run("MapKeyAndGetLink", func(t *testing.T) {
		// First register the digest
		if err := hlm.RegisterDigestFile(testDigest, sourceFile); err != nil {
			t.Fatalf("RegisterDigestFile failed: %v", err)
		}

		// Map a key to the digest
		key := "test-key"
		if err := hlm.MapKeyToDigest(key, testDigest); err != nil {
			t.Fatalf("MapKeyToDigest failed: %v", err)
		}

		// Verify key is mapped to the digest and resolves to the file
		linkPath, exists := hlm.GetLink(key)
		if !exists {
			t.Fatal("link should exist")
		}
		if linkPath != sourceFile {
			t.Fatalf("expected file path %q, got %q", sourceFile, linkPath)
		}
	})

	t.Run("NonExistentDigest", func(t *testing.T) {
		err := hlm.MapKeyToDigest("bad-key", "sha256:nonexistent")
		if err == nil {
			t.Fatal("should fail with nonexistent digest")
		}
	})

	t.Run("RemappingKey", func(t *testing.T) {
		// Create another file with different content
		anotherFile := filepath.Join(t.TempDir(), "another.txt")
		anotherContent := []byte("different content")
		if err := os.WriteFile(anotherFile, anotherContent, 0600); err != nil {
			t.Fatalf("failed to create another file: %v", err)
		}
		anotherDigest := generateTestDigest("different content")

		// Register the digest
		if err := hlm.RegisterDigestFile(anotherDigest, anotherFile); err != nil {
			t.Fatalf("RegisterDigestFile failed for another digest: %v", err)
		}

		// Map a key to the first digest
		key := "remap-test"
		if err := hlm.MapKeyToDigest(key, testDigest); err != nil {
			t.Fatalf("MapKeyToDigest failed for first digest: %v", err)
		}

		// Verify initial mapping
		linkPath, exists := hlm.GetLink(key)
		if !exists || linkPath != sourceFile {
			t.Fatalf("expected link to first file")
		}

		// Remap the key to the second digest
		if err := hlm.MapKeyToDigest(key, anotherDigest); err != nil {
			t.Fatalf("MapKeyToDigest failed for remapping: %v", err)
		}

		// Verify remapping worked
		linkPath, exists = hlm.GetLink(key)
		if !exists || linkPath != anotherFile {
			t.Fatalf("expected link to be remapped to second file")
		}
	})
}

// Test CreateLink functionality
func TestHardlinkManager_CreateLink(t *testing.T) {
	_, _, sourceFile, hlm := setupTestEnvironment(t)
	testDigest := generateTestDigest("test content")

	t.Run("CreateLinkSuccess", func(t *testing.T) {
		key := "create-link-test"
		if err := hlm.CreateLink(key, sourceFile, testDigest); err != nil {
			t.Fatalf("CreateLink failed: %v", err)
		}

		// Verify both the key-digest mapping and digest-file mapping
		digestPath, exists := hlm.GetLinkByDigest(testDigest)
		if !exists || digestPath != sourceFile {
			t.Fatalf("digest should be mapped to source file")
		}

		// Verify GetLink works
		linkPath, exists := hlm.GetLink(key)
		if !exists || linkPath != sourceFile {
			t.Fatalf("key should resolve to source file")
		}
	})

	t.Run("CreateLinkWithExistingKey", func(t *testing.T) {
		key := "duplicate-key"
		if err := hlm.CreateLink(key, sourceFile, testDigest); err != nil {
			t.Fatalf("First CreateLink failed: %v", err)
		}

		// Try to create the same key again
		err := hlm.CreateLink(key, sourceFile, "sha256:different")
		if err == nil {
			t.Fatal("should fail when creating link with existing key")
		}
	})
}

// Test TryCreateHardlink utility function
func TestHardlinkManager_TryCreateHardlink(t *testing.T) {
	tmpDir, _, sourceFile, hlm := setupTestEnvironment(t)
	testDigest := generateTestDigest("test content")

	t.Run("SuccessfulHardlink", func(t *testing.T) {
		// Register the digest first
		if err := hlm.RegisterDigestFile(testDigest, sourceFile); err != nil {
			t.Fatalf("RegisterDigestFile failed: %v", err)
		}

		// Try to create hardlink
		targetPath := filepath.Join(tmpDir, "hardlink-target.txt")
		success := hlm.TryCreateHardlink("hardlink-key", testDigest, targetPath)
		if !success {
			t.Fatal("TryCreateHardlink should succeed")
		}

		// Verify hardlink was created
		sourceStat, err := os.Stat(sourceFile)
		if err != nil {
			t.Fatalf("failed to stat source: %v", err)
		}
		targetStat, err := os.Stat(targetPath)
		if err != nil {
			t.Fatalf("failed to stat target: %v", err)
		}
		if !os.SameFile(sourceStat, targetStat) {
			t.Fatal("files should reference the same inode")
		}
	})

	t.Run("SamePathHardlink", func(t *testing.T) {
		// Register the digest first
		if err := hlm.RegisterDigestFile(testDigest, sourceFile); err != nil {
			t.Fatalf("RegisterDigestFile failed: %v", err)
		}

		// Try to create hardlink to same path
		success := hlm.TryCreateHardlink("same-path-key", testDigest, sourceFile)
		if success {
			t.Fatal("TryCreateHardlink to same path should return false")
		}
	})
}

// Test cleanup functionality
func TestHardlinkManager_Cleanup(t *testing.T) {
	tmpDir, _, sourceFile, _ := setupTestEnvironment(t)
	testDigest := generateTestDigest("test content")

	cleanupDir := filepath.Join(tmpDir, "cleanup")
	cleanupHLM, err := NewHardlinkManager(cleanupDir)
	if err != nil {
		t.Fatalf("failed to create cleanup manager: %v", err)
	}
	t.Cleanup(func() {
		if err := cleanupHLM.Close(); err != nil {
			t.Errorf("failed to close cleanup manager: %v", err)
		}
		os.RemoveAll(cleanupDir)
	})

	t.Run("UnusedDigest", func(t *testing.T) {
		// Register and map the digest
		if err := cleanupHLM.RegisterDigestFile(testDigest, sourceFile); err != nil {
			t.Fatalf("RegisterDigestFile failed: %v", err)
		}

		key := "cleanup-test"
		if err := cleanupHLM.MapKeyToDigest(key, testDigest); err != nil {
			t.Fatalf("MapKeyToDigest failed: %v", err)
		}

		// Force persist
		if err := cleanupHLM.persist(); err != nil {
			t.Fatalf("failed to persist: %v", err)
		}

		// Remove key to create unused digest
		cleanupHLM.mu.Lock()
		delete(cleanupHLM.keyToDigest, key)
		cleanupHLM.digestToKeys[testDigest].Keys = []string{}
		cleanupHLM.mu.Unlock()

		// Run cleanup with timeout
		done := make(chan error)
		go func() {
			done <- cleanupHLM.cleanup()
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("cleanup failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cleanup timed out")
		}

		// Verify cleanup
		cleanupHLM.mu.RLock()
		_, exists := cleanupHLM.digestToFile[testDigest]
		cleanupHLM.mu.RUnlock()
		if exists {
			t.Fatal("unused digest should be cleaned up")
		}
	})
}

// Test persistence and restoration
func TestHardlinkManager_PersistRestore(t *testing.T) {
	tmpDir, _, sourceFile, hlm := setupTestEnvironment(t)
	testDigest := generateTestDigest("test content")

	t.Run("PersistAndRestore", func(t *testing.T) {
		// Create mappings
		if err := hlm.RegisterDigestFile(testDigest, sourceFile); err != nil {
			t.Fatalf("RegisterDigestFile failed: %v", err)
		}

		key := "persist-test"
		if err := hlm.MapKeyToDigest(key, testDigest); err != nil {
			t.Fatalf("MapKeyToDigest failed: %v", err)
		}

		// Force persist
		if err := hlm.persist(); err != nil {
			t.Fatalf("failed to persist: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		// Create new manager
		hlm2, err := NewHardlinkManager(tmpDir)
		if err != nil {
			t.Fatalf("failed to create second manager: %v", err)
		}
		t.Cleanup(func() {
			if err := hlm2.Close(); err != nil {
				t.Errorf("failed to close second manager: %v", err)
			}
		})

		// Verify restoration
		linkPath, exists := hlm2.GetLink(key)
		if !exists {
			t.Fatal("link should be restored")
		}
		if linkPath != sourceFile {
			t.Fatalf("expected file path %q, got %q", sourceFile, linkPath)
		}

		// Verify digest mapping was restored
		digestPath, exists := hlm2.GetLinkByDigest(testDigest)
		if !exists {
			t.Fatal("digest mapping should be restored")
		}
		if digestPath != sourceFile {
			t.Fatalf("expected digest path %q, got %q", sourceFile, digestPath)
		}
	})
}

// Test concurrent operations
func TestHardlinkManager_Concurrent(t *testing.T) {
	_, _, sourceFile, hlm := setupTestEnvironment(t)
	testDigest := generateTestDigest("test content")

	// Register the digest first
	if err := hlm.RegisterDigestFile(testDigest, sourceFile); err != nil {
		t.Fatalf("RegisterDigestFile failed: %v", err)
	}

	done := make(chan bool)
	timeout := time.After(10 * time.Second)

	go func() {
		for i := 0; i < 100; i++ {
			hlm.GetLink("test-key")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("test-key-%d", i)
			hlm.MapKeyToDigest(key, testDigest)
		}
		done <- true
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-done:
			continue
		case <-timeout:
			t.Fatal("concurrent test timed out")
		}
	}
}

// Test utility functions
func TestHardlinkManager_UtilityFunctions(t *testing.T) {
	_, _, _, hlm := setupTestEnvironment(t)

	t.Run("GenerateInternalKey", func(t *testing.T) {
		key1 := hlm.GenerateInternalKey("/path/to/dir", "key1")
		key2 := hlm.GenerateInternalKey("/path/to/dir", "key2")

		if key1 == key2 {
			t.Fatal("different keys should generate different internal keys")
		}

		// Same inputs should generate same output
		key1Again := hlm.GenerateInternalKey("/path/to/dir", "key1")
		if key1 != key1Again {
			t.Fatal("same inputs should generate same internal key")
		}
	})

	t.Run("IsEnabled", func(t *testing.T) {
		if !hlm.IsEnabled() {
			t.Fatal("hardlink manager should be enabled")
		}

		var nilManager *HardlinkManager
		if nilManager.IsEnabled() {
			t.Fatal("nil manager should report as disabled")
		}
	})
}

// Test disabled hardlink functionality
func TestHardlinkManagerDisabled(t *testing.T) {
	tmpDir := t.TempDir()
	dc, err := NewDirectoryCache(tmpDir, DirectoryCacheConfig{
		EnableHardlink: false,
	})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer dc.Close()

	dirCache := dc.(*directoryCache)
	if dirCache.hlManager != nil {
		t.Fatal("hardlink manager should be nil when disabled")
	}

	t.Cleanup(func() {
		if err := dc.Close(); err != nil {
			t.Errorf("failed to close directory cache: %v", err)
		}
	})

	// Basic operations should work without hardlinks
	t.Run("BasicOperations", func(t *testing.T) {
		// Write test
		w, err := dc.Add("test-key")
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
		if _, err := w.Write([]byte("test")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if err := w.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		w.Close()

		// Read test
		r, err := dc.Get("test-key")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		defer r.Close()
	})
}
