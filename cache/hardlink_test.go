package cache

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHardlinkManager(t *testing.T) {
	// Setup test directory
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0700); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}

	// Create test source file
	sourceFile := filepath.Join(sourceDir, "test.txt")
	content := []byte("test content")
	if err := os.WriteFile(sourceFile, content, 0600); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Create hardlink manager
	hlm, err := NewHardlinkManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create hardlink manager: %v", err)
	}

	t.Run("CreateAndGetLink", func(t *testing.T) {
		key := "test-key"
		if err := hlm.CreateLink(key, sourceFile); err != nil {
			t.Fatalf("failed to create link: %v", err)
		}

		// Verify link exists
		linkPath, exists := hlm.GetLink(key)
		if !exists {
			t.Fatal("link should exist")
		}

		// Verify link content
		linkContent, err := os.ReadFile(linkPath)
		if err != nil {
			t.Fatalf("failed to read link: %v", err)
		}
		if string(linkContent) != string(content) {
			t.Fatalf("link content mismatch: got %q, want %q", string(linkContent), string(content))
		}

		// Verify hardlink
		sourceStat, err := os.Stat(sourceFile)
		if err != nil {
			t.Fatalf("failed to stat source: %v", err)
		}
		linkStat, err := os.Stat(linkPath)
		if err != nil {
			t.Fatalf("failed to stat link: %v", err)
		}
		if !os.SameFile(sourceStat, linkStat) {
			t.Fatal("files should be hardlinked")
		}
	})

	t.Run("NonExistentSource", func(t *testing.T) {
		err := hlm.CreateLink("bad-key", "nonexistent")
		if err == nil {
			t.Fatal("should fail with nonexistent source")
		}
	})

	t.Run("GetNonExistentLink", func(t *testing.T) {
		_, exists := hlm.GetLink("nonexistent")
		if exists {
			t.Fatal("should not find nonexistent link")
		}
	})

	t.Run("Cleanup", func(t *testing.T) {
		key := "cleanup-test"
		if err := hlm.CreateLink(key, sourceFile); err != nil {
			t.Fatalf("failed to create link: %v", err)
		}

		// Modify last used time to trigger cleanup
		hlm.mu.Lock()
		hlm.links[key].LastUsed = time.Now().Add(-31 * 24 * time.Hour)
		hlm.mu.Unlock()

		if err := hlm.cleanup(); err != nil {
			t.Fatalf("cleanup failed: %v", err)
		}

		// Verify link was removed
		if _, exists := hlm.GetLink(key); exists {
			t.Fatal("link should be cleaned up")
		}
	})

	t.Run("PersistAndRestore", func(t *testing.T) {
		// Create new link
		key := "persist-test"
		if err := hlm.CreateLink(key, sourceFile); err != nil {
			t.Fatalf("failed to create link: %v", err)
		}

		// Create new manager to test restore
		hlm2, err := NewHardlinkManager(tmpDir)
		if err != nil {
			t.Fatalf("failed to create second manager: %v", err)
		}

		// Verify link was restored
		if _, exists := hlm2.GetLink(key); !exists {
			t.Fatal("link should be restored")
		}
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		done := make(chan bool)
		go func() {
			for i := 0; i < 100; i++ {
				hlm.GetLink("test-key")
			}
			done <- true
		}()
		go func() {
			for i := 0; i < 100; i++ {
				hlm.CreateLink("test-key"+string(rune(i)), sourceFile)
			}
			done <- true
		}()
		<-done
		<-done
	})
}

func TestHardlinkManagerDisabled(t *testing.T) {
	tmpDir := t.TempDir()
	dc, err := NewDirectoryCache(tmpDir, DirectoryCacheConfig{
		EnableHardlink: false,
	})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer dc.Close()

	// Verify hardlink operations are no-op when disabled
	dirCache := dc.(*directoryCache)
	if dirCache.hlManager != nil {
		t.Fatal("hardlink manager should be nil when disabled")
	}

	// Write should succeed without hardlink
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

	// Read should succeed without hardlink
	r, err := dc.Get("test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer r.Close()
}
