package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/containerd/log"
)

const (
	hardlinkDirName = "hardlinks"
	linksFileName   = "links.json"
)

var (
	globalHLManager *HardlinkManager
	hlManagerOnce   sync.Once
	hlManagerMu     sync.RWMutex
)

// HardlinkManager manages creation, recovery and cleanup of hardlinks
type HardlinkManager struct {
	root            string
	hlDir           string
	mu              sync.RWMutex
	links           map[string]*linkInfo
	cleanupInterval time.Duration
}

type linkInfo struct {
	SourcePath string    `json:"source"`
	LinkPath   string    `json:"link"`
	CreatedAt  time.Time `json:"created_at"`
	LastUsed   time.Time `json:"last_used"`
}

// NewHardlinkManager creates a new hardlink manager
func NewHardlinkManager(root string) (*HardlinkManager, error) {
	// Create hardlinks directory under root
	hlDir := filepath.Join(root, hardlinkDirName)
	if err := os.MkdirAll(hlDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create hardlink dir: %w", err)
	}

	hm := &HardlinkManager{
		root:            root,  // Store root directory
		hlDir:           hlDir, // Store hardlinks directory
		links:           make(map[string]*linkInfo),
		cleanupInterval: 24 * time.Hour,
	}

	// Restore persisted hardlink information from root directory
	if err := hm.restore(); err != nil {
		return nil, err
	}

	// Start periodic cleanup
	go hm.periodicCleanup()

	return hm, nil
}

// GetGlobalHardlinkManager returns the global hardlink manager instance
func GetGlobalHardlinkManager(root string) (*HardlinkManager, error) {
	hlManagerMu.RLock()
	if globalHLManager != nil {
		defer hlManagerMu.RUnlock()
		return globalHLManager, nil
	}
	hlManagerMu.RUnlock()

	hlManagerMu.Lock()
	defer hlManagerMu.Unlock()

	var initErr error
	hlManagerOnce.Do(func() {
		globalHLManager, initErr = NewHardlinkManager(root)
	})

	if initErr != nil {
		return nil, fmt.Errorf("failed to initialize global hardlink manager: %w", initErr)
	}

	return globalHLManager, nil
}

// CreateLink creates a hardlink
func (hm *HardlinkManager) CreateLink(key string, sourcePath string) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Add debug info about source file
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		log.L.Debugf("Source file stat failed %q: %v", sourcePath, err)
	} else {
		log.L.Debugf("Source file %q exists, size: %d", sourcePath, sourceInfo.Size())
	}

	// Check source file exists and wait for it to be fully written
	var sourceErr error
	for retries := 0; retries < 3; retries++ {
		if _, err := os.Stat(sourcePath); err != nil {
			sourceErr = err
			// Wait a bit and retry
			time.Sleep(100 * time.Millisecond)
			continue
		}
		sourceErr = nil
		break
	}
	if sourceErr != nil {
		log.L.Debugf("Failed to create hardlink: source file %q does not exist: %v", sourcePath, sourceErr)
		return fmt.Errorf("failed to stat source file: %w", sourceErr)
	}

	// Create hardlink in hardlinks directory
	linkPath := filepath.Join(hm.hlDir, key[:2], key)
	if err := os.MkdirAll(filepath.Dir(linkPath), 0700); err != nil {
		log.L.Debugf("Failed to create hardlink directory for key %q: %v", key, err)
		return fmt.Errorf("failed to create link dir: %w", err)
	}

	// Create hardlink
	if err := os.Link(sourcePath, linkPath); err != nil {
		if !os.IsExist(err) {
			log.L.Debugf("Failed to create hardlink from %q to %q: %v", sourcePath, linkPath, err)
			return fmt.Errorf("failed to create hardlink: %w", err)
		}
		// Add inode check for existing link
		srcStat, _ := os.Stat(sourcePath)
		linkStat, _ := os.Stat(linkPath)
		if os.SameFile(srcStat, linkStat) {
			log.L.Debugf("Verified hardlink exists with same inode for %q", key)
		} else {
			log.L.Warnf("Files exist but are not hardlinked for key %q", key)
		}
		log.L.Debugf("Hardlink already exists from %q to %q", sourcePath, linkPath)
	} else {
		// Verify the hardlink was created successfully
		srcStat, _ := os.Stat(sourcePath)
		linkStat, _ := os.Stat(linkPath)
		if os.SameFile(srcStat, linkStat) {
			log.L.Debugf("Successfully created and verified hardlink from %q to %q (inode: %v)",
				sourcePath, linkPath, srcStat.Sys().(*syscall.Stat_t).Ino)
		} else {
			log.L.Warnf("Failed to verify hardlink - files have different inodes")
		}
	}

	// Record link info
	hm.links[key] = &linkInfo{
		SourcePath: sourcePath,
		LinkPath:   linkPath,
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
	}

	// Persist link info
	return hm.persist()
}

// GetLink gets the hardlink
func (hm *HardlinkManager) GetLink(key string) (string, bool) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	if info, exists := hm.links[key]; exists {
		// Check if link is still valid
		if _, err := os.Stat(info.LinkPath); err != nil {
			log.L.Infof("Hardlink %q is no longer valid: %v", info.LinkPath, err)
			delete(hm.links, key)
			return "", false
		}
		// Update last used time
		info.LastUsed = time.Now()
		log.L.Debugf("Found valid hardlink at %q", info.LinkPath)
		return info.LinkPath, true
	}
	log.L.Debugf("No hardlink found for key %q", key)
	return "", false
}

// cleanup cleans up expired hardlinks
func (hm *HardlinkManager) cleanup() error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	now := time.Now()
	removed := 0
	for key, info := range hm.links {
		// Clean up links unused for over 30 days
		if now.Sub(info.LastUsed) > 30*24*time.Hour {
			if err := os.Remove(info.LinkPath); err != nil && !os.IsNotExist(err) {
				log.L.Infof("Failed to remove expired hardlink %q: %v", info.LinkPath, err)
				return fmt.Errorf("failed to remove expired link: %w", err)
			}
			delete(hm.links, key)
			removed++
			log.L.Debugf("Removed expired hardlink %q (last used: %v)", info.LinkPath, info.LastUsed)
		}
	}

	if removed > 0 {
		log.L.Debugf("Cleaned up %d expired hardlinks", removed)
	}

	return hm.persist()
}

// periodicCleanup performs periodic cleanup
func (hm *HardlinkManager) periodicCleanup() {
	ticker := time.NewTicker(hm.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := hm.cleanup(); err != nil {
			fmt.Printf("Failed to cleanup hardlinks: %v\n", err)
		}
		// Add status check
		hm.CheckHardlinkStatus()
	}
}

// persist persists link information to root directory
func (hm *HardlinkManager) persist() error {
	if len(hm.links) == 0 {
		log.L.Debugf("No links to persist")
		return nil
	}

	linksFile := filepath.Join(hm.root, linksFileName)

	// Create temporary file for atomic write
	tmpFile := linksFile + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temporary links file: %w", err)
	}

	// Use a closure to handle file close and cleanup
	if err := func() error {
		defer f.Close()

		// Pretty print JSON for better readability
		encoder := json.NewEncoder(f)
		encoder.SetIndent("", "    ")
		if err := encoder.Encode(hm.links); err != nil {
			return fmt.Errorf("failed to encode links data: %w", err)
		}

		// Ensure data is written to disk
		if err := f.Sync(); err != nil {
			return fmt.Errorf("failed to sync links file: %w", err)
		}

		return nil
	}(); err != nil {
		os.Remove(tmpFile) // Clean up temp file on error
		return err
	}

	// Atomic rename
	if err := os.Rename(tmpFile, linksFile); err != nil {
		os.Remove(tmpFile) // Clean up temp file on error
		return fmt.Errorf("failed to rename links file: %w", err)
	}

	log.L.Debugf("Successfully persisted %d links to %s", len(hm.links), linksFile)
	return nil
}

// restore restores link information from root directory
func (hm *HardlinkManager) restore() error {
	linksFile := filepath.Join(hm.root, linksFileName)

	f, err := os.Open(linksFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.L.Debugf("No existing links file found at %s", linksFile)
			return nil
		}
		return fmt.Errorf("failed to open links file: %w", err)
	}
	defer f.Close()

	// Create a temporary map to avoid corrupting the existing one on error
	tempLinks := make(map[string]*linkInfo)
	if err := json.NewDecoder(f).Decode(&tempLinks); err != nil {
		return fmt.Errorf("failed to decode links file: %w", err)
	}

	// Validate restored links
	validLinks := make(map[string]*linkInfo)
	for key, info := range tempLinks {
		// Check if both source and link files exist
		if _, err := os.Stat(info.SourcePath); err != nil {
			log.L.Debugf("Skipping invalid link %q: source file missing: %v", key, err)
			continue
		}
		if _, err := os.Stat(info.LinkPath); err != nil {
			log.L.Debugf("Skipping invalid link %q: link file missing: %v", key, err)
			continue
		}
		validLinks[key] = info
	}

	// Update links map only after validation
	hm.links = validLinks
	log.L.Debugf("Successfully restored %d valid links from %s", len(validLinks), linksFile)

	return nil
}

// CheckHardlinkStatus prints statistics about hardlinks
func (hm *HardlinkManager) CheckHardlinkStatus() {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	type inodeInfo struct {
		size  int64
		count int
		paths []string
	}
	inodeMap := make(map[uint64]*inodeInfo)

	// Collect statistics
	for _, info := range hm.links {
		srcStat, err := os.Stat(info.SourcePath)
		if err != nil {
			log.L.Debugf("Failed to stat source %q: %v", info.SourcePath, err)
			continue
		}

		inode := srcStat.Sys().(*syscall.Stat_t).Ino
		if _, exists := inodeMap[inode]; !exists {
			inodeMap[inode] = &inodeInfo{
				size: srcStat.Size(),
			}
		}
		inodeMap[inode].count++
		inodeMap[inode].paths = append(inodeMap[inode].paths, info.SourcePath)
	}

	// Print statistics
	var totalSaved int64
	for inode, info := range inodeMap {
		if info.count > 1 {
			saved := info.size * int64(info.count-1)
			totalSaved += saved
			log.L.Infof("Inode %d: %d links, size %d bytes, saved %d bytes",
				inode, info.count, info.size, saved)
			log.L.Debugf("Paths for inode %d: %v", inode, info.paths)
		}
	}

	log.L.Infof("Total space saved by hardlinks: %d bytes", totalSaved)
}
