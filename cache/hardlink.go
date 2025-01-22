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

type linkInfo struct {
	SourcePath string    `json:"source"`
	LinkPath   string    `json:"link"`
	CreatedAt  time.Time `json:"created_at"`
	LastUsed   time.Time `json:"last_used"`
}

// HardlinkManager manages creation, recovery and cleanup of hardlinks
type HardlinkManager struct {
	root            string
	hlDir           string
	mu              sync.RWMutex
	links           map[string]*linkInfo
	cleanupInterval time.Duration
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

// CreateLink creates a hardlink with retry logic
func (hm *HardlinkManager) CreateLink(key string, sourcePath string) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Add debug info about source file
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		log.L.Debugf("Source file stat failed %q: %v", sourcePath, err)
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	// Create hardlink in hardlinks directory
	linkPath := filepath.Join(hm.hlDir, key[:2], key)
	if err := os.MkdirAll(filepath.Dir(linkPath), 0700); err != nil {
		return fmt.Errorf("failed to create link dir: %w", err)
	}

	// Create temporary link path
	tmpLinkPath := linkPath + ".tmp"

	// Remove temporary link if it exists
	os.Remove(tmpLinkPath)

	// Retry logic for creating hardlink
	const maxRetries = 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := os.Link(sourcePath, tmpLinkPath); err != nil {
			lastErr = err
			// Add delay between retries
			if i < maxRetries-1 {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return fmt.Errorf("failed to create temporary hardlink after %d attempts: %w", maxRetries, lastErr)
		}
		break
	}

	// Verify the temporary link
	tmpInfo, err := os.Stat(tmpLinkPath)
	if err != nil {
		os.Remove(tmpLinkPath)
		return fmt.Errorf("failed to stat temporary link: %w", err)
	}

	if !os.SameFile(sourceInfo, tmpInfo) {
		os.Remove(tmpLinkPath)
		return fmt.Errorf("temporary link verification failed - different inodes")
	}

	// Atomically rename temporary link to final location
	if err := os.Rename(tmpLinkPath, linkPath); err != nil {
		os.Remove(tmpLinkPath)
		return fmt.Errorf("failed to rename temporary link: %w", err)
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

// CreateLinks creates multiple hardlinks in batch
func (hm *HardlinkManager) CreateLinks(links map[string]string) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Pre-create all directories
	dirs := make(map[string]struct{})
	for key := range links {
		dir := filepath.Join(hm.hlDir, key[:2])
		dirs[dir] = struct{}{}
	}
	for dir := range dirs {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return fmt.Errorf("failed to create link dir %s: %w", dir, err)
		}
	}

	// Create all links
	for key, sourcePath := range links {
		if err := hm.createSingleLink(key, sourcePath); err != nil {
			return fmt.Errorf("failed to create link for %s: %w", key, err)
		}
	}

	// Persist all link info at once
	return hm.persist()
}

func (hm *HardlinkManager) createSingleLink(key string, sourcePath string) error {
	// Add debug info about source file
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		log.L.Debugf("Source file stat failed %q: %v", sourcePath, err)
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	// Create hardlink path
	linkPath := filepath.Join(hm.hlDir, key[:2], key)
	tmpLinkPath := linkPath + ".tmp"

	// Remove temporary link if it exists
	os.Remove(tmpLinkPath)

	// Create hardlink using temporary path
	if err := os.Link(sourcePath, tmpLinkPath); err != nil {
		return fmt.Errorf("failed to create temporary hardlink: %w", err)
	}

	// Verify the temporary link
	tmpInfo, err := os.Stat(tmpLinkPath)
	if err != nil {
		os.Remove(tmpLinkPath)
		return fmt.Errorf("failed to stat temporary link: %w", err)
	}

	if !os.SameFile(sourceInfo, tmpInfo) {
		os.Remove(tmpLinkPath)
		return fmt.Errorf("temporary link verification failed - different inodes")
	}

	// Atomically rename temporary link to final location
	if err := os.Rename(tmpLinkPath, linkPath); err != nil {
		os.Remove(tmpLinkPath)
		return fmt.Errorf("failed to rename temporary link: %w", err)
	}

	// Record link info
	hm.links[key] = &linkInfo{
		SourcePath: sourcePath,
		LinkPath:   linkPath,
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
	}

	return nil
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
	expiredKeys := make([]string, 0)

	// Find expired links
	for key, info := range hm.links {
		if now.Sub(info.LastUsed) > 30*24*time.Hour {
			expiredKeys = append(expiredKeys, key)
		}
	}

	// Remove expired links in batch
	for _, key := range expiredKeys {
		info := hm.links[key]
		if err := os.Remove(info.LinkPath); err != nil && !os.IsNotExist(err) {
			log.L.Warnf("Failed to remove expired hardlink %q: %v", info.LinkPath, err)
			continue
		}
		delete(hm.links, key)
	}

	if len(expiredKeys) > 0 {
		return hm.persist()
	}
	return nil
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
