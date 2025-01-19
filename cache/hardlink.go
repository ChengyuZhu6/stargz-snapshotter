package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	hardlinkDirName = "hardlinks"
	linksFileName   = "links.json"
)

// HardlinkManager manages creation, recovery and cleanup of hardlinks
type HardlinkManager struct {
	root            string
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
func NewHardlinkManager(cacheRoot string) (*HardlinkManager, error) {
	hlDir := filepath.Join(cacheRoot, hardlinkDirName)
	if err := os.MkdirAll(hlDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create hardlink dir: %w", err)
	}

	hm := &HardlinkManager{
		root:            hlDir,
		links:           make(map[string]*linkInfo),
		cleanupInterval: 24 * time.Hour,
	}

	// Restore persisted hardlink information
	if err := hm.restore(); err != nil {
		return nil, err
	}

	// Start periodic cleanup
	go hm.periodicCleanup()

	return hm, nil
}

// CreateLink creates a hardlink
func (hm *HardlinkManager) CreateLink(key string, sourcePath string) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Check source file exists
	if _, err := os.Stat(sourcePath); err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	// Create hardlink directory
	linkPath := filepath.Join(hm.root, key[:2], key)
	if err := os.MkdirAll(filepath.Dir(linkPath), 0700); err != nil {
		return fmt.Errorf("failed to create link dir: %w", err)
	}

	// Create hardlink
	if err := os.Link(sourcePath, linkPath); err != nil {
		if !os.IsExist(err) {
			return fmt.Errorf("failed to create hardlink: %w", err)
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
			delete(hm.links, key)
			return "", false
		}
		// Update last used time
		info.LastUsed = time.Now()
		return info.LinkPath, true
	}
	return "", false
}

// cleanup cleans up expired hardlinks
func (hm *HardlinkManager) cleanup() error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	now := time.Now()
	for key, info := range hm.links {
		// Clean up links unused for over 30 days
		if now.Sub(info.LastUsed) > 30*24*time.Hour {
			if err := os.Remove(info.LinkPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("failed to remove expired link: %w", err)
			}
			delete(hm.links, key)
		}
	}

	return hm.persist()
}

// periodicCleanup performs periodic cleanup
func (hm *HardlinkManager) periodicCleanup() {
	ticker := time.NewTicker(hm.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := hm.cleanup(); err != nil {
			// Log error but continue running
			fmt.Printf("Failed to cleanup hardlinks: %v\n", err)
		}
	}
}

// persist persists link information
func (hm *HardlinkManager) persist() error {
	f, err := os.Create(filepath.Join(hm.root, linksFileName))
	if err != nil {
		return fmt.Errorf("failed to create links file: %w", err)
	}
	defer f.Close()

	return json.NewEncoder(f).Encode(hm.links)
}

// restore restores link information
func (hm *HardlinkManager) restore() error {
	f, err := os.Open(filepath.Join(hm.root, linksFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open links file: %w", err)
	}
	defer f.Close()

	return json.NewDecoder(f).Decode(&hm.links)
}
