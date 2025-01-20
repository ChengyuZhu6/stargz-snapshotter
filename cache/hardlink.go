package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/containerd/log"
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
		log.L.Infof("Failed to create hardlink: source file %q does not exist: %v", sourcePath, err)
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	// Create hardlink directory
	linkPath := filepath.Join(hm.root, key[:2], key)
	if err := os.MkdirAll(filepath.Dir(linkPath), 0700); err != nil {
		log.L.Infof("Failed to create hardlink directory for key %q: %v", key, err)
		return fmt.Errorf("failed to create link dir: %w", err)
	}

	// Create hardlink
	if err := os.Link(sourcePath, linkPath); err != nil {
		if !os.IsExist(err) {
			log.L.Infof("Failed to create hardlink from %q to %q: %v", sourcePath, linkPath, err)
			return fmt.Errorf("failed to create hardlink: %w", err)
		}
		log.L.Infof("Hardlink already exists from %q to %q", sourcePath, linkPath)
	} else {
		log.L.Infof("Successfully created hardlink from %q to %q", sourcePath, linkPath)
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
		log.L.Infof("Found valid hardlink at %q", info.LinkPath)
		return info.LinkPath, true
	}
	log.L.Infof("No hardlink found for key %q", key)
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
			log.L.Infof("Removed expired hardlink %q (last used: %v)", info.LinkPath, info.LastUsed)
		}
	}

	if removed > 0 {
		log.L.Infof("Cleaned up %d expired hardlinks", removed)
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
