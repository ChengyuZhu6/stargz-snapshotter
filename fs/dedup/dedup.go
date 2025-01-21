package dedup

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"sync"

	"github.com/containerd/log"
	"github.com/containerd/stargz-snapshotter/cache"
)

// DedupManager 管理文件和块级别的去重
type DedupManager struct {
	// 文件级别去重缓存
	FileCache cache.BlobCache

	// 块级别去重缓存
	ChunkCache cache.BlobCache

	// 文件哈希到引用计数的映射
	FileRefs   map[string]int
	fileRefsMu sync.Mutex

	// 块哈希到引用计数的映射
	ChunkRefs   map[string]int
	chunkRefsMu sync.Mutex

	// 去重块大小
	ChunkSize int64

	// 内存缓存大小限制
	MemCacheSize int64

	// 元数据持久化路径
	MetadataPath string

	// 内存使用统计
	MemUsage   int64
	MemUsageMu sync.Mutex
}

// 持久化的元数据结构
type DedupMetadata struct {
	FileRefs  map[string]int `json:"file_refs"`
	ChunkRefs map[string]int `json:"chunk_refs"`
}

// 计算文件哈希
func (dm *DedupManager) FileHash(data []byte) string {
	h := sha256.New()
	h.Write(data)
	return string(h.Sum(nil))
}

// 计算块哈希
func (dm *DedupManager) ChunkHash(data []byte) string {
	h := sha256.New()
	h.Write(data)
	return string(h.Sum(nil))
}

// 添加文件级别去重
func (dm *DedupManager) AddFile(data []byte) error {
	hash := dm.FileHash(data)

	dm.fileRefsMu.Lock()
	defer dm.fileRefsMu.Unlock()

	if _, ok := dm.FileRefs[hash]; ok {
		// 文件已存在,增加引用计数
		dm.FileRefs[hash]++
		return nil
	}

	// 新文件,写入缓存
	w, err := dm.FileCache.Add(hash)
	if err != nil {
		return err
	}
	defer w.Close()
	if _, err := w.Write(data); err != nil {
		w.Abort()
		return err
	}
	if err := w.Commit(); err != nil {
		return err
	}

	dm.FileRefs[hash] = 1
	return nil
}

// LoadMetadata loads persisted metadata during initialization
func (dm *DedupManager) LoadMetadata() error {
	if dm.MetadataPath == "" {
		return nil
	}

	data, err := os.ReadFile(dm.MetadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var meta DedupMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}

	dm.fileRefsMu.Lock()
	dm.FileRefs = meta.FileRefs
	dm.fileRefsMu.Unlock()

	dm.chunkRefsMu.Lock()
	dm.ChunkRefs = meta.ChunkRefs
	dm.chunkRefsMu.Unlock()

	return nil
}

// 定期保存元数据
func (dm *DedupManager) saveMetadata() error {
	if dm.MetadataPath == "" {
		return nil
	}

	dm.fileRefsMu.Lock()
	dm.chunkRefsMu.Lock()
	meta := DedupMetadata{
		FileRefs:  dm.FileRefs,
		ChunkRefs: dm.ChunkRefs,
	}
	dm.fileRefsMu.Unlock()
	dm.chunkRefsMu.Unlock()

	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	return os.WriteFile(dm.MetadataPath, data, 0644)
}

// 检查并限制内存使用
func (dm *DedupManager) checkMemUsage(size int64) bool {
	dm.MemUsageMu.Lock()
	defer dm.MemUsageMu.Unlock()

	if dm.MemUsage+size > dm.MemCacheSize {
		// 超过限制,需要淘汰
		return false
	}

	dm.MemUsage += size
	return true
}

// 添加块级别去重
func (dm *DedupManager) AddChunk(data []byte) error {
	if !dm.checkMemUsage(int64(len(data))) {
		// 超过内存限制,跳过去重
		return nil
	}

	hash := dm.ChunkHash(data)

	dm.chunkRefsMu.Lock()
	defer dm.chunkRefsMu.Unlock()

	if _, ok := dm.ChunkRefs[hash]; ok {
		// 块已存在,增加引用计数
		dm.ChunkRefs[hash]++
		return nil
	}

	// 新块,写入缓存
	w, err := dm.ChunkCache.Add(hash)
	if err != nil {
		return err
	}
	defer w.Close()
	if _, err := w.Write(data); err != nil {
		w.Abort()
		return err
	}
	if err := w.Commit(); err != nil {
		return err
	}

	dm.ChunkRefs[hash] = 1

	// 定期保存元数据
	if err := dm.saveMetadata(); err != nil {
		log.L.WithError(err).Warn("failed to save dedup metadata")
	}

	return nil
}

// GetChunkSize returns the chunk size used for deduplication
func (dm *DedupManager) GetChunkSize() int64 {
	return dm.ChunkSize
}

// GetChunkCache returns the chunk cache
func (dm *DedupManager) GetChunkCache() cache.BlobCache {
	return dm.ChunkCache
}
