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

	// 层间共享的去重缓存
	sharedCache sync.Map

	// 块偏移到哈希的映射
	offsetHashMap sync.Map
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
	hash := dm.ChunkHash(data)
	log.L.Infof("Adding chunk: size=%d, hash=%s", len(data), hash)

	// 先检查是否在共享缓存中
	if _, ok := dm.sharedCache.Load(hash); ok {
		log.L.Infof("Chunk already in shared cache: %s", hash)
		return nil
	}

	dm.chunkRefsMu.Lock()
	defer dm.chunkRefsMu.Unlock()

	if _, ok := dm.ChunkRefs[hash]; ok {
		log.L.Infof("Chunk exists, incrementing ref count: %s", hash)
		dm.ChunkRefs[hash]++
		return nil
	}

	log.L.Infof("Writing new chunk to cache: %s", hash)
	// 新块写入缓存
	w, err := dm.ChunkCache.Add(hash)
	if err != nil {
		log.L.Warnf("Failed to add chunk to cache: %v", err)
		return err
	}
	defer w.Close()

	if _, err := w.Write(data); err != nil {
		log.L.Warnf("Failed to write chunk data: %v", err)
		w.Abort()
		return err
	}

	if err := w.Commit(); err != nil {
		log.L.Warnf("Failed to commit chunk: %v", err)
		return err
	}

	dm.ChunkRefs[hash] = 1
	dm.sharedCache.Store(hash, struct{}{})
	log.L.Infof("Successfully added new chunk: %s", hash)

	return dm.saveMetadata()
}

// GetChunkSize returns the chunk size used for deduplication
func (dm *DedupManager) GetChunkSize() int64 {
	return dm.ChunkSize
}

// GetChunkCache returns the chunk cache
func (dm *DedupManager) GetChunkCache() cache.BlobCache {
	return dm.ChunkCache
}

// GetChunkHashByOffset returns the hash for a chunk at the given offset
func (dm *DedupManager) GetChunkHashByOffset(offset int64) string {
	if val, ok := dm.offsetHashMap.Load(offset); ok {
		return val.(string)
	}
	return ""
}

// SetChunkHashByOffset sets the hash for a chunk at the given offset
func (dm *DedupManager) SetChunkHashByOffset(offset int64, hash string) {
	dm.offsetHashMap.Store(offset, hash)
}
