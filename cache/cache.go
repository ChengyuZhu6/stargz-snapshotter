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
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerd/log"
	"github.com/containerd/stargz-snapshotter/util/cacheutil"
	"github.com/containerd/stargz-snapshotter/util/namedmutex"
)

const (
	defaultMaxLRUCacheEntry = 10
	defaultMaxCacheFds      = 10
)

type DirectoryCacheConfig struct {

	// Number of entries of LRU cache (default: 10).
	// This won't be used when DataCache is specified.
	MaxLRUCacheEntry int

	// Number of file descriptors to cache (default: 10).
	// This won't be used when FdCache is specified.
	MaxCacheFds int

	// On Add, wait until the data is fully written to the cache directory.
	SyncAdd bool

	// DataCache is an on-memory cache of the data.
	// OnEvicted will be overridden and replaced for internal use.
	DataCache *cacheutil.LRUCache

	// FdCache is a cache for opened file descriptors.
	// OnEvicted will be overridden and replaced for internal use.
	FdCache *cacheutil.LRUCache

	// BufPool will be used for pooling bytes.Buffer.
	BufPool *sync.Pool

	// Direct forcefully enables direct mode for all operation in cache.
	// Thus operation won't use on-memory caches.
	Direct bool

	// EnableHardlink enables hardlinking of cache files to reduce memory usage
	EnableHardlink bool
}

// TODO: contents validation.

// BlobCache represents a cache for bytes data
type BlobCache interface {
	// Add returns a writer to add contents to cache
	Add(key string, opts ...Option) (Writer, error)

	// Get returns a reader to read the specified contents
	// from cache
	Get(key string, opts ...Option) (Reader, error)

	// Close closes the cache
	Close() error
}

// Reader provides the data cached.
type Reader interface {
	io.ReaderAt
	Close() error

	// If a blob is backed by a file, it should return *os.File so that it can be used for FUSE passthrough
	GetReaderAt() io.ReaderAt
}

// Writer enables the client to cache byte data. Commit() must be
// called after data is fully written to Write(). To abort the written
// data, Abort() must be called.
type Writer interface {
	io.WriteCloser
	Commit() error
	Abort() error
}

type cacheOpt struct {
	direct      bool
	passThrough bool
	chunkDigest string
}

type Option func(o *cacheOpt) *cacheOpt

// Direct option lets FetchAt and Add methods not to use on-memory caches. When
// you know that the targeting value won't be  used immediately, you can prevent
// the limited space of on-memory caches from being polluted by these unimportant
// values.
func Direct() Option {
	return func(o *cacheOpt) *cacheOpt {
		o.direct = true
		return o
	}
}

// PassThrough option indicates whether to enable FUSE passthrough mode
// to improve local file read performance.
func PassThrough() Option {
	return func(o *cacheOpt) *cacheOpt {
		o.passThrough = true
		return o
	}
}

// ChunkDigest option allows specifying a chunk digest for the cache
func ChunkDigest(digest string) Option {
	return func(o *cacheOpt) *cacheOpt {
		o.chunkDigest = digest
		return o
	}
}

func NewDirectoryCache(directory string, config DirectoryCacheConfig) (BlobCache, error) {
	if !filepath.IsAbs(directory) {
		return nil, fmt.Errorf("dir cache path must be an absolute path; got %q", directory)
	}
	bufPool := config.BufPool
	if bufPool == nil {
		bufPool = &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		}
	}
	dataCache := config.DataCache
	if dataCache == nil {
		maxEntry := config.MaxLRUCacheEntry
		if maxEntry == 0 {
			maxEntry = defaultMaxLRUCacheEntry
		}
		dataCache = cacheutil.NewLRUCache(maxEntry)
		dataCache.OnEvicted = func(key string, value interface{}) {
			value.(*bytes.Buffer).Reset()
			bufPool.Put(value)
		}
	}
	fdCache := config.FdCache
	if fdCache == nil {
		maxEntry := config.MaxCacheFds
		if maxEntry == 0 {
			maxEntry = defaultMaxCacheFds
		}
		fdCache = cacheutil.NewLRUCache(maxEntry)
		fdCache.OnEvicted = func(key string, value interface{}) {
			value.(*os.File).Close()
		}
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	wipdir := filepath.Join(directory, "wip")
	if err := os.MkdirAll(wipdir, 0700); err != nil {
		return nil, err
	}
	dc := &directoryCache{
		cache:          dataCache,
		fileCache:      fdCache,
		wipLock:        new(namedmutex.NamedMutex),
		directory:      directory,
		wipDirectory:   wipdir,
		bufPool:        bufPool,
		direct:         config.Direct,
		enableHardlink: config.EnableHardlink,
	}
	dc.syncAdd = config.SyncAdd

	// Log hardlink configuration
	if config.EnableHardlink {
		log.L.Infof("Hardlink feature is enabled for cache directory: %q", directory)
		// Get root directory for hardlink manager (../../)
		rootDir := filepath.Dir(filepath.Dir(directory))
		hlManager, err := GetGlobalHardlinkManager(rootDir)
		if err != nil {
			return nil, err
		}
		log.L.Infof("Using global hardlink manager with root directory: %q", rootDir)
		dc.hlManager = hlManager
	} else {
		log.L.Infof("Hardlink feature is disabled for cache directory: %q", directory)
	}

	return dc, nil
}

// directoryCache is a cache implementation which backend is a directory.
type directoryCache struct {
	cache        *cacheutil.LRUCache
	fileCache    *cacheutil.LRUCache
	wipDirectory string
	directory    string
	wipLock      *namedmutex.NamedMutex

	bufPool *sync.Pool

	syncAdd bool
	direct  bool

	closed   bool
	closedMu sync.Mutex

	enableHardlink bool
	hlManager      *HardlinkManager
}

func (dc *directoryCache) Get(key string, opts ...Option) (Reader, error) {
	if dc.isClosed() {
		return nil, fmt.Errorf("cache is already closed")
	}

	opt := &cacheOpt{}
	for _, o := range opts {
		opt = o(opt)
	}

	// Try to get from memory cache
	if !dc.direct && !opt.direct {
		if dc.hlManager != nil && dc.enableHardlink && opt.chunkDigest != "" {
			if b, done, ok := dc.cache.Get(opt.chunkDigest); ok {
				return &reader{
					ReaderAt: bytes.NewReader(b.(*bytes.Buffer).Bytes()),
					closeFunc: func() error {
						done()
						return nil
					},
				}, nil
			}
			// Get data from disk. If the file is already opened, use it.
			if f, done, ok := dc.fileCache.Get(opt.chunkDigest); ok {
				return &reader{
					ReaderAt: f.(*os.File),
					closeFunc: func() error {
						done() // file will be closed when it's evicted from the cache
						return nil
					},
				}, nil
			}
		}
		if b, done, ok := dc.cache.Get(key); ok {
			return &reader{
				ReaderAt: bytes.NewReader(b.(*bytes.Buffer).Bytes()),
				closeFunc: func() error {
					done()
					return nil
				},
			}, nil
		}

		// Get data from disk. If the file is already opened, use it.
		if f, done, ok := dc.fileCache.Get(key); ok {
			return &reader{
				ReaderAt: f.(*os.File),
				closeFunc: func() error {
					done() // file will be closed when it's evicted from the cache
					return nil
				},
			}, nil
		}
	}

	// First try regular file path
	filepath := BuildCachePath(dc.directory, key)

	// If chunkDigest is provided, try to find it in the hardlink manager
	if dc.hlManager != nil && dc.enableHardlink && opt.chunkDigest != "" {
		if digestPath, exists := dc.hlManager.GetLinkByDigest(opt.chunkDigest); exists {
			// Use the found path for this digest
			log.L.Debugf("Using existing file for digest %q instead of key %q", opt.chunkDigest, key)
			filepath = digestPath
		}
	}

	// Open the cache file and read the target region
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open blob file for %q: %w", key, err)
	}

	// If in direct mode, don't cache file descriptor
	if dc.direct || opt.direct {
		return &reader{
			ReaderAt: file,
			closeFunc: func() error {
				if opt.passThrough {
					return nil
				}
				return file.Close()
			},
		}, nil
	}

	// Cache file descriptor
	return &reader{
		ReaderAt: file,
		closeFunc: func() error {
			if dc.hlManager != nil && dc.enableHardlink && opt.chunkDigest != "" {
				_, done, added := dc.fileCache.Add(opt.chunkDigest, file)
				defer done()
				if !added {
					return file.Close()
				}
			} else {
				_, done, added := dc.fileCache.Add(key, file)
				defer done()
				if !added {
					return file.Close()
				}
			}
			return nil
		},
	}, nil
}

func (dc *directoryCache) Add(key string, opts ...Option) (Writer, error) {
	if dc.isClosed() {
		return nil, fmt.Errorf("cache is already closed")
	}

	opt := &cacheOpt{}
	for _, o := range opts {
		opt = o(opt)
	}

	// If chunkdigest is provided, check if it already exists
	if dc.hlManager != nil && dc.enableHardlink && opt.chunkDigest != "" {
		// Try to get existing file for this digest
		if digestPath, exists := dc.hlManager.GetLinkByDigest(opt.chunkDigest); exists {
			// Create a hardlink from the existing digest file to the key path
			keyPath := BuildCachePath(dc.directory, key)

			// Ensure target directory exists
			if err := os.MkdirAll(filepath.Dir(keyPath), 0700); err != nil {
				return nil, fmt.Errorf("failed to create cache directory: %w", err)
			}

			// Remove existing file if any
			_ = os.Remove(keyPath)

			// Create hardlink
			if err := os.Link(digestPath, keyPath); err != nil {
				log.L.Warnf("Failed to create hardlink from digest %q to key %q: %v",
					opt.chunkDigest, key, err)
			} else {
				log.L.Debugf("Created hardlink from digest %q to key %q", opt.chunkDigest, key)

				// Map key to digest
				if err := dc.hlManager.MapKeyToDigest(key, opt.chunkDigest); err != nil {
					log.L.Warnf("Failed to map key to digest: %v", err)
				}

				// Return a no-op writer since the file already exists
				return &writer{
					WriteCloser: nopWriteCloser(io.Discard),
					commitFunc:  func() error { return nil },
					abortFunc:   func() error { return nil },
				}, nil
			}
		}
	}

	// Create temporary file
	w, err := WipFile(dc.wipDirectory, key)
	if err != nil {
		return nil, err
	}

	// Create writer
	writer := &writer{
		WriteCloser: w,
		commitFunc: func() error {
			if dc.isClosed() {
				return fmt.Errorf("cache is already closed")
			}

			// Commit file
			targetPath := BuildCachePath(dc.directory, key)
			if err := os.MkdirAll(filepath.Dir(targetPath), 0700); err != nil {
				return fmt.Errorf("failed to create cache directory: %w", err)
			}

			if err := os.Rename(w.Name(), targetPath); err != nil {
				return fmt.Errorf("failed to commit cache file: %w", err)
			}

			// If chunkdigest is provided, store the mapping
			if dc.hlManager != nil && dc.enableHardlink && opt.chunkDigest != "" {
				// Register this file as the primary source for this digest
				if err := dc.hlManager.RegisterDigestFile(opt.chunkDigest, targetPath); err != nil {
					log.L.Debugf("Failed to register digest file: %v", err)
				}

				// Map key to digest
				if err := dc.hlManager.MapKeyToDigest(key, opt.chunkDigest); err != nil {
					log.L.Debugf("Failed to map key to digest: %v", err)
				}
			}

			return nil
		},
		abortFunc: func() error {
			return os.Remove(w.Name())
		},
	}

	// Return directly if in direct mode
	if dc.direct || opt.direct {
		return writer, nil
	}

	// Create memory cache
	b := dc.bufPool.Get().(*bytes.Buffer)
	return dc.wrapMemoryWriter(b, writer, key)
}

func (dc *directoryCache) putBuffer(b *bytes.Buffer) {
	b.Reset()
	dc.bufPool.Put(b)
}

func (dc *directoryCache) Close() error {
	dc.closedMu.Lock()
	defer dc.closedMu.Unlock()
	if dc.closed {
		return nil
	}
	dc.closed = true
	return os.RemoveAll(dc.directory)
}

func (dc *directoryCache) isClosed() bool {
	dc.closedMu.Lock()
	closed := dc.closed
	dc.closedMu.Unlock()
	return closed
}

func NewMemoryCache() BlobCache {
	return &MemoryCache{
		Membuf: map[string]*bytes.Buffer{},
	}
}

// MemoryCache is a cache implementation which backend is a memory.
type MemoryCache struct {
	Membuf map[string]*bytes.Buffer
	mu     sync.Mutex
}

func (mc *MemoryCache) Get(key string, opts ...Option) (Reader, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	b, ok := mc.Membuf[key]
	if !ok {
		return nil, fmt.Errorf("Missed cache: %q", key)
	}
	return &reader{bytes.NewReader(b.Bytes()), func() error { return nil }}, nil
}

func (mc *MemoryCache) Add(key string, opts ...Option) (Writer, error) {
	b := new(bytes.Buffer)
	return &writer{
		WriteCloser: nopWriteCloser(io.Writer(b)),
		commitFunc: func() error {
			mc.mu.Lock()
			defer mc.mu.Unlock()
			mc.Membuf[key] = b
			return nil
		},
		abortFunc: func() error { return nil },
	}, nil
}

func (mc *MemoryCache) Close() error {
	return nil
}

type reader struct {
	io.ReaderAt
	closeFunc func() error
}

func (r *reader) Close() error { return r.closeFunc() }

func (r *reader) GetReaderAt() io.ReaderAt {
	return r.ReaderAt
}

type writer struct {
	io.WriteCloser
	commitFunc func() error
	abortFunc  func() error
}

func (w *writer) Commit() error {
	return w.commitFunc()
}

func (w *writer) Abort() error {
	return w.abortFunc()
}

type writeCloser struct {
	io.Writer
	closeFunc func() error
}

func (w *writeCloser) Close() error { return w.closeFunc() }

func nopWriteCloser(w io.Writer) io.WriteCloser {
	return &writeCloser{w, func() error { return nil }}
}

// HardlinkCapability represents a cache that supports hardlinking
type HardlinkCapability interface {
	// RegisterDigestFile registers a file as the primary source for a digest
	RegisterDigestFile(chunkDigest string, filepath string) error
	// GetLinkByDigest returns the file path for a given digest
	GetLinkByDigest(chunkDigest string) (string, bool)
	// MapKeyToDigest maps a key to a digest
	MapKeyToDigest(key string, chunkDigest string) error
}

// RegisterDigestFile registers a file as the primary source for a digest
func (dc *directoryCache) RegisterDigestFile(chunkDigest string, filepath string) error {
	if !dc.enableHardlink || dc.hlManager == nil {
		log.L.Debugf("Digest file registration skipped: feature not enabled")
		return nil
	}

	if chunkDigest == "" {
		return fmt.Errorf("chunk digest cannot be empty")
	}

	return dc.hlManager.RegisterDigestFile(chunkDigest, filepath)
}

// GetLinkByDigest returns the file path for a given digest
func (dc *directoryCache) GetLinkByDigest(chunkDigest string) (string, bool) {
	if !dc.enableHardlink || dc.hlManager == nil {
		return "", false
	}

	return dc.hlManager.GetLinkByDigest(chunkDigest)
}

// MapKeyToDigest maps a key to a digest
func (dc *directoryCache) MapKeyToDigest(key string, chunkDigest string) error {
	if !dc.enableHardlink || dc.hlManager == nil {
		return nil
	}

	return dc.hlManager.MapKeyToDigest(key, chunkDigest)
}

// wrapMemoryWriter wraps a writer with memory caching
func (dc *directoryCache) wrapMemoryWriter(b *bytes.Buffer, w *writer, key string) (Writer, error) {
	return &writer{
		WriteCloser: nopWriteCloser(b),
		commitFunc: func() error {
			if dc.isClosed() {
				w.Close()
				return fmt.Errorf("cache is already closed")
			}

			cached, done, added := dc.cache.Add(key, b)
			if !added {
				dc.putBuffer(b)
			}

			commit := func() error {
				defer done()
				defer w.Close()

				n, err := w.Write(cached.(*bytes.Buffer).Bytes())
				if err != nil || n != cached.(*bytes.Buffer).Len() {
					w.Abort()
					return err
				}
				return w.Commit()
			}

			if dc.syncAdd {
				return commit()
			}

			go func() {
				if err := commit(); err != nil {
					log.L.Infof("failed to commit to file: %v", err)
				}
			}()
			return nil
		},
		abortFunc: func() error {
			defer w.Close()
			defer w.Abort()
			dc.putBuffer(b)
			return nil
		},
	}, nil
}
