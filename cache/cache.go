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
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

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
		cache:        dataCache,
		fileCache:    fdCache,
		wipLock:      new(namedmutex.NamedMutex),
		directory:    directory,
		wipDirectory: wipdir,
		bufPool:      bufPool,
		direct:       config.Direct,
	}
	dc.syncAdd = config.SyncAdd
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
}

func printStack() {
	pcs := make([]uintptr, 10)   // 10 is the number of stack frames to capture
	n := runtime.Callers(2, pcs) // Skip 2 frames (printStack and caller)
	pcs = pcs[:n]

	for _, pc := range pcs {
		fn := runtime.FuncForPC(pc)
		file, line := fn.FileLine(pc)
		fmt.Printf("%s:%d %s\n", file, line, fn.Name())
	}
}

func CustomPrint(v ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		fmt.Printf("********** [debug] %s:%d: ", file, line)
	}
	fmt.Println(fmt.Sprint(v...))
	printStack()
}

func (dc *directoryCache) Get(key string, opts ...Option) (Reader, error) {
	if dc.isClosed() {
		return nil, fmt.Errorf("cache is already closed")
	}

	// Validate key
	if key == "" {
		return nil, fmt.Errorf("empty key is not allowed")
	}

	opt := &cacheOpt{}
	for _, o := range opts {
		opt = o(opt)
	}

	if !dc.direct && !opt.direct {
		// Get data from memory
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

	// Open the cache file and read the target region
	cachePath := dc.cachePath(key)
	log.Printf("directoryCache Get dc.cachePath(key) = %s", cachePath)
	file, err := os.Open(cachePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open blob file for %q: %w", key, err)
	}

	// If "direct" option is specified, do not cache the file on memory.
	// This option is useful for preventing memory cache from being polluted by data
	// that won't be accessed immediately.
	if dc.direct || opt.direct {
		return &reader{
			ReaderAt: file,
			closeFunc: func() error {
				// In passthough model, close will be toke over by go-fuse
				// If "passThrough" option is specified, "direct" option also will
				// be specified, so adding this branch here is enough
				if opt.passThrough {
					return nil
				}
				return file.Close()
			},
		}, nil
	}

	// TODO: should we cache the entire file data on memory?
	//       but making I/O (possibly huge) on every fetching
	//       might be costly.
	return &reader{
		ReaderAt: file,
		closeFunc: func() error {
			_, done, added := dc.fileCache.Add(key, file)
			defer done() // Release it immediately. Cleaned up on eviction.
			if !added {
				return file.Close() // file already exists in the cache. close it.
			}
			return nil
		},
	}, nil
}

func (dc *directoryCache) Add(key string, opts ...Option) (Writer, error) {
	if dc.isClosed() {
		return nil, fmt.Errorf("cache is already closed")
	}

	// Validate key
	if key == "" {
		return nil, fmt.Errorf("empty key is not allowed")
	}

	cachePath := dc.cachePath(key)
	CustomPrint("cachePath", cachePath)
	// Check whether the file exists
	_, err := os.Stat(cachePath)
	log.Printf("err = %s", err)
	if os.IsExist(err) {
		return nil, fmt.Errorf("cache file for %q does exist", key)
	}
	// CustomPrint("opts")
	log.Printf("opts = %+v", opts)
	opt := &cacheOpt{}
	for _, o := range opts {
		opt = o(opt)
	}
	log.Printf("opt = %+v", opt)
	// CustomPrint("opt")
	wip, err := dc.wipFile(key)
	if err != nil {
		return nil, err
	}
	w := &writer{
		WriteCloser: wip,
		commitFunc: func() error {
			if dc.isClosed() {
				return fmt.Errorf("cache is already closed")
			}
			// Commit the cache contents
			cachePath := dc.cachePath(key)
			log.Printf("directoryCache Add dc.cachePath(key) = %s", cachePath)
			log.Printf("directoryCache Add c = %s", cachePath)

			// Ensure the directory exists
			cacheDir := filepath.Dir(cachePath)
			if err := os.MkdirAll(cacheDir, os.ModePerm); err != nil {
				var errs []error
				if err := os.Remove(wip.Name()); err != nil {
					errs = append(errs, err)
				}
				errs = append(errs, fmt.Errorf("failed to create cache directory %q: %w", cacheDir, err))
				return errors.Join(errs...)
			}
			log.Printf("Ensure the directory exists = %s", cacheDir)
			// Perform the rename
			if err := os.Rename(wip.Name(), cachePath); err != nil {
				return fmt.Errorf("failed to rename cache file: %w", err)
			}
			log.Printf("Rename cache file = %s", cachePath)
			return nil
		},
		abortFunc: func() error {
			return os.Remove(wip.Name())
		},
	}

	// If "direct" option is specified, do not cache the passed data on memory.
	// This option is useful for preventing memory cache from being polluted by data
	// that won't be accessed immediately.
	if dc.direct || opt.direct {
		return w, nil
	}

	b := dc.bufPool.Get().(*bytes.Buffer)
	memW := &writer{
		WriteCloser: nopWriteCloser(io.Writer(b)),
		commitFunc: func() error {
			if dc.isClosed() {
				w.Close()
				return fmt.Errorf("cache is already closed")
			}
			log.Printf("cached, done, added := dc.cache.Add(key, b)")
			// CustomPrint("memW")
			cached, done, added := dc.cache.Add(key, b)
			log.Printf("cached, added = %+v", added)
			if !added {
				dc.putBuffer(b) // already exists in the cache. abort it.
			}
			log.Printf("commit := func() error {")
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
			// CustomPrint("dc.syncAdd")
			if dc.syncAdd {
				return commit()
			}
			go func() {
				// CustomPrint("commit to file")
				if err := commit(); err != nil {
					fmt.Println("failed to commit to file:", err)
				}
			}()
			// CustomPrint("successful")
			return nil
		},
		abortFunc: func() error {
			defer w.Close()
			defer w.Abort()
			dc.putBuffer(b) // abort it.
			return nil
		},
	}

	return memW, nil
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

func (dc *directoryCache) cachePath(key string) string {
	log.Printf("cachePath key = %s", key)
	log.Printf("dc.directory = %s", dc.directory)
	// Check if key has at least 2 characters before slicing
	var prefix string
	if len(key) >= 2 {
		prefix = key[:2]
		log.Printf("cachePath key[:2] = %s", prefix)
	} else {
		prefix = key
		log.Printf("cachePath using full key as prefix = %s", prefix)
	}

	path := filepath.Join(dc.directory, prefix, key)
	log.Printf("cachePath filepath.Join(dc.directory, prefix, key) = %s", path)
	return path
}

func (dc *directoryCache) wipFile(key string) (*os.File, error) {
	return os.CreateTemp(dc.wipDirectory, key+"-*")
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

// Helper function for min
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
