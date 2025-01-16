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
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerd/log"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/sys/unix"
)

var (
	globalDB     *CacheDb
	globalDBOnce sync.Once
	globalDBErr  error
)

// CacheDb represents a content-addressable storage database
type CacheDb struct {
	db   *sql.DB
	path string
	mu   sync.RWMutex
}

// NewCacheDb creates a new Cache database at the specified path
func NewCacheDb(path string) (*CacheDb, error) {
	dbPath := filepath.Join(path, "cache.db")
	return FromFile(dbPath)
}

// FromFile creates a Cache database from the specified file path
func FromFile(dbPath string) (*CacheDb, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS Blobs (
			BlobId     TEXT PRIMARY KEY,
			FilePath   TEXT NOT NULL UNIQUE
		);
		
		CREATE TABLE IF NOT EXISTS Chunks (
			ChunkId           TEXT NOT NULL,
			ChunkOffset       INTEGER,
			BlobId            TEXT,
			UNIQUE(ChunkId, BlobId) ON CONFLICT IGNORE,
			FOREIGN KEY(BlobId) REFERENCES Blobs(BlobId)
		);
		
		CREATE INDEX IF NOT EXISTS ChunkIndex ON Chunks(ChunkId);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return &CacheDb{
		db:   db,
		path: dbPath,
	}, nil
}

// GetBlobID retrieves the blob ID for a given path
func (c *CacheDb) GetBlobID(blob string) (string, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var id string
	err := c.db.QueryRow("SELECT BlobId FROM Blobs WHERE FilePath = ?", blob).Scan(&id)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

// GetBlobPath retrieves the file path for a given blob ID
func (c *CacheDb) GetBlobPath(id string) (string, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var path string
	err := c.db.QueryRow("SELECT FilePath FROM Blobs WHERE BlobId = ?", id).Scan(&path)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return path, true, nil
}

// GetAllBlobs retrieves all blobs from the database
func (c *CacheDb) GetAllBlobs() ([]struct {
	ID   string
	Path string
}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	rows, err := c.db.Query("SELECT BlobId, FilePath FROM Blobs")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []struct {
		ID   string
		Path string
	}

	for rows.Next() {
		var result struct {
			ID   string
			Path string
		}
		if err := rows.Scan(&result.ID, &result.Path); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

// AddBlobs adds multiple blobs to the database
func (c *CacheDb) AddBlobs(blobs []string) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO Blobs (FilePath) VALUES (?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, blob := range blobs {
		if _, err := stmt.Exec(blob); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// AddBlob adds a single blob with the given ID and file path
func (c *CacheDb) AddBlob(blobID string, filePath string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.db.Exec("INSERT OR IGNORE INTO Blobs (BlobId, FilePath) VALUES (?, ?)", blobID, filePath)
	if err != nil {
		return "", err
	}
	return blobID, nil
}

// DeleteBlobs removes multiple blobs and their associated chunks
func (c *CacheDb) DeleteBlobs(blobs []string) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, blob := range blobs {
		var id string
		err := tx.QueryRow("SELECT BlobId FROM Blobs WHERE FilePath = ?", blob).Scan(&id)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return err
		}

		if _, err := tx.Exec("DELETE FROM Chunks WHERE BlobId = ?", id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM Blobs WHERE BlobId = ?", id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetChunkInfo retrieves information about a chunk
func (c *CacheDb) GetChunkInfo(chunkID string) (string, uint64, bool, error) {
	var filePath string
	var offset uint64

	err := c.db.QueryRow(`
		SELECT FilePath, ChunkOffset 
		FROM Chunks INDEXED BY ChunkIndex 
		JOIN Blobs ON Chunks.BlobId = Blobs.BlobId 
		WHERE ChunkId = ?
		ORDER BY Blobs.BlobId LIMIT 1 OFFSET 0`,
		chunkID).Scan(&filePath, &offset)

	if err == sql.ErrNoRows {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, err
	}
	return filePath, offset, true, nil
}

// AddChunks adds multiple chunks to the database
func (c *CacheDb) AddChunks(chunks []struct {
	ChunkID     string
	ChunkOffset uint64
	BlobPath    string
}) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, chunk := range chunks {
		var blobID string
		err := tx.QueryRow("SELECT BlobId FROM Blobs WHERE FilePath = ?", chunk.BlobPath).Scan(&blobID)
		if err != nil {
			return err
		}

		_, err = tx.Exec(
			"INSERT OR IGNORE INTO Chunks (ChunkId, ChunkOffset, BlobId) VALUES (?, ?, ?)",
			chunk.ChunkID, chunk.ChunkOffset, blobID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// AddChunk adds a single chunk to the database
func (c *CacheDb) AddChunk(chunkID string, chunkOffset uint64, blobPath string) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var blobID string
	err = tx.QueryRow("SELECT BlobId FROM Blobs WHERE FilePath = ?", blobPath).Scan(&blobID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		"INSERT OR IGNORE INTO Chunks (ChunkId, ChunkOffset, BlobId) VALUES (?, ?, ?)",
		chunkID, chunkOffset, blobID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Close closes the database connection
func (c *CacheDb) Close() error {
	return c.db.Close()
}

// GetCacheDB returns the global singleton instance of CacheDb
func GetCacheDB(rootDir string) (*CacheDb, error) {
	log.L.Info("GetCacheDB", "rootDir", rootDir)
	globalDBOnce.Do(func() {
		globalDB, globalDBErr = NewCacheDb(rootDir)
		log.L.Info("NewCacheDB", "rootDir", rootDir)
	})
	return globalDB, globalDBErr
}

// CopyFileRange copies data between files using copy_file_range syscall on Linux
func CopyFileRange(srcFile *os.File, srcOffset int64, dstFile *os.File, dstOffset int64, length int64) (uint64, error) {
	// Validate parameters
	if srcFile == nil || dstFile == nil {
		return 0, fmt.Errorf("invalid file descriptor: source or destination file is nil")
	}
	if length <= 0 {
		return 0, fmt.Errorf("invalid length: %d", length)
	}
	if srcOffset < 0 || dstOffset < 0 {
		return 0, fmt.Errorf("negative offset not allowed: src=%d dst=%d", srcOffset, dstOffset)
	}

	// Check if files are regular files
	srcStat, err := srcFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat source file: %w", err)
	}
	dstStat, err := dstFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat destination file: %w", err)
	}

	// Log file types and modes
	log.L.Debug("File types",
		"srcPath", srcFile.Name(),
		"srcMode", srcStat.Mode().String(),
		"srcIsRegular", srcStat.Mode().IsRegular(),
		"dstPath", dstFile.Name(),
		"dstMode", dstStat.Mode().String(),
		"dstIsRegular", dstStat.Mode().IsRegular())

	// Initialize variables before any goto statements
	var isXFS bool
	var statfs unix.Statfs_t

	// Get filesystem info
	if err := unix.Fstatfs(int(dstFile.Fd()), &statfs); err != nil {
		log.L.Debug("failed to get filesystem info, falling back to manual copy", "error", err)
		goto fallback
	}

	// Check if it's XFS
	isXFS = statfs.Type == 0x58465342 // XFS_SUPER_MAGIC

	// Try copy_file_range if conditions are met
	if srcStat.Mode().IsRegular() && dstStat.Mode().IsRegular() {
		// Align offset to filesystem block size for XFS
		if isXFS {
			blockSize := int64(statfs.Bsize)
			srcOffset = (srcOffset + blockSize - 1) &^ (blockSize - 1)
			dstOffset = (dstOffset + blockSize - 1) &^ (blockSize - 1)
		}

		written, err := unix.CopyFileRange(int(srcFile.Fd()), &srcOffset, int(dstFile.Fd()), &dstOffset, int(length), 0)
		if err == nil {
			return uint64(written), nil
		}
		log.L.Debug("copy_file_range failed",
			"error", err,
			"isXFS", isXFS,
			"srcSize", srcStat.Size(),
			"length", length)
	}

fallback:
	// Manual copy with optimized buffer size
	bufSize := 1024 * 1024 // 1MB default
	if isXFS {
		// Use larger buffer for XFS, aligned to block size
		bufSize = 2 * 1024 * 1024 // 2MB for XFS
		if blockSize := int(statfs.Bsize); blockSize > 0 {
			bufSize = (bufSize + blockSize - 1) &^ (blockSize - 1)
		}
	}
	buf := make([]byte, bufSize)
	var totalWritten int64

	for totalWritten < length {
		n := length - totalWritten
		if n > int64(len(buf)) {
			n = int64(len(buf))
		}

		nr, err := srcFile.ReadAt(buf[:n], srcOffset+totalWritten)
		if err != nil && err != io.EOF {
			return uint64(totalWritten), fmt.Errorf("read error at offset %d: %w", srcOffset+totalWritten, err)
		}
		if nr == 0 {
			break
		}

		nw, err := dstFile.WriteAt(buf[:nr], dstOffset+totalWritten)
		if err != nil {
			return uint64(totalWritten), fmt.Errorf("write error at offset %d: %w", dstOffset+totalWritten, err)
		}

		totalWritten += int64(nw)
		if nr < int(n) || totalWritten >= length {
			break
		}
	}

	log.L.Debug("Manual copy completed",
		"totalWritten", totalWritten,
		"requestedLength", length,
		"isXFS", isXFS)
	return uint64(totalWritten), nil
}
