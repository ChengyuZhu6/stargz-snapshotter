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
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

// CacheDb represents a content-addressable storage database
type CacheDb struct {
	db   *sql.DB
	path string
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
