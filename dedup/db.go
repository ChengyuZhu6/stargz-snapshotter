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

package dedup

import (
	"database/sql"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

// DedupDb represents a deduplication storage database
type DedupDb struct {
	db   *sql.DB
	path string
}

// NewDedupDb creates a new deduplication database at the specified path
func NewDedupDb(path string) (*DedupDb, error) {
	dbPath := filepath.Join(path, "dedup.db")
	return NewDedupDbFromFile(dbPath)
}

// NewDedupDbFromFile creates a new deduplication database from the specified file path
func NewDedupDbFromFile(dbPath string) (*DedupDb, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		return nil, err
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS Files (
			FileId     INTEGER PRIMARY KEY,
			FilePath   TEXT NOT NULL UNIQUE
		)`)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS Chunks (
			ChunkId           TEXT NOT NULL,
			ChunkOffset      INTEGER,
			FileId           INTEGER,
			UNIQUE(ChunkId, FileId) ON CONFLICT IGNORE,
			FOREIGN KEY(FileId) REFERENCES Files(FileId)
		)`)
	if err != nil {
		return nil, err
	}

	return &DedupDb{
		db:   db,
		path: dbPath,
	}, nil
}

// GetFileId retrieves the file ID for a given file path
func (d *DedupDb) GetFileId(file string) (uint64, bool, error) {
	var id uint64
	err := d.db.QueryRow("SELECT FileId FROM Files WHERE FilePath = ?", file).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// GetFilePath retrieves the file path for a given file ID
func (d *DedupDb) GetFilePath(id uint64) (string, bool, error) {
	var path string
	err := d.db.QueryRow("SELECT FilePath FROM Files WHERE FileId = ?", id).Scan(&path)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return path, true, nil
}

// GetAllFiles retrieves all files from the database
func (d *DedupDb) GetAllFiles() ([]struct {
	ID   uint64
	Path string
}, error) {
	rows, err := d.db.Query("SELECT FileId, FilePath FROM Files")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []struct {
		ID   uint64
		Path string
	}

	for rows.Next() {
		var result struct {
			ID   uint64
			Path string
		}
		if err := rows.Scan(&result.ID, &result.Path); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

// AddFiles adds multiple files to the database
func (d *DedupDb) AddFiles(files []string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO Files (FilePath) VALUES (?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, file := range files {
		if _, err := stmt.Exec(file); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// AddFile adds a single file to the database
func (d *DedupDb) AddFile(file string) (uint64, error) {
	result, err := d.db.Exec("INSERT OR IGNORE INTO Files (FilePath) VALUES (?)", file)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	return uint64(id), err
}

// DeleteFiles deletes multiple files from the database
func (d *DedupDb) DeleteFiles(files []string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, file := range files {
		var id uint64
		err := tx.QueryRow("SELECT FileId FROM Files WHERE FilePath = ?", file).Scan(&id)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return err
		}

		if _, err := tx.Exec("DELETE FROM Chunks WHERE FileId = ?", id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM Files WHERE FileId = ?", id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetChunkInfo retrieves information about a chunk
func (d *DedupDb) GetChunkInfo(chunkID string) (string, uint64, bool, error) {
	var filePath string
	var offset uint64

	err := d.db.QueryRow(`
		SELECT FilePath, ChunkOffset 
		FROM Chunks 
		JOIN Files ON Chunks.FileId = Files.FileId 
		WHERE ChunkId = ?
		ORDER BY Files.FileId LIMIT 1 OFFSET 0`,
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
func (d *DedupDb) AddChunks(chunks []struct {
	ChunkID     string
	ChunkOffset uint64
	FilePath    string
}) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO Chunks (ChunkId, ChunkOffset, FileId) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, chunk := range chunks {
		var fileID uint64
		err := tx.QueryRow("SELECT FileId FROM Files WHERE FilePath = ?", chunk.FilePath).Scan(&fileID)
		if err != nil {
			return err
		}

		if _, err := stmt.Exec(chunk.ChunkID, chunk.ChunkOffset, fileID); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// AddChunk adds a single chunk to the database
func (d *DedupDb) AddChunk(chunkID string, chunkOffset uint64, filePath string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var fileID uint64
	err = tx.QueryRow("SELECT FileId FROM Files WHERE FilePath = ?", filePath).Scan(&fileID)
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT OR IGNORE INTO Chunks (ChunkId, ChunkOffset, FileId) VALUES (?, ?, ?)",
		chunkID, chunkOffset, fileID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Close closes the database connection
func (d *DedupDb) Close() error {
	return d.db.Close()
}

// GetFileChunks retrieves all chunks associated with a file
func (d *DedupDb) GetFileChunks(filePath string) ([]struct {
	ChunkID     string
	ChunkOffset uint64
}, error) {
	var fileID uint64
	err := d.db.QueryRow("SELECT FileId FROM Files WHERE FilePath = ?", filePath).Scan(&fileID)
	if err != nil {
		return nil, err
	}

	rows, err := d.db.Query(`
		SELECT ChunkId, ChunkOffset 
		FROM Chunks 
		WHERE FileId = ? 
		ORDER BY ChunkOffset`,
		fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []struct {
		ChunkID     string
		ChunkOffset uint64
	}

	for rows.Next() {
		var chunk struct {
			ChunkID     string
			ChunkOffset uint64
		}
		if err := rows.Scan(&chunk.ChunkID, &chunk.ChunkOffset); err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return chunks, nil
}
