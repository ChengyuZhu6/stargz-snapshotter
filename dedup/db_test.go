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
	"os"
	"path/filepath"
	"testing"
)

func setupTestDB(t *testing.T) (*DedupDb, string, func()) {
	// Create a temporary directory for the test database
	tmpDir, err := os.MkdirTemp("", "dedup-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create a new database
	db, err := NewDedupDb(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create database: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db, tmpDir, cleanup
}

func TestNewDedupDb(t *testing.T) {
	db, tmpDir, cleanup := setupTestDB(t)
	defer cleanup()

	// Check if database file exists
	dbPath := filepath.Join(tmpDir, "dedup.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file was not created")
	}

	if db == nil {
		t.Error("Database should not be nil")
	}
}

func TestFileOperations(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	// Test adding a single file
	filePath := "/test/file1"
	fileID, err := db.AddFile(filePath)
	if err != nil {
		t.Fatalf("Failed to add file: %v", err)
	}

	// Test getting file ID
	gotID, exists, err := db.GetFileId(filePath)
	if err != nil {
		t.Fatalf("Failed to get file ID: %v", err)
	}
	if !exists {
		t.Error("File should exist")
	}
	if gotID != fileID {
		t.Errorf("File ID mismatch: got %d, want %d", gotID, fileID)
	}

	// Test getting file path
	gotPath, exists, err := db.GetFilePath(fileID)
	if err != nil {
		t.Fatalf("Failed to get file path: %v", err)
	}
	if !exists {
		t.Error("File should exist")
	}
	if gotPath != filePath {
		t.Errorf("File path mismatch: got %s, want %s", gotPath, filePath)
	}
}

func TestAddAndGetChunk(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	// Add a file first
	filePath := "/test/file1"
	_, err := db.AddFile(filePath)
	if err != nil {
		t.Fatalf("Failed to add file: %v", err)
	}

	// Test data
	chunkID := "test-chunk-1"
	offset := uint64(1000)

	// Add chunk
	err = db.AddChunk(chunkID, offset, filePath)
	if err != nil {
		t.Fatalf("Failed to add chunk: %v", err)
	}

	// Get chunk
	gotFilePath, gotOffset, exists, err := db.GetChunkInfo(chunkID)
	if err != nil {
		t.Fatalf("Failed to get chunk: %v", err)
	}

	// Verify results
	if !exists {
		t.Error("Chunk should exist")
	}
	if gotFilePath != filePath {
		t.Errorf("FilePath mismatch: got %s, want %s", gotFilePath, filePath)
	}
	if gotOffset != offset {
		t.Errorf("Offset mismatch: got %d, want %d", gotOffset, offset)
	}
}

func TestAddChunks(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	// Add files first
	filePaths := []string{"/path/1", "/path/2", "/path/3"}
	err := db.AddFiles(filePaths)
	if err != nil {
		t.Fatalf("Failed to add files: %v", err)
	}

	chunks := []struct {
		ChunkID     string
		ChunkOffset uint64
		FilePath    string
	}{
		{"chunk1", 0, "/path/1"},
		{"chunk2", 100, "/path/2"},
		{"chunk3", 300, "/path/3"},
	}

	// Add chunks
	err = db.AddChunks(chunks)
	if err != nil {
		t.Fatalf("Failed to add chunks: %v", err)
	}

	// Verify each chunk
	for _, chunk := range chunks {
		filePath, offset, exists, err := db.GetChunkInfo(chunk.ChunkID)
		if err != nil {
			t.Fatalf("Failed to get chunk %s: %v", chunk.ChunkID, err)
		}
		if !exists {
			t.Errorf("Chunk %s should exist", chunk.ChunkID)
			continue
		}
		if filePath != chunk.FilePath {
			t.Errorf("FilePath mismatch for %s: got %s, want %s", chunk.ChunkID, filePath, chunk.FilePath)
		}
		if offset != chunk.ChunkOffset {
			t.Errorf("Offset mismatch for %s: got %d, want %d", chunk.ChunkID, offset, chunk.ChunkOffset)
		}
	}
}

func TestDeleteFiles(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	// Add test files
	files := []string{"/test/file1", "/test/file2", "/test/file3"}
	err := db.AddFiles(files)
	if err != nil {
		t.Fatalf("Failed to add files: %v", err)
	}

	// Add some chunks
	chunks := []struct {
		ChunkID     string
		ChunkOffset uint64
		FilePath    string
	}{
		{"chunk1", 0, "/test/file1"},
		{"chunk2", 100, "/test/file2"},
		{"chunk3", 300, "/test/file3"},
	}
	err = db.AddChunks(chunks)
	if err != nil {
		t.Fatalf("Failed to add chunks: %v", err)
	}

	// Delete two files
	filesToDelete := []string{"/test/file1", "/test/file2"}
	err = db.DeleteFiles(filesToDelete)
	if err != nil {
		t.Fatalf("Failed to delete files: %v", err)
	}

	// Verify deleted files don't exist
	for _, file := range filesToDelete {
		_, exists, err := db.GetFileId(file)
		if err != nil {
			t.Fatalf("Error checking deleted file %s: %v", file, err)
		}
		if exists {
			t.Errorf("File %s should not exist after deletion", file)
		}

		// Verify associated chunks are deleted
		for _, chunk := range chunks {
			if chunk.FilePath == file {
				_, _, exists, err := db.GetChunkInfo(chunk.ChunkID)
				if err != nil {
					t.Fatalf("Error checking chunk for deleted file: %v", err)
				}
				if exists {
					t.Errorf("Chunk %s should not exist after file deletion", chunk.ChunkID)
				}
			}
		}
	}

	// Verify remaining file still exists
	_, exists, err := db.GetFileId("/test/file3")
	if err != nil {
		t.Fatalf("Error checking remaining file: %v", err)
	}
	if !exists {
		t.Error("File /test/file3 should still exist")
	}
}

func TestGetAllFiles(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	// Add test files
	files := []string{"/test/file1", "/test/file2", "/test/file3"}
	err := db.AddFiles(files)
	if err != nil {
		t.Fatalf("Failed to add files: %v", err)
	}

	// Get all files
	gotFiles, err := db.GetAllFiles()
	if err != nil {
		t.Fatalf("Failed to get all files: %v", err)
	}

	// Verify number of files
	if len(gotFiles) != len(files) {
		t.Errorf("Wrong number of files: got %d, want %d", len(gotFiles), len(files))
	}

	// Verify each file exists in the result
	for _, file := range files {
		found := false
		for _, gotFile := range gotFiles {
			if gotFile.Path == file {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("File %s not found in results", file)
		}
	}
}
