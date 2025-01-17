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

package db

import (
	"os"
	"testing"
)

func TestCacheDb(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cachedb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := NewCacheDb(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Test adding blobs
	err = db.AddBlobs([]string{"/tmp/blob1", "/tmp/blob2"})
	if err != nil {
		t.Fatal(err)
	}

	// Test getting blob ID
	id, exists, err := db.GetBlobID("/tmp/blob1")
	if err != nil || !exists {
		t.Errorf("Expected blob1 to exist, got id=%q, exists=%v, err=%v", id, exists, err)
	}

	// Add more tests as needed...
}
