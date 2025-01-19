package remote

import (
    "database/sql"
    "fmt"
    "sync"
    _ "github.com/mattn/go-sqlite3"
)

type ChunkDB struct {
    db *sql.DB
    mu sync.Mutex
}

func NewChunkDB(dbPath string) (*ChunkDB, error) {
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil {
        return nil, fmt.Errorf("failed to open database: %w", err)
    }

    // 创建表结构
    if err := db.Ping(); err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to ping database: %w", err)
    }

    // 创建chunks表，用于存储chunk hash到文件路径的映射和引用计数
    if _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS chunks (
            hash TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            ref_count INTEGER DEFAULT 1
        )
    `); err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to create table: %w", err)
    }

    return &ChunkDB{db: db}, nil
}

func (c *ChunkDB) Close() error {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.db.Close()
}

// AddChunk 添加新的chunk记录或增加引用计数
func (c *ChunkDB) AddChunk(hash, path string) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    tx, err := c.db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // 尝试更新已存在的记录
    result, err := tx.Exec(
        "UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = ?",
        hash,
    )
    if err != nil {
        return err
    }

    rows, err := result.RowsAffected()
    if err != nil {
        return err
    }

    // 如果记录不存在，创建新记录
    if rows == 0 {
        _, err = tx.Exec(
            "INSERT INTO chunks (hash, path, ref_count) VALUES (?, ?, 1)",
            hash, path,
        )
        if err != nil {
            return err
        }
    }

    return tx.Commit()
}

// GetChunkPath 获取chunk的文件路径
func (c *ChunkDB) GetChunkPath(hash string) (string, bool, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    var path string
    err := c.db.QueryRow(
        "SELECT path FROM chunks WHERE hash = ?",
        hash,
    ).Scan(&path)

    if err == sql.ErrNoRows {
        return "", false, nil
    }
    if err != nil {
        return "", false, err
    }

    return path, true, nil
}

// RemoveChunk 减少引用计数，当计数为0时删除记录
func (c *ChunkDB) RemoveChunk(hash string) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    tx, err := c.db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // 减少引用计数
    result, err := tx.Exec(
        "UPDATE chunks SET ref_count = ref_count - 1 WHERE hash = ?",
        hash,
    )
    if err != nil {
        return err
    }

    // 删除引用计数为0的记录
    _, err = tx.Exec(
        "DELETE FROM chunks WHERE ref_count <= 0",
    )
    if err != nil {
        return err
    }

    return tx.Commit()
} 