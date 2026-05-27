package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var ErrOffsetNotFound = errors.New("offset not found")

// CommitLog represents a file-backed append-only log for a topic.
type CommitLog struct {
	mu         sync.RWMutex
	dir        string
	dataFile   *os.File
	indexFile  *os.File
	offsets    []int64 // maps sequence ID to byte position in dataFile
	nextOffset uint64
}

func NewCommitLog(dir string) (*CommitLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	dataPath := filepath.Join(dir, "messages.dat")
	indexPath := filepath.Join(dir, "messages.idx")

	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	cl := &CommitLog{
		dir:       dir,
		dataFile:  dataFile,
		indexFile: indexFile,
		offsets:   make([]int64, 0),
	}

	if err := cl.recoverIndex(); err != nil {
		return nil, err
	}

	return cl, nil
}

// recoverIndex reads the .idx file to rebuild the in-memory offset map.
func (cl *CommitLog) recoverIndex() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	stat, err := cl.indexFile.Stat()
	if err != nil {
		return err
	}

	size := stat.Size()
	cl.offsets = make([]int64, size/8) // 8 bytes per position

	if size == 0 {
		cl.nextOffset = 0
		return nil
	}

	_, err = cl.indexFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	for i := int64(0); i < size/8; i++ {
		var pos int64
		if err := binary.Read(cl.indexFile, binary.BigEndian, &pos); err != nil {
			return err
		}
		cl.offsets[i] = pos
	}

	cl.nextOffset = uint64(len(cl.offsets))
	return nil
}

// Append writes a message to the disk and updates the index. Thread-safe.
func (cl *CommitLog) Append(msg []byte) (uint64, error) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	offset := cl.nextOffset

	// Get current file size / append position
	stat, err := cl.dataFile.Stat()
	if err != nil {
		return 0, err
	}
	pos := stat.Size()

	// Write size (4 bytes) + payload
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(msg)))

	if _, err := cl.dataFile.Write(sizeBuf); err != nil {
		return 0, err
	}
	if _, err := cl.dataFile.Write(msg); err != nil {
		return 0, err
	}

	// Write index
	if err := binary.Write(cl.indexFile, binary.BigEndian, pos); err != nil {
		return 0, err
	}

	// Sync to disk to ensure durability (fsync)
	cl.dataFile.Sync()
	cl.indexFile.Sync()

	cl.offsets = append(cl.offsets, pos)
	cl.nextOffset++

	return offset, nil
}

// Read fetches a message by offset. Uses ReadAt for thread-safe concurrent reads.
func (cl *CommitLog) Read(offset uint64) ([]byte, error) {
	cl.mu.RLock()
	if offset >= cl.nextOffset {
		cl.mu.RUnlock()
		return nil, ErrOffsetNotFound
	}
	pos := cl.offsets[offset]
	cl.mu.RUnlock()

	// ReadAt is thread-safe on Unix, no lock needed around dataFile for reads
	sizeBuf := make([]byte, 4)
	if _, err := cl.dataFile.ReadAt(sizeBuf, pos); err != nil {
		return nil, err
	}

	msgSize := binary.BigEndian.Uint32(sizeBuf)
	msgBuf := make([]byte, msgSize)
	if _, err := cl.dataFile.ReadAt(msgBuf, pos+4); err != nil {
		return nil, err
	}

	return msgBuf, nil
}

// Size returns the number of messages.
func (cl *CommitLog) Size() uint64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.nextOffset
}

func (cl *CommitLog) Close() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.dataFile.Close()
	return cl.indexFile.Close()
}
