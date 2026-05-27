package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// OffsetManager tracks consumer group offsets.
type OffsetManager struct {
	mu       sync.RWMutex
	filePath string
	offsets  map[string]map[string]uint64 // map[topic]map[group]offset
}

func NewOffsetManager(dir string) (*OffsetManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	filePath := filepath.Join(dir, "consumer_offsets.json")

	om := &OffsetManager{
		filePath: filePath,
		offsets:  make(map[string]map[string]uint64),
	}

	if err := om.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return om, nil
}

func (om *OffsetManager) load() error {
	data, err := os.ReadFile(om.filePath)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &om.offsets)
}

func (om *OffsetManager) save() error {
	data, err := json.Marshal(om.offsets)
	if err != nil {
		return err
	}
	// Atomic write: write to temp file then rename
	tmpFile := om.filePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpFile, om.filePath)
}

func (om *OffsetManager) Commit(topic, group string, offset uint64) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	if _, ok := om.offsets[topic]; !ok {
		om.offsets[topic] = make(map[string]uint64)
	}
	om.offsets[topic][group] = offset
	return om.save()
}

func (om *OffsetManager) Get(topic, group string) uint64 {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if tMap, ok := om.offsets[topic]; ok {
		if off, ok := tMap[group]; ok {
			return off
		}
	}
	return 0
}
