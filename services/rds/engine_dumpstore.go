package rds

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	dumpDirMode  = 0o700
	dumpFileMode = 0o600
	dumpFileExt  = ".json"
)

// dumpRecord is one logical dump on disk: a snapshot's content or a restore point.
type dumpRecord struct {
	At       time.Time `json:"at,omitzero"`
	Key      string    `json:"key"`
	Dump     string    `json:"dump"`
	Password string    `json:"password"`
	DBName   string    `json:"dbName"`
}

// dumpStore persists logical dumps under one directory; a nil store keeps everything in memory.
type dumpStore struct{ dir string }

func newDumpStore(dir string) *dumpStore {
	if dir == "" {
		return nil
	}

	return &dumpStore{dir: dir}
}

func (s *dumpStore) path(key string) string {
	return filepath.Join(s.dir, hex.EncodeToString([]byte(key))+dumpFileExt)
}

func (s *dumpStore) save(key string, at time.Time, d engineData) error {
	if s == nil {
		return nil
	}

	rec := dumpRecord{At: at, Key: key, Dump: d.dump, Password: d.password, DBName: d.dbName}

	raw, err := json.Marshal(rec) //nolint:gosec // the master password must come back with the restored data
	if err != nil {
		return fmt.Errorf("marshal dump: %w", err)
	}

	if err = os.MkdirAll(s.dir, dumpDirMode); err != nil {
		return fmt.Errorf("create dump dir: %w", err)
	}

	dst := s.path(key)
	tmp := dst + ".tmp"

	if err = os.WriteFile(tmp, raw, dumpFileMode); err != nil {
		return fmt.Errorf("write dump: %w", err)
	}

	if err = os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("commit dump: %w", err)
	}

	return nil
}

func (s *dumpStore) remove(key string) {
	if s != nil {
		_ = os.Remove(s.path(key))
	}
}

// load reads every stored dump; unreadable files are skipped.
func (s *dumpStore) load() []dumpRecord {
	if s == nil {
		return nil
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}

	var out []dumpRecord

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), dumpFileExt) {
			continue
		}

		raw, rerr := os.ReadFile(filepath.Join(s.dir, e.Name()))
		if rerr != nil {
			continue
		}

		var rec dumpRecord
		if json.Unmarshal(raw, &rec) == nil && rec.Key != "" {
			out = append(out, rec)
		}
	}

	return out
}
