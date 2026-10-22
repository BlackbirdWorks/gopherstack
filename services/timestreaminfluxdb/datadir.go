package timestreaminfluxdb

import (
	"os"
	"path/filepath"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	instancesSubdir = "instances"
	backupsSubdir   = "backups"
	tempRootPattern = "gopherstack-timestream-influxdb-*"
)

// SetDataDir keeps instance data and backups under dir; empty uses a temporary directory removed on Close.
func (b *InMemoryBackend) SetDataDir(dir string) {
	b.mu.Lock("SetDataDir")
	defer b.mu.Unlock()

	b.rootDir, b.rootOwned = dir, false
}

// persistentRoot reports whether data lives in a configured directory that outlasts the process.
func (b *InMemoryBackend) persistentRoot() bool { return b.rootDir != "" && !b.rootOwned }

func (b *InMemoryBackend) rootLocked() string {
	if b.rootDir == "" {
		dir, err := os.MkdirTemp("", tempRootPattern)
		if err != nil {
			logger.Load(b.bgOrBackground()).WarnContext(b.bgOrBackground(),
				"timestreaminfluxdb: temporary data directory unavailable", "error", err)

			return ""
		}

		b.rootDir, b.rootOwned = dir, true
	}

	return b.rootDir
}

func (b *InMemoryBackend) instanceDirLocked(id string) string {
	return b.subdirLocked(instancesSubdir, id)
}

func (b *InMemoryBackend) backupsDirLocked() string { return b.subdirLocked(backupsSubdir, "") }

func (b *InMemoryBackend) backupDirLocked(id string) string { return b.subdirLocked(backupsSubdir, id) }

func (b *InMemoryBackend) subdirLocked(sub, id string) string {
	root := b.rootLocked()
	if root == "" {
		return ""
	}

	return filepath.Join(root, sub, id)
}

// ensureDirs creates the host directories an engine start mounts.
func ensureDirs(spec EngineSpec) error {
	for _, d := range []string{spec.DataDir, spec.BackupsDir} {
		if d == "" {
			continue
		}

		if err := os.MkdirAll(d, dirPerm); err != nil {
			return err
		}
	}

	return nil
}

func removeDir(dir string) {
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
}

func (b *InMemoryBackend) removeAllData() {
	b.mu.RLock("removeAllData")
	root := b.rootDir
	b.mu.RUnlock()

	if root != "" {
		removeDir(filepath.Join(root, instancesSubdir))
		removeDir(filepath.Join(root, backupsSubdir))
	}
}

// dropTempRoot deletes a temporary data directory; configured directories are never removed here.
func (b *InMemoryBackend) dropTempRoot() {
	b.mu.Lock("dropTempRoot")
	root, owned := b.rootDir, b.rootOwned
	b.mu.Unlock()

	if owned {
		removeDir(root)
	}
}
