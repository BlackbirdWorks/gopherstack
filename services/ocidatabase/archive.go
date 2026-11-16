package ocidatabase

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
)

const (
	backupFailed  = "FAILED"
	dumpExt       = ".dmp"
	dumpDirMode   = 0o700
	dumpFileMode  = 0o600
	maxDumpBytes  = int64(2) << 30
	dumpTempLabel = "gopherstack-ocidatabase-"
)

// ErrRolledBack marks a failed Import that left the previous schema in place, so the database stays usable.
var ErrRolledBack = errors.New("the previous contents were kept")

var (
	errDumpTooLarge = errors.New("the backup exceeds the 2 GiB emulator limit")
	errDumpCorrupt  = errors.New("the stored backup failed its integrity check")
	errNotRunning   = errors.New("the database is not running")
	errNoArchiver   = errors.New("the database cannot be dumped")
)

// Archiver is implemented by running databases that can dump and reload the ADMIN schema.
type Archiver interface {
	// Export writes a logical dump of the ADMIN schema to w and returns the database SCN it was taken at.
	Export(ctx context.Context, w io.Writer) (int64, error)
	// Import replaces the ADMIN schema with the dump in r; a failed import leaves the previous contents in place.
	Import(ctx context.Context, r io.Reader) error
}

// limitWriter fails once more than left bytes would be written.
type limitWriter struct {
	w    io.Writer
	left int64
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > l.left {
		return 0, errDumpTooLarge
	}

	l.left -= int64(len(p))

	return l.w.Write(p)
}

func dumpName(bkID string) string { return bkID + dumpExt }

// dumpRoot returns the directory backups are kept in, creating it on first use.
func (b *InMemoryBackend) dumpRoot() (*os.Root, error) {
	b.mu.Lock("dumpRoot")

	if b.dumpDir == "" {
		dir, err := os.MkdirTemp("", dumpTempLabel)
		if err != nil {
			b.mu.Unlock()

			return nil, fmt.Errorf("create the backup directory: %w", err)
		}

		b.dumpDir = dir
	}

	dir := b.dumpDir
	b.mu.Unlock()

	if err := os.MkdirAll(dir, dumpDirMode); err != nil {
		return nil, fmt.Errorf("create the backup directory: %w", err)
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open the backup directory: %w", err)
	}

	return root, nil
}

// removeDump deletes one backup's file.
func removeDump(dir, bkID string) {
	if dir == "" {
		return
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return
	}
	defer root.Close()

	_ = root.Remove(dumpName(bkID))
}

// removeDumps deletes the stored backup files; the directory is recreated on demand.
func removeDumps(dir string) {
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
}

// SetDumpLimit overrides the largest backup file the emulator keeps.
func (b *InMemoryBackend) SetDumpLimit(n int64) {
	b.mu.Lock("SetDumpLimit")
	b.dumpLimit = n
	b.mu.Unlock()
}

// SetDumpDir keeps backup files under dir instead of a temporary directory.
func (b *InMemoryBackend) SetDumpDir(dir string) {
	b.mu.Lock("SetDumpDir")
	b.dumpDir = dir
	b.mu.Unlock()
}

func (b *InMemoryBackend) dumpCap() int64 {
	b.mu.RLock("dumpCap")
	defer b.mu.RUnlock()

	if b.dumpLimit > 0 {
		return b.dumpLimit
	}

	return maxDumpBytes
}

// archiverOf returns the live database's dump support. Callers hold the backend lock.
func (b *InMemoryBackend) archiverOf(id string) (Archiver, bool) {
	if b.eng == nil {
		return nil, false
	}

	lv := b.eng.live[id]
	if lv == nil {
		return nil, false
	}

	a, ok := lv.inst.(Archiver)

	return a, ok
}

// backupWork dumps the database into the backup's file and settles the backup.
func (b *InMemoryBackend) backupWork(dbID, bkID string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		size, sum, scn, err := b.captureDump(ctx, dbID, bkID)
		b.finishBackup(bkID, size, sum, scn, err)

		return nil
	}
}

func (b *InMemoryBackend) captureDump(ctx context.Context, dbID, bkID string) (int64, string, int64, error) {
	lv := b.liveOf(dbID)
	if lv == nil {
		return 0, "", 0, errNotRunning
	}

	ar, ok := lv.inst.(Archiver)
	if !ok {
		return 0, "", 0, errNoArchiver
	}

	root, err := b.dumpRoot()
	if err != nil {
		return 0, "", 0, err
	}
	defer root.Close()

	f, err := root.OpenFile(dumpName(bkID), os.O_WRONLY|os.O_CREATE|os.O_EXCL, dumpFileMode)
	if err != nil {
		return 0, "", 0, fmt.Errorf("create the backup file: %w", err)
	}

	h := sha256.New()
	limit := b.dumpCap()
	lw := &limitWriter{w: io.MultiWriter(f, h), left: limit}
	scn, err := ar.Export(ctx, lw)

	if cerr := f.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("write the backup file: %w", cerr)
	}

	if err != nil {
		_ = root.Remove(dumpName(bkID))

		return 0, "", 0, err
	}

	return limit - lw.left, hex.EncodeToString(h.Sum(nil)), scn, nil
}

func (b *InMemoryBackend) finishBackup(bkID string, size int64, sum string, scn int64, err error) {
	b.mu.Lock("finishBackup")
	defer b.mu.Unlock()

	bk, ok := b.backups[bkID]
	if !ok || bk.State != backupCreating {
		return
	}

	now := b.nowUTC()

	if err != nil {
		bk.State, bk.Details = backupFailed, "Backup failed: "+err.Error()

		return
	}

	bk.State, bk.Ended = backupActive, &now
	bk.DumpBytes, bk.DumpSHA256, bk.SCN = size, sum, scn
}

// restorePoint picks the newest backup with data that satisfies the restore request.
func (b *InMemoryBackend) restorePoint(dbID string, in restoreIn) *Backup {
	var best *Backup

	scn, scnErr := int64(0), error(nil)
	if in.SCN != nil {
		scn, scnErr = strconv.ParseInt(*in.SCN, 10, 64)
	}

	for _, bk := range b.backups {
		if bk.DBID != dbID || bk.State != backupActive || bk.DumpSHA256 == "" {
			continue
		}

		if in.Timestamp != nil && bk.Started.After(*in.Timestamp) {
			continue
		}

		if in.SCN != nil && (scnErr != nil || bk.SCN == 0 || bk.SCN > scn) {
			continue
		}

		if best == nil || bk.Seq > best.Seq {
			best = bk
		}
	}

	if best == nil {
		return nil
	}

	cp := *best

	return &cp
}

// restoreWork reloads the newest matching backup; without one the restore changes no data.
func (b *InMemoryBackend) restoreWork(dbID string, in restoreIn) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		b.mu.RLock("restoreWork")
		ar, ok := b.archiverOf(dbID)
		bk := b.restorePoint(dbID, in)
		b.mu.RUnlock()

		if !ok || bk == nil {
			return nil
		}

		root, err := b.dumpRoot()
		if err != nil {
			return err
		}
		defer root.Close()

		if _, err = root.Stat(dumpName(bk.ID)); errors.Is(err, fs.ErrNotExist) {
			return nil
		}

		if err = verifyDump(root, bk); err != nil {
			return fmt.Errorf("%w: %w", ErrRolledBack, err)
		}

		f, err := root.Open(dumpName(bk.ID))
		if err != nil {
			return fmt.Errorf("open the backup file: %w", err)
		}
		defer f.Close()

		return ar.Import(ctx, io.LimitReader(f, bk.DumpBytes))
	}
}

// verifyDump re-reads the backup file and compares its size and SHA-256 with the recorded ones.
func verifyDump(root *os.Root, bk *Backup) error {
	f, err := root.Open(dumpName(bk.ID))
	if err != nil {
		return fmt.Errorf("open the backup file: %w", err)
	}
	defer f.Close()

	h := sha256.New()

	n, err := io.Copy(h, io.LimitReader(f, bk.DumpBytes+1))
	if err != nil {
		return fmt.Errorf("read the backup file: %w", err)
	}

	want, derr := hex.DecodeString(bk.DumpSHA256)
	if n != bk.DumpBytes || derr != nil || subtle.ConstantTimeCompare(h.Sum(nil), want) != 1 {
		return errDumpCorrupt
	}

	return nil
}
