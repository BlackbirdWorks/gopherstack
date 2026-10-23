package kinesisanalyticsv2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	flinkCodeSubdir  = "code"
	flinkStateSubdir = "savepoints"
	flinkRootMode    = 0o700
	flinkBlobMode    = 0o600
	flinkHashLen     = 16
)

// persistedStopSave is the on-disk form of the stop-with-savepoint location kept per application.
type persistedStopSave struct {
	At   time.Time `json:"at"`
	Path string    `json:"path"`
}

func absDataDir(dir string) string {
	if dir == "" {
		return ""
	}

	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}

	return dir
}

func (e *flinkEngine) persistent() bool { return e != nil && e.cfg.DataDir != "" }

func appHash(key string) string {
	sum := sha256.Sum256([]byte(key))

	return hex.EncodeToString(sum[:])[:flinkHashLen]
}

// stateDir is the host directory bind-mounted as the app's savepoint dir.
func (e *flinkEngine) stateDir(key string) string {
	return filepath.Join(e.cfg.DataDir, flinkStateSubdir, appHash(key))
}

// ensureStateDir creates the savepoint dir; the leaf is world-writable because the container's flink user
// (uid 9999) writes into it, while the 0700 parents keep other host users out.
func (e *flinkEngine) ensureStateDir(key string) (string, error) {
	parent := filepath.Join(e.cfg.DataDir, flinkStateSubdir)
	if err := os.MkdirAll(parent, flinkRootMode); err != nil {
		return "", fmt.Errorf("create savepoint root: %w", err)
	}

	d := e.stateDir(key)
	if err := os.MkdirAll(d, flinkRootMode); err != nil {
		return "", fmt.Errorf("create savepoint dir: %w", err)
	}

	if err := os.Chmod(d, flinkDirMode); err != nil {
		return "", fmt.Errorf("chmod savepoint dir: %w", err)
	}

	return d, nil
}

func (e *flinkEngine) codeFile(key, md5sum string) string {
	return filepath.Join(e.cfg.DataDir, flinkCodeSubdir, appHash(key)+"-"+md5sum+".zip")
}

// removeCode deletes every persisted code blob of the application.
func (e *flinkEngine) removeCode(key string) {
	e.pruneCode(key, "")
}

func (e *flinkEngine) pruneCode(key, keep string) {
	matches, _ := filepath.Glob(filepath.Join(e.cfg.DataDir, flinkCodeSubdir, appHash(key)+"-*.zip"))
	for _, m := range matches {
		if filepath.Base(m) != keep {
			_ = os.Remove(m)
		}
	}
}

// wipePersistent drops all persisted code and savepoints (Reset).
func (e *flinkEngine) wipePersistent() {
	if !e.persistent() {
		return
	}

	_ = os.RemoveAll(filepath.Join(e.cfg.DataDir, flinkCodeSubdir))
	_ = os.RemoveAll(filepath.Join(e.cfg.DataDir, flinkStateSubdir))
}

// writeFileAtomic writes data through a temp file and rename so readers never see a partial blob.
func writeFileAtomic(name string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(name), flinkRootMode); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(name), ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	_, werr := tmp.Write(data)
	cerr := tmp.Close()

	if err = firstErr(werr, cerr, os.Chmod(tmp.Name(), flinkBlobMode), os.Rename(tmp.Name(), name)); err != nil {
		_ = os.Remove(tmp.Name())

		return fmt.Errorf("write %s: %w", filepath.Base(name), err)
	}

	return nil
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// persistCodeLocked stores the app's ZIPFILE code under the data dir and returns its file name.
func (b *InMemoryBackend) persistCodeLocked(ctx context.Context, app *Application) string {
	e := b.flink
	if !e.persistent() || app.CodeConfig == nil || app.CodeConfig.CodeContentDescription == nil {
		return ""
	}

	desc := app.CodeConfig.CodeContentDescription
	if len(desc.zipContent) == 0 || desc.CodeMD5 == "" {
		return ""
	}

	key := applicationKey(app.Region, app.ApplicationName)
	name := e.codeFile(key, desc.CodeMD5)

	if _, err := os.Stat(name); err != nil {
		if err = writeFileAtomic(name, desc.zipContent); err != nil {
			logger.Load(ctx).WarnContext(ctx, "kinesisanalyticsv2: persist application code failed",
				"application", app.ApplicationName, "error", err)

			return ""
		}
	}

	e.pruneCode(key, filepath.Base(name))

	return filepath.Base(name)
}

// loadCodeLocked reloads persisted ZIPFILE code into app after a restore.
func (b *InMemoryBackend) loadCodeLocked(app *Application, file string) {
	e := b.flink
	if !e.persistent() || file == "" || file != filepath.Base(file) ||
		app.CodeConfig == nil || app.CodeConfig.CodeContentDescription == nil {
		return
	}

	raw, err := os.ReadFile(filepath.Join(e.cfg.DataDir, flinkCodeSubdir, file))
	if err != nil {
		return
	}

	app.CodeConfig.CodeContentDescription.zipContent = raw
}

func toPersistedStopSaves(m map[string]flinkStopSave) map[string]persistedStopSave {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]persistedStopSave, len(m))
	for k, v := range m {
		out[k] = persistedStopSave{At: v.at, Path: v.path}
	}

	return out
}

func fromPersistedStopSaves(m map[string]persistedStopSave) map[string]flinkStopSave {
	out := make(map[string]flinkStopSave, len(m))
	for k, v := range m {
		out[k] = flinkStopSave{at: v.At, path: v.Path}
	}

	return out
}

// removeAllLogged removes a tree, logging failures such as container-owned files the host user cannot delete.
func removeAllLogged(target string) {
	if err := os.RemoveAll(target); err != nil {
		ctx := context.Background()
		logger.Load(ctx).WarnContext(ctx, "kinesisanalyticsv2: remove flink state failed", "path", target, "error", err)
	}
}
