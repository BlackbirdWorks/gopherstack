package azurearm

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const aciShareSyncEvery = 2 * time.Second

type aciSum = [sha256.Size]byte

// shareBase is the content both sides agreed on at the last pass; a side that differs from it has changed.
type shareBase struct {
	files map[string]aciSum
	dirs  map[string]bool
}

func sumsOf(files map[string][]byte) map[string]aciSum {
	out := make(map[string]aciSum, len(files))
	for p, b := range files {
		out[p] = sha256.Sum256(b)
	}

	return out
}

func newShareBase(tree ACIShareTree) *shareBase {
	b := &shareBase{files: sumsOf(tree.Files), dirs: map[string]bool{}}
	for _, d := range tree.Dirs {
		b.dirs[d] = true
	}

	return b
}

// shareSyncLoop keeps writable and read-only azureFile volumes in step with their shares while the group runs.
func (e *ACIEngine) shareSyncLoop(run *aciRun) {
	for e.pauseFor(run.ctx, aciShareSyncEvery) {
		e.syncShares(run)
	}
}

// mergeACIShare reconciles one mount: a change on one side since the base is copied to the other, and the container
// side wins when both changed. A read-only mount only receives share changes.
func mergeACIShare(base *shareBase, share, dir ACIShareTree, readOnly bool) (ACIShareTree, bool, bool) {
	files, toDir, toShare := mergeACIFiles(base, share, dir, readOnly)
	out := ACIShareTree{Files: files}

	dirs, dToDir, dToShare := mergeACIDirs(base, share.Dirs, dir.Dirs, readOnly)
	out.Dirs = dirs

	for p := range out.Files {
		for parent := path.Dir(p); parent != "."; parent = path.Dir(parent) {
			if !slices.Contains(out.Dirs, parent) {
				out.Dirs = append(out.Dirs, parent)
				dToShare, dToDir = true, true
			}
		}
	}

	slices.Sort(out.Dirs)

	return out, toDir || dToDir, (toShare || dToShare) && !readOnly
}

func mergeACIFiles(base *shareBase, share, dir ACIShareTree, readOnly bool) (map[string][]byte, bool, bool) {
	ss, ds := sumsOf(share.Files), sumsOf(dir.Files)
	out := map[string][]byte{}

	var toDir, toShare bool

	for p := range unionKeys(ss, ds, base.files) {
		_, inShare := ss[p]
		d, inDir := ds[p]
		b, inBase := base.files[p]
		dirChg := inDir != inBase || (inDir && d != b)

		present, data := inShare, share.Files[p]
		if dirChg && !readOnly {
			present, data = inDir, dir.Files[p]
		}

		if present {
			out[p] = data
		}

		toShare = toShare || present != inShare || (present && !bytes.Equal(data, share.Files[p]))
		toDir = toDir || present != inDir || (present && !bytes.Equal(data, dir.Files[p]))
	}

	return out, toDir, toShare && !readOnly
}

func mergeACIDirs(base *shareBase, share, dir []string, readOnly bool) ([]string, bool, bool) {
	inShare, inDir := map[string]bool{}, map[string]bool{}

	for _, d := range share {
		inShare[d] = true
	}

	for _, d := range dir {
		inDir[d] = true
	}

	all := map[string]bool{}
	for _, m := range []map[string]bool{inShare, inDir, base.dirs} {
		maps.Copy(all, m)
	}

	var out []string

	var toDir, toShare bool

	for p := range all {
		dirChg := inDir[p] != base.dirs[p]

		present := inShare[p]
		if dirChg && !readOnly {
			present = inDir[p]
		}

		if present {
			out = append(out, p)
		}

		toShare = toShare || present != inShare[p]
		toDir = toDir || present != inDir[p]
	}

	return out, toDir, toShare
}

// writeACIDir makes dir match tree (files written, absent files and directories removed) through an os.Root.
func writeACIDir(dir string, tree ACIShareTree, current ACIShareTree) error {
	rt, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("open volume dir: %w", err)
	}
	defer rt.Close()

	for _, d := range tree.Dirs {
		if err = rt.MkdirAll(filepath.FromSlash(d), aciScratchPerm); err != nil {
			return fmt.Errorf("create %q: %w", d, err)
		}

		_ = rt.Chmod(filepath.FromSlash(d), aciScratchPerm)
	}

	for p, data := range tree.Files {
		if old, ok := current.Files[p]; ok && bytes.Equal(old, data) {
			continue
		}

		if err = rt.WriteFile(filepath.FromSlash(p), data, aciFilePerm); err != nil {
			return fmt.Errorf("write %q: %w", p, err)
		}
	}

	for p := range current.Files {
		if _, keep := tree.Files[p]; !keep {
			_ = rt.Remove(filepath.FromSlash(p))
		}
	}

	gone := make([]string, 0)

	for _, d := range current.Dirs {
		if !slices.Contains(tree.Dirs, d) {
			gone = append(gone, d)
		}
	}

	slices.SortFunc(gone, func(a, b string) int { return strings.Count(b, "/") - strings.Count(a, "/") })

	for _, d := range gone {
		_ = rt.RemoveAll(filepath.FromSlash(d))
	}

	return nil
}

// syncMount runs one reconciliation pass for a mount and advances its base.
func syncMount(shares ACIFileShares, m *shareMount) error {
	share, err := shares.ExportShare(m.account, m.share)
	if err != nil {
		return fmt.Errorf("export share: %w", err)
	}

	dir, err := readACITree(m.dir)
	if err != nil {
		return err
	}

	merged, toDir, toShare := mergeACIShare(m.base, share, dir, m.readOnly)

	if toDir {
		if err = writeACIDir(m.dir, merged, dir); err != nil {
			return err
		}
	}

	if toShare {
		if err = shares.ApplyShare(m.account, m.share, merged); err != nil {
			return fmt.Errorf("apply share: %w", err)
		}
	}

	m.base = newShareBase(merged)

	return nil
}

func logACISyncFailure(run *aciRun, m shareMount, err error) {
	logger.Load(run.ctx).WarnContext(run.ctx, "azurearm: syncing azure file volume failed",
		"account", m.account, "share", m.share, "error", err)
}

func unionKeys(maps ...map[string]aciSum) map[string]bool {
	out := map[string]bool{}

	for _, m := range maps {
		for p := range m {
			out[p] = true
		}
	}

	return out
}
