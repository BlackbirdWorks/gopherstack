package azurearm

import (
	"encoding/base64"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	aciScratchPerm = 0o777
	aciFilePerm    = 0o666
)

// ACIShareTree is the content of an Azure Files share: slash paths to file bytes, and directory paths.
type ACIShareTree struct {
	Files map[string][]byte
	Dirs  []string
}

// ACIFileShares is the seam azureFile volumes use to reach the Azure Files service. ExportShare must fail when
// the share does not exist.
type ACIFileShares interface {
	ExportShare(account, share string) (ACIShareTree, error)
	ApplyShare(account, share string, tree ACIShareTree) error
}

func mkACIScratch() (string, error) {
	dir, err := os.MkdirTemp("", "gopherstack-aci-")
	if err != nil {
		return "", fmt.Errorf("create scratch dir: %w", err)
	}

	if err = os.Chmod(dir, aciScratchPerm); err != nil {
		_ = os.RemoveAll(dir)

		return "", fmt.Errorf("chmod scratch dir: %w", err)
	}

	return dir, nil
}

// prepare creates volumes and publishes ports; it returns a failure reason or "".
func (e *ACIEngine) prepare(run *aciRun, cfg aciConfig) string {
	e.mu.RLock("prepare")
	shares, pool := e.shares, e.ports
	e.mu.RUnlock()

	for _, v := range cfg.Props.Volumes {
		dir, err := mkACIScratch()
		if err != nil {
			return err.Error()
		}

		e.mu.Lock("prepare")
		run.scratch[strings.ToLower(v.Name)] = dir
		run.allDirs = append(run.allDirs, dir)
		e.mu.Unlock()

		if reason := e.fillVolume(run, v, dir, shares); reason != "" {
			return reason
		}
	}

	if cfg.Props.IPAddress == nil {
		return ""
	}

	for _, pt := range cfg.Props.IPAddress.Ports {
		hp, err := gcpserve.AcquirePort(run.ctx, pool, "azurearm-aci")
		if err != nil {
			return fmt.Sprintf("allocate host port for %d: %v", pt.Port, err)
		}

		e.mu.Lock("prepare")
		run.hostPorts[pt.Port] = hp
		e.record(&run.events, aciEventNormal, "PortPublished",
			fmt.Sprintf("port %d/TCP is published on 127.0.0.1:%d", pt.Port, hp))
		e.mu.Unlock()
	}

	return ""
}

func (e *ACIEngine) fillVolume(run *aciRun, v aciVolumeWire, dir string, shares ACIFileShares) string {
	switch {
	case v.Secret != nil:
		for name, val := range v.Secret {
			data, _ := base64.StdEncoding.DecodeString(val)
			if err := os.WriteFile(filepath.Join(dir, name), data, aciFilePerm); err != nil {
				return fmt.Sprintf("write secret volume %q: %v", v.Name, err)
			}
		}
	case v.AzureFile != nil:
		return e.fillAzureFile(run, v, dir, shares)
	}

	return ""
}

func (e *ACIEngine) fillAzureFile(run *aciRun, v aciVolumeWire, dir string, shares ACIFileShares) string {
	af := v.AzureFile
	if shares == nil {
		return fmt.Sprintf("azureFile volume %q needs the Azure Files service, which is not available", v.Name)
	}

	tree, err := shares.ExportShare(af.StorageAccountName, af.ShareName)
	if err != nil {
		return fmt.Sprintf("azureFile volume %q: share %s/%s: %v", v.Name, af.StorageAccountName, af.ShareName, err)
	}

	for _, d := range tree.Dirs {
		if err = os.MkdirAll(filepath.Join(dir, filepath.FromSlash(d)), aciScratchPerm); err != nil {
			return fmt.Sprintf("materialize share %q: %v", af.ShareName, err)
		}

		_ = os.Chmod(filepath.Join(dir, filepath.FromSlash(d)), aciScratchPerm)
	}

	for p, data := range tree.Files {
		target := filepath.Join(dir, filepath.FromSlash(p))
		if err = os.WriteFile(target, data, aciFilePerm); err != nil {
			return fmt.Sprintf("materialize share %q: %v", af.ShareName, err)
		}
	}

	e.mu.Lock("fillAzureFile")
	run.shares = append(run.shares, shareMount{
		dir: dir, account: af.StorageAccountName, share: af.ShareName, readOnly: af.ReadOnly,
	})
	e.mu.Unlock()

	return ""
}

// syncShares copies each writable azureFile volume's directory back into its share.
func (e *ACIEngine) syncShares(run *aciRun) {
	e.mu.RLock("syncShares")
	shares, mounts := e.shares, append([]shareMount(nil), run.shares...)
	e.mu.RUnlock()

	if shares == nil {
		return
	}

	for _, m := range mounts {
		if m.readOnly {
			continue
		}

		tree, err := readACITree(m.dir)
		if err == nil {
			err = shares.ApplyShare(m.account, m.share, tree)
		}

		if err != nil {
			logger.Load(run.ctx).WarnContext(run.ctx, "azurearm: syncing azure file volume failed",
				"account", m.account, "share", m.share, "error", err)
		}
	}
}

func readACITree(root string) (ACIShareTree, error) {
	tree := ACIShareTree{Files: map[string][]byte{}}

	rt, err := os.OpenRoot(root)
	if err != nil {
		return tree, fmt.Errorf("open volume dir: %w", err)
	}
	defer rt.Close()

	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == root {
			return err
		}

		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}

		rel = filepath.ToSlash(rel)

		switch {
		case d.IsDir():
			tree.Dirs = append(tree.Dirs, rel)
		case d.Type().IsRegular():
			data, readErr := rt.ReadFile(rel)
			if readErr != nil {
				return readErr
			}

			tree.Files[rel] = data
		}

		return nil
	})
	sort.Strings(tree.Dirs)

	return tree, err
}

// releaseResources removes volume directories and returns host ports; safe to call twice.
func (e *ACIEngine) releaseResources(run *aciRun) {
	e.mu.Lock("releaseResources")
	dirs, ports, pool := run.allDirs, run.hostPorts, e.ports
	run.allDirs, run.hostPorts, run.shares = nil, map[int]int{}, nil
	e.mu.Unlock()

	for _, d := range dirs {
		_ = os.RemoveAll(d)
	}

	for _, hp := range ports {
		gcpserve.ReleasePort(pool, hp)
	}
}
