package azurearm

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
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

	if reason := e.prepareDNS(run, cfg); reason != "" {
		return reason
	}

	return e.preparePorts(run, cfg, pool)
}

func (e *ACIEngine) prepareDNS(run *aciRun, cfg aciConfig) string {
	d := cfg.Props.DNSConfig
	if d == nil {
		return ""
	}

	dir, err := mkACIScratch()
	if err != nil {
		return err.Error()
	}

	path := filepath.Join(dir, "resolv.conf")

	e.mu.Lock("prepareDNS")
	run.allDirs = append(run.allDirs, dir)
	run.resolv = path
	e.mu.Unlock()

	if err = os.WriteFile(path, []byte(resolvConf(d)), aciFilePerm); err != nil {
		return fmt.Sprintf("write resolv.conf: %v", err)
	}

	return ""
}

func resolvConf(d *aciDNSWire) string {
	var b strings.Builder

	for _, ns := range d.NameServers {
		b.WriteString("nameserver " + ns + "\n")
	}

	if d.SearchDomains != "" {
		b.WriteString("search " + d.SearchDomains + "\n")
	}

	if d.Options != "" {
		b.WriteString("options " + d.Options + "\n")
	}

	return b.String()
}

func hasReadinessProbe(cfg aciConfig) bool {
	for _, c := range cfg.Props.Containers {
		if c.Properties.ReadinessProbe != nil {
			return true
		}
	}

	return false
}

func (e *ACIEngine) acquire(run *aciRun, pool ACIPortPool) (int, error) {
	hp, err := gcpserve.AcquirePort(run.ctx, pool, "azurearm-aci")
	if err != nil {
		return 0, err //nolint:wrapcheck // callers add the port context
	}

	e.mu.Lock("acquire")
	run.owned = append(run.owned, hp)
	e.mu.Unlock()

	return hp, nil
}

// preparePorts publishes the group address ports, fronts them with a readiness gate when a container has a
// readiness probe, and publishes the HTTP probe ports; it returns a failure reason or "".
func (e *ACIEngine) preparePorts(run *aciRun, cfg aciConfig, pool ACIPortPool) string {
	gate := cfg.Props.IPAddress != nil && hasReadinessProbe(cfg)

	if ip := cfg.Props.IPAddress; ip != nil {
		for _, pt := range ip.Ports {
			if pt.Protocol == aciProtoUDP {
				if reason := e.publishUDPPort(run, pool, pt.Port); reason != "" {
					return reason
				}

				continue
			}

			if reason := e.publishPort(run, pool, pt.Port, gate); reason != "" {
				return reason
			}
		}
	}

	return e.prepareProbePorts(run, cfg, pool)
}

func (e *ACIEngine) publishPort(run *aciRun, pool ACIPortPool, port int, gate bool) string {
	front, err := port, error(nil)
	if run.addr == "" {
		front, err = e.acquire(run, pool)
	}

	if err != nil {
		return fmt.Sprintf("allocate host port for %d: %v", port, err)
	}

	back, backHost := front, run.hostFor(0)

	if gate {
		backHost = container.LoopbackHost
		if back, err = e.acquire(run, pool); err == nil {
			err = e.startGate(run, run.hostFor(0), front, back)
		}

		if err != nil {
			return fmt.Sprintf("readiness gate for port %d: %v", port, err)
		}
	}

	e.mu.Lock("publishPort")
	run.hostPorts[port], run.backPorts[port], run.backHost[port] = front, back, backHost
	e.record(&run.events, aciEventNormal, "PortPublished",
		fmt.Sprintf("port %d/TCP is published on %s:%d", port, run.hostFor(0), front))
	e.mu.Unlock()

	return ""
}

func (e *ACIEngine) publishUDPPort(run *aciRun, pool ACIPortPool, port int) string {
	hp, err := port, error(nil)
	if run.addr == "" {
		hp, err = e.acquire(run, pool)
	}

	if err != nil {
		return fmt.Sprintf("allocate host port for %d/UDP: %v", port, err)
	}

	e.mu.Lock("publishUDPPort")
	run.udpPorts[port] = hp
	e.record(&run.events, aciEventNormal, "PortPublished",
		fmt.Sprintf("port %d/UDP is published on %s:%d", port, run.hostFor(0), hp))
	e.mu.Unlock()

	return ""
}

func (e *ACIEngine) prepareProbePorts(run *aciRun, cfg aciConfig, pool ACIPortPool) string {
	for _, c := range cfg.Props.Containers {
		for _, port := range publishedPorts(c.Properties) {
			e.mu.RLock("prepareProbePorts")
			_, have := run.backPorts[port]
			e.mu.RUnlock()

			if have {
				continue
			}

			hp, err := e.acquire(run, pool)
			if err != nil {
				return fmt.Sprintf("allocate host port for probe port %d: %v", port, err)
			}

			e.mu.Lock("prepareProbePorts")
			run.backPorts[port], run.backHost[port] = hp, container.LoopbackHost
			e.mu.Unlock()
		}
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
	case v.GitRepo != nil:
		return e.fillGitRepo(run, v, dir)
	}

	return ""
}

func (e *ACIEngine) fillAzureFile(run *aciRun, v aciVolumeWire, dir string, shares ACIFileShares) string {
	af := v.AzureFile
	if shares == nil {
		return fmt.Sprintf("azureFile volume %q needs the Azure Files service, which is not available", v.Name)
	}

	if subtle.ConstantTimeCompare([]byte(af.StorageAccountKey), []byte(azureauth.DefaultAccountKey)) != 1 {
		return fmt.Sprintf(
			"azureFile volume %q: the key of storage account %s is not valid",
			v.Name,
			af.StorageAccountName,
		)
	}

	tree, err := shares.ExportShare(af.StorageAccountName, af.ShareName)
	if err != nil {
		return fmt.Sprintf("azureFile volume %q: share %s/%s: %v", v.Name, af.StorageAccountName, af.ShareName, err)
	}

	if err = writeACIDir(dir, tree, ACIShareTree{}); err != nil {
		return fmt.Sprintf("materialize share %q: %v", af.ShareName, err)
	}

	e.mu.Lock("fillAzureFile")
	run.shares = append(run.shares, shareMount{
		dir: dir, account: af.StorageAccountName, share: af.ShareName, readOnly: af.ReadOnly, base: newShareBase(tree),
	})
	e.mu.Unlock()

	return ""
}

// syncShares reconciles each azureFile volume with its share.
func (e *ACIEngine) syncShares(run *aciRun) {
	e.mu.RLock("syncShares")
	shares, mounts := e.shares, make([]*shareMount, len(run.shares))

	for i := range run.shares {
		mounts[i] = &run.shares[i]
	}
	e.mu.RUnlock()

	if shares == nil {
		return
	}

	run.syncMu.Lock("syncShares")
	defer run.syncMu.Unlock()

	for _, m := range mounts {
		if err := syncMount(shares, m); err != nil {
			logACISyncFailure(run, *m, err)
		}
	}
}

var errACITreeTooLarge = errors.New("the volume holds more files or bytes than the emulator syncs")

// readACITree reads the regular files and directories under root without following symlinks; a tree beyond
// maxFiles files or maxBytes bytes is refused before the oversized file is read.
func readACITree(root string, maxFiles int, maxBytes int64) (ACIShareTree, error) {
	tree := ACIShareTree{Files: map[string][]byte{}}

	var total int64

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
			info, infoErr := d.Info()
			if infoErr != nil {
				return infoErr
			}

			total += info.Size()
			if len(tree.Files) >= maxFiles || total > maxBytes {
				return errACITreeTooLarge
			}

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
	dirs, ports, pool := run.allDirs, run.owned, e.ports
	run.allDirs, run.hostPorts, run.backPorts, run.owned, run.shares = nil, map[int]int{}, map[int]int{}, nil, nil
	run.udpPorts, run.backHost = map[int]int{}, map[int]string{}
	delete(e.addrs, run.addr)
	run.addr = ""
	e.mu.Unlock()

	for _, d := range dirs {
		_ = os.RemoveAll(d)
	}

	for _, hp := range ports {
		gcpserve.ReleasePort(pool, hp)
	}
}
