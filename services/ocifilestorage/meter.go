package ocifilestorage

import (
	"io/fs"
	"os"
	"path/filepath"
)

type fileIdentity struct {
	path  string
	size  int64
	mtime int64
}

type meterResult struct {
	exclusive map[string]int64
	total     int64
}

func (p *NFSPlane) walkIdentities(dir string, fn func(fileIdentity)) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return
	}

	defer root.Close()

	_ = fs.WalkDir(root.FS(), ".", func(rel string, d fs.DirEntry, werr error) error {
		if werr != nil || !d.Type().IsRegular() {
			return nil //nolint:nilerr // best-effort metering
		}

		if info, ierr := d.Info(); ierr == nil {
			fn(fileIdentity{path: rel, size: info.Size(), mtime: info.ModTime().UnixNano()})
		}

		return nil
	})
}

// meter counts each distinct file version once, however many of the live tree and snapshots hold it: a snapshot
// copy of unchanged data adds nothing (Oracle: snapshots are metered against differentiated data only). A file
// version is its path, size and modification time.
func (p *NFSPlane) meter(id string) meterResult {
	refs := map[fileIdentity]int{}
	held := map[string][]fileIdentity{}

	p.walkIdentities(p.fsDir(id), func(f fileIdentity) { refs[f]++ })

	if ents, err := os.ReadDir(p.snapRoot(id)); err == nil {
		for _, e := range ents {
			if !e.IsDir() {
				continue
			}

			name := e.Name()

			p.walkIdentities(filepath.Join(p.snapRoot(id), name), func(f fileIdentity) {
				refs[f]++

				held[name] = append(held[name], f)
			})
		}
	}

	res := meterResult{exclusive: map[string]int64{}}

	for f := range refs {
		res.total += f.size
	}

	for name, files := range held {
		for _, f := range files {
			if refs[f] == 1 {
				res.exclusive[name] += f.size
			}
		}
	}

	return res
}
