package ocifilestorage

import (
	"io"
	"io/fs"
	"os"
	"syscall"

	"github.com/go-git/go-billy/v5"
)

const bytesPerGB = int64(1) << 30

// QuotaLimit is one quota rule of an enabled file system, with its limit in bytes.
type QuotaLimit struct {
	PrincipalID *int
	Type        string
	LimitBytes  int64
	Hard        bool
}

// QuotaUsage is the logical size of a file system tree, in total and by owner uid and gid.
type QuotaUsage struct {
	Users  map[int]int64
	Groups map[int]int64
	Total  int64
}

// measureUsage sums regular file sizes below root, attributed to each file's owner (NFS carries no
// credentials into the file system, so ownership on disk is the only principal known).
func measureUsage(root *os.Root) QuotaUsage {
	u := QuotaUsage{Users: map[int]int64{}, Groups: map[int]int64{}}

	_ = fs.WalkDir(root.FS(), ".", func(_ string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return nil //nolint:nilerr // best-effort metering
		}

		info, ierr := d.Info()
		if ierr != nil {
			return nil //nolint:nilerr // best-effort metering
		}

		uid, gid := ownerOf(info)
		u.Total += info.Size()
		u.Users[uid] += info.Size()
		u.Groups[gid] += info.Size()

		return nil
	})

	return u
}

// applicable returns the rules that bound a principal pair: the file-system rule, the individual or else the
// default user rule, and likewise for the group.
func applicable(rules []QuotaLimit, uid, gid int) []QuotaLimit {
	var out []QuotaLimit

	pick := func(individualType, defaultType string, id int) {
		var def *QuotaLimit

		for i := range rules {
			r := &rules[i]

			switch {
			case r.Type == individualType && r.PrincipalID != nil && *r.PrincipalID == id:
				out = append(out, *r)

				return
			case r.Type == defaultType:
				def = r
			}
		}

		if def != nil {
			out = append(out, *def)
		}
	}

	for _, r := range rules {
		if r.Type == qFileSystemLevel {
			out = append(out, r)
		}
	}

	pick(qIndividualUser, qDefaultUser, uid)
	pick(qIndividualGroup, qDefaultGroup, gid)

	return out
}

func (u QuotaUsage) of(r QuotaLimit, uid, gid int) int64 {
	switch r.Type {
	case qIndividualUser, qDefaultUser:
		return u.Users[uid]
	case qIndividualGroup, qDefaultGroup:
		return u.Groups[gid]
	}

	return u.Total
}

// admitGrowth refuses growth that would push a principal past a hard quota (EDQUOT, NFS3ERR_DQUOT).
func (v *vfs) admitGrowth(rel string, grow int64) error {
	if grow <= 0 || v.stat.Quotas == nil {
		return nil
	}

	rules := v.stat.Quotas()
	if len(rules) == 0 {
		return nil
	}

	info, err := v.root.Stat(rel)
	if err != nil {
		return nil //nolint:nilerr // the write itself reports a missing file
	}

	uid, gid := ownerOf(info)
	usage := measureUsage(v.root)

	for _, r := range applicable(rules, uid, gid) {
		if r.Hard && usage.of(r, uid, gid)+grow > r.LimitBytes {
			return &os.PathError{Op: "write", Path: rel, Err: syscall.EDQUOT}
		}
	}

	return nil
}

// guardedFile serialises the writes of one open file against snapshots and enforces hard quotas on them.
type guardedFile struct {
	billy.File
	v   *vfs
	rel string
}

func (f *guardedFile) size() int64 {
	info, err := f.v.root.Stat(f.rel)
	if err != nil {
		return 0
	}

	return info.Size()
}

func (f *guardedFile) Write(p []byte) (int, error) {
	f.v.gate.RLock("write")
	defer f.v.gate.RUnlock()

	pos, err := f.File.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	if err = f.v.admitGrowth(f.rel, pos+int64(len(p))-f.size()); err != nil {
		return 0, err
	}

	return f.File.Write(p)
}

func (f *guardedFile) Truncate(size int64) error {
	f.v.gate.RLock("truncate")
	defer f.v.gate.RUnlock()

	if err := f.v.admitGrowth(f.rel, size-f.size()); err != nil {
		return err
	}

	return f.File.Truncate(size)
}
