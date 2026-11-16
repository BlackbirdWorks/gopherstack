package ocifilestorage

import (
	"io"
	"io/fs"
	"os"
	"sync/atomic"
	"syscall"

	"github.com/go-git/go-billy/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

// quotaState is the per-file-system state of hard-quota enforcement: a lock that makes a check and the write it
// admits one step, and the usage measured by the last full walk, kept current by the writes admitted since.
type quotaState struct {
	mu       *lockmetrics.RWMutex
	usage    QuotaUsage
	gen      atomic.Uint64
	usageGen uint64
	walks    int
	cached   bool
}

func newQuotaState() *quotaState { return &quotaState{mu: lockmetrics.New("ocifilestorage.quota")} }

// invalidate marks the cached usage stale after a mutation the quota code did not account for.
func (q *quotaState) invalidate() {
	if q != nil {
		q.gen.Add(1)
	}
}

// current returns the usage, walking the tree only when something changed behind the cache. Caller holds q.mu.
func (q *quotaState) current(root *os.Root) QuotaUsage {
	g := q.gen.Load()
	if !q.cached || q.usageGen != g {
		q.usage, q.usageGen, q.cached = measureUsage(root), g, true
		q.walks++
	}

	return q.usage
}

// account applies the size change of one admitted write to the cached usage. Caller holds q.mu.
func (q *quotaState) account(info fs.FileInfo, delta int64) {
	if !q.cached || q.usageGen != q.gen.Load() {
		return
	}

	uid, gid := ownerOf(info)
	q.usage.Total += delta
	q.usage.Users[uid] += delta
	q.usage.Groups[gid] += delta
}

// invalidateUsage forgets the cached usage of a file system after its tree changed outside NFS.
func (p *NFSPlane) invalidateUsage(fsID string) {
	p.mu.Lock("invalidateUsage")
	q := p.quotaLocked(fsID)
	p.mu.Unlock()

	q.invalidate()
}

// admitGrowth refuses growth that would push a principal past a hard quota (EDQUOT, NFS3ERR_DQUOT).
func (v *vfs) admitGrowth(rules []QuotaLimit, rel string, info fs.FileInfo, grow int64) error {
	if grow <= 0 {
		return nil
	}

	uid, gid := ownerOf(info)
	usage := v.qs.current(v.root)

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

// rules reads the quota rules before any file-system lock is taken: the backend lock is held across snapshot
// copies, which wait for the gate, so a writer inside the gate must never wait for the backend lock.
func (f *guardedFile) rules() []QuotaLimit {
	if f.v.stat.Quotas == nil {
		return nil
	}

	return f.v.stat.Quotas()
}

// admitted runs op after checking that growing the file by grow(size, pos) keeps every hard quota. The check,
// the write and the usage update are one step per file system, so concurrent writers cannot all pass on stale
// usage.
func (f *guardedFile) admitted(grow func(size, pos int64) int64, op func() error) error {
	rules := f.rules()

	f.v.gate.RLock("write")
	defer f.v.gate.RUnlock()

	if len(rules) == 0 {
		defer f.v.qs.invalidate()

		return op()
	}

	f.v.qs.mu.Lock("write")
	defer f.v.qs.mu.Unlock()

	before, err := f.v.root.Stat(f.rel)
	if err != nil {
		return op()
	}

	pos, err := f.File.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	if err = f.v.admitGrowth(rules, f.rel, before, grow(before.Size(), pos)); err != nil {
		return err
	}

	err = op()

	if after, serr := f.v.root.Stat(f.rel); serr == nil {
		f.v.qs.account(after, after.Size()-before.Size())
	} else {
		f.v.qs.invalidate()
	}

	return err
}

func (f *guardedFile) Write(p []byte) (int, error) {
	var written int

	err := f.admitted(
		func(size, pos int64) int64 { return pos + int64(len(p)) - size },
		func() error {
			var werr error

			written, werr = f.File.Write(p)

			return werr
		})

	return written, err
}

func (f *guardedFile) Truncate(size int64) error {
	return f.admitted(
		func(cur, _ int64) int64 { return size - cur },
		func() error { return f.File.Truncate(size) })
}
