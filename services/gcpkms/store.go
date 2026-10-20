package gcpkms

import (
	"context"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultPageSize  = 25
	maxPageSize      = 1000
	maxLabels        = 64
	maxLabelLen      = 63
	minRotationEvery = 24 * time.Hour
	maxRotationEvery = 876000 * time.Hour
	minDestroyTTL    = 24 * time.Hour
	maxDestroyTTL    = 120 * 24 * time.Hour
	defaultDestroy   = 30 * 24 * time.Hour
	importJobTTL     = 3 * 24 * time.Hour
)

type versionRec struct {
	pb       *kmspb.CryptoKeyVersion
	material []byte
	id       int64
	deleted  bool
}

type keyRec struct {
	pb       *kmspb.CryptoKey
	versions []*versionRec
	primary  int64
}

type jobRec struct {
	pb      *kmspb.ImportJob
	private []byte
}

// InMemoryBackend holds all key rings, keys, versions, import jobs and IAM policies under one lock.
type InMemoryBackend struct {
	now      func() time.Time
	mu       *lockmetrics.RWMutex
	rings    map[string]*kmspb.KeyRing
	keys     map[string]*keyRec
	jobs     map[string]*jobRec
	retired  map[string]*kmspb.RetiredResource
	policies map[string]*iampb.Policy
	ops      *gcplro.Store
	etagSeq  uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("gcpkms"), now: time.Now, ops: gcplro.New("gcpkms")}
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.rings = make(map[string]*kmspb.KeyRing)
	b.keys = make(map[string]*keyRec)
	b.jobs = make(map[string]*jobRec)
	b.retired = make(map[string]*kmspb.RetiredResource)
	b.policies = make(map[string]*iampb.Policy)
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetMaps()
	b.ops.Reset()
	b.etagSeq = 0
}

// RunJanitor sweeps due rotations, destructions and import-job expiry until ctx ends.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.Sweep()
		}
	}
}

// Sweep applies due automatic rotations, scheduled destructions and import-job expiry.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	b.sweepLocked()
}

func (b *InMemoryBackend) sweepLocked() {
	now := b.now()

	for _, j := range b.jobs {
		if j.pb.GetState() == kmspb.ImportJob_ACTIVE && !j.pb.GetExpireTime().AsTime().After(now) {
			j.pb.State = kmspb.ImportJob_EXPIRED
		}
	}

	for _, k := range b.keys {
		for _, v := range k.versions {
			if v.pb.GetState() == kmspb.CryptoKeyVersion_DESTROY_SCHEDULED &&
				!v.pb.GetDestroyTime().AsTime().After(now) {
				finalizeDestroy(v, now)
			}
		}

		b.rotateLocked(k, now)
	}
}

func finalizeDestroy(v *versionRec, now time.Time) {
	v.pb.State = kmspb.CryptoKeyVersion_DESTROYED
	v.pb.DestroyEventTime = tsOf(now)
	v.material = nil
}

func (b *InMemoryBackend) rotateLocked(k *keyRec, now time.Time) {
	next := k.pb.GetNextRotationTime()
	if next == nil || next.AsTime().After(now) || k.pb.GetImportOnly() {
		return
	}

	v, err := b.newVersionLocked(k, now)
	if err != nil {
		return
	}

	if k.pb.GetPurpose() == kmspb.CryptoKey_ENCRYPT_DECRYPT {
		k.primary = v.id
	}

	period := k.pb.GetRotationPeriod()
	if period == nil || period.AsDuration() <= 0 {
		k.pb.NextRotationTime = nil

		return
	}

	t := next.AsTime()
	for !t.After(now) {
		t = t.Add(period.AsDuration())
	}

	k.pb.NextRotationTime = tsOf(t)
}

// templateOf returns the algorithm spec and protection level new versions of k are created with.
func templateOf(k *keyRec) (algSpec, kmspb.ProtectionLevel, alg) {
	t := k.pb.GetVersionTemplate()
	a := t.GetAlgorithm()
	s, _ := specOf(a)

	return s, t.GetProtectionLevel(), a
}

// newVersionLocked generates a fresh ENABLED version from the key's template.
func (b *InMemoryBackend) newVersionLocked(k *keyRec, now time.Time) (*versionRec, error) {
	s, level, a := templateOf(k)

	material, err := generateMaterial(s)
	if err != nil {
		return nil, err
	}

	return b.appendVersionLocked(k, a, level, material, now), nil
}

func (b *InMemoryBackend) appendVersionLocked(
	k *keyRec, a alg, level kmspb.ProtectionLevel, material []byte, now time.Time,
) *versionRec {
	id := int64(len(k.versions) + 1)
	v := &versionRec{
		id:       id,
		material: material,
		pb: &kmspb.CryptoKeyVersion{
			Name:            versionName(k.pb.GetName(), id),
			State:           kmspb.CryptoKeyVersion_ENABLED,
			ProtectionLevel: level,
			Algorithm:       a,
			CreateTime:      tsOf(now),
			GenerateTime:    tsOf(now),
		},
	}
	k.versions = append(k.versions, v)

	return v
}

func (k *keyRec) version(id int64) *versionRec {
	if id < 1 || id > int64(len(k.versions)) || k.versions[id-1].deleted {
		return nil
	}

	return k.versions[id-1]
}

func (k *keyRec) view() *kmspb.CryptoKey {
	out, _ := proto.Clone(k.pb).(*kmspb.CryptoKey)
	if v := k.version(k.primary); v != nil {
		out.Primary = cloneVersion(v.pb)
	}

	return out
}

func cloneVersion(v *kmspb.CryptoKeyVersion) *kmspb.CryptoKeyVersion {
	out, _ := proto.Clone(v).(*kmspb.CryptoKeyVersion)

	return out
}

func pageSizeOf(n int32) (int, error) {
	if n < 0 {
		return 0, errInvalid("page_size must not be negative.")
	}

	return min(int(n), maxPageSize), nil
}

// childrenOf returns the names of m's entries directly under parent, sorted.
func childrenOf[V any](m map[string]V, parent, collection string) []string {
	prefix := parent + "/" + collection + "/"

	var out []string

	for name := range m {
		if strings.HasPrefix(name, prefix) && !strings.Contains(name[len(prefix):], "/") {
			out = append(out, name)
		}
	}

	slices.Sort(out)

	return out
}
