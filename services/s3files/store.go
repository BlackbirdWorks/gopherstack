package s3files

import (
	"context"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

type regionContextKey struct{}

func getRegion(ctx context.Context, fallback string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return fallback
}

// BucketResolver checks that an S3 bucket exists in the emulated S3 service.
type BucketResolver interface {
	BucketExists(ctx context.Context, bucket string) bool
}

// SubnetInfo is the placement of an EC2 subnet.
type SubnetInfo struct {
	VPCID              string
	AvailabilityZoneID string
	CIDR               string
}

// SubnetResolver looks up EC2 subnets.
type SubnetResolver interface {
	Subnet(id string) (SubnetInfo, bool)
}

// InMemoryBackend is the in-memory S3 Files backend. One coarse lock guards all tables.
type InMemoryBackend struct {
	mu           *lockmetrics.RWMutex
	registry     *store.Registry
	fileSystems  *store.Table[FileSystem]
	mountTargets *store.Table[MountTarget]
	accessPoints *store.Table[AccessPoint]
	buckets      BucketResolver
	subnets      SubnetResolver
	network      NetworkResolver
	roles        RoleResolver
	keys         KeyResolver
	now          func() time.Time
	accountID    string
	region       string
	delay        time.Duration
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("s3files"),
		registry:  store.NewRegistry(),
		now:       time.Now,
		accountID: accountID,
		region:    region,
	}

	b.fileSystems = store.Register(b.registry, "fileSystems", store.New(func(v *FileSystem) string { return v.ID }))
	b.mountTargets = store.Register(b.registry, "mountTargets", store.New(func(v *MountTarget) string { return v.ID }))
	b.accessPoints = store.Register(b.registry, "accessPoints", store.New(func(v *AccessPoint) string { return v.ID }))

	return b
}

// Region returns the default region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.registry.ResetAll()
}

// SetBucketResolver wires S3 bucket validation; nil leaves buckets unvalidated.
func (b *InMemoryBackend) SetBucketResolver(r BucketResolver) {
	b.mu.Lock("SetBucketResolver")
	defer b.mu.Unlock()
	b.buckets = r
}

// SetSubnetResolver wires EC2 subnet validation; nil leaves subnets unvalidated.
func (b *InMemoryBackend) SetSubnetResolver(r SubnetResolver) {
	b.mu.Lock("SetSubnetResolver")
	defer b.mu.Unlock()
	b.subnets = r
}

// SetLifecycleDelay sets how long resources stay creating/deleting; zero is instant.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()
	b.delay = d
}

// SetClock replaces the time source (tests).
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()
	b.now = now
}

// state resolves the lifecycle state visible to callers; gone reports a finished deletion.
func (b *InMemoryBackend) state(l *Lifecycle) (string, bool) {
	now := b.now()

	if l.DeleteRequestedAt != nil {
		if !now.Before(l.DeleteRequestedAt.Add(b.delay)) {
			return statusDeleting, true
		}

		return statusDeleting, false
	}

	if l.Status == statusCreating && !now.Before(l.CreatedAt.Add(b.delay)) {
		return statusAvailable, false
	}

	return l.Status, false
}

func (b *InMemoryBackend) newLifecycle() Lifecycle {
	status := statusAvailable
	if b.delay > 0 {
		status = statusCreating
	}

	return Lifecycle{CreatedAt: b.now().UTC(), Status: status}
}

// beginDelete marks l deleting; it reports true when the resource can be removed immediately.
func (b *InMemoryBackend) beginDelete(l *Lifecycle) bool {
	if b.delay <= 0 {
		return true
	}

	if l.DeleteRequestedAt == nil {
		now := b.now().UTC()
		l.DeleteRequestedAt = &now
	}

	return false
}

// sweepLocked removes resources whose deletion window has elapsed. Callers hold the write lock.
func (b *InMemoryBackend) sweepLocked() {
	for _, v := range b.fileSystems.All() {
		if _, gone := b.state(&v.Lifecycle); gone {
			b.fileSystems.Delete(v.ID)
		}
	}

	for _, v := range b.mountTargets.All() {
		if _, gone := b.state(&v.Lifecycle); gone {
			b.removeMountTarget(v)
		}
	}

	for _, v := range b.accessPoints.All() {
		if _, gone := b.state(&v.Lifecycle); gone {
			b.accessPoints.Delete(v.ID)
		}
	}
}

// idOf accepts a bare ID or an ARN and returns the trailing ID.
func idOf(s string) string {
	if strings.HasPrefix(s, "arn:") {
		return s[strings.LastIndex(s, "/")+1:]
	}

	return s
}

func (b *InMemoryBackend) liveFileSystem(region, ref string) (*FileSystem, error) {
	fs, ok := b.fileSystems.Get(idOf(ref))
	if ok && fs.Region == region {
		if _, gone := b.state(&fs.Lifecycle); !gone {
			return fs, nil
		}
	}

	return nil, notFound("File system " + ref + " not found")
}

func (b *InMemoryBackend) liveMountTarget(region, id string) (*MountTarget, error) {
	mt, ok := b.mountTargets.Get(id)
	if ok && mt.Region == region {
		if _, gone := b.state(&mt.Lifecycle); !gone {
			return mt, nil
		}
	}

	return nil, notFound("Mount target " + id + " not found")
}

func (b *InMemoryBackend) liveAccessPoint(region, ref string) (*AccessPoint, error) {
	ap, ok := b.accessPoints.Get(idOf(ref))
	if ok && ap.Region == region {
		if _, gone := b.state(&ap.Lifecycle); !gone {
			return ap, nil
		}
	}

	return nil, notFound("Access point " + ref + " not found")
}

// fsView returns a detached copy of fs with its lifecycle state resolved.
func (b *InMemoryBackend) fsView(fs *FileSystem) *FileSystem {
	cp := *fs
	cp.Status, _ = b.state(&fs.Lifecycle)
	cp.Tags = maps.Clone(fs.Tags)

	return &cp
}

func (b *InMemoryBackend) mtView(mt *MountTarget) *MountTarget {
	cp := *mt
	cp.Status, _ = b.state(&mt.Lifecycle)
	cp.SecurityGroups = append([]string(nil), mt.SecurityGroups...)

	return &cp
}

func (b *InMemoryBackend) apView(ap *AccessPoint) *AccessPoint {
	cp := *ap
	cp.Status, _ = b.state(&ap.Lifecycle)
	cp.Tags = maps.Clone(ap.Tags)

	return &cp
}
