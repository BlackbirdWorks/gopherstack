package gcpcompute

import (
	"context"
	"time"

	computeapi "google.golang.org/api/compute/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Timing controls how long operations and instance state transitions take on the backend clock.
type Timing struct {
	Pending   time.Duration
	Done      time.Duration
	Provision time.Duration
	Stage     time.Duration
	Stop      time.Duration
}

// DefaultTiming returns the transition delays used unless a test overrides them.
func DefaultTiming() Timing {
	return Timing{
		Pending:   50 * time.Millisecond,
		Done:      400 * time.Millisecond,
		Provision: 100 * time.Millisecond,
		Stage:     300 * time.Millisecond,
		Stop:      300 * time.Millisecond,
	}
}

type instRec struct {
	Inst  *computeapi.Instance `json:"inst"`
	Phase string               `json:"phase"`
	Since time.Time            `json:"since"`
}

type routerRec struct {
	Router  *computeapi.Router  `json:"router"`
	AutoIPs map[string][]string `json:"autoIps"`
}

type opRec struct {
	Op      *computeapi.Operation `json:"op"`
	Project string                `json:"project"`
	Created time.Time             `json:"created"`
	Pending time.Duration         `json:"pending"`
	Done    time.Duration         `json:"done"`
}

// InMemoryBackend holds all Compute Engine state under one lock.
type InMemoryBackend struct {
	now       func() time.Time
	sleep     func(context.Context, time.Duration)
	mu        *lockmetrics.RWMutex
	networks  map[string]*computeapi.Network
	subnets   map[string]*computeapi.Subnetwork
	firewalls map[string]*computeapi.Firewall
	routes    map[string]*computeapi.Route
	addresses map[string]*computeapi.Address
	routers   map[string]*routerRec
	disks     map[string]*computeapi.Disk
	images    map[string]*computeapi.Image
	snapshots map[string]*computeapi.Snapshot
	instances map[string]*instRec
	ops       map[string]*opRec
	projects  map[string]*computeapi.Metadata
	timing    Timing
}

// NewInMemoryBackend creates an empty backend on the wall clock.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu: lockmetrics.New("gcpcompute"), now: time.Now, timing: DefaultTiming(), sleep: realSleep,
	}
	b.resetMaps()

	return b
}

func realSleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// SetClock replaces the clock and the sleep used by Operations.Wait; tests inject a fake clock here.
func (b *InMemoryBackend) SetClock(now func() time.Time, sleep func(context.Context, time.Duration)) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now, b.sleep = now, sleep
}

// SetTiming replaces the operation and instance transition delays.
func (b *InMemoryBackend) SetTiming(t Timing) {
	b.mu.Lock("SetTiming")
	defer b.mu.Unlock()

	b.timing = t
}

func (b *InMemoryBackend) resetMaps() {
	b.networks = map[string]*computeapi.Network{}
	b.subnets = map[string]*computeapi.Subnetwork{}
	b.firewalls = map[string]*computeapi.Firewall{}
	b.routes = map[string]*computeapi.Route{}
	b.addresses = map[string]*computeapi.Address{}
	b.routers = map[string]*routerRec{}
	b.disks = map[string]*computeapi.Disk{}
	b.images = map[string]*computeapi.Image{}
	b.snapshots = map[string]*computeapi.Snapshot{}
	b.instances = map[string]*instRec{}
	b.ops = map[string]*opRec{}
	b.projects = map[string]*computeapi.Metadata{}
}

// Reset removes all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetMaps()
}
