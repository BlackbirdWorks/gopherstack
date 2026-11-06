package gcpcompute

import (
	"context"
	"time"

	computeapi "google.golang.org/api/compute/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defPending   = 50 * time.Millisecond
	defDone      = 400 * time.Millisecond
	defProvision = 100 * time.Millisecond
	defStage     = 300 * time.Millisecond
	defStop      = 300 * time.Millisecond
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
		Pending:   defPending,
		Done:      defDone,
		Provision: defProvision,
		Stage:     defStage,
		Stop:      defStop,
	}
}

type instRec struct {
	Since time.Time            `json:"since"`
	Inst  *computeapi.Instance `json:"inst"`
	Phase string               `json:"phase"`
}

type routerRec struct {
	Router  *computeapi.Router  `json:"router"`
	AutoIPs map[string][]string `json:"autoIps"`
}

type memberRec struct {
	DeleteAt     time.Time         `json:"deleteAt"`
	LastProbe    time.Time         `json:"lastProbe,omitzero"`
	Zone         string            `json:"zone"`
	LastError    string            `json:"lastError,omitempty"`
	Name         string            `json:"name"`
	Health       string            `json:"health,omitempty"`
	Template     string            `json:"template"`
	InstanceID   uint64            `json:"instanceId,omitempty"`
	HealthyRun   int               `json:"healthyRun,omitempty"`
	UnhealthyRun int               `json:"unhealthyRun,omitempty"`
	Verified     bool              `json:"verified,omitempty"`
	Deleting     bool              `json:"deleting"`
	Recreating   bool              `json:"recreating"`
	RunUntil     time.Time         `json:"runUntil,omitzero"`
	Startup      string            `json:"startup,omitempty"`
	Parked       string            `json:"parked,omitempty"`
	Request      string            `json:"request,omitempty"`
	KeptInt      map[string]string `json:"keptInternal,omitempty"`
	KeptExt      map[string]string `json:"keptExternal,omitempty"`
}

type migRec struct {
	Project string                           `json:"project"`
	Mgr     *computeapi.InstanceGroupManager `json:"mgr"`
	Members []*memberRec                     `json:"members"`
	Configs []*picRec                        `json:"configs,omitempty"`
	Resizes []*resizeRec                     `json:"resizes,omitempty"`
}

type groupRec struct {
	Group   *computeapi.InstanceGroup `json:"group"`
	Members []string                  `json:"members"`
}

type opRec struct {
	Created time.Time             `json:"created"`
	Op      *computeapi.Operation `json:"op"`
	Project string                `json:"project"`
	Pending time.Duration         `json:"pending"`
	Done    time.Duration         `json:"done"`
}

// InMemoryBackend holds all Compute Engine state under one lock.
type InMemoryBackend struct {
	now             func() time.Time
	sleep           func(context.Context, time.Duration)
	prober          HealthProber
	mu              *lockmetrics.RWMutex
	networks        map[string]*computeapi.Network
	subnets         map[string]*computeapi.Subnetwork
	firewalls       map[string]*computeapi.Firewall
	routes          map[string]*computeapi.Route
	addresses       map[string]*computeapi.Address
	routers         map[string]*routerRec
	disks           map[string]*computeapi.Disk
	images          map[string]*computeapi.Image
	snapshots       map[string]*computeapi.Snapshot
	instantSnaps    map[string]*computeapi.InstantSnapshot
	machineImages   map[string]*computeapi.MachineImage
	reservations    map[string]*computeapi.Reservation
	instances       map[string]*instRec
	templates       map[string]*computeapi.InstanceTemplate
	healths         map[string]*computeapi.HealthCheck
	migs            map[string]*migRec
	groups          map[string]*groupRec
	scalers         map[string]*computeapi.Autoscaler
	backendSvcs     map[string]*computeapi.BackendService
	backendBuckets  map[string]*computeapi.BackendBucket
	urlMaps         map[string]*computeapi.UrlMap
	httpProxies     map[string]*computeapi.TargetHttpProxy
	httpsProxies    map[string]*computeapi.TargetHttpsProxy
	tcpProxies      map[string]*computeapi.TargetTcpProxy
	sslProxies      map[string]*computeapi.TargetSslProxy
	sslCerts        map[string]*computeapi.SslCertificate
	sslPolicies     map[string]*computeapi.SslPolicy
	targetPools     map[string]*computeapi.TargetPool
	targetInstances map[string]*computeapi.TargetInstance
	httpHealths     map[string]*computeapi.HttpHealthCheck
	httpsHealths    map[string]*computeapi.HttpsHealthCheck
	fwdRules        map[string]*computeapi.ForwardingRule
	negs            map[string]*computeapi.NetworkEndpointGroup
	negEndpoints    map[string][]*computeapi.NetworkEndpoint
	secPolicies     map[string]*computeapi.SecurityPolicy
	resPolicies     map[string]*computeapi.ResourcePolicy
	ops             map[string]*opRec
	projects        map[string]*computeapi.Metadata
	timing          Timing
}

// NewInMemoryBackend creates an empty backend on the wall clock.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu: lockmetrics.New(
			"gcpcompute",
		),
		now:    time.Now,
		timing: DefaultTiming(),
		sleep:  realSleep,
		prober: newNetProber(),
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
	b.instantSnaps = map[string]*computeapi.InstantSnapshot{}
	b.machineImages = map[string]*computeapi.MachineImage{}
	b.reservations = map[string]*computeapi.Reservation{}
	b.instances = map[string]*instRec{}
	b.templates = map[string]*computeapi.InstanceTemplate{}
	b.healths = map[string]*computeapi.HealthCheck{}
	b.migs = map[string]*migRec{}
	b.groups = map[string]*groupRec{}
	b.scalers = map[string]*computeapi.Autoscaler{}
	b.backendSvcs = map[string]*computeapi.BackendService{}
	b.backendBuckets = map[string]*computeapi.BackendBucket{}
	b.urlMaps = map[string]*computeapi.UrlMap{}
	b.httpProxies = map[string]*computeapi.TargetHttpProxy{}
	b.httpsProxies = map[string]*computeapi.TargetHttpsProxy{}
	b.tcpProxies = map[string]*computeapi.TargetTcpProxy{}
	b.sslProxies = map[string]*computeapi.TargetSslProxy{}
	b.sslCerts = map[string]*computeapi.SslCertificate{}
	b.sslPolicies = map[string]*computeapi.SslPolicy{}
	b.targetPools = map[string]*computeapi.TargetPool{}
	b.targetInstances = map[string]*computeapi.TargetInstance{}
	b.httpHealths = map[string]*computeapi.HttpHealthCheck{}
	b.httpsHealths = map[string]*computeapi.HttpsHealthCheck{}
	b.fwdRules = map[string]*computeapi.ForwardingRule{}
	b.negs = map[string]*computeapi.NetworkEndpointGroup{}
	b.negEndpoints = map[string][]*computeapi.NetworkEndpoint{}
	b.secPolicies = map[string]*computeapi.SecurityPolicy{}
	b.resPolicies = map[string]*computeapi.ResourcePolicy{}
	b.ops = map[string]*opRec{}
	b.projects = map[string]*computeapi.Metadata{}
}

// Reset removes all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetMaps()
}
