package redshift

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// clusterIDRegex matches valid Redshift ClusterIdentifier values:
// begins with a letter, only lowercase letters/digits/hyphens, 1-63 chars.
var clusterIDRegex = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// Named status constants for cluster and resource states.
const (
	clusterStatusAvailable       = "available"
	partnerStatusActive          = "Active"
	dataShareStatusAuthorized    = "AUTHORIZED"
	dataShareStatusActive        = "ACTIVE"
	endpointAuthStatusAuthorized = "Authorized"
	ingressStatusAuthorized      = "authorized"
	resizeStatusCancelled        = "CANCELLED"
	clusterTypeMultiNode         = "multi-node"
	clusterTypeSingleNode        = "single-node"
	defaultNodeType              = "dc2.large"
	defaultDBName                = "dev"
	defaultMasterUsername        = "admin"
	defaultPort                  = 5439
)

// InMemoryBackend is the in-memory store for Redshift clusters.
type InMemoryBackend struct {
	dnsRegistrar   DNSRegistrar
	registry       *store.Registry
	customDomains  *store.Table[CustomDomainAssociation]
	events         *store.Table[Event]
	partners       *store.Table[Partner]
	dataShares     *store.Table[DataShare]
	securityGroups *store.Table[ClusterSecurityGroup]
	snapshots      *store.Table[Snapshot]
	endpointAuths  *store.Table[EndpointAuthorization]
	// activeResizes is intentionally NOT a store.Table: ResizeProgress carries
	// no ClusterIdentifier field of its own, so its key (clusterID) is not a
	// pure function of the value -- see store_setup.go.
	activeResizes   map[string]*ResizeProgress
	parameterGroups *store.Table[ClusterParameterGroup]
	subnetGroups    *store.Table[ClusterSubnetGroup]
	// loggingStatuses is intentionally NOT a store.Table; see activeResizes above.
	loggingStatuses    map[string]*LoggingStatus
	eventSubscriptions *store.Table[EventSubscription]
	clusters           *store.Table[Cluster]
	snapshotCopyGrants *store.Table[SnapshotCopyGrant]
	snapshotSchedules  *store.Table[SnapshotSchedule]
	usageLimits        *store.Table[UsageLimit]
	authProfiles       *store.Table[AuthenticationProfile]
	resourcePolicies   *store.Table[ResourcePolicy]
	tableRestores      *store.Table[TableRestoreStatus]
	// snapshotCopyConfigs is intentionally NOT a store.Table; see activeResizes above.
	snapshotCopyConfigs map[string]*SnapshotCopyConfig
	hsmClientCerts      *store.Table[HsmClientCertificate]
	hsmConfigs          *store.Table[HsmConfiguration]
	reservedNodes       *store.Table[ReservedNode]
	scheduledActions    *store.Table[ScheduledAction]
	slScheduledActions  *store.Table[ServerlessScheduledAction]
	integrations        *store.Table[Integration]
	idcApplications     *store.Table[IdcApplication]
	slNamespaces        *store.Table[Namespace]
	slWorkgroups        *store.Table[Workgroup]
	slSnapshots         *store.Table[ServerlessSnapshot]
	slUsageLimits       *store.Table[ServerlessUsageLimit]
	endpointAccesses    *store.Table[EndpointAccess]
	// clusterTransitions holds in-flight lifecycle state, intentionally never
	// persisted (see Restore) and keyed externally by cluster ID.
	clusterTransitions     map[string]*clusterTransition
	mu                     *lockmetrics.RWMutex
	reconcileStop          chan struct{}
	region                 string
	accountID              string
	slNamespaceIdx         sortedStringIndex
	slWorkgroupIdx         sortedStringIndex
	slSnapshotIdx          sortedStringIndex
	slUsageLimitIdx        sortedStringIndex
	slScheduledActionIdx   sortedStringIndex
	reconcileWG            sync.WaitGroup
	clusterActivationDelay time.Duration
	reconcileInterval      time.Duration
	reconcileMu            sync.Mutex
	reconcileOn            bool
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		activeResizes:       make(map[string]*ResizeProgress),
		loggingStatuses:     make(map[string]*LoggingStatus),
		snapshotCopyConfigs: make(map[string]*SnapshotCopyConfig),
		clusterTransitions:  make(map[string]*clusterTransition),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("redshift"),
		registry:            store.NewRegistry(),
	}

	registerAllTables(b)

	return b
}

// Reset clears all backend state while preserving configuration.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, c := range b.clusters.All() {
		c.Tags.Close()
	}

	b.registry.ResetAll()
	b.activeResizes = make(map[string]*ResizeProgress)
	b.loggingStatuses = make(map[string]*LoggingStatus)
	b.snapshotCopyConfigs = make(map[string]*SnapshotCopyConfig)
	b.clusterTransitions = make(map[string]*clusterTransition)
	b.resetServerlessIndexes()
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// SetDNSRegistrar wires a DNS server so Redshift cluster hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()
	b.dnsRegistrar = dns
}

// cloneCluster returns a deep copy of a Cluster, excluding the live Tags pointer.
// The caller receives a value copy with a nil Tags field; use Tags.Clone() to get tag data.
func cloneCluster(c *Cluster) Cluster {
	cp := *c
	// Tags is a live pointer; callers that need tag data must call c.Tags.Clone() separately.
	// Setting to nil prevents callers from accidentally mutating the backend via the copy.
	cp.Tags = nil

	return cp
}

// CreateCluster creates a new Redshift cluster.
func (b *InMemoryBackend) CreateCluster(id, nodeType, dbName, masterUser string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if !clusterIDRegex.MatchString(id) || strings.HasSuffix(id, "-") || strings.Contains(id, "--") {
		return nil, fmt.Errorf(
			"%w: ClusterIdentifier %q is invalid (must start with a letter, "+
				"contain only lowercase letters/digits/hyphens, not end with a hyphen, "+
				"not contain consecutive hyphens, max 63 chars)",
			ErrInvalidParameter, id,
		)
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(id); exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}

	if nodeType == "" {
		nodeType = defaultNodeType
	}
	if dbName == "" {
		dbName = defaultDBName
	}
	if masterUser == "" {
		masterUser = defaultMasterUsername
	}

	endpoint := fmt.Sprintf("%s.%s.%s.redshift.amazonaws.com", id, b.accountID, b.region)

	initialStatus := clusterStatusAvailable
	if b.clusterActivationDelay > 0 {
		initialStatus = clusterStatusCreating
	}

	cluster := &Cluster{
		ClusterIdentifier: id,
		NodeType:          nodeType,
		ClusterType:       clusterTypeMultiNode,
		Endpoint:          endpoint,
		Status:            initialStatus,
		DBName:            dbName,
		MasterUsername:    masterUser,
		Port:              defaultPort,
		NumberOfNodes:     1,
		Tags:              tags.New("redshift.cluster." + id + ".tags"),
	}
	b.clusters.Put(cluster)

	// Schedule the creating→available transition instead of spawning an
	// unmanaged per-cluster goroutine. The managed reconciler (or a lazy read)
	// advances it, so cluster deletion and Reset cancel it deterministically.
	if b.clusterActivationDelay > 0 {
		b.scheduleClusterTransitionLocked(id, &clusterTransition{
			effectiveAt: time.Now().Add(b.clusterActivationDelay),
			status:      clusterStatusAvailable,
		})
	}

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}

// DeleteCluster removes the cluster with the given identifier.
func (b *InMemoryBackend) DeleteCluster(id string) (*Cluster, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	// When an activation delay is configured, model AWS's asynchronous deletion:
	// the cluster enters "deleting" and is removed by the reconciler once the
	// delay elapses. This supersedes any pending creating→available transition.
	if b.clusterActivationDelay > 0 {
		cluster.Status = clusterStatusDeleting
		cp := cloneCluster(cluster)
		b.scheduleClusterTransitionLocked(id, &clusterTransition{
			effectiveAt: time.Now().Add(b.clusterActivationDelay),
			remove:      true,
		})

		return &cp, nil
	}

	cp := cloneCluster(cluster)
	delete(b.clusterTransitions, id)
	cluster.Tags.Close()
	b.clusters.Delete(id)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cp.Endpoint)
	}

	return &cp, nil
}

// DescribeClusters returns clusters. If id is non-empty, returns only that cluster.
// When marker and maxRecords are used, returns a page of results sorted by ClusterIdentifier.
func (b *InMemoryBackend) DescribeClusters(id, marker string, maxRecords int) ([]Cluster, string, error) {
	// Advance any due lifecycle transitions before reading so SDK waiters that
	// poll DescribeClusters always observe the current state, even when the
	// background reconciler is not running.
	b.advanceClusterStates(time.Now())

	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.clusters.Get(id)
		if !exists {
			return nil, "", fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		return []Cluster{cloneCluster(c)}, "", nil
	}

	// Snapshot returns every cluster ordered by key (ClusterIdentifier)
	// ascending, matching the previous sort.Strings(ids) behaviour.
	sorted := b.clusters.Snapshot()

	// Advance past the marker (exclusive — marker is the last ID on the previous page).
	if marker != "" {
		cut := 0
		for cut < len(sorted) && sorted[cut].ClusterIdentifier <= marker {
			cut++
		}

		sorted = sorted[cut:]
	}

	nextMarker := ""
	if maxRecords > 0 && len(sorted) > maxRecords {
		sorted = sorted[:maxRecords]
		nextMarker = sorted[len(sorted)-1].ClusterIdentifier
	}

	clusters := make([]Cluster, 0, len(sorted))
	for _, c := range sorted {
		clusters = append(clusters, cloneCluster(c))
	}

	return clusters, nextMarker, nil
}
