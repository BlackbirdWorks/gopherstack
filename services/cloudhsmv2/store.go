// Package cloudhsmv2 emulates the AWS CloudHSM v2 control plane: clusters, HSMs and backups.
package cloudhsmv2

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName   = "cloudhsmv2"
	arnService    = "cloudhsm"
	defaultRegion = "us-east-1"
	defaultDwell  = 500 * time.Millisecond
	idSuffixLen   = 11
	maxClusters   = 4
	maxHsms       = 28
	maxTags       = 50
	defaultPage   = 25
	maxPage       = 100
)

// Network is the VPC surface the backend needs: subnet lookup, security group and ENI management.
type Network interface {
	// SubnetInfo returns the VPC and Availability Zone of each requested subnet that exists in the region.
	SubnetInfo(region string, subnetIDs []string) map[string]Subnet
	CreateSecurityGroup(region, name, description, vpcID string) (string, error)
	DeleteSecurityGroup(region, id string)
	CreateENI(region, subnetID, ip, securityGroupID, description string) (eniID, eniIP string, err error)
	DeleteENI(region, id string)
}

// Subnet is the EC2 view of a subnet.
type Subnet struct{ VpcID, Zone string }

// InMemoryBackend is the CloudHSM v2 state; one coarse lock guards every table.
type InMemoryBackend struct {
	clusters      *store.Table[Cluster]
	hsms          *store.Table[Hsm]
	hsmsByCluster *store.Index[Hsm]
	backups       *store.Table[Backup]
	registry      *store.Registry
	mu            *lockmetrics.RWMutex
	now           func() time.Time
	network       Network
	accountID     string
	region        string
	dwellOverride time.Duration
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = defaultRegion
	}

	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)

	return b
}

// Region returns the default region.
func (b *InMemoryBackend) Region() string { return b.region }

// SetLifecycleDelay overrides how long clusters, HSMs and backups stay in a transitional state.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.dwellOverride = d
}

// SetNetwork wires the EC2 lookups used for subnets, security groups and ENIs.
func (b *InMemoryBackend) SetNetwork(n Network) {
	b.mu.Lock("SetNetwork")
	defer b.mu.Unlock()

	b.network = n
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

func (b *InMemoryBackend) dwell() time.Duration {
	if b.dwellOverride > 0 {
		return b.dwellOverride
	}

	return defaultDwell
}

type scope struct{ account, region string }

func (b *InMemoryBackend) scopeOf(ctx context.Context) scope {
	sc := scope{account: awsmeta.Account(ctx), region: awsmeta.Region(ctx)}
	if sc.account == "" {
		sc.account = b.accountID
	}

	if sc.region == "" {
		sc.region = b.region
	}

	return sc
}

func backupARN(account, region, id string) string {
	return arn.Build(arnService, region, account, "backup/"+id)
}

func clusterARN(account, region, id string) string {
	return arn.Build(arnService, region, account, "cluster/"+id)
}

func newClusterID() string { return awsid.NewN("cluster-", idSuffixLen) }
func newHsmID() string     { return awsid.NewN("hsm-", idSuffixLen) }
func newBackupID() string  { return awsid.NewN("backup-", idSuffixLen) }

func clusterKey(v *Cluster) string { return v.ID }
func hsmKey(v *Hsm) string         { return v.ID }
func backupKey(v *Backup) string   { return v.ID }

func registerAllTables(b *InMemoryBackend) {
	b.clusters = store.Register(b.registry, "clusters", store.New(clusterKey))
	b.hsms = store.Register(b.registry, "hsms", store.New(hsmKey))
	b.hsmsByCluster = b.hsms.AddIndex("byCluster", func(v *Hsm) string { return v.ClusterID })
	b.backups = store.Register(b.registry, "backups", store.New(backupKey))
}
