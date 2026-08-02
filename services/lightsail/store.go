package lightsail

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// CloudFormationBackend lets CreateCloudFormationStack hand off to a real
// services/cloudformation stack-creation call -- the one confirmed genuine
// cross-service link in this surface (PARITY.md 5.2 point 4). Optional: a
// nil backend (the default) means CreateCloudFormationStack still records a
// real CloudFormationStackRecord and Operation, just without an actual
// backing stack -- an honest, documented scoped-down behavior rather than a
// fabricated stack ARN.
type CloudFormationBackend interface {
	CreateStackFromLightsail(stackName string, instanceNames []string) (stackID string, err error)
}

// InMemoryBackend is the in-memory store for Amazon Lightsail. A single
// coarse lock guards every collection: Lightsail ops routinely cross
// resource boundaries (AttachInstancesToLoadBalancer reads+writes both a
// LoadBalancer and the Instances it references; CreateCloudFormationStack
// reads InstanceEntry sources while writing a new record), and Operation
// bookkeeping alone touches nearly every table on every mutating call -- see
// .claude/memories/pkgs-catalog.md's locking rule.
type InMemoryBackend struct {
	cfnBackend               CloudFormationBackend
	alarms                   *store.Table[Alarm]
	disks                    *store.Table[Disk]
	staticIPs                *store.Table[StaticIP]
	keyPairs                 *store.Table[KeyPair]
	instanceSnapshots        *store.Table[InstanceSnapshot]
	diskSnapshots            *store.Table[DiskSnapshot]
	exportSnapshotRecords    *store.Table[ExportSnapshotRecord]
	cfnStackRecords          *store.Table[CloudFormationStackRecord]
	loadBalancers            *store.Table[LoadBalancer]
	lbTLSCertificates        *store.Table[LoadBalancerTLSCertificate]
	databases                *store.Table[RelationalDatabase]
	dbSnapshots              *store.Table[RelationalDatabaseSnapshot]
	containerServices        *store.Table[ContainerService]
	buckets                  *store.Table[Bucket]
	distributions            *store.Table[Distribution]
	domains                  *store.Table[Domain]
	registry                 *store.Registry
	certificates             *store.Table[Certificate]
	work                     *worker.Group
	guiSessions              *store.Table[GUISession]
	operations               *store.Table[Operation]
	opsByResource            *store.Index[Operation]
	setupHistory             *store.Table[SetupHistoryEntry]
	setupHistoryByResource   *store.Index[SetupHistoryEntry]
	activeNames              map[string]string
	defaultKeyPair           *KeyPair
	mu                       *lockmetrics.RWMutex
	instances                *store.Table[Instance]
	contactMethods           *store.Table[ContactMethod]
	distributionCacheResets  map[string]time.Time
	defaultKeyPairPublicKey  string
	defaultKeyPairPrivateKey string
	accountID                string
	region                   string
	nextAccessKeySeq         int
	vpcPeered                bool
}

// NewInMemoryBackend creates a new in-memory Amazon Lightsail backend.
func NewInMemoryBackend(ctx context.Context, accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:                store.NewRegistry(),
		activeNames:             make(map[string]string),
		distributionCacheResets: make(map[string]time.Time),
		accountID:               accountID,
		region:                  region,
		mu:                      lockmetrics.New("lightsail"),
		work:                    worker.NewGroup(ctx, "lightsail"),
	}
	registerAllTables(b)

	return b
}

// SetCloudFormationBackend wires the real services/cloudformation stack
// creator for CreateCloudFormationStack -- see CloudFormationBackend's doc
// comment. Called from cli.go's wireLightsailCloudFormation.
func (b *InMemoryBackend) SetCloudFormationBackend(cfn CloudFormationBackend) {
	b.mu.Lock("SetCloudFormationBackend")
	defer b.mu.Unlock()

	b.cfnBackend = cfn
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Close stops all scheduled state-transition timers so none outlives the
// backend. Safe to call multiple times.
func (b *InMemoryBackend) Close() { b.work.Stop() }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	closeAllTags(b)
	b.registry.ResetAll()
	b.activeNames = make(map[string]string)
	b.distributionCacheResets = make(map[string]time.Time)
	b.defaultKeyPair = nil
	b.vpcPeered = false
	b.nextAccessKeySeq = 0
}

// closeTagsOf closes every non-nil *tags.Tags among items, via tagsOf --
// factored out so closeAllTags itself is one call per table rather than one
// for-loop-plus-if per table (each of which independently added to
// closeAllTags' own cyclomatic/cognitive complexity).
func closeTagsOf[T any](items []*T, tagsOf func(*T) *tags.Tags) {
	for _, v := range items {
		if t := tagsOf(v); t != nil {
			t.Close()
		}
	}
}

func closeAllTags(b *InMemoryBackend) {
	closeTagsOf(b.instances.All(), func(v *Instance) *tags.Tags { return v.Tags })
	closeTagsOf(b.disks.All(), func(v *Disk) *tags.Tags { return v.Tags })
	closeTagsOf(b.keyPairs.All(), func(v *KeyPair) *tags.Tags { return v.Tags })
	closeTagsOf(b.instanceSnapshots.All(), func(v *InstanceSnapshot) *tags.Tags { return v.Tags })
	closeTagsOf(b.diskSnapshots.All(), func(v *DiskSnapshot) *tags.Tags { return v.Tags })
	closeTagsOf(b.loadBalancers.All(), func(v *LoadBalancer) *tags.Tags { return v.Tags })
	closeTagsOf(b.lbTLSCertificates.All(), func(v *LoadBalancerTLSCertificate) *tags.Tags { return v.Tags })
	closeTagsOf(b.databases.All(), func(v *RelationalDatabase) *tags.Tags { return v.Tags })
	closeTagsOf(b.dbSnapshots.All(), func(v *RelationalDatabaseSnapshot) *tags.Tags { return v.Tags })
	closeTagsOf(b.containerServices.All(), func(v *ContainerService) *tags.Tags { return v.Tags })
	closeTagsOf(b.buckets.All(), func(v *Bucket) *tags.Tags { return v.Tags })
	closeTagsOf(b.distributions.All(), func(v *Distribution) *tags.Tags { return v.Tags })
	closeTagsOf(b.domains.All(), func(v *Domain) *tags.Tags { return v.Tags })
	closeTagsOf(b.certificates.All(), func(v *Certificate) *tags.Tags { return v.Tags })
	closeTagsOf(b.alarms.All(), func(v *Alarm) *tags.Tags { return v.Tags })
	closeTagsOf(b.contactMethods.All(), func(v *ContactMethod) *tags.Tags { return v.Tags })
}

// regionalARN builds an ARN for a regional Lightsail resource kind
// (Instance/Disk/StaticIp/KeyPair/InstanceSnapshot/... -- confirmed regional
// via literal SDK doc-comment examples for at least Instance/StaticIp/
// KeyPair/InstanceSnapshot, PARITY.md 5.1). The ARN resource-type segment is
// CAPITALIZED and matches the ResourceType enum's own casing exactly (e.g.
// "Instance/{uuid}"), unlike this repo's own EC2 which lowercases it.
func (b *InMemoryBackend) regionalARN(resourceType, id string) string {
	return arn.Build("lightsail", b.region, b.accountID, resourceType+"/"+id)
}

// globalARN builds an ARN for a Lightsail resource kind confirmed GLOBAL
// (only Domain, per Domain.Arn's own doc-comment example
// "arn:aws:lightsail:global:...", PARITY.md 5.1). NOTE: this deliberately
// calls arn.Build with the literal region string "global" rather than
// arn.BuildGlobal -- BuildGlobal produces an EMPTY region segment
// ("arn:{partition}:{service}::{account}:{resource}"), the convention
// networkmanager's prior audit confirmed for ITS global resources, but
// Domain's own doc-comment example spells out the literal word "global" in
// the region slot instead ("arn:aws:lightsail:global:..."). Using
// BuildGlobal here would silently produce the wrong (empty-segment) ARN --
// caught by this package's own SDK round-trip test asserting the literal
// substring. See PARITY.md 4.7/5.1's explicit warning that this is a
// genuinely different global-ARN convention from networkmanager's and
// should not be assumed to generalize.
func (b *InMemoryBackend) globalARN(resourceType, id string) string {
	return arn.Build("lightsail", domainGlobalRegion, b.accountID, resourceType+"/"+id)
}

// distributionARN builds an ARN for a Distribution -- modeled as global
// (Distribution has no region segment in its own real product identity,
// PARITY.md 4.7) even though Location.RegionName always literally reports
// "us-east-1" (see consts.go's distributionRegion).
func (b *InMemoryBackend) distributionARN(name string) string {
	return arn.BuildGlobal("lightsail", b.region, b.accountID, ResourceTypeDistribution+"/"+name)
}

const randomIDBytes = 4

// randomHex returns a random 8-character lowercase hex string.
func randomHex() string {
	buf := make([]byte, randomIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%08x", time.Now().UnixNano())
	}

	return hex.EncodeToString(buf)
}

// newUUID returns a fresh random UUID string -- used for every Lightsail ID
// this SDK module does not document a specific format for (Operation.Id,
// AccessKeyId, SupportCode, etc.), matching services/outposts'/
// services/directconnect's treatment of unconfirmed ID formats.
func newUUID() string { return uuid.NewString() }

// newSupportCode returns a placeholder AWS support-code string. Real
// Lightsail SupportCode values are opaque internal AWS identifiers this SDK
// module documents no format for; this is a defensible, clearly-synthetic
// placeholder (never presented as a real AWS support code).
func newSupportCode() string { return randomHex() + "/" + randomHex() }

// nowUTC returns the current time in UTC.
func nowUTC() time.Time { return time.Now().UTC() }

// availabilityZoneA returns the conventional "a" availability zone for
// region -- used whenever a caller omits AvailabilityZone on a create call
// (this SDK does not require one). Real Lightsail AZ names follow the
// region+letter convention (e.g. "us-east-1a"); "a" is the only zone this
// backend ever needs to pick since it does not model true multi-AZ capacity.
func availabilityZoneA(region string) string { return region + "a" }

// registerName records name under kind in the global activeNames index,
// enforcing Lightsail's real cross-kind name-uniqueness invariant. Callers
// must hold b.mu. Returns a validationError if name is already taken by any
// resource kind (including the same kind).
func (b *InMemoryBackend) registerNameLocked(kind, name string) error {
	if existing, ok := b.activeNames[name]; ok {
		return validationError(fmt.Sprintf("a resource named %q already exists (%s)", name, existing))
	}

	b.activeNames[name] = kind

	return nil
}

// unregisterNameLocked removes name from the global activeNames index.
// Callers must hold b.mu.
func (b *InMemoryBackend) unregisterNameLocked(name string) {
	delete(b.activeNames, name)
}
