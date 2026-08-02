package lightsail

// This file backs family X (3 ops: PeerVpc, UnpeerVpc, IsVpcPeered -- all
// three take ZERO input fields, PARITY.md family X), family Y (2 ops:
// TagResource, UntagResource -- see PARITY.md 5.1's name-first inversion),
// family AA (3 ops: CreateGUISessionAccessDetails, StartGUISession,
// StopGUISession), and family BB (2 ops: GetActiveNames, GetCostEstimate).

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	opTypePeerVpc         = "PeerVpc"
	opTypeUnpeerVpc       = "UnpeerVpc"
	opTypeStartGUISession = "StartGUISession"
	opTypeStopGUISession  = "StopGUISession"
)

// PeerVpc peers the account-wide implicit Lightsail<->default-EC2-VPC
// connection (a single boolean, not a named resource, PARITY.md family X).
func (b *InMemoryBackend) PeerVpc() (*Operation, error) {
	b.mu.Lock("PeerVpc")
	defer b.mu.Unlock()

	b.vpcPeered = true
	ops := b.newOperationsLocked(opTypePeerVpc, ResourceTypePeeredVpc, []string{"PeeredVpc"})

	return &ops[0], nil
}

// UnpeerVpc unpeers the account-wide implicit VPC connection.
func (b *InMemoryBackend) UnpeerVpc() (*Operation, error) {
	b.mu.Lock("UnpeerVpc")
	defer b.mu.Unlock()

	b.vpcPeered = false
	ops := b.newOperationsLocked(opTypeUnpeerVpc, ResourceTypePeeredVpc, []string{"PeeredVpc"})

	return &ops[0], nil
}

// IsVpcPeered reports whether the account-wide VPC connection is peered.
func (b *InMemoryBackend) IsVpcPeered() bool {
	b.mu.RLock("IsVpcPeered")
	defer b.mu.RUnlock()

	return b.vpcPeered
}

// tagsNotSupportedKinds is the 4 of 20 ResourceType kinds (StaticIp,
// PeeredVpc, ExportSnapshotRecord, CloudFormationStackRecord) whose own SDK
// struct carries NO Tags field at all (confirmed by direct field-by-field
// SDK read, unlike the other 16 kinds) despite being valid ResourceType
// enum values -- a genuine wire-shape asymmetry (PARITY.md 5.1's "handle
// the inversion deliberately" instruction). TagResource against one of
// these resolves the resource (name found) but honestly refuses to
// fabricate a place to persist the tag, rather than silently no-op'ing.
//
//nolint:gochecknoglobals // static lookup table, read-only
var tagsNotSupportedKinds = map[string]bool{
	ResourceTypeStaticIP:                  true,
	ResourceTypePeeredVpc:                 true,
	ResourceTypeExportSnapshotRecord:      true,
	ResourceTypeCloudFormationStackRecord: true,
}

// taggableResolvers returns, for every one of the 16 taggable ResourceType
// kinds, a method value resolving a resource name to its live *tags.Tags
// and real ARN -- factored into a dispatch table (rather than a 16-case
// switch directly inside resolveTaggableLocked) so resolveTaggableLocked's
// own cyclomatic complexity stays low. Each entry is a METHOD VALUE (a
// reference to one of the resolveXxxTag methods below), not an inline
// closure literal, so its body is scored as that separate method's own
// complexity, not folded into taggableResolvers' or resolveTaggableLocked's.
func (b *InMemoryBackend) taggableResolvers() map[string]func(string) (*tags.Tags, bool) {
	return map[string]func(string) (*tags.Tags, bool){
		ResourceTypeInstance:                   b.resolveInstanceTag,
		ResourceTypeDisk:                       b.resolveDiskTag,
		ResourceTypeKeyPair:                    b.resolveKeyPairTag,
		ResourceTypeInstanceSnapshot:           b.resolveInstanceSnapshotTag,
		ResourceTypeDiskSnapshot:               b.resolveDiskSnapshotTag,
		ResourceTypeLoadBalancer:               b.resolveLoadBalancerTag,
		ResourceTypeLoadBalancerTLSCertificate: b.resolveLBTLSCertificateTag,
		ResourceTypeRelationalDatabase:         b.resolveDatabaseTag,
		ResourceTypeRelationalDatabaseSnapshot: b.resolveDBSnapshotTag,
		ResourceTypeContainerService:           b.resolveContainerServiceTag,
		ResourceTypeBucket:                     b.resolveBucketTag,
		ResourceTypeDistribution:               b.resolveDistributionTag,
		ResourceTypeDomain:                     b.resolveDomainTag,
		ResourceTypeCertificate:                b.resolveCertificateTag,
		ResourceTypeAlarm:                      b.resolveAlarmTag,
		ResourceTypeContactMethod:              b.resolveContactMethodTag,
	}
}

func (b *InMemoryBackend) resolveInstanceTag(n string) (*tags.Tags, bool) {
	v, ok := b.instances.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveDiskTag(n string) (*tags.Tags, bool) {
	v, ok := b.disks.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveKeyPairTag(n string) (*tags.Tags, bool) {
	v, ok := b.keyPairs.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveInstanceSnapshotTag(n string) (*tags.Tags, bool) {
	v, ok := b.instanceSnapshots.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveDiskSnapshotTag(n string) (*tags.Tags, bool) {
	v, ok := b.diskSnapshots.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveLoadBalancerTag(n string) (*tags.Tags, bool) {
	v, ok := b.loadBalancers.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveLBTLSCertificateTag(n string) (*tags.Tags, bool) {
	v, ok := b.lbTLSCertificates.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveDatabaseTag(n string) (*tags.Tags, bool) {
	v, ok := b.databases.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveDBSnapshotTag(n string) (*tags.Tags, bool) {
	v, ok := b.dbSnapshots.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveContainerServiceTag(n string) (*tags.Tags, bool) {
	v, ok := b.containerServices.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveBucketTag(n string) (*tags.Tags, bool) {
	v, ok := b.buckets.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveDistributionTag(n string) (*tags.Tags, bool) {
	v, ok := b.distributions.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveDomainTag(n string) (*tags.Tags, bool) {
	v, ok := b.domains.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveCertificateTag(n string) (*tags.Tags, bool) {
	v, ok := b.certificates.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveAlarmTag(n string) (*tags.Tags, bool) {
	v, ok := b.alarms.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

func (b *InMemoryBackend) resolveContactMethodTag(n string) (*tags.Tags, bool) {
	v, ok := b.contactMethods.Get(n)
	if !ok {
		return nil, false
	}

	return v.Tags, true
}

// resolveTaggableLocked resolves resourceName (ResourceArn is accepted at
// the wire layer but this backend resolves purely by ResourceName, the
// wire spec's actually-required field, PARITY.md 5.1) to its live
// *tags.Tags. Callers must hold b.mu (read or write, per the caller's own
// need).
func (b *InMemoryBackend) resolveTaggableLocked(resourceName string) (*tags.Tags, error) {
	kind, ok := b.activeNames[resourceName]
	if !ok {
		return nil, notFoundError("resource", resourceName)
	}

	if tagsNotSupportedKinds[kind] {
		return nil, validationError(
			kind + " " + resourceName + " does not support tags (no Tags field on this resource kind)",
		)
	}

	resolver, known := b.taggableResolvers()[kind]
	if !known {
		return nil, validationError("unsupported resource kind for tagging: " + kind)
	}

	t, found := resolver(resourceName)
	if !found {
		return nil, notFoundError(kind, resourceName)
	}

	return t, nil
}

// TagResource tags resourceName (ResourceArn is accepted but this backend
// resolves purely by ResourceName, the wire spec's actually-required field,
// PARITY.md 5.1).
func (b *InMemoryBackend) TagResource(resourceName, _ string, newTags map[string]string) ([]Operation, error) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.resolveTaggableLocked(resourceName)
	if err != nil {
		return nil, err
	}

	t.Merge(newTags)

	kind := b.activeNames[resourceName]

	return b.newOperationsLocked("TagResource", kind, []string{resourceName}), nil
}

// UntagResource removes tagKeys from resourceName.
func (b *InMemoryBackend) UntagResource(resourceName, _ string, tagKeys []string) ([]Operation, error) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.resolveTaggableLocked(resourceName)
	if err != nil {
		return nil, err
	}

	t.DeleteKeys(tagKeys)

	kind := b.activeNames[resourceName]

	return b.newOperationsLocked("UntagResource", kind, []string{resourceName}), nil
}

// TaggedEntry is one resource's ARN and current tag map, for the
// resourcegroupstaggingapi cross-service bridge (cli.go's wireTaggingLightsail).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// appendTaggedOf appends a TaggedEntry for every item in items whose tags
// (via tagsOf) are non-empty -- factored out so TaggedResources itself is
// one call per table rather than one for-loop-plus-if per table.
func appendTaggedOf[T any](
	out []TaggedEntry,
	items []*T,
	arnOf func(*T) string,
	tagsOf func(*T) *tags.Tags,
) []TaggedEntry {
	for _, v := range items {
		t := tagsOf(v)
		if t == nil || t.Len() == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: arnOf(v), Tags: t.Clone()})
	}

	return out
}

// TaggedResources returns every taggable resource's current ARN and tags,
// across all 16 taggable kinds (of the 20 ResourceType values --
// tagsNotSupportedKinds' 4 excluded, see its doc comment).
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	out = appendTaggedOf(
		out,
		b.instances.All(),
		func(v *Instance) string { return v.Arn },
		func(v *Instance) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out,
		b.disks.All(),
		func(v *Disk) string { return v.Arn },
		func(v *Disk) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out,
		b.keyPairs.All(),
		func(v *KeyPair) string { return v.Arn },
		func(v *KeyPair) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out, b.instanceSnapshots.All(), func(v *InstanceSnapshot) string { return v.Arn },
		func(v *InstanceSnapshot) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out, b.diskSnapshots.All(), func(v *DiskSnapshot) string { return v.Arn },
		func(v *DiskSnapshot) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out, b.loadBalancers.All(), func(v *LoadBalancer) string { return v.Arn },
		func(v *LoadBalancer) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out, b.lbTLSCertificates.All(), func(v *LoadBalancerTLSCertificate) string { return v.Arn },
		func(v *LoadBalancerTLSCertificate) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out, b.databases.All(), func(v *RelationalDatabase) string { return v.Arn },
		func(v *RelationalDatabase) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out, b.dbSnapshots.All(), func(v *RelationalDatabaseSnapshot) string { return v.Arn },
		func(v *RelationalDatabaseSnapshot) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out, b.containerServices.All(), func(v *ContainerService) string { return v.Arn },
		func(v *ContainerService) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out,
		b.buckets.All(),
		func(v *Bucket) string { return v.Arn },
		func(v *Bucket) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out, b.distributions.All(), func(v *Distribution) string { return v.Arn },
		func(v *Distribution) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out,
		b.domains.All(),
		func(v *Domain) string { return v.Arn },
		func(v *Domain) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out, b.certificates.All(), func(v *Certificate) string { return v.Arn },
		func(v *Certificate) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out,
		b.alarms.All(),
		func(v *Alarm) string { return v.Arn },
		func(v *Alarm) *tags.Tags { return v.Tags },
	)
	out = appendTaggedOf(
		out, b.contactMethods.All(), func(v *ContactMethod) string { return v.Arn },
		func(v *ContactMethod) *tags.Tags { return v.Tags },
	)

	sort.Slice(out, func(i, j int) bool { return out[i].ARN < out[j].ARN })

	return out
}

// findNameByARN scans items for the one whose arnOf matches arnStr,
// returning nameOf of the match -- factored out so arnToNameLocked itself is
// one call per table rather than one for-loop-plus-if per table.
func findNameByARN[T any](items []*T, arnStr string, arnOf func(*T) string, nameOf func(*T) string) (string, bool) {
	for _, v := range items {
		if arnOf(v) == arnStr {
			return nameOf(v), true
		}
	}

	return "", false
}

func (b *InMemoryBackend) findInstanceNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.instances.All(),
		arnStr,
		func(v *Instance) string { return v.Arn },
		func(v *Instance) string { return v.Name },
	)
}

func (b *InMemoryBackend) findDiskNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.disks.All(),
		arnStr,
		func(v *Disk) string { return v.Arn },
		func(v *Disk) string { return v.Name },
	)
}

func (b *InMemoryBackend) findKeyPairNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.keyPairs.All(),
		arnStr,
		func(v *KeyPair) string { return v.Arn },
		func(v *KeyPair) string { return v.Name },
	)
}

func (b *InMemoryBackend) findInstanceSnapshotNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.instanceSnapshots.All(), arnStr,
		func(v *InstanceSnapshot) string { return v.Arn }, func(v *InstanceSnapshot) string { return v.Name },
	)
}

func (b *InMemoryBackend) findDiskSnapshotNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.diskSnapshots.All(), arnStr,
		func(v *DiskSnapshot) string { return v.Arn }, func(v *DiskSnapshot) string { return v.Name },
	)
}

func (b *InMemoryBackend) findLoadBalancerNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.loadBalancers.All(), arnStr,
		func(v *LoadBalancer) string { return v.Arn }, func(v *LoadBalancer) string { return v.Name },
	)
}

func (b *InMemoryBackend) findLBTLSCertificateNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.lbTLSCertificates.All(), arnStr,
		func(v *LoadBalancerTLSCertificate) string { return v.Arn },
		func(v *LoadBalancerTLSCertificate) string { return v.Name },
	)
}

func (b *InMemoryBackend) findDatabaseNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.databases.All(), arnStr,
		func(v *RelationalDatabase) string { return v.Arn }, func(v *RelationalDatabase) string { return v.Name },
	)
}

func (b *InMemoryBackend) findDBSnapshotNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.dbSnapshots.All(), arnStr,
		func(v *RelationalDatabaseSnapshot) string { return v.Arn },
		func(v *RelationalDatabaseSnapshot) string { return v.Name },
	)
}

func (b *InMemoryBackend) findContainerServiceNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.containerServices.All(), arnStr,
		func(v *ContainerService) string { return v.Arn }, func(v *ContainerService) string { return v.Name },
	)
}

func (b *InMemoryBackend) findBucketNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.buckets.All(),
		arnStr,
		func(v *Bucket) string { return v.Arn },
		func(v *Bucket) string { return v.Name },
	)
}

func (b *InMemoryBackend) findDistributionNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.distributions.All(), arnStr,
		func(v *Distribution) string { return v.Arn }, func(v *Distribution) string { return v.Name },
	)
}

func (b *InMemoryBackend) findDomainNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.domains.All(),
		arnStr,
		func(v *Domain) string { return v.Arn },
		func(v *Domain) string { return v.Name },
	)
}

func (b *InMemoryBackend) findCertificateNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.certificates.All(), arnStr,
		func(v *Certificate) string { return v.Arn }, func(v *Certificate) string { return v.Name },
	)
}

func (b *InMemoryBackend) findAlarmNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.alarms.All(),
		arnStr,
		func(v *Alarm) string { return v.Arn },
		func(v *Alarm) string { return v.Name },
	)
}

func (b *InMemoryBackend) findContactMethodNameByARN(arnStr string) (string, bool) {
	return findNameByARN(
		b.contactMethods.All(), arnStr,
		func(v *ContactMethod) string { return v.Arn }, func(v *ContactMethod) string { return v.Protocol },
	)
}

// arnResolvers returns one findXxxNameByARN method value per taggable kind
// -- a slice of METHOD VALUES (not inline closures), so arnToNameLocked can
// iterate them in a single loop instead of a 16-deep if-chain (which would
// itself exceed the cyclomatic-complexity budget).
func (b *InMemoryBackend) arnResolvers() []func(string) (string, bool) {
	return []func(string) (string, bool){
		b.findInstanceNameByARN, b.findDiskNameByARN, b.findKeyPairNameByARN,
		b.findInstanceSnapshotNameByARN, b.findDiskSnapshotNameByARN, b.findLoadBalancerNameByARN,
		b.findLBTLSCertificateNameByARN, b.findDatabaseNameByARN, b.findDBSnapshotNameByARN,
		b.findContainerServiceNameByARN, b.findBucketNameByARN, b.findDistributionNameByARN,
		b.findDomainNameByARN, b.findCertificateNameByARN, b.findAlarmNameByARN, b.findContactMethodNameByARN,
	}
}

// arnToNameLocked resolves arn back to the ResourceName TagResource/
// UntagResource actually need, by scanning every taggable kind's own Arn
// field -- used only by the ARN-first cli.go cross-service tagging bridge
// (wireTaggingLightsail), which is why this is O(n) rather than a
// maintained reverse index: it is not on any hot per-request path within
// this package's own handlers. Callers must hold b.mu.
func (b *InMemoryBackend) arnToNameLocked(arnStr string) (string, bool) {
	for _, resolve := range b.arnResolvers() {
		if n, ok := resolve(arnStr); ok {
			return n, true
		}
	}

	return "", false
}

// TagResourceByARN and UntagResourceByARN are the ARN-first adapters
// cli.go's wireTaggingLightsail calls (resourcegroupstaggingapi's own
// convention is ARN-first, the reverse of this service's own
// ResourceName-first wire ops, PARITY.md 5.1) -- both resolve arn back to a
// ResourceName via arnToNameLocked, then delegate to the real
// TagResource/UntagResource resolution path above.
func (b *InMemoryBackend) TagResourceByARN(arnStr string, newTags map[string]string) error {
	b.mu.Lock("TagResourceByARN")

	name, ok := b.arnToNameLocked(arnStr)
	if !ok {
		b.mu.Unlock()

		return notFoundError("resource", arnStr)
	}

	b.mu.Unlock()

	_, err := b.TagResource(name, arnStr, newTags)

	return err
}

func (b *InMemoryBackend) UntagResourceByARN(arnStr string, tagKeys []string) error {
	b.mu.Lock("UntagResourceByARN")

	name, ok := b.arnToNameLocked(arnStr)
	if !ok {
		b.mu.Unlock()

		return notFoundError("resource", arnStr)
	}

	b.mu.Unlock()

	_, err := b.UntagResource(name, arnStr, tagKeys)

	return err
}

// CreateGUISessionAccessDetails creates (or returns the existing) GUI
// session for resourceName (Lightsail for Research, PARITY.md family AA).
func (b *InMemoryBackend) CreateGUISessionAccessDetails(resourceName string) (*GUISession, error) {
	b.mu.Lock("CreateGUISessionAccessDetails")
	defer b.mu.Unlock()

	if _, ok := b.instances.Get(resourceName); !ok {
		return nil, notFoundError("Instance", resourceName)
	}

	s, ok := b.guiSessions.Get(resourceName)
	if !ok {
		s = &GUISession{ResourceName: resourceName, Status: GUISessionStatusSettingUp}
		b.guiSessions.Put(s)

		b.work.After("GUISessionReady", asyncTransitionDelay, func() {
			b.mu.Lock("GUISession-async-ready")
			defer b.mu.Unlock()

			if sess, found := b.guiSessions.Get(resourceName); found && sess.Status == GUISessionStatusSettingUp {
				sess.Status = GUISessionStatusReady
				sess.URL = "https://gui-session." + randomHex() + ".cs.amazonlightsail.com/"
			}
		})
	}

	return s.clone(), nil
}

// StartGUISession starts (or restarts) the GUI session for resourceName.
func (b *InMemoryBackend) StartGUISession(resourceName string) (*Operation, error) {
	b.mu.Lock("StartGUISession")
	defer b.mu.Unlock()

	if _, ok := b.instances.Get(resourceName); !ok {
		return nil, notFoundError("Instance", resourceName)
	}

	s, ok := b.guiSessions.Get(resourceName)
	if !ok {
		s = &GUISession{ResourceName: resourceName}
		b.guiSessions.Put(s)
	}

	s.Status = GUISessionStatusSettingUp

	ops := b.newOperationsLocked(opTypeStartGUISession, ResourceTypeInstance, []string{resourceName})

	return &ops[0], nil
}

// StopGUISession stops the GUI session for resourceName.
func (b *InMemoryBackend) StopGUISession(resourceName string) (*Operation, error) {
	b.mu.Lock("StopGUISession")
	defer b.mu.Unlock()

	s, ok := b.guiSessions.Get(resourceName)
	if !ok {
		return nil, notFoundError("GUISession", resourceName)
	}

	s.Status = GUISessionStatusStopped
	s.URL = ""

	ops := b.newOperationsLocked(opTypeStopGUISession, ResourceTypeInstance, []string{resourceName})

	return &ops[0], nil
}

// GetActiveNames returns every resource name currently in use account-wide
// -- Lightsail enforces global name uniqueness across ALL resource kinds
// (PARITY.md family BB), which this backend's activeNames index directly
// backs.
func (b *InMemoryBackend) GetActiveNames() []string {
	b.mu.RLock("GetActiveNames")
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.activeNames))
	for name := range b.activeNames {
		out = append(out, name)
	}

	sort.Strings(out)

	return out
}

// GetCostEstimate returns a real, well-formed, EMPTY cost-estimate response
// for resourceName -- a real cost estimate requires real usage-based
// billing logic this emulator has no grounds to fabricate (PARITY.md
// 4.10's cost-estimate-adjacent risk, same rationale as the six MetricData
// ops).
func (b *InMemoryBackend) GetCostEstimate(resourceName string) error {
	b.mu.RLock("GetCostEstimate")
	defer b.mu.RUnlock()

	if _, ok := b.activeNames[resourceName]; !ok {
		return notFoundError("resource", resourceName)
	}

	return nil
}
