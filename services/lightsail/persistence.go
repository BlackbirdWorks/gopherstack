package lightsail

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// lightsailSnapshotVersion identifies the shape of backendSnapshot's Tables
// blob. Bump whenever a change there would make an older snapshot unsafe to
// decode as the current shape; Restore discards (rather than partially
// decodes) any mismatch.
const lightsailSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Lightsail backend.
type backendSnapshot struct {
	Tables           map[string]json.RawMessage `json:"tables"`
	DefaultKeyPair   *KeyPair                   `json:"defaultKeyPair,omitempty"`
	AccountID        string                     `json:"accountId"`
	Region           string                     `json:"region"`
	Version          int                        `json:"version"`
	NextAccessKeySeq int                        `json:"nextAccessKeySeq"`
	VpcPeered        bool                       `json:"vpcPeered"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "lightsail: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:          lightsailSnapshotVersion,
		Tables:           tables,
		AccountID:        b.accountID,
		Region:           b.region,
		DefaultKeyPair:   b.defaultKeyPair,
		VpcPeered:        b.vpcPeered,
		NextAccessKeySeq: b.nextAccessKeySeq,
	}

	return persistence.MarshalSnapshot(ctx, "lightsail", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "lightsail", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != lightsailSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"lightsail: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", lightsailSnapshotVersion)

		b.registry.ResetAll()
		b.activeNames = make(map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("lightsail: restore tables: %w", err)
	}

	restoreTagsLocked(b)
	b.recomputeActiveNamesLocked()

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.defaultKeyPair = snap.DefaultKeyPair
	b.vpcPeered = snap.VpcPeered
	b.nextAccessKeySeq = snap.NextAccessKeySeq

	return nil
}

// registerNamesOf records kind for every item's own name (via nameOf) into
// b.activeNames -- factored out so recomputeActiveNamesLocked itself is one
// call per table rather than one for-loop per table (each of which
// independently added to its own cyclomatic complexity).
func registerNamesOf[T any](b *InMemoryBackend, items []*T, kind string, nameOf func(*T) string) {
	for _, v := range items {
		b.activeNames[nameOf(v)] = kind
	}
}

// recomputeActiveNamesLocked rebuilds the global name->kind index from
// every restored table's own resources, rather than persisting the index
// separately -- a single source of truth avoids the two ever drifting after
// a JSON round-trip. Callers must hold b.mu.
func (b *InMemoryBackend) recomputeActiveNamesLocked() {
	b.activeNames = make(map[string]string)

	registerNamesOf(b, b.instances.All(), ResourceTypeInstance, func(v *Instance) string { return v.Name })
	registerNamesOf(b, b.disks.All(), ResourceTypeDisk, func(v *Disk) string { return v.Name })
	registerNamesOf(b, b.staticIPs.All(), ResourceTypeStaticIP, func(v *StaticIP) string { return v.Name })
	registerNamesOf(b, b.keyPairs.All(), ResourceTypeKeyPair, func(v *KeyPair) string { return v.Name })
	registerNamesOf(
		b, b.instanceSnapshots.All(), ResourceTypeInstanceSnapshot,
		func(v *InstanceSnapshot) string { return v.Name },
	)
	registerNamesOf(b, b.diskSnapshots.All(), ResourceTypeDiskSnapshot, func(v *DiskSnapshot) string { return v.Name })
	registerNamesOf(b, b.loadBalancers.All(), ResourceTypeLoadBalancer, func(v *LoadBalancer) string { return v.Name })
	registerNamesOf(
		b, b.lbTLSCertificates.All(), ResourceTypeLoadBalancerTLSCertificate,
		func(v *LoadBalancerTLSCertificate) string { return v.Name },
	)
	registerNamesOf(
		b, b.databases.All(), ResourceTypeRelationalDatabase,
		func(v *RelationalDatabase) string { return v.Name },
	)
	registerNamesOf(
		b, b.dbSnapshots.All(), ResourceTypeRelationalDatabaseSnapshot,
		func(v *RelationalDatabaseSnapshot) string { return v.Name },
	)
	registerNamesOf(
		b, b.containerServices.All(), ResourceTypeContainerService,
		func(v *ContainerService) string { return v.Name },
	)
	registerNamesOf(b, b.buckets.All(), ResourceTypeBucket, func(v *Bucket) string { return v.Name })
	registerNamesOf(b, b.distributions.All(), ResourceTypeDistribution, func(v *Distribution) string { return v.Name })
	registerNamesOf(b, b.domains.All(), ResourceTypeDomain, func(v *Domain) string { return v.Name })
	registerNamesOf(b, b.certificates.All(), ResourceTypeCertificate, func(v *Certificate) string { return v.Name })
	registerNamesOf(b, b.alarms.All(), ResourceTypeAlarm, func(v *Alarm) string { return v.Name })
}

// restoreTagsOf rebuilds a properly-named tags.Tags for every item, via
// tagsOf/setTags/key -- factored out so restoreTagsLocked itself is one
// call per table.
func restoreTagsOf[T any](
	items []*T,
	prefix string,
	key func(*T) string,
	tagsOf func(*T) *tags.Tags,
	setTags func(*T, *tags.Tags),
) {
	for _, v := range items {
		setTags(v, rebuildTags(tagsOf(v), prefix+key(v)+".tags"))
	}
}

// restoreTagsLocked rebuilds a properly-named tags.Tags instance for every
// taggable resource after a JSON round-trip, which deserializes every
// tags.Tags with the generic "json.tags" name -- matching services/
// directconnect/services/outposts/services/resiliencehub's identical
// restoreTagsLocked. Callers must hold b.mu.
func restoreTagsLocked(b *InMemoryBackend) {
	restoreTagsOf(b.instances.All(), "lightsail.instance.", func(v *Instance) string { return v.Name },
		func(v *Instance) *tags.Tags { return v.Tags }, func(v *Instance, t *tags.Tags) { v.Tags = t })
	restoreTagsOf(b.disks.All(), "lightsail.disk.", func(v *Disk) string { return v.Name },
		func(v *Disk) *tags.Tags { return v.Tags }, func(v *Disk, t *tags.Tags) { v.Tags = t })
	restoreTagsOf(b.keyPairs.All(), "lightsail.keypair.", func(v *KeyPair) string { return v.Name },
		func(v *KeyPair) *tags.Tags { return v.Tags }, func(v *KeyPair, t *tags.Tags) { v.Tags = t })
	restoreTagsOf(
		b.instanceSnapshots.All(),
		"lightsail.instancesnapshot.",
		func(v *InstanceSnapshot) string { return v.Name },
		func(v *InstanceSnapshot) *tags.Tags { return v.Tags },
		func(v *InstanceSnapshot, t *tags.Tags) { v.Tags = t },
	)
	restoreTagsOf(b.diskSnapshots.All(), "lightsail.disksnapshot.", func(v *DiskSnapshot) string { return v.Name },
		func(v *DiskSnapshot) *tags.Tags { return v.Tags }, func(v *DiskSnapshot, t *tags.Tags) { v.Tags = t })
	restoreTagsOf(b.loadBalancers.All(), "lightsail.loadbalancer.", func(v *LoadBalancer) string { return v.Name },
		func(v *LoadBalancer) *tags.Tags { return v.Tags }, func(v *LoadBalancer, t *tags.Tags) { v.Tags = t })
	restoreTagsOf(
		b.lbTLSCertificates.All(),
		"lightsail.lbtlscert.",
		func(v *LoadBalancerTLSCertificate) string { return v.Name },
		func(v *LoadBalancerTLSCertificate) *tags.Tags { return v.Tags },
		func(v *LoadBalancerTLSCertificate, t *tags.Tags) { v.Tags = t },
	)
	restoreTagsOf(
		b.databases.All(),
		"lightsail.database.",
		func(v *RelationalDatabase) string { return v.Name },
		func(v *RelationalDatabase) *tags.Tags { return v.Tags },
		func(v *RelationalDatabase, t *tags.Tags) { v.Tags = t },
	)
	restoreTagsOf(
		b.dbSnapshots.All(),
		"lightsail.dbsnapshot.",
		func(v *RelationalDatabaseSnapshot) string { return v.Name },
		func(v *RelationalDatabaseSnapshot) *tags.Tags { return v.Tags },
		func(v *RelationalDatabaseSnapshot, t *tags.Tags) { v.Tags = t },
	)
	restoreTagsOf(
		b.containerServices.All(),
		"lightsail.containerservice.",
		func(v *ContainerService) string { return v.Name },
		func(v *ContainerService) *tags.Tags { return v.Tags },
		func(v *ContainerService, t *tags.Tags) { v.Tags = t },
	)
	restoreTagsOf(b.buckets.All(), "lightsail.bucket.", func(v *Bucket) string { return v.Name },
		func(v *Bucket) *tags.Tags { return v.Tags }, func(v *Bucket, t *tags.Tags) { v.Tags = t })
	restoreTagsOf(b.distributions.All(), "lightsail.distribution.", func(v *Distribution) string { return v.Name },
		func(v *Distribution) *tags.Tags { return v.Tags }, func(v *Distribution, t *tags.Tags) { v.Tags = t })
	restoreTagsOf(b.domains.All(), "lightsail.domain.", func(v *Domain) string { return v.Name },
		func(v *Domain) *tags.Tags { return v.Tags }, func(v *Domain, t *tags.Tags) { v.Tags = t })
	restoreTagsOf(b.certificates.All(), "lightsail.certificate.", func(v *Certificate) string { return v.Name },
		func(v *Certificate) *tags.Tags { return v.Tags }, func(v *Certificate, t *tags.Tags) { v.Tags = t })
	restoreTagsOf(b.alarms.All(), "lightsail.alarm.", func(v *Alarm) string { return v.Name },
		func(v *Alarm) *tags.Tags { return v.Tags }, func(v *Alarm, t *tags.Tags) { v.Tags = t })
	restoreTagsOf(
		b.contactMethods.All(),
		"lightsail.contactmethod.",
		func(v *ContactMethod) string { return v.Protocol },
		func(v *ContactMethod) *tags.Tags { return v.Tags },
		func(v *ContactMethod, t *tags.Tags) { v.Tags = t },
	)
}

// rebuildTags returns a fresh, correctly-named tags.Tags carrying the same
// entries as old (which may be nil after a JSON round-trip lost its name).
func rebuildTags(old *tags.Tags, name string) *tags.Tags {
	if old == nil {
		return tags.New(name)
	}

	raw := old.Clone()
	old.Close()

	return tags.FromMap(name, raw)
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
