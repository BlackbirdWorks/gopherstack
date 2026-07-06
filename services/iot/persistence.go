package iot

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// iotSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set/shape of resources registered on b.registry -- see
// registerAllTables in store_setup.go -- plus the one "dirty" DTO table,
// topicRuleDestinations). It must be bumped whenever a change there would
// make an older snapshot unsafe to decode as the current shape. Restore
// compares this against the persisted value and discards (rather than
// attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/ec2 (12e611a4) and services/sqs (0f09d77c) pilots.
const iotSnapshotVersion = 1

type backendSnapshot struct {
	Tables                          map[string]json.RawMessage                    `json:"tables"`
	AuditTasks                      map[string]string                             `json:"auditTasks"`
	MetricValues                    map[string][]*MetricDatapoint                 `json:"metricValues"`
	CertificateTransfers            map[string]string                             `json:"certificateTransfers"`
	ThingBillingGroups              map[string]string                             `json:"thingBillingGroups"`
	ThingThingGroups                map[string][]string                           `json:"thingThingGroups"`
	PackageVersionSboms             map[string]*SbomDocument                      `json:"packageVersionSboms"`
	JobTargets                      map[string][]string                           `json:"jobTargets"`
	PolicyTargets                   map[string][]string                           `json:"policyTargets"`
	SecurityProfileTargets          map[string][]string                           `json:"securityProfileTargets"`
	ThingPrincipals                 map[string][]string                           `json:"thingPrincipals"`
	ThingGroupIndexingConfiguration *ThingGroupIndexingConfiguration              `json:"thingGroupIndexingConfiguration"`
	AuditMitigationTasks            map[string]string                             `json:"auditMitigationTasks"`
	AuditMitigationExecutions       map[string][]*AuditMitigationActionExecution  `json:"auditMitigationExecutions"`
	DetectMitigationExecutions      map[string][]*DetectMitigationActionExecution `json:"detectMitigationExecutions"`
	BehaviorTrainingSummaries       map[string][]*BehaviorModelTrainingSummary    `json:"behaviorTrainingSummaries"`
	AccountEncryptionConfig         *AccountEncryptionConfiguration               `json:"accountEncryptionConfig"`
	SbomValidationResults           map[string][]*SbomValidationResult            `json:"sbomValidationResults"`
	ThingIndexingConfiguration      *ThingIndexingConfiguration                   `json:"thingIndexingConfiguration"`
	ThingConnectivity               map[string]*ThingConnectivityData             `json:"thingConnectivity"`
	ThingGroupMembers               map[string][]string                           `json:"thingGroupMembers"`
	PolicyVersions                  map[string][]*PolicyVersion                   `json:"policyVersions"`
	ResourceTags                    map[string]map[string]string                  `json:"resourceTags"`
	ProvTemplateVersions            map[string][]*ProvisioningTemplateVersion     `json:"provTemplateVersions"`
	PackageVersions2                map[string]map[string]*IoTPackageVersion      `json:"packageVersions2"`
	CommandExecutions               map[string]*IoTCommandExecution               `json:"commandExecutions"`
	AuditConfiguration              *AccountAuditConfiguration                    `json:"auditConfiguration"`
	PackageConfig                   *PackageConfiguration                         `json:"packageConfig"`
	V2LoggingOptions                *V2LoggingOptions                             `json:"v2LoggingOptions"`
	LoggingOptions                  *LoggingOptions                               `json:"loggingOptions"`
	EventConfigurations             *EventConfigurations                          `json:"eventConfigurations"`
	RegistrationCode                string                                        `json:"registrationCode"`
	DefaultAuthorizer               string                                        `json:"defaultAuthorizer"`
	ViolationEvents                 []*ViolationEvent                             `json:"violationEvents"`
	Version                         int                                           `json:"version"`
}

// topicRuleDestinationsTableName is the Tables blob key used for the
// topicRuleDestinations DTO entry, both in the ephemeral DTO registry built
// by Snapshot/Restore below and as the map key inside backendSnapshot.Tables.
const topicRuleDestinationsTableName = "topicRuleDestinations"

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "iot: snapshot table marshal failed", "error", err)

		return nil
	}

	destTables, err := b.snapshotTopicRuleDestinationsTable()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "iot: snapshot table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, destTables)

	ddSnap := b.snapshotDeviceDefender()
	finalSnap := b.snapshotFinalOps()
	thingResSnap := b.snapshotThingResources()
	provSnap := b.snapshotProvisioning()
	miscSnap := b.snapshotResourceMisc()
	cfgSnap := b.snapshotConfig()

	sboms := make(map[string]*SbomDocument, len(b.packageVersionSboms))
	for k, v := range b.packageVersionSboms {
		sboms[k] = cloneSbomDocument(v)
	}

	var thingIndexingConfig *ThingIndexingConfiguration
	if b.thingIndexingConfig != nil {
		thingIndexingConfig = cloneThingIndexingConfiguration(b.thingIndexingConfig)
	}

	var thingGroupIndexingConfig *ThingGroupIndexingConfiguration
	if b.thingGroupIndexingConfig != nil {
		thingGroupIndexingConfig = cloneThingGroupIndexingConfiguration(b.thingGroupIndexingConfig)
	}

	snap := backendSnapshot{
		Version:                iotSnapshotVersion,
		Tables:                 tables,
		CertificateTransfers:   copyStringMap(b.certificateTransfers),
		ThingBillingGroups:     copyStringMap(b.thingBillingGroups),
		ThingThingGroups:       copyStringSliceMap(b.thingThingGroups),
		PackageVersionSboms:    sboms,
		JobTargets:             copyStringSliceMap(b.jobTargets),
		PolicyTargets:          copyStringSliceMap(b.policyTargets),
		SecurityProfileTargets: copyStringSliceMap(b.securityProfileTargets),
		ThingPrincipals:        copyStringSliceMap(b.thingPrincipals),
		AuditMitigationTasks:   copyStringMap(b.auditMitigationTasks),
		AuditTasks:             copyStringMap(b.auditTasks),

		ThingIndexingConfiguration:      thingIndexingConfig,
		ThingGroupIndexingConfiguration: thingGroupIndexingConfig,

		AuditMitigationExecutions:  ddSnap.AuditMitigationExecutions,
		DetectMitigationExecutions: ddSnap.DetectMitigationExecutions,
		ViolationEvents:            ddSnap.ViolationEvents,

		AccountEncryptionConfig:   finalSnap.AccountEncryptionConfig,
		SbomValidationResults:     finalSnap.SbomValidationResults,
		MetricValues:              finalSnap.MetricValues,
		ThingConnectivity:         finalSnap.ThingConnectivity,
		BehaviorTrainingSummaries: finalSnap.BehaviorTrainingSummaries,

		ThingGroupMembers: thingResSnap.ThingGroupMembers,
		PolicyVersions:    thingResSnap.PolicyVersions,
		ResourceTags:      thingResSnap.ResourceTags,

		ProvTemplateVersions: provSnap.ProvTemplateVersions,

		PackageVersions2:  miscSnap.PackageVersions2,
		CommandExecutions: miscSnap.CommandExecutions,

		AuditConfiguration:  cfgSnap.AuditConfiguration,
		PackageConfig:       cfgSnap.PackageConfig,
		V2LoggingOptions:    cfgSnap.V2LoggingOptions,
		LoggingOptions:      cfgSnap.LoggingOptions,
		EventConfigurations: cfgSnap.EventConfigurations,
		RegistrationCode:    cfgSnap.RegistrationCode,
		DefaultAuthorizer:   cfgSnap.DefaultAuthorizer,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "iot: failed to snapshot backend state", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "iot", data, &snap); err != nil {
		return err
	}

	ensureNonNilSnap(&snap)

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Version != iotSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/ec2/sqs pilots.
		logger.Load(ctx).WarnContext(ctx,
			"iot: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", iotSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("iot: restore snapshot tables: %w", err)
	}

	// Re-derive topicRuleDestinations from its DTO entry so ConfirmationToken
	// (dropped by the generic per-table decode above, since it is tagged
	// json:"-" on the live type) round-trips. See Snapshot's comment.
	if err := b.restoreTopicRuleDestinationsTable(snap.Tables); err != nil {
		return err
	}

	b.certificateTransfers = copyStringMap(snap.CertificateTransfers)
	b.thingBillingGroups = copyStringMap(snap.ThingBillingGroups)
	b.thingThingGroups = copyStringSliceMap(snap.ThingThingGroups)
	b.packageVersionSboms = make(map[string]*SbomDocument, len(snap.PackageVersionSboms))

	for k, v := range snap.PackageVersionSboms {
		b.packageVersionSboms[k] = cloneSbomDocument(v)
	}

	b.jobTargets = copyStringSliceMap(snap.JobTargets)
	b.policyTargets = copyStringSliceMap(snap.PolicyTargets)
	b.securityProfileTargets = copyStringSliceMap(snap.SecurityProfileTargets)
	b.thingPrincipals = copyStringSliceMap(snap.ThingPrincipals)
	b.auditMitigationTasks = copyStringMap(snap.AuditMitigationTasks)
	b.auditTasks = copyStringMap(snap.AuditTasks)

	if snap.ThingIndexingConfiguration != nil {
		b.thingIndexingConfig = cloneThingIndexingConfiguration(snap.ThingIndexingConfiguration)
	} else {
		b.thingIndexingConfig = nil
	}

	if snap.ThingGroupIndexingConfiguration != nil {
		b.thingGroupIndexingConfig = cloneThingGroupIndexingConfiguration(snap.ThingGroupIndexingConfiguration)
	} else {
		b.thingGroupIndexingConfig = nil
	}

	b.restoreDeviceDefender(deviceDefenderSnapshot{
		AuditMitigationExecutions:  snap.AuditMitigationExecutions,
		DetectMitigationExecutions: snap.DetectMitigationExecutions,
		ViolationEvents:            snap.ViolationEvents,
	})

	b.restoreFinalOps(finalOpsSnapshot{
		AccountEncryptionConfig:   snap.AccountEncryptionConfig,
		SbomValidationResults:     snap.SbomValidationResults,
		MetricValues:              snap.MetricValues,
		ThingConnectivity:         snap.ThingConnectivity,
		BehaviorTrainingSummaries: snap.BehaviorTrainingSummaries,
	})

	b.restoreThingResources(thingResourceSnapshot{
		ThingGroupMembers: snap.ThingGroupMembers,
		PolicyVersions:    snap.PolicyVersions,
		ResourceTags:      snap.ResourceTags,
	})
	b.restoreProvisioning(provisioningSnapshot{ProvTemplateVersions: snap.ProvTemplateVersions})
	b.restoreResourceMisc(resourceMiscSnapshot{
		PackageVersions2:  snap.PackageVersions2,
		CommandExecutions: snap.CommandExecutions,
	})
	b.restoreConfig(configSnapshot{
		AuditConfiguration:  snap.AuditConfiguration,
		PackageConfig:       snap.PackageConfig,
		V2LoggingOptions:    snap.V2LoggingOptions,
		LoggingOptions:      snap.LoggingOptions,
		EventConfigurations: snap.EventConfigurations,
		RegistrationCode:    snap.RegistrationCode,
		DefaultAuthorizer:   snap.DefaultAuthorizer,
	})

	return nil
}

func ensureNonNilSnap(snap *backendSnapshot) {
	if snap.CertificateTransfers == nil {
		snap.CertificateTransfers = make(map[string]string)
	}

	if snap.ThingBillingGroups == nil {
		snap.ThingBillingGroups = make(map[string]string)
	}

	if snap.ThingThingGroups == nil {
		snap.ThingThingGroups = make(map[string][]string)
	}

	if snap.PackageVersionSboms == nil {
		snap.PackageVersionSboms = make(map[string]*SbomDocument)
	}

	if snap.JobTargets == nil {
		snap.JobTargets = make(map[string][]string)
	}

	if snap.PolicyTargets == nil {
		snap.PolicyTargets = make(map[string][]string)
	}

	if snap.SecurityProfileTargets == nil {
		snap.SecurityProfileTargets = make(map[string][]string)
	}

	if snap.ThingPrincipals == nil {
		snap.ThingPrincipals = make(map[string][]string)
	}

	if snap.AuditMitigationTasks == nil {
		snap.AuditMitigationTasks = make(map[string]string)
	}

	if snap.AuditTasks == nil {
		snap.AuditTasks = make(map[string]string)
	}

	ensureNonNilFinalOpsSnap(snap)
	ensureNonNilThingResourceSnap(snap)
	ensureNonNilProvisioningSnap(snap)
	ensureNonNilResourceMiscSnap(snap)
}

func copyStringMap(m map[string]string) map[string]string {
	cp := make(map[string]string, len(m))
	maps.Copy(cp, m)

	return cp
}

func copyStringSliceMap(m map[string][]string) map[string][]string {
	cp := make(map[string][]string, len(m))

	for k, v := range m {
		s := make([]string, len(v))
		copy(s, v)
		cp[k] = s
	}

	return cp
}

// Snapshot implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Returns nil for non-snapshottable backends.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Non-snapshottable backends are skipped.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(ctx, data)
	}

	return nil
}
