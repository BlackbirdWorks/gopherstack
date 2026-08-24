package iot

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// iotSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (the resources registered via registerAllTables in store_setup.go, plus
// the one "dirty" DTO table, topicRuleDestinations). Bump whenever a change
// there would make an older snapshot unsafe to decode; Restore compares
// this against the persisted value and discards (rather than partially
// decodes) any mismatch.
//
// Bumped 1 -> 2 (d23239d40): the otaUpdates table's OTAUpdate.Files field was
// retagged json:"files" -> json:"otaUpdateFiles" to match the real
// OTAUpdateInfo.otaUpdateFiles wire key. Same Go field, different on-disk
// key -- an older snapshot would decode Files as its zero value (nil),
// silently dropping every OTA update's file list.
//
// Bumped 2 -> 3: the scheduledAudits table's ScheduledAudit.Tags field was
// retagged json:"tags,omitempty" -> json:"-" (real DescribeScheduledAuditOutput
// has no "tags" member; harmless to drop functionally, since the canonical
// tag store is the separately-persisted resourceTags map and Tags on
// ScheduledAudit is write-only scratch state, but the field removal is a
// real on-disk shape change the guard cannot tell apart from a dangerous
// one without this bump).
const iotSnapshotVersion = 3

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
	ThingPrincipalTypes             map[string]map[string]string                  `json:"thingPrincipalTypes"`
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
		ThingPrincipalTypes:    copyNestedStringMap(b.thingPrincipalTypes),
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
	b.thingPrincipalTypes = copyNestedStringMap(snap.ThingPrincipalTypes)
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

	if snap.ThingPrincipalTypes == nil {
		snap.ThingPrincipalTypes = make(map[string]map[string]string)
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

// The helpers below each cover one group of raw (non-Table) backend state —
// slice-valued maps, nested maps, and the one "dirty" table
// (TopicRuleDestination, handled directly above via its own small DTO
// registry) — that isn't a store.Table[T] on b.registry (store_setup.go)
// and so doesn't round-trip via registry.SnapshotAll()/RestoreAll(). Each
// group gets its own bundle struct plus a snapshot/restore method pair,
// keeping Snapshot()/Restore() themselves within the cyclop/funlen limits.

// topicRuleDestSnap mirrors TopicRuleDestination for persistence purposes.
// TopicRuleDestination.ConfirmationToken is tagged json:"-" so it never
// leaks into API responses (AWS delivers it out-of-band); this shadow type
// carries it through Snapshot/Restore so a pending HTTP destination
// confirmation survives a restart instead of being silently dropped.
type topicRuleDestSnap struct {
	HTTPURLProperties *HTTPURLDestinationProperties `json:"httpUrlProperties,omitempty"`
	ARN               string                        `json:"arn"`
	Status            string                        `json:"status"`
	ConfirmationToken string                        `json:"confirmationToken,omitempty"`
}

// topicRuleDestSnapKey is the store.Table key function used for the
// ephemeral DTO table built inside Snapshot/Restore for topicRuleDestinations.
func topicRuleDestSnapKey(s *topicRuleDestSnap) string { return s.ARN }

func toTopicRuleDestSnap(d *TopicRuleDestination) *topicRuleDestSnap {
	var props *HTTPURLDestinationProperties
	if d.HTTPURLProperties != nil {
		cp := *d.HTTPURLProperties
		props = &cp
	}

	return &topicRuleDestSnap{
		HTTPURLProperties: props,
		ARN:               d.ARN,
		Status:            d.Status,
		ConfirmationToken: d.ConfirmationToken,
	}
}

func fromTopicRuleDestSnap(s *topicRuleDestSnap) *TopicRuleDestination {
	var props *HTTPURLDestinationProperties
	if s.HTTPURLProperties != nil {
		cp := *s.HTTPURLProperties
		props = &cp
	}

	return &TopicRuleDestination{
		HTTPURLProperties: props,
		ARN:               s.ARN,
		Status:            s.Status,
		ConfirmationToken: s.ConfirmationToken,
	}
}

// snapshotTopicRuleDestinationsTable builds the "dirty" topicRuleDestinations
// entry for backendSnapshot.Tables via a small throwaway DTO registry.
// TopicRuleDestination.ConfirmationToken is tagged json:"-" (AWS delivers it
// out-of-band), so registry.SnapshotAll's generic encoding would drop it;
// the topicRuleDestSnap DTO carries it through instead. Must be called with
// b.mu held (read or write).
func (b *InMemoryBackend) snapshotTopicRuleDestinationsTable() (map[string]json.RawMessage, error) {
	destDTOReg := store.NewRegistry()
	destDTOs := store.Register(destDTOReg, topicRuleDestinationsTableName, store.New(topicRuleDestSnapKey))

	for _, d := range b.topicRuleDestinations.Snapshot() {
		destDTOs.Put(toTopicRuleDestSnap(d))
	}

	tables, err := destDTOReg.SnapshotAll()
	if err != nil {
		return nil, fmt.Errorf("iot: snapshot topicRuleDestinations marshal failed: %w", err)
	}

	return tables, nil
}

// restoreTopicRuleDestinationsTable restores b.topicRuleDestinations from its
// DTO entry in tables (the inverse of snapshotTopicRuleDestinationsTable).
// Extracted from Restore to keep it within the repo's funlen limit. Must be
// called with b.mu held (write).
func (b *InMemoryBackend) restoreTopicRuleDestinationsTable(tables map[string]json.RawMessage) error {
	destDTOReg := store.NewRegistry()
	destDTOs := store.Register(destDTOReg, topicRuleDestinationsTableName, store.New(topicRuleDestSnapKey))

	if err := destDTOReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("iot: restore topicRuleDestinations: %w", err)
	}

	liveDests := make([]*TopicRuleDestination, 0, destDTOs.Len())
	for _, s := range destDTOs.All() {
		liveDests = append(liveDests, fromTopicRuleDestSnap(s))
	}

	b.topicRuleDestinations.Restore(liveDests)

	return nil
}

func clonePolicyVersion(v *PolicyVersion) *PolicyVersion {
	cp := *v

	return &cp
}

func clonePolicyVersions(src []*PolicyVersion) []*PolicyVersion {
	out := make([]*PolicyVersion, len(src))
	for i, v := range src {
		out[i] = clonePolicyVersion(v)
	}

	return out
}

func cloneProvTemplateVersion(v *ProvisioningTemplateVersion) *ProvisioningTemplateVersion {
	cp := *v

	return &cp
}

func cloneProvTemplateVersions(src []*ProvisioningTemplateVersion) []*ProvisioningTemplateVersion {
	out := make([]*ProvisioningTemplateVersion, len(src))
	for i, v := range src {
		out[i] = cloneProvTemplateVersion(v)
	}

	return out
}

func cloneIoTCommandExecution(e *IoTCommandExecution) *IoTCommandExecution {
	cp := *e

	return &cp
}

func cloneEventConfigurations(e *EventConfigurations) *EventConfigurations {
	cp := EventConfigurations{
		EventConfigurations: make(map[string]*EventConfigEntry, len(e.EventConfigurations)),
		CreationDate:        e.CreationDate,
		LastModifiedDate:    e.LastModifiedDate,
	}
	for k, v := range e.EventConfigurations {
		entry := *v
		cp.EventConfigurations[k] = &entry
	}

	return &cp
}

func cloneAccountAuditConfiguration(c *AccountAuditConfiguration) *AccountAuditConfiguration {
	cp := *c
	if c.AuditCheckConfigurations != nil {
		cp.AuditCheckConfigurations = make(map[string]*AuditCheckConfig, len(c.AuditCheckConfigurations))
		for k, v := range c.AuditCheckConfigurations {
			e := *v
			cp.AuditCheckConfigurations[k] = &e
		}
	}

	return &cp
}

func copyNestedStringMap(m map[string]map[string]string) map[string]map[string]string {
	cp := make(map[string]map[string]string, len(m))
	for k, v := range m {
		cp[k] = copyStringMap(v)
	}

	return cp
}

// ---------------------------------------------------------------------------
// Group 1: Raw thing/group/certificate-family state (everything in this
// family that is *T-valued now lives in a store.Table on b.registry instead).
// ---------------------------------------------------------------------------

// thingResourceSnapshot bundles the raw (non-Table) ThingGroup/Certificate
// -family fields of backendSnapshot so Snapshot/Restore can delegate to a
// single helper each, keeping their own cyclomatic complexity low.
type thingResourceSnapshot struct {
	ThingGroupMembers map[string][]string
	PolicyVersions    map[string][]*PolicyVersion
	ResourceTags      map[string]map[string]string
}

// snapshotThingResources deep-copies the raw thing/group/certificate state.
// Must be called with b.mu held (read or write).
func (b *InMemoryBackend) snapshotThingResources() thingResourceSnapshot {
	policyVersions := make(map[string][]*PolicyVersion, len(b.policyVersions))
	for k, v := range b.policyVersions {
		policyVersions[k] = clonePolicyVersions(v)
	}

	return thingResourceSnapshot{
		ThingGroupMembers: copyStringSliceMap(b.thingGroupMembers),
		PolicyVersions:    policyVersions,
		ResourceTags:      copyNestedStringMap(b.resourceTags),
	}
}

// restoreThingResources restores the raw thing/group/certificate state from
// a snapshot. Must be called with b.mu held (write).
func (b *InMemoryBackend) restoreThingResources(snap thingResourceSnapshot) {
	b.thingGroupMembers = copyStringSliceMap(snap.ThingGroupMembers)

	b.policyVersions = make(map[string][]*PolicyVersion, len(snap.PolicyVersions))
	for k, v := range snap.PolicyVersions {
		b.policyVersions[k] = clonePolicyVersions(v)
	}

	b.resourceTags = copyNestedStringMap(snap.ResourceTags)
}

// ensureNonNilThingResourceSnap defaults nil maps in a restored snapshot's
// raw thing/group/certificate fields to empty maps.
func ensureNonNilThingResourceSnap(snap *backendSnapshot) {
	if snap.ThingGroupMembers == nil {
		snap.ThingGroupMembers = make(map[string][]string)
	}

	if snap.PolicyVersions == nil {
		snap.PolicyVersions = make(map[string][]*PolicyVersion)
	}

	if snap.ResourceTags == nil {
		snap.ResourceTags = make(map[string]map[string]string)
	}
}

// ---------------------------------------------------------------------------
// Group 2: Raw provisioning-template state (Job/JobTemplate/RoleAlias/
// DomainConfiguration/Authorizer/BillingGroup/ProvisioningTemplate are now
// store.Table[T]s; only the slice-valued ProvTemplateVersions stays raw).
// ---------------------------------------------------------------------------

// provisioningSnapshot bundles the raw (non-Table) provisioning-template
// field of backendSnapshot.
type provisioningSnapshot struct {
	ProvTemplateVersions map[string][]*ProvisioningTemplateVersion
}

// snapshotProvisioning deep-copies the raw provisioning-template state. Must
// be called with b.mu held (read or write).
func (b *InMemoryBackend) snapshotProvisioning() provisioningSnapshot {
	provTemplateVersions := make(map[string][]*ProvisioningTemplateVersion, len(b.provTemplateVersions))
	for k, v := range b.provTemplateVersions {
		provTemplateVersions[k] = cloneProvTemplateVersions(v)
	}

	return provisioningSnapshot{
		ProvTemplateVersions: provTemplateVersions,
	}
}

// restoreProvisioning restores the raw provisioning-template state from a
// snapshot. Must be called with b.mu held (write).
func (b *InMemoryBackend) restoreProvisioning(snap provisioningSnapshot) {
	b.provTemplateVersions = make(map[string][]*ProvisioningTemplateVersion, len(snap.ProvTemplateVersions))
	for k, v := range snap.ProvTemplateVersions {
		b.provTemplateVersions[k] = cloneProvTemplateVersions(v)
	}
}

// ensureNonNilProvisioningSnap defaults nil maps in a restored snapshot's raw
// provisioning-template field to an empty map.
func ensureNonNilProvisioningSnap(snap *backendSnapshot) {
	if snap.ProvTemplateVersions == nil {
		snap.ProvTemplateVersions = make(map[string][]*ProvisioningTemplateVersion)
	}
}

// ---------------------------------------------------------------------------
// Group 3 (audit-extra) is gone: ScheduledAudit, MitigationAction,
// SecurityProfile, AuditSuppression, AuditFinding, AuditTask, and Dimension
// were its only fields and all moved to store.Table[T]s on b.registry (see
// store_setup.go), so nothing raw remains in this family.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Group 4: Raw stream/package/command/metric state (Stream, OTAUpdate,
// IoTPackage, Command, FleetMetric, CustomMetric, and V2LoggingLevel are now
// store.Table[T]s; the nested PackageVersions2 map and the commandExecutions
// map -- whose key isn't recoverable from its value -- stay raw).
// ---------------------------------------------------------------------------

// resourceMiscSnapshot bundles the raw (non-Table) stream/package/command
// /metric fields of backendSnapshot.
type resourceMiscSnapshot struct {
	PackageVersions2  map[string]map[string]*IoTPackageVersion
	CommandExecutions map[string]*IoTCommandExecution
}

// snapshotResourceMisc deep-copies the raw stream/package/command/metric
// state. Must be called with b.mu held (read or write).
func (b *InMemoryBackend) snapshotResourceMisc() resourceMiscSnapshot {
	packageVersions2 := make(map[string]map[string]*IoTPackageVersion, len(b.packageVersions2))
	for pkg, versions := range b.packageVersions2 {
		cp := make(map[string]*IoTPackageVersion, len(versions))
		for name, v := range versions {
			cp[name] = cloneIoTPackageVersion(v)
		}
		packageVersions2[pkg] = cp
	}

	commandExecutions := make(map[string]*IoTCommandExecution, len(b.commandExecutions))
	for k, v := range b.commandExecutions {
		commandExecutions[k] = cloneIoTCommandExecution(v)
	}

	return resourceMiscSnapshot{
		PackageVersions2:  packageVersions2,
		CommandExecutions: commandExecutions,
	}
}

// restoreResourceMisc restores the raw stream/package/command/metric state
// from a snapshot. Must be called with b.mu held (write).
func (b *InMemoryBackend) restoreResourceMisc(snap resourceMiscSnapshot) {
	b.packageVersions2 = make(map[string]map[string]*IoTPackageVersion, len(snap.PackageVersions2))
	for pkg, versions := range snap.PackageVersions2 {
		cp := make(map[string]*IoTPackageVersion, len(versions))
		for name, v := range versions {
			cp[name] = cloneIoTPackageVersion(v)
		}
		b.packageVersions2[pkg] = cp
	}

	b.commandExecutions = make(map[string]*IoTCommandExecution, len(snap.CommandExecutions))
	for k, v := range snap.CommandExecutions {
		b.commandExecutions[k] = cloneIoTCommandExecution(v)
	}
}

// ensureNonNilResourceMiscSnap defaults nil maps in a restored snapshot's raw
// stream/package/command/metric fields to empty maps.
func ensureNonNilResourceMiscSnap(snap *backendSnapshot) {
	if snap.PackageVersions2 == nil {
		snap.PackageVersions2 = make(map[string]map[string]*IoTPackageVersion)
	}

	if snap.CommandExecutions == nil {
		snap.CommandExecutions = make(map[string]*IoTCommandExecution)
	}
}

// ---------------------------------------------------------------------------
// Group 5: Account-level singleton configuration.
// ---------------------------------------------------------------------------

// configSnapshot bundles the account-level singleton configuration fields
// of backendSnapshot so Snapshot/Restore can delegate to a single helper
// each.
type configSnapshot struct {
	AuditConfiguration  *AccountAuditConfiguration
	PackageConfig       *PackageConfiguration
	V2LoggingOptions    *V2LoggingOptions
	LoggingOptions      *LoggingOptions
	EventConfigurations *EventConfigurations
	RegistrationCode    string
	DefaultAuthorizer   string
}

// snapshotConfig deep-copies the account-level singleton configuration.
// Must be called with b.mu held (read or write).
func (b *InMemoryBackend) snapshotConfig() configSnapshot {
	var auditConfiguration *AccountAuditConfiguration
	if b.auditConfiguration != nil {
		auditConfiguration = cloneAccountAuditConfiguration(b.auditConfiguration)
	}

	var packageConfig *PackageConfiguration
	if b.packageConfig != nil {
		cp := *b.packageConfig
		cp.VersionUpdateByJobsConfig = maps.Clone(b.packageConfig.VersionUpdateByJobsConfig)
		packageConfig = &cp
	}

	var v2LoggingOptions *V2LoggingOptions
	if b.v2LoggingOptions != nil {
		cp := *b.v2LoggingOptions
		v2LoggingOptions = &cp
	}

	var loggingOptions *LoggingOptions
	if b.loggingOptions != nil {
		cp := *b.loggingOptions
		loggingOptions = &cp
	}

	var eventConfigurations *EventConfigurations
	if b.eventConfigurations != nil {
		eventConfigurations = cloneEventConfigurations(b.eventConfigurations)
	}

	return configSnapshot{
		AuditConfiguration:  auditConfiguration,
		PackageConfig:       packageConfig,
		V2LoggingOptions:    v2LoggingOptions,
		LoggingOptions:      loggingOptions,
		EventConfigurations: eventConfigurations,
		RegistrationCode:    b.registrationCode,
		DefaultAuthorizer:   b.defaultAuthorizer,
	}
}

// restoreConfig restores the account-level singleton configuration from a
// snapshot. Must be called with b.mu held (write).
func (b *InMemoryBackend) restoreConfig(snap configSnapshot) {
	if snap.AuditConfiguration != nil {
		b.auditConfiguration = cloneAccountAuditConfiguration(snap.AuditConfiguration)
	} else {
		b.auditConfiguration = nil
	}

	if snap.PackageConfig != nil {
		cp := *snap.PackageConfig
		cp.VersionUpdateByJobsConfig = maps.Clone(snap.PackageConfig.VersionUpdateByJobsConfig)
		b.packageConfig = &cp
	} else {
		b.packageConfig = nil
	}

	if snap.V2LoggingOptions != nil {
		cp := *snap.V2LoggingOptions
		b.v2LoggingOptions = &cp
	} else {
		b.v2LoggingOptions = nil
	}

	if snap.LoggingOptions != nil {
		cp := *snap.LoggingOptions
		b.loggingOptions = &cp
	} else {
		b.loggingOptions = nil
	}

	if snap.EventConfigurations != nil {
		b.eventConfigurations = cloneEventConfigurations(snap.EventConfigurations)
	} else {
		b.eventConfigurations = nil
	}

	b.registrationCode = snap.RegistrationCode
	b.defaultAuthorizer = snap.DefaultAuthorizer
}

// finalOpsSnapshot holds the deep-copied final-stub-batch state for
// persistence, mirroring the deviceDefenderSnapshot pattern.
type finalOpsSnapshot struct {
	AccountEncryptionConfig   *AccountEncryptionConfiguration
	SbomValidationResults     map[string][]*SbomValidationResult
	MetricValues              map[string][]*MetricDatapoint
	ThingConnectivity         map[string]*ThingConnectivityData
	BehaviorTrainingSummaries map[string][]*BehaviorModelTrainingSummary
}

// snapshotFinalOps deep-copies all final-stub-batch state. Must be called
// with b.mu held (read or write).
func (b *InMemoryBackend) snapshotFinalOps() finalOpsSnapshot {
	var accountEncryptionConfig *AccountEncryptionConfiguration
	if b.accountEncryptionConfig != nil {
		cp := *b.accountEncryptionConfig
		accountEncryptionConfig = &cp
	}

	sbomValidationResults := make(map[string][]*SbomValidationResult, len(b.sbomValidationResults))
	for k, v := range b.sbomValidationResults {
		sbomValidationResults[k] = cloneSbomValidationResults(v)
	}

	metricValues := make(map[string][]*MetricDatapoint, len(b.metricValues))
	for k, v := range b.metricValues {
		metricValues[k] = cloneMetricDatapoints(v)
	}

	thingConnectivity := make(map[string]*ThingConnectivityData, len(b.thingConnectivity))
	for k, v := range b.thingConnectivity {
		cp := *v
		thingConnectivity[k] = &cp
	}

	behaviorTrainingSummaries := make(map[string][]*BehaviorModelTrainingSummary, len(b.behaviorTrainingSummaries))
	for k, v := range b.behaviorTrainingSummaries {
		behaviorTrainingSummaries[k] = cloneBehaviorTrainingSummaries(v)
	}

	return finalOpsSnapshot{
		AccountEncryptionConfig:   accountEncryptionConfig,
		SbomValidationResults:     sbomValidationResults,
		MetricValues:              metricValues,
		ThingConnectivity:         thingConnectivity,
		BehaviorTrainingSummaries: behaviorTrainingSummaries,
	}
}

// restoreFinalOps restores final-stub-batch state from a snapshot. Must be
// called with b.mu held (write).
func (b *InMemoryBackend) restoreFinalOps(snap finalOpsSnapshot) {
	if snap.AccountEncryptionConfig != nil {
		cp := *snap.AccountEncryptionConfig
		b.accountEncryptionConfig = &cp
	} else {
		b.accountEncryptionConfig = nil
	}

	b.sbomValidationResults = make(map[string][]*SbomValidationResult, len(snap.SbomValidationResults))
	for k, v := range snap.SbomValidationResults {
		b.sbomValidationResults[k] = cloneSbomValidationResults(v)
	}

	b.metricValues = make(map[string][]*MetricDatapoint, len(snap.MetricValues))
	for k, v := range snap.MetricValues {
		b.metricValues[k] = cloneMetricDatapoints(v)
	}

	b.thingConnectivity = make(map[string]*ThingConnectivityData, len(snap.ThingConnectivity))
	for k, v := range snap.ThingConnectivity {
		cp := *v
		b.thingConnectivity[k] = &cp
	}

	b.behaviorTrainingSummaries = make(map[string][]*BehaviorModelTrainingSummary, len(snap.BehaviorTrainingSummaries))
	for k, v := range snap.BehaviorTrainingSummaries {
		b.behaviorTrainingSummaries[k] = cloneBehaviorTrainingSummaries(v)
	}
}

func cloneSbomValidationResults(src []*SbomValidationResult) []*SbomValidationResult {
	out := make([]*SbomValidationResult, len(src))
	for i, r := range src {
		cp := *r
		out[i] = &cp
	}

	return out
}

func cloneMetricDatapoints(src []*MetricDatapoint) []*MetricDatapoint {
	out := make([]*MetricDatapoint, len(src))
	for i, dp := range src {
		cp := *dp
		out[i] = &cp
	}

	return out
}

func cloneBehaviorTrainingSummaries(src []*BehaviorModelTrainingSummary) []*BehaviorModelTrainingSummary {
	out := make([]*BehaviorModelTrainingSummary, len(src))
	for i, s := range src {
		cp := *s
		out[i] = &cp
	}

	return out
}

// ensureNonNilFinalOpsSnap defaults nil maps in a restored snapshot's
// final-stub-batch fields to empty maps.
func ensureNonNilFinalOpsSnap(snap *backendSnapshot) {
	if snap.SbomValidationResults == nil {
		snap.SbomValidationResults = make(map[string][]*SbomValidationResult)
	}

	if snap.MetricValues == nil {
		snap.MetricValues = make(map[string][]*MetricDatapoint)
	}

	if snap.ThingConnectivity == nil {
		snap.ThingConnectivity = make(map[string]*ThingConnectivityData)
	}

	if snap.BehaviorTrainingSummaries == nil {
		snap.BehaviorTrainingSummaries = make(map[string][]*BehaviorModelTrainingSummary)
	}
}
