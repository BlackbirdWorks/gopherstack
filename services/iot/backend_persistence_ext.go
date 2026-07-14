package iot

import (
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// ---------------------------------------------------------------------------
// This file closes a persistence gap (gopherstack-264): several live
// InMemoryBackend maps were never wired into backendSnapshot, so they
// silently dropped on Snapshot/Restore. The helpers below mirror the
// snapshotDeviceDefender/snapshotFinalOps pattern already used in
// backend_devicedefender.go and backend_final_ops.go: each group of related
// fields gets its own bundle struct plus a pair of snapshot/restore methods,
// keeping Snapshot()/Restore() themselves within the cyclop/funlen limits.
//
// Phase 3.3 note: most of the *T-valued maps these groups used to carry
// (ThingTypes, ThingGroups, Certificates, CertificateProviders,
// CACertificates, Jobs, JobExecutions, JobTemplates, RoleAliases,
// DomainConfigs, Authorizers, BillingGroups, ProvTemplates,
// ScheduledAudits, MitigationActions, SecurityProfiles, AuditSuppressions,
// AuditFindings, AuditTaskObjects, Dimensions, Streams, OTAUpdates,
// IoTPackages, Commands, FleetMetrics, CustomMetrics, V2LoggingLevels) moved
// to store.Table[T]s registered on b.registry (see store_setup.go) and now
// round-trip via registry.SnapshotAll()/RestoreAll() in persistence.go
// instead of through these bundles. What remains here is exactly the raw
// (non-Table) state: slice-valued maps, nested maps, and the one "dirty"
// table (TopicRuleDestination, handled directly in persistence.go via its
// own small DTO registry, mirroring the services/sqs pilot).
// ---------------------------------------------------------------------------

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
// entry for backendSnapshot.Tables via a small throwaway DTO registry,
// mirroring the services/sqs pilot's DTO-registry pattern but scoped to just
// this one table. TopicRuleDestination.ConfirmationToken is tagged json:"-"
// (AWS delivers it out-of-band and it must never leak into API responses),
// so registry.SnapshotAll's generic per-table encoding of the live type would
// otherwise silently drop it; the topicRuleDestSnap DTO carries it through
// instead. Extracted from Snapshot to keep it within the repo's funlen limit.
// Must be called with b.mu held (read or write).
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
