package iot

import "maps"

// ---------------------------------------------------------------------------
// This file closes a persistence gap (gopherstack-264): several live
// InMemoryBackend maps were never wired into backendSnapshot, so they
// silently dropped on Snapshot/Restore. The helpers below mirror the
// snapshotDeviceDefender/snapshotFinalOps pattern already used in
// backend_devicedefender.go and backend_final_ops.go: each group of related
// fields gets its own bundle struct plus a pair of snapshot/restore methods,
// keeping Snapshot()/Restore() themselves within the cyclop/funlen limits.
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

func cloneThingType(t *ThingType) *ThingType {
	cp := *t
	cp.SearchableAttributes = append([]string(nil), t.SearchableAttributes...)

	return &cp
}

func cloneThingGroup(g *ThingGroup) *ThingGroup {
	cp := *g
	if g.Attributes != nil {
		cp.Attributes = maps.Clone(g.Attributes)
	}

	cp.Members = append([]string(nil), g.Members...)

	return &cp
}

func cloneCertificate(c *Certificate) *Certificate {
	cp := *c

	return &cp
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

func cloneCertificateProvider(p *CertificateProvider) *CertificateProvider {
	cp := *p
	cp.AccountDefaultForOperations = append([]string(nil), p.AccountDefaultForOperations...)

	return &cp
}

func cloneJobExecution(e *JobExecution) *JobExecution {
	cp := *e

	return &cp
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

func cloneAuditTaskObj(t *AuditTask) *AuditTask {
	cp := *t

	return &cp
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

// applyExtSnapshot copies the five gopherstack-264 group snapshots onto snap.
// Extracted from Snapshot() to keep it within the repo's funlen limit.
func applyExtSnapshot(
	snap *backendSnapshot,
	thingRes thingResourceSnapshot,
	prov provisioningSnapshot,
	auditExtra auditExtraSnapshot,
	misc resourceMiscSnapshot,
	cfg configSnapshot,
) {
	snap.ThingTypes = thingRes.ThingTypes
	snap.ThingGroups = thingRes.ThingGroups
	snap.ThingGroupMembers = thingRes.ThingGroupMembers
	snap.Certificates = thingRes.Certificates
	snap.CertificateProviders = thingRes.CertificateProviders
	snap.CACertificates = thingRes.CACertificates
	snap.PolicyVersions = thingRes.PolicyVersions
	snap.TopicRuleDestinations = thingRes.TopicRuleDestinations
	snap.ResourceTags = thingRes.ResourceTags

	snap.Jobs = prov.Jobs
	snap.JobExecutions = prov.JobExecutions
	snap.JobTemplates = prov.JobTemplates
	snap.RoleAliases = prov.RoleAliases
	snap.DomainConfigs = prov.DomainConfigs
	snap.Authorizers = prov.Authorizers
	snap.BillingGroups = prov.BillingGroups
	snap.ProvTemplates = prov.ProvTemplates
	snap.ProvTemplateVersions = prov.ProvTemplateVersions

	snap.ScheduledAudits = auditExtra.ScheduledAudits
	snap.MitigationActions = auditExtra.MitigationActions
	snap.SecurityProfiles = auditExtra.SecurityProfiles
	snap.AuditSuppressions = auditExtra.AuditSuppressions
	snap.AuditFindings = auditExtra.AuditFindings
	snap.AuditTaskObjects = auditExtra.AuditTaskObjects
	snap.Dimensions = auditExtra.Dimensions

	snap.Streams = misc.Streams
	snap.OTAUpdates = misc.OTAUpdates
	snap.IoTPackages = misc.IoTPackages
	snap.PackageVersions2 = misc.PackageVersions2
	snap.Commands = misc.Commands
	snap.CommandExecutions = misc.CommandExecutions
	snap.FleetMetrics = misc.FleetMetrics
	snap.CustomMetrics = misc.CustomMetrics
	snap.V2LoggingLevels = misc.V2LoggingLevels

	snap.AuditConfiguration = cfg.AuditConfiguration
	snap.PackageConfig = cfg.PackageConfig
	snap.V2LoggingOptions = cfg.V2LoggingOptions
	snap.LoggingOptions = cfg.LoggingOptions
	snap.EventConfigurations = cfg.EventConfigurations
	snap.RegistrationCode = cfg.RegistrationCode
	snap.DefaultAuthorizer = cfg.DefaultAuthorizer
}

// extGroupsFromSnapshot extracts the five gopherstack-264 group snapshots
// from a restored backendSnapshot. Extracted from Restore() to keep it
// within the repo's funlen limit.
func extGroupsFromSnapshot(
	snap *backendSnapshot,
) (thingResourceSnapshot, provisioningSnapshot, auditExtraSnapshot, resourceMiscSnapshot, configSnapshot) {
	thingRes := thingResourceSnapshot{
		ThingTypes:            snap.ThingTypes,
		ThingGroups:           snap.ThingGroups,
		ThingGroupMembers:     snap.ThingGroupMembers,
		Certificates:          snap.Certificates,
		CertificateProviders:  snap.CertificateProviders,
		CACertificates:        snap.CACertificates,
		PolicyVersions:        snap.PolicyVersions,
		TopicRuleDestinations: snap.TopicRuleDestinations,
		ResourceTags:          snap.ResourceTags,
	}

	prov := provisioningSnapshot{
		Jobs:                 snap.Jobs,
		JobExecutions:        snap.JobExecutions,
		JobTemplates:         snap.JobTemplates,
		RoleAliases:          snap.RoleAliases,
		DomainConfigs:        snap.DomainConfigs,
		Authorizers:          snap.Authorizers,
		BillingGroups:        snap.BillingGroups,
		ProvTemplates:        snap.ProvTemplates,
		ProvTemplateVersions: snap.ProvTemplateVersions,
	}

	auditExtra := auditExtraSnapshot{
		ScheduledAudits:   snap.ScheduledAudits,
		MitigationActions: snap.MitigationActions,
		SecurityProfiles:  snap.SecurityProfiles,
		AuditSuppressions: snap.AuditSuppressions,
		AuditFindings:     snap.AuditFindings,
		AuditTaskObjects:  snap.AuditTaskObjects,
		Dimensions:        snap.Dimensions,
	}

	misc := resourceMiscSnapshot{
		Streams:           snap.Streams,
		OTAUpdates:        snap.OTAUpdates,
		IoTPackages:       snap.IoTPackages,
		PackageVersions2:  snap.PackageVersions2,
		Commands:          snap.Commands,
		CommandExecutions: snap.CommandExecutions,
		FleetMetrics:      snap.FleetMetrics,
		CustomMetrics:     snap.CustomMetrics,
		V2LoggingLevels:   snap.V2LoggingLevels,
	}

	cfg := configSnapshot{
		AuditConfiguration:  snap.AuditConfiguration,
		PackageConfig:       snap.PackageConfig,
		V2LoggingOptions:    snap.V2LoggingOptions,
		LoggingOptions:      snap.LoggingOptions,
		EventConfigurations: snap.EventConfigurations,
		RegistrationCode:    snap.RegistrationCode,
		DefaultAuthorizer:   snap.DefaultAuthorizer,
	}

	return thingRes, prov, auditExtra, misc, cfg
}

// ---------------------------------------------------------------------------
// Group 1: Things, groups, and certificates.
// ---------------------------------------------------------------------------

// thingResourceSnapshot bundles ThingType/ThingGroup/Certificate-family
// fields of backendSnapshot so Snapshot/Restore can delegate to a single
// helper each, keeping their own cyclomatic complexity low.
type thingResourceSnapshot struct {
	ThingTypes            map[string]*ThingType
	ThingGroups           map[string]*ThingGroup
	ThingGroupMembers     map[string][]string
	Certificates          map[string]*Certificate
	CertificateProviders  map[string]*CertificateProvider
	CACertificates        map[string]*CACertificate
	PolicyVersions        map[string][]*PolicyVersion
	TopicRuleDestinations map[string]*topicRuleDestSnap
	ResourceTags          map[string]map[string]string
}

// snapshotThingResources deep-copies all thing/group/certificate state. Must
// be called with b.mu held (read or write).
func (b *InMemoryBackend) snapshotThingResources() thingResourceSnapshot {
	thingTypes := make(map[string]*ThingType, len(b.thingTypes))
	for k, v := range b.thingTypes {
		thingTypes[k] = cloneThingType(v)
	}

	thingGroups := make(map[string]*ThingGroup, len(b.thingGroups))
	for k, v := range b.thingGroups {
		thingGroups[k] = cloneThingGroup(v)
	}

	certificates := make(map[string]*Certificate, len(b.certificates))
	for k, v := range b.certificates {
		certificates[k] = cloneCertificate(v)
	}

	certProviders := make(map[string]*CertificateProvider, len(b.certificateProviders))
	for k, v := range b.certificateProviders {
		certProviders[k] = cloneCertificateProvider(v)
	}

	caCerts := make(map[string]*CACertificate, len(b.caCertificates))
	for k, v := range b.caCertificates {
		caCerts[k] = cloneCACert(v)
	}

	policyVersions := make(map[string][]*PolicyVersion, len(b.policyVersions))
	for k, v := range b.policyVersions {
		policyVersions[k] = clonePolicyVersions(v)
	}

	destinations := make(map[string]*topicRuleDestSnap, len(b.topicRuleDestinations))
	for k, v := range b.topicRuleDestinations {
		destinations[k] = toTopicRuleDestSnap(v)
	}

	return thingResourceSnapshot{
		ThingTypes:            thingTypes,
		ThingGroups:           thingGroups,
		ThingGroupMembers:     copyStringSliceMap(b.thingGroupMembers),
		Certificates:          certificates,
		CertificateProviders:  certProviders,
		CACertificates:        caCerts,
		PolicyVersions:        policyVersions,
		TopicRuleDestinations: destinations,
		ResourceTags:          copyNestedStringMap(b.resourceTags),
	}
}

// restoreThingResources restores thing/group/certificate state from a
// snapshot. Must be called with b.mu held (write).
func (b *InMemoryBackend) restoreThingResources(snap thingResourceSnapshot) {
	b.thingTypes = make(map[string]*ThingType, len(snap.ThingTypes))
	for k, v := range snap.ThingTypes {
		b.thingTypes[k] = cloneThingType(v)
	}

	b.thingGroups = make(map[string]*ThingGroup, len(snap.ThingGroups))
	for k, v := range snap.ThingGroups {
		b.thingGroups[k] = cloneThingGroup(v)
	}

	b.thingGroupMembers = copyStringSliceMap(snap.ThingGroupMembers)

	b.certificates = make(map[string]*Certificate, len(snap.Certificates))
	for k, v := range snap.Certificates {
		b.certificates[k] = cloneCertificate(v)
	}

	b.certificateProviders = make(map[string]*CertificateProvider, len(snap.CertificateProviders))
	for k, v := range snap.CertificateProviders {
		b.certificateProviders[k] = cloneCertificateProvider(v)
	}

	b.caCertificates = make(map[string]*CACertificate, len(snap.CACertificates))
	for k, v := range snap.CACertificates {
		b.caCertificates[k] = cloneCACert(v)
	}

	b.policyVersions = make(map[string][]*PolicyVersion, len(snap.PolicyVersions))
	for k, v := range snap.PolicyVersions {
		b.policyVersions[k] = clonePolicyVersions(v)
	}

	b.topicRuleDestinations = make(map[string]*TopicRuleDestination, len(snap.TopicRuleDestinations))
	for k, v := range snap.TopicRuleDestinations {
		b.topicRuleDestinations[k] = fromTopicRuleDestSnap(v)
	}

	b.resourceTags = copyNestedStringMap(snap.ResourceTags)
}

// ensureNonNilThingResourceSnap defaults nil maps in a restored snapshot's
// thing/group/certificate fields to empty maps.
func ensureNonNilThingResourceSnap(snap *backendSnapshot) {
	if snap.ThingTypes == nil {
		snap.ThingTypes = make(map[string]*ThingType)
	}

	if snap.ThingGroups == nil {
		snap.ThingGroups = make(map[string]*ThingGroup)
	}

	if snap.ThingGroupMembers == nil {
		snap.ThingGroupMembers = make(map[string][]string)
	}

	if snap.Certificates == nil {
		snap.Certificates = make(map[string]*Certificate)
	}

	if snap.CertificateProviders == nil {
		snap.CertificateProviders = make(map[string]*CertificateProvider)
	}

	if snap.CACertificates == nil {
		snap.CACertificates = make(map[string]*CACertificate)
	}

	if snap.PolicyVersions == nil {
		snap.PolicyVersions = make(map[string][]*PolicyVersion)
	}

	if snap.TopicRuleDestinations == nil {
		snap.TopicRuleDestinations = make(map[string]*topicRuleDestSnap)
	}

	if snap.ResourceTags == nil {
		snap.ResourceTags = make(map[string]map[string]string)
	}
}

// ---------------------------------------------------------------------------
// Group 2: Jobs and provisioning resources.
// ---------------------------------------------------------------------------

// provisioningSnapshot bundles Job/JobTemplate/RoleAlias/DomainConfiguration/
// Authorizer/BillingGroup/ProvisioningTemplate-family fields of
// backendSnapshot so Snapshot/Restore can delegate to a single helper each.
type provisioningSnapshot struct {
	Jobs                 map[string]*Job
	JobExecutions        map[string]*JobExecution
	JobTemplates         map[string]*JobTemplate
	RoleAliases          map[string]*RoleAlias
	DomainConfigs        map[string]*DomainConfiguration
	Authorizers          map[string]*Authorizer
	BillingGroups        map[string]*BillingGroup
	ProvTemplates        map[string]*ProvisioningTemplate
	ProvTemplateVersions map[string][]*ProvisioningTemplateVersion
}

// snapshotProvisioning deep-copies all job/provisioning state. Must be
// called with b.mu held (read or write).
func (b *InMemoryBackend) snapshotProvisioning() provisioningSnapshot {
	jobs := make(map[string]*Job, len(b.jobs))
	for k, v := range b.jobs {
		jobs[k] = cloneJob(v)
	}

	jobExecutions := make(map[string]*JobExecution, len(b.jobExecutions))
	for k, v := range b.jobExecutions {
		jobExecutions[k] = cloneJobExecution(v)
	}

	jobTemplates := make(map[string]*JobTemplate, len(b.jobTemplates))
	for k, v := range b.jobTemplates {
		jobTemplates[k] = cloneJobTemplate(v)
	}

	roleAliases := make(map[string]*RoleAlias, len(b.roleAliases))
	for k, v := range b.roleAliases {
		roleAliases[k] = cloneRoleAlias(v)
	}

	domainConfigs := make(map[string]*DomainConfiguration, len(b.domainConfigs))
	for k, v := range b.domainConfigs {
		domainConfigs[k] = cloneDomainConfig(v)
	}

	authorizers := make(map[string]*Authorizer, len(b.authorizers))
	for k, v := range b.authorizers {
		authorizers[k] = cloneAuthorizer(v)
	}

	billingGroups := make(map[string]*BillingGroup, len(b.billingGroups))
	for k, v := range b.billingGroups {
		billingGroups[k] = cloneBillingGroup(v)
	}

	provTemplates := make(map[string]*ProvisioningTemplate, len(b.provTemplates))
	for k, v := range b.provTemplates {
		provTemplates[k] = cloneProvTemplate(v)
	}

	provTemplateVersions := make(map[string][]*ProvisioningTemplateVersion, len(b.provTemplateVersions))
	for k, v := range b.provTemplateVersions {
		provTemplateVersions[k] = cloneProvTemplateVersions(v)
	}

	return provisioningSnapshot{
		Jobs:                 jobs,
		JobExecutions:        jobExecutions,
		JobTemplates:         jobTemplates,
		RoleAliases:          roleAliases,
		DomainConfigs:        domainConfigs,
		Authorizers:          authorizers,
		BillingGroups:        billingGroups,
		ProvTemplates:        provTemplates,
		ProvTemplateVersions: provTemplateVersions,
	}
}

// restoreProvisioning restores job/provisioning state from a snapshot. Must
// be called with b.mu held (write).
func (b *InMemoryBackend) restoreProvisioning(snap provisioningSnapshot) {
	b.jobs = make(map[string]*Job, len(snap.Jobs))
	for k, v := range snap.Jobs {
		b.jobs[k] = cloneJob(v)
	}

	b.jobExecutions = make(map[string]*JobExecution, len(snap.JobExecutions))
	for k, v := range snap.JobExecutions {
		b.jobExecutions[k] = cloneJobExecution(v)
	}

	b.jobTemplates = make(map[string]*JobTemplate, len(snap.JobTemplates))
	for k, v := range snap.JobTemplates {
		b.jobTemplates[k] = cloneJobTemplate(v)
	}

	b.roleAliases = make(map[string]*RoleAlias, len(snap.RoleAliases))
	for k, v := range snap.RoleAliases {
		b.roleAliases[k] = cloneRoleAlias(v)
	}

	b.domainConfigs = make(map[string]*DomainConfiguration, len(snap.DomainConfigs))
	for k, v := range snap.DomainConfigs {
		b.domainConfigs[k] = cloneDomainConfig(v)
	}

	b.authorizers = make(map[string]*Authorizer, len(snap.Authorizers))
	for k, v := range snap.Authorizers {
		b.authorizers[k] = cloneAuthorizer(v)
	}

	b.billingGroups = make(map[string]*BillingGroup, len(snap.BillingGroups))
	for k, v := range snap.BillingGroups {
		b.billingGroups[k] = cloneBillingGroup(v)
	}

	b.provTemplates = make(map[string]*ProvisioningTemplate, len(snap.ProvTemplates))
	for k, v := range snap.ProvTemplates {
		b.provTemplates[k] = cloneProvTemplate(v)
	}

	b.provTemplateVersions = make(map[string][]*ProvisioningTemplateVersion, len(snap.ProvTemplateVersions))
	for k, v := range snap.ProvTemplateVersions {
		b.provTemplateVersions[k] = cloneProvTemplateVersions(v)
	}
}

// ensureNonNilProvisioningSnap defaults nil maps in a restored snapshot's
// job/provisioning fields to empty maps.
func ensureNonNilProvisioningSnap(snap *backendSnapshot) {
	if snap.Jobs == nil {
		snap.Jobs = make(map[string]*Job)
	}

	if snap.JobExecutions == nil {
		snap.JobExecutions = make(map[string]*JobExecution)
	}

	if snap.JobTemplates == nil {
		snap.JobTemplates = make(map[string]*JobTemplate)
	}

	if snap.RoleAliases == nil {
		snap.RoleAliases = make(map[string]*RoleAlias)
	}

	if snap.DomainConfigs == nil {
		snap.DomainConfigs = make(map[string]*DomainConfiguration)
	}

	if snap.Authorizers == nil {
		snap.Authorizers = make(map[string]*Authorizer)
	}

	if snap.BillingGroups == nil {
		snap.BillingGroups = make(map[string]*BillingGroup)
	}

	if snap.ProvTemplates == nil {
		snap.ProvTemplates = make(map[string]*ProvisioningTemplate)
	}

	if snap.ProvTemplateVersions == nil {
		snap.ProvTemplateVersions = make(map[string][]*ProvisioningTemplateVersion)
	}
}

// ---------------------------------------------------------------------------
// Group 3: Audit-related resources not already covered by
// deviceDefenderSnapshot.
// ---------------------------------------------------------------------------

// auditExtraSnapshot bundles ScheduledAudit/MitigationAction/SecurityProfile/
// AuditSuppression/AuditFinding/AuditTask/Dimension fields of
// backendSnapshot so Snapshot/Restore can delegate to a single helper each.
type auditExtraSnapshot struct {
	ScheduledAudits   map[string]*ScheduledAudit
	MitigationActions map[string]*MitigationAction
	SecurityProfiles  map[string]*SecurityProfile
	AuditSuppressions map[string]*AuditSuppression
	AuditFindings     map[string]*AuditFinding
	AuditTaskObjects  map[string]*AuditTask
	Dimensions        map[string]*Dimension
}

// snapshotAuditExtra deep-copies the remaining audit-related state. Must be
// called with b.mu held (read or write).
func (b *InMemoryBackend) snapshotAuditExtra() auditExtraSnapshot {
	scheduledAudits := make(map[string]*ScheduledAudit, len(b.scheduledAudits))
	for k, v := range b.scheduledAudits {
		scheduledAudits[k] = cloneScheduledAudit(v)
	}

	mitigationActions := make(map[string]*MitigationAction, len(b.mitigationActions))
	for k, v := range b.mitigationActions {
		mitigationActions[k] = cloneMitigationAction(v)
	}

	securityProfiles := make(map[string]*SecurityProfile, len(b.securityProfiles))
	for k, v := range b.securityProfiles {
		securityProfiles[k] = cloneSecurityProfile(v)
	}

	auditSuppressions := make(map[string]*AuditSuppression, len(b.auditSuppressions))
	for k, v := range b.auditSuppressions {
		auditSuppressions[k] = cloneAuditSuppression(v)
	}

	auditFindings := make(map[string]*AuditFinding, len(b.auditFindings))
	for k, v := range b.auditFindings {
		auditFindings[k] = cloneAuditFinding(v)
	}

	auditTaskObjects := make(map[string]*AuditTask, len(b.auditTaskObjects))
	for k, v := range b.auditTaskObjects {
		auditTaskObjects[k] = cloneAuditTaskObj(v)
	}

	dimensions := make(map[string]*Dimension, len(b.dimensions))
	for k, v := range b.dimensions {
		dimensions[k] = cloneDimension(v)
	}

	return auditExtraSnapshot{
		ScheduledAudits:   scheduledAudits,
		MitigationActions: mitigationActions,
		SecurityProfiles:  securityProfiles,
		AuditSuppressions: auditSuppressions,
		AuditFindings:     auditFindings,
		AuditTaskObjects:  auditTaskObjects,
		Dimensions:        dimensions,
	}
}

// restoreAuditExtra restores the remaining audit-related state from a
// snapshot. Must be called with b.mu held (write).
func (b *InMemoryBackend) restoreAuditExtra(snap auditExtraSnapshot) {
	b.scheduledAudits = make(map[string]*ScheduledAudit, len(snap.ScheduledAudits))
	for k, v := range snap.ScheduledAudits {
		b.scheduledAudits[k] = cloneScheduledAudit(v)
	}

	b.mitigationActions = make(map[string]*MitigationAction, len(snap.MitigationActions))
	for k, v := range snap.MitigationActions {
		b.mitigationActions[k] = cloneMitigationAction(v)
	}

	b.securityProfiles = make(map[string]*SecurityProfile, len(snap.SecurityProfiles))
	for k, v := range snap.SecurityProfiles {
		b.securityProfiles[k] = cloneSecurityProfile(v)
	}

	b.auditSuppressions = make(map[string]*AuditSuppression, len(snap.AuditSuppressions))
	for k, v := range snap.AuditSuppressions {
		b.auditSuppressions[k] = cloneAuditSuppression(v)
	}

	b.auditFindings = make(map[string]*AuditFinding, len(snap.AuditFindings))
	for k, v := range snap.AuditFindings {
		b.auditFindings[k] = cloneAuditFinding(v)
	}

	b.auditTaskObjects = make(map[string]*AuditTask, len(snap.AuditTaskObjects))
	for k, v := range snap.AuditTaskObjects {
		b.auditTaskObjects[k] = cloneAuditTaskObj(v)
	}

	b.dimensions = make(map[string]*Dimension, len(snap.Dimensions))
	for k, v := range snap.Dimensions {
		b.dimensions[k] = cloneDimension(v)
	}
}

// ensureNonNilAuditExtraSnap defaults nil maps in a restored snapshot's
// remaining audit-related fields to empty maps.
func ensureNonNilAuditExtraSnap(snap *backendSnapshot) {
	if snap.ScheduledAudits == nil {
		snap.ScheduledAudits = make(map[string]*ScheduledAudit)
	}

	if snap.MitigationActions == nil {
		snap.MitigationActions = make(map[string]*MitigationAction)
	}

	if snap.SecurityProfiles == nil {
		snap.SecurityProfiles = make(map[string]*SecurityProfile)
	}

	if snap.AuditSuppressions == nil {
		snap.AuditSuppressions = make(map[string]*AuditSuppression)
	}

	if snap.AuditFindings == nil {
		snap.AuditFindings = make(map[string]*AuditFinding)
	}

	if snap.AuditTaskObjects == nil {
		snap.AuditTaskObjects = make(map[string]*AuditTask)
	}

	if snap.Dimensions == nil {
		snap.Dimensions = make(map[string]*Dimension)
	}
}

// ---------------------------------------------------------------------------
// Group 4: Streams, packages, commands, and metrics.
// ---------------------------------------------------------------------------

// resourceMiscSnapshot bundles Stream/OTAUpdate/IoTPackage(Version)/Command
// (Execution)/FleetMetric/CustomMetric/V2LoggingLevel fields of
// backendSnapshot so Snapshot/Restore can delegate to a single helper each.
type resourceMiscSnapshot struct {
	Streams           map[string]*IoTStream
	OTAUpdates        map[string]*OTAUpdate
	IoTPackages       map[string]*IoTPackage
	PackageVersions2  map[string]map[string]*IoTPackageVersion
	Commands          map[string]*IoTCommand
	CommandExecutions map[string]*IoTCommandExecution
	FleetMetrics      map[string]*FleetMetric
	CustomMetrics     map[string]*CustomMetric
	V2LoggingLevels   map[string]*V2LoggingLevel
}

// snapshotResourceMisc deep-copies stream/package/command/metric state. Must
// be called with b.mu held (read or write).
func (b *InMemoryBackend) snapshotResourceMisc() resourceMiscSnapshot {
	streams := make(map[string]*IoTStream, len(b.streams))
	for k, v := range b.streams {
		streams[k] = cloneStream(v)
	}

	otaUpdates := make(map[string]*OTAUpdate, len(b.otaUpdates))
	for k, v := range b.otaUpdates {
		otaUpdates[k] = cloneOTAUpdate(v)
	}

	iotPackages := make(map[string]*IoTPackage, len(b.iotPackages))
	for k, v := range b.iotPackages {
		iotPackages[k] = cloneIoTPackage(v)
	}

	packageVersions2 := make(map[string]map[string]*IoTPackageVersion, len(b.packageVersions2))
	for pkg, versions := range b.packageVersions2 {
		cp := make(map[string]*IoTPackageVersion, len(versions))
		for name, v := range versions {
			cp[name] = cloneIoTPackageVersion(v)
		}
		packageVersions2[pkg] = cp
	}

	commands := make(map[string]*IoTCommand, len(b.commands))
	for k, v := range b.commands {
		commands[k] = cloneIoTCommand(v)
	}

	commandExecutions := make(map[string]*IoTCommandExecution, len(b.commandExecutions))
	for k, v := range b.commandExecutions {
		commandExecutions[k] = cloneIoTCommandExecution(v)
	}

	fleetMetrics := make(map[string]*FleetMetric, len(b.fleetMetrics))
	for k, v := range b.fleetMetrics {
		fleetMetrics[k] = cloneFleetMetric(v)
	}

	customMetrics := make(map[string]*CustomMetric, len(b.customMetrics))
	for k, v := range b.customMetrics {
		customMetrics[k] = cloneCustomMetric(v)
	}

	v2LoggingLevels := make(map[string]*V2LoggingLevel, len(b.v2LoggingLevels))
	for k, v := range b.v2LoggingLevels {
		v2LoggingLevels[k] = cloneV2LogLevel(v)
	}

	return resourceMiscSnapshot{
		Streams:           streams,
		OTAUpdates:        otaUpdates,
		IoTPackages:       iotPackages,
		PackageVersions2:  packageVersions2,
		Commands:          commands,
		CommandExecutions: commandExecutions,
		FleetMetrics:      fleetMetrics,
		CustomMetrics:     customMetrics,
		V2LoggingLevels:   v2LoggingLevels,
	}
}

// restoreResourceMisc restores stream/package/command/metric state from a
// snapshot. Must be called with b.mu held (write).
func (b *InMemoryBackend) restoreResourceMisc(snap resourceMiscSnapshot) {
	b.streams = make(map[string]*IoTStream, len(snap.Streams))
	for k, v := range snap.Streams {
		b.streams[k] = cloneStream(v)
	}

	b.otaUpdates = make(map[string]*OTAUpdate, len(snap.OTAUpdates))
	for k, v := range snap.OTAUpdates {
		b.otaUpdates[k] = cloneOTAUpdate(v)
	}

	b.iotPackages = make(map[string]*IoTPackage, len(snap.IoTPackages))
	for k, v := range snap.IoTPackages {
		b.iotPackages[k] = cloneIoTPackage(v)
	}

	b.packageVersions2 = make(map[string]map[string]*IoTPackageVersion, len(snap.PackageVersions2))
	for pkg, versions := range snap.PackageVersions2 {
		cp := make(map[string]*IoTPackageVersion, len(versions))
		for name, v := range versions {
			cp[name] = cloneIoTPackageVersion(v)
		}
		b.packageVersions2[pkg] = cp
	}

	b.commands = make(map[string]*IoTCommand, len(snap.Commands))
	for k, v := range snap.Commands {
		b.commands[k] = cloneIoTCommand(v)
	}

	b.commandExecutions = make(map[string]*IoTCommandExecution, len(snap.CommandExecutions))
	for k, v := range snap.CommandExecutions {
		b.commandExecutions[k] = cloneIoTCommandExecution(v)
	}

	b.fleetMetrics = make(map[string]*FleetMetric, len(snap.FleetMetrics))
	for k, v := range snap.FleetMetrics {
		b.fleetMetrics[k] = cloneFleetMetric(v)
	}

	b.customMetrics = make(map[string]*CustomMetric, len(snap.CustomMetrics))
	for k, v := range snap.CustomMetrics {
		b.customMetrics[k] = cloneCustomMetric(v)
	}

	b.v2LoggingLevels = make(map[string]*V2LoggingLevel, len(snap.V2LoggingLevels))
	for k, v := range snap.V2LoggingLevels {
		b.v2LoggingLevels[k] = cloneV2LogLevel(v)
	}
}

// ensureNonNilResourceMiscSnap defaults nil maps in a restored snapshot's
// stream/package/command/metric fields to empty maps.
func ensureNonNilResourceMiscSnap(snap *backendSnapshot) {
	if snap.Streams == nil {
		snap.Streams = make(map[string]*IoTStream)
	}

	if snap.OTAUpdates == nil {
		snap.OTAUpdates = make(map[string]*OTAUpdate)
	}

	if snap.IoTPackages == nil {
		snap.IoTPackages = make(map[string]*IoTPackage)
	}

	if snap.PackageVersions2 == nil {
		snap.PackageVersions2 = make(map[string]map[string]*IoTPackageVersion)
	}

	if snap.Commands == nil {
		snap.Commands = make(map[string]*IoTCommand)
	}

	if snap.CommandExecutions == nil {
		snap.CommandExecutions = make(map[string]*IoTCommandExecution)
	}

	if snap.FleetMetrics == nil {
		snap.FleetMetrics = make(map[string]*FleetMetric)
	}

	if snap.CustomMetrics == nil {
		snap.CustomMetrics = make(map[string]*CustomMetric)
	}

	if snap.V2LoggingLevels == nil {
		snap.V2LoggingLevels = make(map[string]*V2LoggingLevel)
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
