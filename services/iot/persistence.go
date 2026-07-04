package iot

import (
	"encoding/json"
	"log/slog"
	"maps"
)

type backendSnapshot struct {
	AuditTasks                      map[string]string                             `json:"auditTasks"`
	MetricValues                    map[string][]*MetricDatapoint                 `json:"metricValues"`
	Rules                           map[string]*TopicRule                         `json:"rules"`
	CertificateTransfers            map[string]string                             `json:"certificateTransfers"`
	ThingBillingGroups              map[string]string                             `json:"thingBillingGroups"`
	ThingThingGroups                map[string][]string                           `json:"thingThingGroups"`
	PackageVersionSboms             map[string]*SbomDocument                      `json:"packageVersionSboms"`
	JobTargets                      map[string][]string                           `json:"jobTargets"`
	PolicyTargets                   map[string][]string                           `json:"policyTargets"`
	SecurityProfileTargets          map[string][]string                           `json:"securityProfileTargets"`
	ThingPrincipals                 map[string][]string                           `json:"thingPrincipals"`
	ThingGroupIndexingConfiguration *ThingGroupIndexingConfiguration              `json:"thingGroupIndexingConfiguration"`
	Policies                        map[string]*Policy                            `json:"policies"`
	Things                          map[string]*Thing                             `json:"things"`
	AuditMitigationTasks            map[string]string                             `json:"auditMitigationTasks"`
	RegistrationTasks               map[string]*ThingRegistrationTask             `json:"registrationTasks"`
	AuditMitigationTaskObjects      map[string]*AuditMitigationTask               `json:"auditMitigationTaskObjects"`
	AuditMitigationExecutions       map[string][]*AuditMitigationActionExecution  `json:"auditMitigationExecutions"`
	DetectMitigationTasks           map[string]*DetectMitigationTask              `json:"detectMitigationTasks"`
	DetectMitigationExecutions      map[string][]*DetectMitigationActionExecution `json:"detectMitigationExecutions"`
	ActiveViolations                map[string]*ActiveViolation                   `json:"activeViolations"`
	BehaviorTrainingSummaries       map[string][]*BehaviorModelTrainingSummary    `json:"behaviorTrainingSummaries"`
	AccountEncryptionConfig         *AccountEncryptionConfiguration               `json:"accountEncryptionConfig"`
	SbomValidationResults           map[string][]*SbomValidationResult            `json:"sbomValidationResults"`
	ThingIndexingConfiguration      *ThingIndexingConfiguration                   `json:"thingIndexingConfiguration"`
	ThingConnectivity               map[string]*ThingConnectivityData             `json:"thingConnectivity"`
	ThingTypes                      map[string]*ThingType                         `json:"thingTypes"`
	ThingGroups                     map[string]*ThingGroup                        `json:"thingGroups"`
	ThingGroupMembers               map[string][]string                           `json:"thingGroupMembers"`
	Certificates                    map[string]*Certificate                       `json:"certificates"`
	CertificateProviders            map[string]*CertificateProvider               `json:"certificateProviders"`
	CACertificates                  map[string]*CACertificate                     `json:"caCertificates"`
	PolicyVersions                  map[string][]*PolicyVersion                   `json:"policyVersions"`
	TopicRuleDestinations           map[string]*topicRuleDestSnap                 `json:"topicRuleDestinations"`
	ResourceTags                    map[string]map[string]string                  `json:"resourceTags"`
	Jobs                            map[string]*Job                               `json:"jobs"`
	JobExecutions                   map[string]*JobExecution                      `json:"jobExecutions"`
	JobTemplates                    map[string]*JobTemplate                       `json:"jobTemplates"`
	RoleAliases                     map[string]*RoleAlias                         `json:"roleAliases"`
	DomainConfigs                   map[string]*DomainConfiguration               `json:"domainConfigs"`
	Authorizers                     map[string]*Authorizer                        `json:"authorizers"`
	BillingGroups                   map[string]*BillingGroup                      `json:"billingGroups"`
	ProvTemplates                   map[string]*ProvisioningTemplate              `json:"provTemplates"`
	ProvTemplateVersions            map[string][]*ProvisioningTemplateVersion     `json:"provTemplateVersions"`
	ScheduledAudits                 map[string]*ScheduledAudit                    `json:"scheduledAudits"`
	MitigationActions               map[string]*MitigationAction                  `json:"mitigationActions"`
	SecurityProfiles                map[string]*SecurityProfile                   `json:"securityProfiles"`
	AuditSuppressions               map[string]*AuditSuppression                  `json:"auditSuppressions"`
	AuditFindings                   map[string]*AuditFinding                      `json:"auditFindings"`
	AuditTaskObjects                map[string]*AuditTask                         `json:"auditTaskObjects"`
	Dimensions                      map[string]*Dimension                         `json:"dimensions"`
	Streams                         map[string]*IoTStream                         `json:"streams"`
	OTAUpdates                      map[string]*OTAUpdate                         `json:"otaUpdates"`
	IoTPackages                     map[string]*IoTPackage                        `json:"iotPackages"`
	PackageVersions2                map[string]map[string]*IoTPackageVersion      `json:"packageVersions2"`
	Commands                        map[string]*IoTCommand                        `json:"commands"`
	CommandExecutions               map[string]*IoTCommandExecution               `json:"commandExecutions"`
	FleetMetrics                    map[string]*FleetMetric                       `json:"fleetMetrics"`
	CustomMetrics                   map[string]*CustomMetric                      `json:"customMetrics"`
	V2LoggingLevels                 map[string]*V2LoggingLevel                    `json:"v2LoggingLevels"`
	AuditConfiguration              *AccountAuditConfiguration                    `json:"auditConfiguration"`
	PackageConfig                   *PackageConfiguration                         `json:"packageConfig"`
	V2LoggingOptions                *V2LoggingOptions                             `json:"v2LoggingOptions"`
	LoggingOptions                  *LoggingOptions                               `json:"loggingOptions"`
	EventConfigurations             *EventConfigurations                          `json:"eventConfigurations"`
	RegistrationCode                string                                        `json:"registrationCode"`
	DefaultAuthorizer               string                                        `json:"defaultAuthorizer"`
	ViolationEvents                 []*ViolationEvent                             `json:"violationEvents"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	things := make(map[string]*Thing, len(b.things))
	for k, v := range b.things {
		cp := cloneThing(v)
		things[k] = cp
	}

	policies := make(map[string]*Policy, len(b.policies))
	for k, v := range b.policies {
		policies[k] = clonePolicy(v)
	}

	rules := make(map[string]*TopicRule, len(b.rules))
	for k, v := range b.rules {
		rules[k] = cloneTopicRule(v)
	}

	certTransfers := make(map[string]string, len(b.certificateTransfers))
	maps.Copy(certTransfers, b.certificateTransfers)

	billingGroups := make(map[string]string, len(b.thingBillingGroups))
	maps.Copy(billingGroups, b.thingBillingGroups)

	thingGroups := copyStringSliceMap(b.thingThingGroups)

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

	registrationTasks := make(map[string]*ThingRegistrationTask, len(b.registrationTasks))
	for k, v := range b.registrationTasks {
		registrationTasks[k] = cloneRegistrationTask(v)
	}

	ddSnap := b.snapshotDeviceDefender()
	finalSnap := b.snapshotFinalOps()
	thingResSnap := b.snapshotThingResources()
	provSnap := b.snapshotProvisioning()
	auditExtraSnap := b.snapshotAuditExtra()
	miscSnap := b.snapshotResourceMisc()
	cfgSnap := b.snapshotConfig()

	snap := backendSnapshot{
		Things:                 things,
		Policies:               policies,
		Rules:                  rules,
		CertificateTransfers:   certTransfers,
		ThingBillingGroups:     billingGroups,
		ThingThingGroups:       thingGroups,
		PackageVersionSboms:    sboms,
		JobTargets:             copyStringSliceMap(b.jobTargets),
		PolicyTargets:          copyStringSliceMap(b.policyTargets),
		SecurityProfileTargets: copyStringSliceMap(b.securityProfileTargets),
		ThingPrincipals:        copyStringSliceMap(b.thingPrincipals),
		AuditMitigationTasks:   copyStringMap(b.auditMitigationTasks),
		AuditTasks:             copyStringMap(b.auditTasks),

		ThingIndexingConfiguration:      thingIndexingConfig,
		ThingGroupIndexingConfiguration: thingGroupIndexingConfig,

		RegistrationTasks: registrationTasks,

		AuditMitigationTaskObjects: ddSnap.AuditMitigationTaskObjects,
		AuditMitigationExecutions:  ddSnap.AuditMitigationExecutions,
		DetectMitigationTasks:      ddSnap.DetectMitigationTasks,
		DetectMitigationExecutions: ddSnap.DetectMitigationExecutions,
		ActiveViolations:           ddSnap.ActiveViolations,
		ViolationEvents:            ddSnap.ViolationEvents,

		AccountEncryptionConfig:   finalSnap.AccountEncryptionConfig,
		SbomValidationResults:     finalSnap.SbomValidationResults,
		MetricValues:              finalSnap.MetricValues,
		ThingConnectivity:         finalSnap.ThingConnectivity,
		BehaviorTrainingSummaries: finalSnap.BehaviorTrainingSummaries,
	}

	applyExtSnapshot(&snap, thingResSnap, provSnap, auditExtraSnap, miscSnap, cfgSnap)

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("iot: failed to snapshot backend state", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilSnap(&snap)

	b.mu.Lock()
	defer b.mu.Unlock()

	things := make(map[string]*Thing, len(snap.Things))
	for k, v := range snap.Things {
		cp := cloneThing(v)
		things[k] = cp
	}

	policies := make(map[string]*Policy, len(snap.Policies))
	for k, v := range snap.Policies {
		policies[k] = clonePolicy(v)
	}

	rules := make(map[string]*TopicRule, len(snap.Rules))
	for k, v := range snap.Rules {
		rules[k] = cloneTopicRule(v)
	}

	b.things = things
	b.policies = policies
	b.rules = rules
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

	b.registrationTasks = make(map[string]*ThingRegistrationTask, len(snap.RegistrationTasks))
	for k, v := range snap.RegistrationTasks {
		b.registrationTasks[k] = cloneRegistrationTask(v)
	}

	b.restoreDeviceDefender(deviceDefenderSnapshot{
		AuditMitigationTaskObjects: snap.AuditMitigationTaskObjects,
		AuditMitigationExecutions:  snap.AuditMitigationExecutions,
		DetectMitigationTasks:      snap.DetectMitigationTasks,
		DetectMitigationExecutions: snap.DetectMitigationExecutions,
		ActiveViolations:           snap.ActiveViolations,
		ViolationEvents:            snap.ViolationEvents,
	})

	b.restoreFinalOps(finalOpsSnapshot{
		AccountEncryptionConfig:   snap.AccountEncryptionConfig,
		SbomValidationResults:     snap.SbomValidationResults,
		MetricValues:              snap.MetricValues,
		ThingConnectivity:         snap.ThingConnectivity,
		BehaviorTrainingSummaries: snap.BehaviorTrainingSummaries,
	})

	thingRes, prov, auditExtra, misc, cfg := extGroupsFromSnapshot(&snap)
	b.restoreThingResources(thingRes)
	b.restoreProvisioning(prov)
	b.restoreAuditExtra(auditExtra)
	b.restoreResourceMisc(misc)
	b.restoreConfig(cfg)

	return nil
}

func ensureNonNilSnap(snap *backendSnapshot) {
	if snap.Things == nil {
		snap.Things = make(map[string]*Thing)
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]*Policy)
	}

	if snap.Rules == nil {
		snap.Rules = make(map[string]*TopicRule)
	}

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

	if snap.RegistrationTasks == nil {
		snap.RegistrationTasks = make(map[string]*ThingRegistrationTask)
	}

	ensureNonNilFinalOpsSnap(snap)
	ensureNonNilThingResourceSnap(snap)
	ensureNonNilProvisioningSnap(snap)
	ensureNonNilAuditExtraSnap(snap)
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
func (h *Handler) Snapshot() []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Non-snapshottable backends are skipped.
func (h *Handler) Restore(data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(data)
	}

	return nil
}
