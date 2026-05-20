package iot

// StorageBackend defines the interface for the IoT control-plane backend.
type StorageBackend interface {
	CreateThing(input *CreateThingInput) (*CreateThingOutput, error)
	DescribeThing(thingName string) (*Thing, error)
	ListThings() []*Thing
	DeleteThing(thingName string) error
	UpdateThing(input *UpdateThingInput) error

	CreateTopicRule(input *CreateTopicRuleInput) error
	GetTopicRule(ruleName string) (*TopicRule, error)
	ListTopicRules() []*TopicRule
	DeleteTopicRule(ruleName string) error
	DisableTopicRule(ruleName string) error
	EnableTopicRule(ruleName string) error
	ReplaceTopicRule(input *ReplaceTopicRuleInput) error

	GetPolicy(policyName string) (*GetPolicyOutput, error)
	DeletePolicy(policyName string) error
	ListPolicies() []*Policy
	CreatePolicy(input *CreatePolicyInput) (*CreatePolicyOutput, error)
	AttachPrincipalPolicy(input *AttachPrincipalPolicyInput) error

	DescribeEndpoint(endpointType string) (*DescribeEndpointOutput, error)

	AcceptCertificateTransfer(input *AcceptCertificateTransferInput) error
	AddThingToBillingGroup(input *AddThingToBillingGroupInput) error
	AddThingToThingGroup(input *AddThingToThingGroupInput) error
	AssociateSbomWithPackageVersion(
		input *AssociateSbomWithPackageVersionInput,
	) (*AssociateSbomWithPackageVersionOutput, error)
	AssociateTargetsWithJob(input *AssociateTargetsWithJobInput) (*AssociateTargetsWithJobOutput, error)
	AttachPolicy(input *AttachPolicyInput) error
	AttachSecurityProfile(input *AttachSecurityProfileInput) error
	AttachThingPrincipal(input *AttachThingPrincipalInput) error
	CancelAuditMitigationActionsTask(input *CancelAuditMitigationActionsTaskInput) error
	CancelAuditTask(input *CancelAuditTaskInput) error
	ListThingPrincipals(thingName string) ([]string, error)

	// ThingType operations.
	CreateThingType(input *CreateThingTypeInput) (*ThingType, error)
	DescribeThingType(thingTypeName string) (*ThingType, error)
	ListThingTypes() []*ThingType
	DeprecateThingType(input *DeprecateThingTypeInput) error
	DeleteThingType(thingTypeName string) error

	// ThingGroup operations.
	CreateThingGroup(input *CreateThingGroupInput) (*ThingGroup, error)
	DescribeThingGroup(thingGroupName string) (*ThingGroup, error)
	ListThingGroups() []*ThingGroup
	UpdateThingGroup(input *UpdateThingGroupInput) (int64, error)
	DeleteThingGroup(thingGroupName string) error
	RemoveThingFromThingGroup(input *RemoveThingFromThingGroupInput) error
	ListThingsInThingGroup(input *ListThingsInThingGroupInput) ([]string, error)

	// Certificate operations.
	CreateCertificateFromCsr(input *CreateCertificateFromCsrInput) (*Certificate, error)
	RegisterCertificate(input *RegisterCertificateInput) (*Certificate, error)
	RegisterCertificateWithoutCA(input *RegisterCertificateInput) (*Certificate, error)
	DescribeCertificate(certificateID string) (*Certificate, error)
	ListCertificates() []*Certificate
	UpdateCertificate(input *UpdateCertificateInput) error
	DeleteCertificate(certificateID string) error

	// Policy attachment operations.
	DetachPolicy(input *DetachPolicyInput) error
	ListAttachedPolicies(input *ListAttachedPoliciesInput) ([]*Policy, error)

	// PolicyVersion operations.
	CreatePolicyVersion(input *CreatePolicyVersionInput) (*PolicyVersion, error)
	GetPolicyVersion(policyName, versionID string) (*PolicyVersion, error)
	ListPolicyVersions(policyName string) ([]*PolicyVersion, error)
	DeletePolicyVersion(policyName, versionID string) error
	SetDefaultPolicyVersion(policyName, versionID string) error

	// TopicRuleDestination operations.
	CreateTopicRuleDestination(input *CreateTopicRuleDestinationInput) (*TopicRuleDestination, error)
	GetTopicRuleDestination(arn string) (*TopicRuleDestination, error)
	ListTopicRuleDestinations() []*TopicRuleDestination
	UpdateTopicRuleDestination(input *UpdateTopicRuleDestinationInput) error
	DeleteTopicRuleDestination(arn string) error

	// CertificateProvider operations.
	CreateCertificateProvider(input *CreateCertificateProviderInput) (*CertificateProvider, error)
	DescribeCertificateProvider(name string) (*CertificateProvider, error)
	ListCertificateProviders() []*CertificateProvider
	UpdateCertificateProvider(input *UpdateCertificateProviderInput) error
	DeleteCertificateProvider(name string) error

	// Job operations.
	CreateJob(input *CreateJobInput) (*Job, error)
	DescribeJob(jobID string) (*Job, error)
	ListJobs() []*Job
	UpdateJob(jobID, description string) error
	CancelJob(jobID, comment string) (*Job, error)
	DeleteJob(jobID string) error
	GetJobDocument(jobID string) (string, error)
	DescribeJobExecution(jobID, thingName string) (*JobExecution, error)
	CancelJobExecution(jobID, thingName string) error
	DeleteJobExecution(jobID, thingName string) error

	// JobTemplate operations.
	CreateJobTemplate(input *CreateJobTemplateInput) (*JobTemplate, error)
	DescribeJobTemplate(id string) (*JobTemplate, error)
	ListJobTemplates() []*JobTemplate
	DeleteJobTemplate(id string) error

	// RoleAlias operations.
	CreateRoleAlias(input *CreateRoleAliasInput) (*RoleAlias, error)
	DescribeRoleAlias(alias string) (*RoleAlias, error)
	ListRoleAliases() []*RoleAlias
	UpdateRoleAlias(alias, roleARN string, credDuration int) (*RoleAlias, error)
	DeleteRoleAlias(alias string) error

	// DomainConfiguration operations.
	CreateDomainConfiguration(input *CreateDomainConfigurationInput) (*DomainConfiguration, error)
	DescribeDomainConfiguration(name string) (*DomainConfiguration, error)
	ListDomainConfigurations() []*DomainConfiguration
	UpdateDomainConfiguration(name, status string) (*DomainConfiguration, error)
	DeleteDomainConfiguration(name string) error

	// ProvisioningTemplate operations.
	CreateProvisioningTemplate(input *CreateProvisioningTemplateInput) (*ProvisioningTemplate, error)
	DescribeProvisioningTemplate(name string) (*ProvisioningTemplate, error)
	ListProvisioningTemplates() []*ProvisioningTemplate
	UpdateProvisioningTemplate(name, description string, enabled *bool, provRoleARN string) error
	DeleteProvisioningTemplate(name string) error
	CreateProvisioningTemplateVersion(name, body string) (*ProvisioningTemplateVersion, error)
	ListProvisioningTemplateVersions(name string) ([]*ProvisioningTemplateVersion, error)
	DeleteProvisioningTemplateVersion(name string, versionID int32) error

	// Authorizer operations.
	CreateAuthorizer(input *CreateAuthorizerInput) (*Authorizer, error)
	DescribeAuthorizer(name string) (*Authorizer, error)
	ListAuthorizers() []*Authorizer
	UpdateAuthorizer(name, functionARN, status string) (*Authorizer, error)
	DeleteAuthorizer(name string) error

	// BillingGroup operations.
	CreateBillingGroup(input *CreateBillingGroupInput) (*BillingGroup, error)
	DescribeBillingGroup(name string) (*BillingGroup, error)
	ListBillingGroups() []*BillingGroup
	UpdateBillingGroup(name string, props BillingGroupProperties) (int64, error)
	DeleteBillingGroup(name string) error

	// ScheduledAudit operations.
	CreateScheduledAudit(input *CreateScheduledAuditInput) (*ScheduledAudit, error)
	DescribeScheduledAudit(name string) (*ScheduledAudit, error)
	ListScheduledAudits() []*ScheduledAudit
	UpdateScheduledAudit(name, frequency, dayOfMonth, dayOfWeek string, checks []string) (*ScheduledAudit, error)
	DeleteScheduledAudit(name string) error

	// MitigationAction operations.
	CreateMitigationAction(input *CreateMitigationActionInput) (*MitigationAction, error)
	DescribeMitigationAction(name string) (*MitigationAction, error)
	ListMitigationActions() []*MitigationAction
	UpdateMitigationAction(name, roleARN string, params map[string]any) (*MitigationAction, error)
	DeleteMitigationAction(name string) error

	// SecurityProfile operations.
	CreateSecurityProfile(input *CreateSecurityProfileInput) (*SecurityProfile, error)
	DescribeSecurityProfile(name string) (*SecurityProfile, error)
	ListSecurityProfiles() []*SecurityProfile
	UpdateSecurityProfile(name, description string) (*SecurityProfile, error)
	DeleteSecurityProfile(name string) error

	// Batch 2: CACertificate operations.
	RegisterCACertificate(pem, status string) (*CACertificate, error)
	DescribeCACertificate(id string) (*CACertificate, error)
	ListCACertificates() []*CACertificate
	UpdateCACertificate(id, status string) error
	DeleteCACertificate(id string) error
	ListCertificatesByCA(caID string) []*Certificate

	// Batch 2: Stream operations.
	CreateStream(input *CreateStreamInput) (*IoTStream, error)
	DescribeStream(id string) (*IoTStream, error)
	ListStreams() []*IoTStream
	UpdateStream(id, description, roleARN string, files []StreamFile) (*IoTStream, error)
	DeleteStream(id string) error

	// Batch 2: FleetMetric operations.
	CreateFleetMetric(input *CreateFleetMetricInput) (*FleetMetric, error)
	DescribeFleetMetric(name string) (*FleetMetric, error)
	ListFleetMetrics() []*FleetMetric
	UpdateFleetMetric(name, queryString, description string, period int32) error
	DeleteFleetMetric(name string) error

	// Batch 2: CustomMetric operations.
	CreateCustomMetric(input *CreateCustomMetricInput) (*CustomMetric, error)
	DescribeCustomMetric(name string) (*CustomMetric, error)
	ListCustomMetrics() []*CustomMetric
	UpdateCustomMetric(name, displayName string) (*CustomMetric, error)
	DeleteCustomMetric(name string) error

	// Batch 2: Dimension operations.
	CreateDimension(input *CreateDimensionInput) (*Dimension, error)
	DescribeDimension(name string) (*Dimension, error)
	ListDimensions() []*Dimension
	UpdateDimension(name string, stringValues []string) (*Dimension, error)
	DeleteDimension(name string) error

	// Batch 2: Tag operations.
	TagResourceGeneric(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) map[string]string

	// Batch 2: Audit configuration.
	UpdateAccountAuditConfiguration(roleARN string, checks map[string]*AuditCheckConfig) error
	DescribeAccountAuditConfiguration() *AccountAuditConfiguration

	// Batch 2: Audit task.
	StartOnDemandAuditTask(checks []string) (string, error)
	DescribeAuditTask(taskID string) (*AuditTask, error)
	ListAuditTasks(taskType string) []*AuditTask

	// Batch 2: Misc.
	DetachThingPrincipal(thingName, principal string) error
	CancelCertificateTransfer(certID string) error
	GetRegistrationCode() string
	DeleteRegistrationCode() error
	ListThingGroupsForThing(thingName string) []string
	ListThingsInBillingGroup(groupName string) []string
	RemoveThingFromBillingGroup(thingName, billingGroupName string) error
	ListPrincipalPolicies(principal string) []*Policy
	ListPolicyPrincipals(policyName string) []string
	ListTargetsForPolicy(policyName string) []string
	ListPrincipalThings(principal string) []string
	GetEffectivePolicies(thingName, principal string) []*Policy
	SetDefaultAuthorizer(authorizerName string) error
	ClearDefaultAuthorizer() error
	DescribeDefaultAuthorizer() (string, error)
	ListJobExecutionsForJob(jobID string) []*JobExecution
	ListJobExecutionsForThing(thingName string) []*JobExecution

	// Batch 3: OTA Updates.
	CreateOTAUpdate(id, description, roleARN string, targets []string, files []any) (*OTAUpdate, error)
	GetOTAUpdate(id string) (*OTAUpdate, error)
	DeleteOTAUpdate(id string) error
	ListOTAUpdates() []*OTAUpdate

	// Batch 3: IoT Packages.
	CreateIoTPackage(name, description string, tags map[string]string) (*IoTPackage, error)
	GetIoTPackage(name string) (*IoTPackage, error)
	UpdateIoTPackage(name, description, defaultVersionName string) error
	DeleteIoTPackage(name string) error
	ListIoTPackages() []*IoTPackage

	// Batch 3: IoT Package Versions.
	CreateIoTPackageVersion(
		packageName, versionName, description string,
		tags map[string]string,
	) (*IoTPackageVersion, error)
	GetIoTPackageVersion(packageName, versionName string) (*IoTPackageVersion, error)
	UpdateIoTPackageVersion(packageName, versionName, description, status string) error
	DeleteIoTPackageVersion(packageName, versionName string) error
	ListIoTPackageVersions(packageName string) []*IoTPackageVersion

	// Batch 3: Package configuration.
	GetPackageConfiguration() *PackageConfiguration
	UpdatePackageConfiguration(cfg map[string]any) error

	// Batch 3: Audit suppressions.
	CreateAuditSuppression(
		checkName string,
		resourceID map[string]any,
		description string,
		suppressIndefinitely bool,
		expirationDate float64,
	) error
	DescribeAuditSuppression(checkName string, resourceID map[string]any) (*AuditSuppression, error)
	UpdateAuditSuppression(
		checkName string,
		resourceID map[string]any,
		description string,
		suppressIndefinitely bool,
		expirationDate float64,
	) error
	DeleteAuditSuppression(checkName string, resourceID map[string]any) error
	ListAuditSuppressions() []*AuditSuppression

	// Batch 3: Audit findings.
	DescribeAuditFinding(findingID string) (*AuditFinding, error)
	ListAuditFindings() []*AuditFinding

	// Batch 3: V2 logging.
	GetV2LoggingOptions() *V2LoggingOptions
	SetV2LoggingOptions(roleARN, defaultLogLevel string, disableAllLogs bool) error
	SetV2LoggingLevel(target map[string]any, logLevel string) error
	DeleteV2LoggingLevel(target map[string]any) error
	ListV2LoggingLevels() []*V2LoggingLevel

	// Batch 3: Logging options.
	GetLoggingOptions() *LoggingOptions
	SetLoggingOptions(roleARN, logLevel string) error

	// Batch 3: Provisioning claim.
	CreateProvisioningClaim(templateName string) (certPEM, publicKey, privateKey string, err error)

	// Batch 3: Keys and certs.
	CreateKeysAndCertificate(setAsActive bool) (*Certificate, string, string, error)
	TransferCertificate(certID, targetAccount string) error
	RejectCertificateTransfer(certID string) error

	// Batch 3: Event configurations.
	DescribeEventConfigurations() *EventConfigurations
	UpdateEventConfigurations(cfgs map[string]*EventConfigEntry) error

	// Batch 3: Security profile targets.
	DetachSecurityProfile(profileName, targetARN string) error
	ListTargetsForSecurityProfile(profileName string) []string
	ListSecurityProfilesForTarget(targetARN string) []string

	// Batch 3: Dynamic thing groups.
	CreateDynamicThingGroup(input *CreateThingGroupInput) (*ThingGroup, error)
	DeleteDynamicThingGroup(name string) error
	UpdateDynamicThingGroup(input *UpdateThingGroupInput) (int64, error)

	// Batch 3: Commands.
	CreateCommand(
		id, displayName, description, namespace string,
		payload map[string]any,
		tags map[string]string,
	) (*IoTCommand, error)
	GetCommand(id string) (*IoTCommand, error)
	UpdateCommand(id, displayName, description string, deprecated bool) error
	DeleteCommand(id string) error
	ListCommands() []*IoTCommand
	GetCommandExecution(commandID, executionID string) (*IoTCommandExecution, error)
	ListCommandExecutions(commandID string) []*IoTCommandExecution
}

// Snapshottable is an optional interface that a StorageBackend may implement
// to support snapshot-based persistence.
type Snapshottable interface {
	Snapshot() []byte
	Restore(data []byte) error
}

// Resettable is an optional interface that a StorageBackend may implement
// to support clearing all state (used for test isolation).
type Resettable interface {
	Reset()
}
