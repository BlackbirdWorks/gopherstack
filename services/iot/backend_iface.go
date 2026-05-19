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
