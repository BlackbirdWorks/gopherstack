package iot

const (
	keyError      = "error"
	keyThingName  = "thingName"
	keyThingArn   = "thingArn"
	keyPolicyName = "policyName"
	keyPolicyArn  = "policyArn"

	// JSON field name constants used across handlers.
	keyThingTypeName           = "thingTypeName"
	keyThingTypeArn            = "thingTypeArn"
	keyThingGroupName          = "thingGroupName"
	keyThingGroupArn           = "thingGroupArn"
	keyCertificateID           = "certificateId"
	keyCertificateArn          = "certificateArn"
	keyCertificateProviderName = "certificateProviderName"
	keyCertificateProviderArn  = "certificateProviderArn"
	keyIsDefaultVersion        = "isDefaultVersion"
	keyPolicyDocument          = "policyDocument"
	keyAttributes              = "attributes"
	keyVersion                 = "version"
	keyTimestamp               = "timestamp"
	keyStatus                  = "status"
	keyArn                     = "arn"
	keyCreationDate            = "creationDate"
	keyLastModifiedDate        = "lastModifiedDate"
	keyPolicyVersionID         = "policyVersionId"
	keyInvalidPath             = "invalid path"

	// URL path prefix constants.
	pathPolicies         = "/policies"
	pathRuleDestinations = "/rule-destinations"
	pathIndices          = "/indices"
	// pathIndexingConfig is the real AWS IoT REST path for Get/UpdateIndexingConfiguration
	// (GET/POST /indexing/config). It is distinct from pathIndices ("/indices"), which is
	// where the fleet-indexing search/aggregation surface (e.g. SearchIndex at
	// /indices/search) lives.
	pathIndexingConfig = "/indexing/config"
	pathThings         = "/things"
)

const (
	keyStreamID           = "streamId"
	keyStreamARN          = "streamArn"
	keyMetricName         = "metricName"
	keyMetricARN          = "metricArn"
	pathTags              = "/tags"
	pathDefaultAuthorizer = "/default-authorizer"
	keyCertificates       = "certificates"
	keyPolicies           = "policies"
	keyThings             = "things"
)

const (
	pathSegmentVersions   = "versions"
	pathSegmentExecutions = "executions"
	pathV2LoggingLevel    = "/v2LoggingLevel"
	keyCertificatePem     = "certificatePem"
	keyBoolTrue           = "true"
	keyName               = "name"
	keyThingGroupID       = "thingGroupId"
	pathSplitTwo          = 2
	pathSplitThree        = 3
)

const (
	pathThingRegistrationTasks = "/thing-registration-tasks"
	pathManagedJobTemplates    = "/managed-job-templates"
	pathUpdateThingGroups      = "/thing-groups/updateThingGroupsForThing"

	suffixCancel  = "/cancel"
	suffixReports = "/reports"

	keyTaskID          = "taskId"
	keyTemplateArn     = "templateArn"
	keyTemplateVersion = "templateVersion"
	keyEnvironments    = "environments"
)

const (
	twoparts = 2 // used for SplitN to get at most 2 parts

	keyJobID               = "jobId"
	keyJobARN              = "jobArn"
	keyDescription         = "description"
	keyCreatedAt           = "createdAt"
	keyLastUpdatedAt       = "lastUpdatedAt"
	keyDomainConfigName    = "domainConfigurationName"
	keyDomainConfigARN     = "domainConfigurationArn"
	keyTemplateName        = "templateName"
	keyAuthorizerName      = "authorizerName"
	keyAuthorizerARN       = "authorizerArn"
	keyScheduledAuditARN   = "scheduledAuditArn"
	keyActionARN           = "actionArn"
	keySecurityProfileName = "securityProfileName"
	keySecurityProfileARN  = "securityProfileArn"
)

const (
	pathEncryptionConfiguration          = "/encryption-configuration"
	pathTestAuthorization                = "/test-authorization"
	pathPrincipalPolicies                = "/principal-policies"
	pathConfirmDestination               = "/confirmdestination"
	pathCommandExecutions                = "/command-executions"
	pathBehaviorModelTrainingSummaries   = "/behavior-model-training/summaries"
	pathCertificatesOutgoing             = "/certificates-out-going"
	pathMetricValues                     = "/metric-values"
	pathValidateSecurityProfileBehaviors = "/security-profile-behaviors/validate"
)
