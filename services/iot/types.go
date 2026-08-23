// Package iot provides a mock AWS IoT Core service with an embedded MQTT broker,
// IoT SQL rules engine, and action dispatch to SQS and Lambda.
package iot

import "time"

// Thing represents an AWS IoT Thing.
//
// BillingGroupName is populated transiently by DescribeThing from the
// backend's thingBillingGroups index; it is not itself the source of truth
// (see AddThingToBillingGroup/RemoveThingFromBillingGroup) and is not
// persisted as part of the Thing record.
type Thing struct {
	CreatedAt        time.Time         `json:"createdAt"`
	Attributes       map[string]string `json:"attributes"`
	ThingName        string            `json:"thingName"`
	ThingTypeName    string            `json:"thingTypeName,omitempty"`
	ThingType        string            `json:"thingType,omitempty"`
	ThingID          string            `json:"thingId"`
	ARN              string            `json:"thingArn"`
	BillingGroupName string            `json:"billingGroupName,omitempty"`
	Version          int64             `json:"version"`
}

// Policy represents an AWS IoT Policy.
type Policy struct {
	CreatedAt      time.Time `json:"creationDate"`
	LastModifiedAt time.Time `json:"lastModifiedDate"`
	PolicyName     string    `json:"policyName"`
	PolicyDocument string    `json:"policyDocument"`
	ARN            string    `json:"policyArn"`
}

// TopicRule represents an AWS IoT Topic Rule.
type TopicRule struct {
	CreatedAt        time.Time    `json:"createdAt"`
	RuleName         string       `json:"ruleName"`
	ARN              string       `json:"arn"`
	SQL              string       `json:"sql"`
	AWSIoTSQLVersion string       `json:"awsIotSqlVersion,omitempty"`
	Description      string       `json:"description,omitempty"`
	Actions          []RuleAction `json:"actions"`
	Enabled          bool         `json:"enabled"`
}

// RuleAction represents an action taken when a rule matches.
type RuleAction struct {
	SQS    *SQSAction    `json:"sqs,omitempty"`
	Lambda *LambdaAction `json:"lambda,omitempty"`
}

// SQSAction sends the matched message to an SQS queue.
type SQSAction struct {
	QueueURL string `json:"queueUrl"`
	RoleARN  string `json:"roleArn"`
}

// LambdaAction invokes a Lambda function with the matched message payload.
type LambdaAction struct {
	FunctionARN string `json:"functionArn"`
}

// CreateThingInput is the input for CreateThing.
type CreateThingInput struct {
	AttributePayload *AttributePayload
	ThingName        string
	ThingTypeName    string
	BillingGroupName string
}

// AttributePayload holds thing attributes.
type AttributePayload struct {
	Attributes map[string]string `json:"attributes"`
	// Merge controls whether attributes are merged (true, default) or replaced (false).
	Merge *bool `json:"merge,omitempty"`
}

// ThingShadow represents the state of an AWS IoT Device Shadow.
type ThingShadow struct {
	State    map[string]any `json:"state"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Version  int64          `json:"version"`
}

// shadowKey is the composite key for named vs classic shadows.
type shadowKey struct {
	thingName  string
	shadowName string // empty string = classic shadow
}

// CreateThingOutput is the output for CreateThing.
type CreateThingOutput struct {
	ThingName string
	ThingARN  string
	ThingID   string
}

// CreateTopicRuleInput is the input for CreateTopicRule.
type CreateTopicRuleInput struct {
	TopicRulePayload *TopicRulePayload
	Tags             map[string]string
	RuleName         string
}

// TopicRulePayload is the payload for a topic rule.
type TopicRulePayload struct {
	SQL              string       `json:"sql"`
	Description      string       `json:"description"`
	AWSIoTSQLVersion string       `json:"awsIotSqlVersion,omitempty"`
	Actions          []RuleAction `json:"actions"`
	RuleDisabled     bool         `json:"ruleDisabled"`
}

// CreatePolicyInput is the input for CreatePolicy.
type CreatePolicyInput struct {
	Tags           map[string]string
	PolicyName     string
	PolicyDocument string
}

// CreatePolicyOutput is the output for CreatePolicy.
type CreatePolicyOutput struct {
	PolicyName      string
	PolicyARN       string
	PolicyDocument  string
	PolicyVersionID string
}

// AttachPrincipalPolicyInput is the input for AttachPrincipalPolicy.
type AttachPrincipalPolicyInput struct {
	PolicyName string
	Principal  string
}

// DescribeEndpointOutput is the output for DescribeEndpoint.
type DescribeEndpointOutput struct {
	EndpointAddress string
}

// AcceptCertificateTransferInput is the input for AcceptCertificateTransfer.
type AcceptCertificateTransferInput struct {
	CertificateID string
	SetAsActive   bool
}

// AddThingToBillingGroupInput is the input for AddThingToBillingGroup.
type AddThingToBillingGroupInput struct {
	BillingGroupName string `json:"billingGroupName"`
	BillingGroupArn  string `json:"billingGroupArn"`
	ThingName        string `json:"thingName"`
	ThingArn         string `json:"thingArn"`
}

// AddThingToThingGroupInput is the input for AddThingToThingGroup.
type AddThingToThingGroupInput struct {
	ThingGroupName        string `json:"thingGroupName"`
	ThingGroupArn         string `json:"thingGroupArn"`
	ThingName             string `json:"thingName"`
	ThingArn              string `json:"thingArn"`
	OverrideDynamicGroups bool   `json:"overrideDynamicGroups"`
}

// AssociateSbomWithPackageVersionInput is the input for AssociateSbomWithPackageVersion.
type AssociateSbomWithPackageVersionInput struct {
	Sbom        *SbomDocument `json:"sbom"`
	PackageName string        `json:"packageName"`
	VersionName string        `json:"versionName"`
}

// SbomDocument represents an SBOM document reference.
type SbomDocument struct {
	S3Location *S3Location `json:"s3Location"`
}

// S3Location represents an S3 object location.
type S3Location struct {
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
	Version string `json:"version"`
}

// AssociateSbomWithPackageVersionOutput is the output for AssociateSbomWithPackageVersion.
type AssociateSbomWithPackageVersionOutput struct {
	PackageName          string        `json:"packageName"`
	VersionName          string        `json:"versionName"`
	Sbom                 *SbomDocument `json:"sbom,omitempty"`
	SbomValidationStatus string        `json:"sbomValidationStatus,omitempty"`
}

// AssociateTargetsWithJobInput is the input for AssociateTargetsWithJob.
type AssociateTargetsWithJobInput struct {
	JobID       string   `json:"jobId"`
	Comment     string   `json:"comment"`
	NamespaceID string   `json:"namespaceId"`
	Targets     []string `json:"targets"`
}

// AssociateTargetsWithJobOutput is the output for AssociateTargetsWithJob.
type AssociateTargetsWithJobOutput struct {
	JobID       string `json:"jobId"`
	JobArn      string `json:"jobArn"`
	Description string `json:"description"`
}

// AttachPolicyInput is the input for AttachPolicy.
type AttachPolicyInput struct {
	PolicyName string `json:"policyName"`
	Target     string `json:"target"`
}

// AttachSecurityProfileInput is the input for AttachSecurityProfile.
type AttachSecurityProfileInput struct {
	SecurityProfileName      string
	SecurityProfileTargetArn string `json:"securityProfileTargetArn"`
}

// AttachThingPrincipalInput is the input for AttachThingPrincipal.
type AttachThingPrincipalInput struct {
	ThingName          string
	Principal          string `json:"principal"`
	ThingPrincipalType string `json:"thingPrincipalType"`
}

// CancelAuditMitigationActionsTaskInput is the input for CancelAuditMitigationActionsTask.
type CancelAuditMitigationActionsTaskInput struct {
	TaskID string
}

// CancelAuditTaskInput is the input for CancelAuditTask.
type CancelAuditTaskInput struct {
	AuditTaskID string
}

// GetPolicyOutput is the output for GetPolicy.
type GetPolicyOutput struct {
	CreatedAt        time.Time `json:"creationDate"`
	LastModifiedAt   time.Time `json:"lastModifiedDate"`
	PolicyName       string    `json:"policyName"`
	PolicyARN        string    `json:"policyArn"`
	PolicyDocument   string    `json:"policyDocument"`
	DefaultVersionID string    `json:"defaultVersionId"`
}

// UpdateThingInput is the input for UpdateThing.
type UpdateThingInput struct {
	AttributePayload *AttributePayload `json:"attributePayload"`
	ThingName        string            `json:"thingName"`
	ThingTypeName    string            `json:"thingTypeName,omitempty"`
	RemoveThingType  bool              `json:"removeThingType,omitempty"`
	ExpectedVersion  int64             `json:"expectedVersion,omitempty"`
}

// ThingType represents an AWS IoT Thing Type.
type ThingType struct {
	CreatedAt            time.Time `json:"createdAt"`
	DeprecationDate      time.Time `json:"deprecationDate"`
	ThingTypeName        string    `json:"thingTypeName"`
	ThingTypeID          string    `json:"thingTypeId"`
	ThingTypeARN         string    `json:"thingTypeArn"`
	Description          string    `json:"description,omitempty"`
	SearchableAttributes []string  `json:"searchableAttributes,omitempty"`
	Deprecated           bool      `json:"deprecated"`
}

// GroupNameAndARN pairs a thing group's name and ARN. Used by
// DescribeThingGroup's thingGroupMetadata.rootToParentThingGroups (real
// types.GroupNameAndArn -- verified field names groupName/groupArn against
// aws-sdk-go-v2/service/iot@v1.77.4's deserializer).
type GroupNameAndARN struct {
	GroupName string `json:"groupName,omitempty"`
	GroupARN  string `json:"groupArn,omitempty"`
}

// ThingGroup represents an AWS IoT Thing Group.
type ThingGroup struct {
	CreatedAt       time.Time         `json:"createdAt"`
	Attributes      map[string]string `json:"attributes"`
	ThingGroupName  string            `json:"thingGroupName"`
	ThingGroupARN   string            `json:"thingGroupArn"`
	ThingGroupID    string            `json:"thingGroupId"`
	Description     string            `json:"description,omitempty"`
	ParentGroupName string            `json:"parentGroupName,omitempty"`
	QueryString     string            `json:"queryString,omitempty"`
	// IndexName/QueryVersion apply only to dynamic thing groups
	// (types.CreateDynamicThingGroupInput/UpdateDynamicThingGroupInput,
	// aws-sdk-go-v2/service/iot@v1.77.4).
	IndexName    string   `json:"indexName,omitempty"`
	QueryVersion string   `json:"queryVersion,omitempty"`
	Members      []string `json:"members"`
	Version      int64    `json:"version"`
	IsDynamic    bool     `json:"isDynamic,omitempty"`
}

// Certificate represents an AWS IoT Certificate.
//
// The Owned/PreviousOwnedBy/Transfer* fields model the cross-account transfer
// lifecycle (TransferCertificate/AcceptCertificateTransfer/
// CancelCertificateTransfer/RejectCertificateTransfer) so DescribeCertificate
// can surface real ownedBy/previousOwnedBy/transferData, matching the real
// CertificateDescription shape instead of only tracking pending transfers in
// a side map.
type Certificate struct {
	CreatedAt            time.Time `json:"createdAt"`
	LastModifiedAt       time.Time `json:"lastModifiedDate"`
	ValidityNotBefore    time.Time `json:"validityNotBefore"`
	ValidityNotAfter     time.Time `json:"validityNotAfter"`
	TransferDate         time.Time `json:"transferDate"`
	TransferAcceptDate   time.Time `json:"transferAcceptDate"`
	TransferRejectDate   time.Time `json:"transferRejectDate"`
	CertificateID        string    `json:"certificateId"`
	CACertificateID      string    `json:"caCertificateId,omitempty"`
	ARN                  string    `json:"certificateArn"`
	Status               string    `json:"status"`
	PEM                  string    `json:"certificatePem"`
	OwnedBy              string    `json:"ownedBy,omitempty"`
	PreviousOwnedBy      string    `json:"previousOwnedBy,omitempty"`
	GenerationID         string    `json:"generationId,omitempty"`
	CertificateMode      string    `json:"certificateMode,omitempty"`
	TransferredTo        string    `json:"transferredTo,omitempty"`
	TransferMessage      string    `json:"transferMessage,omitempty"`
	TransferRejectReason string    `json:"transferRejectReason,omitempty"`
	CustomerVersion      int32     `json:"customerVersion,omitempty"`
}

// PolicyVersion represents a version of an AWS IoT policy.
type PolicyVersion struct {
	CreatedAt        time.Time `json:"createDate"`
	VersionID        string    `json:"versionId"`
	PolicyDocument   string    `json:"policyDocument"`
	GenerationID     string    `json:"generationId,omitempty"`
	IsDefaultVersion bool      `json:"isDefaultVersion"`
}

// TopicRuleDestination represents an AWS IoT Topic Rule Destination.
type TopicRuleDestination struct {
	HTTPURLProperties *HTTPURLDestinationProperties `json:"httpUrlProperties,omitempty"`
	ARN               string                        `json:"arn"`
	Status            string                        `json:"status"`
	// ConfirmationToken is the token that must be presented to
	// ConfirmTopicRuleDestination to transition an HTTP destination from
	// IN_PROGRESS to ENABLED. AWS delivers this out-of-band (via a
	// confirmation request sent to the destination URL), so it is never
	// serialized back to callers.
	ConfirmationToken string `json:"-"`
}

// HTTPURLDestinationProperties holds properties for an HTTP URL destination.
type HTTPURLDestinationProperties struct {
	ConfirmationURL string `json:"confirmationUrl"`
}

// CertificateProvider represents an AWS IoT Certificate Provider.
type CertificateProvider struct {
	CreatedAt                   time.Time `json:"creationDate"`
	LastModifiedAt              time.Time `json:"lastModifiedDate"`
	CertificateProviderName     string    `json:"certificateProviderName"`
	ARN                         string    `json:"certificateProviderArn"`
	LambdaFunctionARN           string    `json:"lambdaFunctionArn"`
	AccountDefaultForOperations []string  `json:"accountDefaultForOperations"`
}

// CreateThingTypeInput is the input for CreateThingType.
type CreateThingTypeInput struct {
	Tags                 map[string]string
	ThingTypeName        string
	Description          string
	SearchableAttributes []string
}

// DeprecateThingTypeInput is the input for DeprecateThingType.
type DeprecateThingTypeInput struct {
	ThingTypeName string
	UndoDeprecate bool
}

// CreateThingGroupInput is the input for CreateThingGroup.
type CreateThingGroupInput struct {
	Attributes      map[string]string
	Tags            map[string]string
	ThingGroupName  string
	ParentGroupName string
	Description     string
	QueryString     string
	IndexName       string
	QueryVersion    string
}

// UpdateThingGroupInput is the input for UpdateThingGroup.
//
// Merge controls whether Attributes are merged (true) or replace the
// existing attribute set (false/nil, the AWS default). See
// applyAttributePayload.
type UpdateThingGroupInput struct {
	Attributes      map[string]string
	Merge           *bool
	ThingGroupName  string
	Description     string
	QueryString     string
	IndexName       string
	QueryVersion    string
	ExpectedVersion int64
}

// RemoveThingFromThingGroupInput is the input for RemoveThingFromThingGroup.
type RemoveThingFromThingGroupInput struct {
	ThingGroupName string `json:"thingGroupName"`
	ThingGroupArn  string `json:"thingGroupArn"`
	ThingName      string `json:"thingName"`
	ThingArn       string `json:"thingArn"`
}

// CreateCertificateFromCsrInput is the input for CreateCertificateFromCsr.
type CreateCertificateFromCsrInput struct {
	CertificateSigningRequest string
	SetAsActive               bool
}

// RegisterCertificateInput is the input for RegisterCertificate.
type RegisterCertificateInput struct {
	CertificatePem string
	Status         string
}

// UpdateCertificateInput is the input for UpdateCertificate.
type UpdateCertificateInput struct {
	CertificateID string
	NewStatus     string
}

// CreatePolicyVersionInput is the input for CreatePolicyVersion.
type CreatePolicyVersionInput struct {
	PolicyName     string
	PolicyDocument string
	SetAsDefault   bool
}

// CreateTopicRuleDestinationInput is the input for CreateTopicRuleDestination.
type CreateTopicRuleDestinationInput struct {
	DestinationConfiguration *TopicRuleDestinationConfiguration
}

// TopicRuleDestinationConfiguration is the configuration for a topic rule destination.
type TopicRuleDestinationConfiguration struct {
	HTTPURLConfiguration *HTTPURLDestinationConfiguration `json:"httpUrlConfiguration,omitempty"`
}

// HTTPURLDestinationConfiguration holds configuration for an HTTP URL destination.
type HTTPURLDestinationConfiguration struct {
	ConfirmationURL string `json:"confirmationUrl"`
}

// UpdateTopicRuleDestinationInput is the input for UpdateTopicRuleDestination.
type UpdateTopicRuleDestinationInput struct {
	ARN    string `json:"arn"`
	Status string `json:"status"`
}

// DetachPolicyInput is the input for DetachPolicy.
type DetachPolicyInput struct {
	PolicyName string `json:"policyName"`
	Target     string `json:"target"`
}

// ListAttachedPoliciesInput is the input for ListAttachedPolicies.
type ListAttachedPoliciesInput struct {
	Target    string
	Recursive bool
}

// CreateCertificateProviderInput is the input for CreateCertificateProvider.
type CreateCertificateProviderInput struct {
	Tags                        map[string]string
	CertificateProviderName     string
	LambdaFunctionARN           string
	AccountDefaultForOperations []string
}

// UpdateCertificateProviderInput is the input for UpdateCertificateProvider.
type UpdateCertificateProviderInput struct {
	CertificateProviderName     string
	LambdaFunctionARN           string
	AccountDefaultForOperations []string
}

// ListThingsInThingGroupInput is the input for ListThingsInThingGroup.
type ListThingsInThingGroupInput struct {
	ThingGroupName string
	Recursive      bool
}

// DisableTopicRuleInput is the input for DisableTopicRule.
type DisableTopicRuleInput struct {
	RuleName string
}

// EnableTopicRuleInput is the input for EnableTopicRule.
type EnableTopicRuleInput struct {
	RuleName string
}

// ReplaceTopicRuleInput is the input for ReplaceTopicRule.
type ReplaceTopicRuleInput struct {
	TopicRulePayload *TopicRulePayload
	RuleName         string
}

// IndexingField describes a custom field included in a fleet index.
type IndexingField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// IndexingFilter restricts which named shadows are indexed.
type IndexingFilter struct {
	NamedShadowNames []string `json:"namedShadowNames,omitempty"`
}

// ThingIndexingConfiguration controls fleet indexing for Things.
type ThingIndexingConfiguration struct {
	Filter                        *IndexingFilter `json:"filter,omitempty"`
	ThingIndexingMode             string          `json:"thingIndexingMode"`
	ThingConnectivityIndexingMode string          `json:"thingConnectivityIndexingMode,omitempty"`
	NamedShadowIndexingMode       string          `json:"namedShadowIndexingMode,omitempty"`
	DeviceDefenderIndexingMode    string          `json:"deviceDefenderIndexingMode,omitempty"`
	CustomFields                  []IndexingField `json:"customFields,omitempty"`
}

// ThingGroupIndexingConfiguration controls fleet indexing for Thing Groups.
type ThingGroupIndexingConfiguration struct {
	ThingGroupIndexingMode string          `json:"thingGroupIndexingMode"`
	CustomFields           []IndexingField `json:"customFields,omitempty"`
}

// UpdateIndexingConfigurationInput is the input for UpdateIndexingConfiguration.
type UpdateIndexingConfigurationInput struct {
	ThingIndexingConfiguration      *ThingIndexingConfiguration
	ThingGroupIndexingConfiguration *ThingGroupIndexingConfiguration
}

// GetIndexingConfigurationOutput is the output for GetIndexingConfiguration.
type GetIndexingConfigurationOutput struct {
	ThingIndexingConfiguration      *ThingIndexingConfiguration
	ThingGroupIndexingConfiguration *ThingGroupIndexingConfiguration
}

// IndexDescription describes a fleet index (returned by DescribeIndex).
type IndexDescription struct {
	IndexName   string
	IndexStatus string
	Schema      string
}

// SearchIndexInput is the input for SearchIndex.
type SearchIndexInput struct {
	IndexName   string
	QueryString string
	NextToken   string
	MaxResults  int32
}

// SearchIndexThingResult is a Thing document returned by SearchIndex.
type SearchIndexThingResult struct {
	Attributes      map[string]string `json:"attributes"`
	ThingName       string            `json:"thingName"`
	ThingID         string            `json:"thingId"`
	ThingTypeName   string            `json:"thingTypeName,omitempty"`
	ThingGroupNames []string          `json:"thingGroupNames,omitempty"`
}

// SearchIndexThingGroupResult is a ThingGroup document returned by
// SearchIndex. "parentGroupNames" is a list of every ancestor group name up
// to the root, not just the direct parent (types.ThingGroupDocument,
// awsRestjson1_deserializeDocumentThingGroupDocument, v1.77.4).
type SearchIndexThingGroupResult struct {
	Attributes            map[string]string `json:"attributes"`
	ThingGroupName        string            `json:"thingGroupName"`
	ThingGroupID          string            `json:"thingGroupId"`
	ThingGroupDescription string            `json:"thingGroupDescription,omitempty"`
	ParentGroupNames      []string          `json:"parentGroupNames,omitempty"`
}

// SearchIndexOutput is the output for SearchIndex.
type SearchIndexOutput struct {
	NextToken   string
	Things      []*SearchIndexThingResult
	ThingGroups []*SearchIndexThingGroupResult
}

// AggregationInput carries the inputs shared by GetCardinality, GetStatistics,
// GetPercentiles and GetBucketsAggregation.
type AggregationInput struct {
	IndexName        string
	QueryString      string
	AggregationField string
}

// Statistics is the output for GetStatistics.
type Statistics struct {
	Count        int64
	Average      float64
	Sum          float64
	SumOfSquares float64
	Minimum      float64
	Maximum      float64
	Variance     float64
	StdDeviation float64
}

// PercentilesInput is the input for GetPercentiles.
type PercentilesInput struct {
	AggregationInput
	Percents []float64
}

// PercentileValue is a single (percent, value) pair returned by GetPercentiles.
type PercentileValue struct {
	Percent float64
	Value   float64
}

// BucketsAggregationInput is the input for GetBucketsAggregation.
type BucketsAggregationInput struct {
	AggregationInput
	MaxBuckets int32
}

// Bucket is a single term bucket with its match count, returned by GetBucketsAggregation.
type Bucket struct {
	KeyValue string
	Count    int64
}

// -----------------------------------------------------------
// Batch 4: RegisterThing / bulk thing registration / thing type
// and thing group updates / ListThingPrincipalsV2 / ManagedJobTemplate
// -----------------------------------------------------------

// RegisterThingInput is the input for RegisterThing.
type RegisterThingInput struct {
	Parameters   map[string]string `json:"parameters"`
	TemplateBody string            `json:"templateBody"`
}

// RegisterThingOutput is the output for RegisterThing.
type RegisterThingOutput struct {
	ResourceArns   map[string]string `json:"resourceArns"`
	CertificatePem string            `json:"certificatePem"`
}

// ThingRegistrationTask represents a bulk thing provisioning task.
//
// CreationDate/LastModifiedDate are epoch-seconds (float64), matching the real AWS IoT JSON
// wire shape and the convention used elsewhere in this package (see thing_registration.go),
// not RFC3339 strings.
type ThingRegistrationTask struct {
	TaskID             string  `json:"taskId"`
	Status             string  `json:"status"`
	Message            string  `json:"message,omitempty"`
	TemplateBody       string  `json:"templateBody,omitempty"`
	InputFileBucket    string  `json:"inputFileBucket,omitempty"`
	InputFileKey       string  `json:"inputFileKey,omitempty"`
	RoleArn            string  `json:"roleArn,omitempty"`
	CreationDate       float64 `json:"creationDate"`
	LastModifiedDate   float64 `json:"lastModifiedDate"`
	SuccessCount       int32   `json:"successCount"`
	FailureCount       int32   `json:"failureCount"`
	PercentageProgress int32   `json:"percentageProgress"`
}

// StartThingRegistrationTaskInput is the input for StartThingRegistrationTask.
type StartThingRegistrationTaskInput struct {
	TemplateBody    string `json:"templateBody"`
	InputFileBucket string `json:"inputFileBucket"`
	InputFileKey    string `json:"inputFileKey"`
	RoleArn         string `json:"roleArn"`
}

// UpdateThingTypeInput is the input for UpdateThingType.
type UpdateThingTypeInput struct {
	ThingTypeName        string
	Description          string
	SearchableAttributes []string
}

// UpdateThingGroupsForThingInput is the input for UpdateThingGroupsForThing.
type UpdateThingGroupsForThingInput struct {
	ThingName             string   `json:"thingName"`
	ThingGroupsToAdd      []string `json:"thingGroupsToAdd"`
	ThingGroupsToRemove   []string `json:"thingGroupsToRemove"`
	OverrideDynamicGroups bool     `json:"overrideDynamicGroups"`
}

// ThingPrincipalObject represents a principal and its relation type to a thing,
// as returned by ListThingPrincipalsV2.
type ThingPrincipalObject struct {
	Principal          string `json:"principal"`
	ThingPrincipalType string `json:"thingPrincipalType"`
}

// ManagedJobTemplateParameter describes one substitutable field in a managed
// job template document.
type ManagedJobTemplateParameter struct {
	Key         string `json:"key"`
	Description string `json:"description,omitempty"`
	Regex       string `json:"regex,omitempty"`
	Example     string `json:"example,omitempty"`
	Optional    bool   `json:"optional"`
}

// ManagedJobTemplate represents an AWS-managed job template.
type ManagedJobTemplate struct {
	TemplateName       string                        `json:"templateName"`
	TemplateArn        string                        `json:"templateArn"`
	TemplateVersion    string                        `json:"templateVersion"`
	Description        string                        `json:"description,omitempty"`
	Document           string                        `json:"document"`
	Environments       []string                      `json:"environments,omitempty"`
	DocumentParameters []ManagedJobTemplateParameter `json:"documentParameters,omitempty"`
}

// ManagedJobTemplateSummary is the summary form of a ManagedJobTemplate
// returned by ListManagedJobTemplates.
type ManagedJobTemplateSummary struct {
	TemplateName    string   `json:"templateName"`
	TemplateArn     string   `json:"templateArn"`
	TemplateVersion string   `json:"templateVersion"`
	Description     string   `json:"description,omitempty"`
	Environments    []string `json:"environments,omitempty"`
}
