// Package iot provides a mock AWS IoT Core service with an embedded MQTT broker,
// IoT SQL rules engine, and action dispatch to SQS and Lambda.
package iot

import "time"

// Thing represents an AWS IoT Thing.
type Thing struct {
	CreatedAt     time.Time         `json:"createdAt"`
	Attributes    map[string]string `json:"attributes"`
	ThingName     string            `json:"thingName"`
	ThingTypeName string            `json:"thingTypeName,omitempty"`
	ThingType     string            `json:"thingType,omitempty"`
	ThingID       string            `json:"thingId"`
	ARN           string            `json:"thingArn"`
	Version       int64             `json:"version"`
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
	Members         []string          `json:"members"`
	Version         int64             `json:"version"`
	IsDynamic       bool              `json:"isDynamic,omitempty"`
}

// Certificate represents an AWS IoT Certificate.
type Certificate struct {
	CreatedAt       time.Time `json:"createdAt"`
	LastModifiedAt  time.Time `json:"lastModifiedDate"`
	CertificateID   string    `json:"certificateId"`
	CACertificateID string    `json:"caCertificateId,omitempty"`
	ARN             string    `json:"certificateArn"`
	Status          string    `json:"status"`
	PEM             string    `json:"certificatePem"`
}

// PolicyVersion represents a version of an AWS IoT policy.
type PolicyVersion struct {
	CreatedAt        time.Time `json:"createDate"`
	VersionID        string    `json:"versionId"`
	PolicyDocument   string    `json:"policyDocument"`
	IsDefaultVersion bool      `json:"isDefaultVersion"`
}

// TopicRuleDestination represents an AWS IoT Topic Rule Destination.
type TopicRuleDestination struct {
	HTTPURLProperties *HTTPURLDestinationProperties `json:"httpUrlProperties,omitempty"`
	ARN               string                        `json:"arn"`
	Status            string                        `json:"status"`
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
	ThingGroupName  string
	ParentGroupName string
	Description     string
	QueryString     string
}

// UpdateThingGroupInput is the input for UpdateThingGroup.
type UpdateThingGroupInput struct {
	Attributes      map[string]string
	ThingGroupName  string
	Description     string
	QueryString     string
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
