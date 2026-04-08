// Package iot provides a mock AWS IoT Core service with an embedded MQTT broker,
// IoT SQL rules engine, and action dispatch to SQS and Lambda.
package iot

import "time"

// Thing represents an AWS IoT Thing.
type Thing struct {
	CreatedAt  time.Time
	Attributes map[string]string
	ThingName  string
	ThingType  string
	ARN        string
	Version    int64
}

// Policy represents an AWS IoT Policy.
type Policy struct {
	PolicyName     string
	PolicyDocument string
	ARN            string
}

// TopicRule represents an AWS IoT Topic Rule.
type TopicRule struct {
	CreatedAt   time.Time
	RuleName    string
	SQL         string
	Description string
	Actions     []RuleAction
	Enabled     bool
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
	SQL          string       `json:"sql"`
	Description  string       `json:"description"`
	Actions      []RuleAction `json:"actions"`
	RuleDisabled bool         `json:"ruleDisabled"`
}

// CreatePolicyInput is the input for CreatePolicy.
type CreatePolicyInput struct {
	PolicyName     string
	PolicyDocument string
}

// CreatePolicyOutput is the output for CreatePolicy.
type CreatePolicyOutput struct {
	PolicyName     string
	PolicyARN      string
	PolicyDocument string
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
