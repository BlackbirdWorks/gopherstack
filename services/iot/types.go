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
