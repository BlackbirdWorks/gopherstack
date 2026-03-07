package iot

// StorageBackend defines the interface for the IoT control-plane backend.
type StorageBackend interface {
	CreateThing(input *CreateThingInput) (*CreateThingOutput, error)
	DescribeThing(thingName string) (*Thing, error)
	ListThings() []*Thing
	DeleteThing(thingName string) error

	CreateTopicRule(input *CreateTopicRuleInput) error
	GetTopicRule(ruleName string) (*TopicRule, error)
	ListTopicRules() []*TopicRule
	DeleteTopicRule(ruleName string) error

	CreatePolicy(input *CreatePolicyInput) (*CreatePolicyOutput, error)
	AttachPrincipalPolicy(input *AttachPrincipalPolicyInput) error

	DescribeEndpoint(endpointType string) (*DescribeEndpointOutput, error)
}
