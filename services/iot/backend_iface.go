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
