package shield

import "context"

// StorageBackend is the interface for Shield Advanced storage operations.
type StorageBackend interface {
	CreateSubscription() error
	DeleteSubscription() error
	UpdateSubscription(autoRenew string) error
	DescribeSubscription() (*Subscription, error)
	GetSubscriptionState() string
	CreateProtection(name, resourceARN string, tags map[string]string) (*Protection, error)
	DescribeProtection(protectionID, resourceARN string) (*Protection, error)
	DeleteProtection(protectionID string) error
	ListProtections() []*Protection
	AssociateHealthCheck(protectionID, healthCheckARN string) error
	DisassociateHealthCheck(protectionID, healthCheckARN string) error
	TagResource(resourceARN string, tags map[string]string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	UntagResource(resourceARN string, tagKeys []string) error
	AssociateDRTLogBucket(bucket string) error
	DisassociateDRTLogBucket(bucket string) error
	AssociateDRTRole(roleARN string) error
	DisassociateDRTRole() error
	DescribeDRTAccess() *DRTAccess
	AssociateProactiveEngagementDetails(contacts []EmergencyContact) error
	UpdateEmergencyContactSettings(contacts []EmergencyContact) error
	DescribeEmergencyContactSettings() []EmergencyContact
	EnableProactiveEngagement() error
	DisableProactiveEngagement() error
	CreateProtectionGroup(id, aggregation, pattern, resourceType string, members []string) (*ProtectionGroup, error)
	DescribeProtectionGroup(id string) (*ProtectionGroup, error)
	ListProtectionGroups() []*ProtectionGroup
	UpdateProtectionGroup(id, aggregation, pattern, resourceType string, members []string) error
	DeleteProtectionGroup(protectionGroupID string) error
	ListAttacks(resourceARNs []string, startTime, endTime int64) []*Attack
	DescribeAttack(attackID string) (*Attack, error)
	DescribeAttackStatistics() *AttackStatistics
	SimulateAttack(resourceARN string, attackVectorTypes []string) (*Attack, error)
	EnableApplicationLayerAutomaticResponse(resourceARN, action string) error
	DisableApplicationLayerAutomaticResponse(resourceARN string) error
	UpdateApplicationLayerAutomaticResponse(resourceARN, action string) error
	ListResourcesInProtectionGroup(protectionGroupID string) ([]string, error)
	GetProactiveEngagementStatus() string
	GetALARConfig(resourceARN string) *ALARConfig
	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
