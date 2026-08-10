package mq

import "context"

// StorageBackend defines the interface for the Amazon MQ in-memory backend.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Broker operations
	CreateBroker(
		name, deploymentMode, engineType, engineVersion, hostInstanceType string,
		publiclyAccessible, autoMinorVersionUpgrade bool,
		securityGroups, subnetIDs []string,
		users []*User,
		tags map[string]string,
	) (*Broker, error)
	CreateBrokerWithOptions(
		name, deploymentMode, engineType, engineVersion, hostInstanceType string,
		publiclyAccessible, autoMinorVersionUpgrade bool,
		securityGroups, subnetIDs []string,
		users []*User,
		tags map[string]string,
		opts *CreateBrokerOptions,
	) (*Broker, error)
	DescribeBroker(brokerID string) (*Broker, error)
	ListBrokers() []*Broker
	UpdateBroker(
		brokerID, engineVersion, hostInstanceType string,
		autoMinorVersionUpgrade *bool,
		securityGroups []string,
	) (*Broker, error)
	UpdateBrokerWithOptions(
		brokerID, engineVersion, hostInstanceType string,
		autoMinorVersionUpgrade *bool,
		securityGroups []string,
		opts *UpdateBrokerOptions,
	) (*Broker, error)
	DeleteBroker(brokerID string) (*Broker, error)
	RebootBroker(brokerID string) error
	Promote(brokerID, mode string) (*Broker, error)
	DescribeSharedResources(brokerID string) ([]SharedResource, error)

	// User operations
	CreateUser(brokerID, username, password string, groups []string, console, replicationUser bool) error
	DescribeUser(brokerID, username string) (*User, error)
	UpdateUser(brokerID, username, password string, groups []string, console, replicationUser *bool) error
	DeleteUser(brokerID, username string) error
	ListUsers(brokerID string) ([]UserSummary, error)

	// Configuration operations
	CreateConfiguration(
		name, description, engineType, engineVersion string,
		tags map[string]string,
	) (*Configuration, error)
	DescribeConfiguration(configID string) (*Configuration, error)
	ListConfigurations() []*Configuration
	UpdateConfiguration(configID, description, data string) (*Configuration, error)
	DeleteConfiguration(configID string) error

	// Configuration revision operations
	DescribeConfigurationRevision(configID string, revision int32) (*ConfigurationRevision, string, error)
	ListConfigurationRevisions(configID string) ([]ConfigurationRevision, error)

	// Broker metadata operations
	DescribeBrokerEngineTypes(engineType string) []BrokerEngineType
	DescribeBrokerInstanceOptions(engineType, hostInstanceType, storageType string) []BrokerInstanceOption

	// Tag operations
	ListTags(resourceARN string) (map[string]string, error)
	CreateTags(resourceARN string, tags map[string]string) error
	DeleteTags(resourceARN string, tagKeys []string) error

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
