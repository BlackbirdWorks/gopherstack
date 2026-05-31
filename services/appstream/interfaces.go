package appstream

import "time"

// StorageBackend is the interface for AppStream 2.0 storage operations.
type StorageBackend interface {
	// Stacks
	CreateStack(name, displayName, description string, tags map[string]string) (*Stack, error)
	DescribeStacks(names []string) ([]*Stack, error)
	UpdateStack(name, displayName, description string) (*Stack, error)
	DeleteStack(name string) error

	// Fleets
	CreateFleet(name, displayName, description, instanceType, fleetType string,
		maxUserDuration, disconnectTimeout int, tags map[string]string) (*Fleet, error)
	DescribeFleets(names []string) ([]*Fleet, error)
	UpdateFleet(name, displayName, description, instanceType string,
		maxUserDuration, disconnectTimeout int) (*Fleet, error)
	DeleteFleet(name string) error
	StartFleet(name string) error
	StopFleet(name string) error

	// Associations
	AssociateFleet(fleetName, stackName string) error
	DisassociateFleet(fleetName, stackName string) error
	ListAssociatedFleets(stackName string) ([]string, error)
	ListAssociatedStacks(fleetName string) ([]string, error)

	// Tags
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, keys []string) error
	ListTagsForResource(arn string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// Stack holds AppStream 2.0 stack details.
type Stack struct {
	CreatedTime time.Time
	Tags        map[string]string
	Name        string
	Arn         string
	DisplayName string
	Description string
}

// Fleet holds AppStream 2.0 fleet details.
type Fleet struct {
	CreatedTime           time.Time
	Tags                  map[string]string
	Name                  string
	Arn                   string
	DisplayName           string
	Description           string
	InstanceType          string
	FleetType             string
	State                 string
	MaxUserDurationSecs   int
	DisconnectTimeoutSecs int
}

var _ StorageBackend = (*InMemoryBackend)(nil)
