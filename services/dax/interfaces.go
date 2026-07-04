package dax

import (
	"context"
	"time"
)

// StorageBackend is the interface for DAX storage operations.
type StorageBackend interface {
	// Cluster operations.
	CreateCluster(input CreateClusterInput) (*Cluster, error)
	DescribeClusters(
		clusterNames []string,
		maxResults int,
		nextToken string,
	) ([]*Cluster, string, error)
	UpdateCluster(input UpdateClusterInput) (*Cluster, error)
	DeleteCluster(clusterName string) (*Cluster, error)
	IncreaseReplicationFactor(input IncreaseReplicationFactorInput) (*Cluster, error)
	DecreaseReplicationFactor(input DecreaseReplicationFactorInput) (*Cluster, error)
	RebootNode(clusterName, nodeID string) (*Cluster, error)

	// Tag operations.
	TagResource(resourceArn string, tags map[string]string) (map[string]string, error)
	UntagResource(resourceArn string, tagKeys []string) (map[string]string, error)
	ListTags(resourceArn string, nextToken string) (map[string]string, string, error)

	// Parameter group operations.
	CreateParameterGroup(name, description string) (*ParameterGroup, error)
	DescribeParameterGroups(
		names []string,
		maxResults int,
		nextToken string,
	) ([]*ParameterGroup, string, error)
	UpdateParameterGroup(input UpdateParameterGroupInput) (*ParameterGroup, error)
	DeleteParameterGroup(name string) error
	DescribeParameters(paramGroupName string, maxResults int, nextToken string) ([]*Parameter, string, error)
	DescribeDefaultParameters(maxResults int, nextToken string) ([]*Parameter, string, error)
	ResetParameterGroup(name string, parameterNames []string) (*ParameterGroup, error)

	// Subnet group operations.
	CreateSubnetGroup(name, description string, subnetIDs []string) (*SubnetGroup, error)
	DescribeSubnetGroups(
		names []string,
		maxResults int,
		nextToken string,
	) ([]*SubnetGroup, string, error)
	UpdateSubnetGroup(input UpdateSubnetGroupInput) (*SubnetGroup, error)
	DeleteSubnetGroup(name string) error

	// Event operations.
	DescribeEvents(
		sourceName string,
		sourceType string,
		startTime *time.Time,
		endTime *time.Time,
		maxResults int,
		nextToken string,
	) ([]*Event, string, error)

	GetDefaultTTL() (recordTTL time.Duration, queryTTL time.Duration)

	// Reset / persistence.
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
