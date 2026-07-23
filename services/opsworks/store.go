package opsworks

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is an in-memory OpsWorks backend.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	stacks *store.Table[storedStack]

	layers             *store.Table[storedLayer]
	layersByStack      *store.Index[storedLayer]
	instances          *store.Table[storedInstance]
	instancesByStack   *store.Index[storedInstance]
	apps               *store.Table[storedApp]
	appsByStack        *store.Index[storedApp]
	deployments        *store.Table[storedDeployment]
	deploymentsByStack *store.Index[storedDeployment]
	commands           *store.Table[storedCommand]

	tags map[string]map[string]string

	userProfiles *store.Table[storedUserProfile]
	elasticLBs   *store.Table[storedElasticLoadBalancer]
	elasticIPs   *store.Table[storedElasticIP]

	volumes               *store.Table[storedVolume]
	volumesByStack        *store.Index[storedVolume]
	rdsDBInstances        *store.Table[storedRdsDBInstance]
	rdsDBInstancesByStack *store.Index[storedRdsDBInstance]
	ecsClusters           *store.Table[storedEcsCluster]
	ecsClustersByStack    *store.Index[storedEcsCluster]
	permissions           *store.Table[storedPermission]
	permissionsByStack    *store.Index[storedPermission]

	timeBasedAutoScale *store.Table[storedTimeBasedAutoScaling]
	loadBasedAutoScale *store.Table[storedLoadBasedAutoScaling]

	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory OpsWorks backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("opsworks"),
		registry:  store.NewRegistry(),
		tags:      make(map[string]map[string]string),
		accountID: accountID,
		region:    region,
	}
	registerAllTables(b)

	return b
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
}

func (b *InMemoryBackend) stackARN(stackID string) string {
	return arn.Build("opsworks", b.region, b.accountID, fmt.Sprintf("stack/%s", stackID))
}

func (b *InMemoryBackend) layerARN(layerID string) string {
	return arn.Build("opsworks", b.region, b.accountID, fmt.Sprintf("layer/%s", layerID))
}

func (b *InMemoryBackend) instanceARN(instanceID string) string {
	return arn.Build("opsworks", b.region, b.accountID, fmt.Sprintf("instance/%s", instanceID))
}

func (b *InMemoryBackend) appARN(appID string) string {
	return arn.Build("opsworks", b.region, b.accountID, fmt.Sprintf("app/%s", appID))
}

func permissionKey(stackID, iamUserArn string) string {
	return stackID + ":" + iamUserArn
}

// stackScoped returns byStack(stackID) when stackID is set, otherwise every
// value in the owning table (via all). It centralizes the "list everything,
// optionally narrowed to one stack" selection shared by several Describe*
// methods (DescribeInstances, DescribeDeployments, ...), each of which then
// applies its own additional in-memory filter on top of this base set.
func stackScoped[V any](stackID string, all func() []*V, byStack func(string) []*V) []*V {
	if stackID == "" {
		return all()
	}

	return byStack(stackID)
}

// resourceExists checks if a resource ARN refers to a known taggable
// resource. TagResource/UntagResource/ListTags only accept "the stack or
// layer's ARN" on the real API (confirmed against
// aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_TagResource.go,
// api_op_UntagResource.go, and api_op_ListTags.go doc comments, all three of
// which say exactly that) -- instances and apps are not independently
// taggable resources in real OpsWorks, so a previous pass accepting their
// ARNs here was broader than the real API.
func (b *InMemoryBackend) resourceExists(resourceArn string) bool {
	if strings.Contains(resourceArn, ":stack/") {
		return b.stacks.Has(arnSuffix(resourceArn))
	}
	if strings.Contains(resourceArn, ":layer/") {
		return b.layers.Has(arnSuffix(resourceArn))
	}

	return false
}

func arnSuffix(resourceArn string) string {
	parts := strings.Split(resourceArn, "/")
	if len(parts) == 0 {
		return ""
	}

	return parts[len(parts)-1]
}
