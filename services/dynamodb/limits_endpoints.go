// Package dynamodb implements the AWS DynamoDB mock service.
// limits_endpoints.go implements DescribeLimits and DescribeEndpoints, which
// return hardcoded account/table capacity limits and regional endpoint info.
package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// --- Hardcoded limits matching AWS defaults ---

const (
	// accountMaxReadCapacityUnits is the default account-level read capacity limit.
	accountMaxReadCapacityUnits int64 = 40000
	// accountMaxWriteCapacityUnits is the default account-level write capacity limit.
	accountMaxWriteCapacityUnits int64 = 40000
	// tableMaxReadCapacityUnits is the default per-table read capacity limit.
	tableMaxReadCapacityUnits int64 = 40000
	// tableMaxWriteCapacityUnits is the default per-table write capacity limit.
	tableMaxWriteCapacityUnits int64 = 40000
	// endpointCachePeriodMinutes is the cache TTL for DynamoDB endpoint discovery.
	endpointCachePeriodMinutes int64 = 300
)

// --- DescribeLimits ---

// DescribeLimits returns hardcoded account and table provisioned throughput limits.
func (db *InMemoryDB) DescribeLimits(
	_ context.Context,
	_ *dynamodb.DescribeLimitsInput,
) (*dynamodb.DescribeLimitsOutput, error) {
	accountMaxRCU := accountMaxReadCapacityUnits
	accountMaxWCU := accountMaxWriteCapacityUnits
	tableMaxRCU := tableMaxReadCapacityUnits
	tableMaxWCU := tableMaxWriteCapacityUnits

	return &dynamodb.DescribeLimitsOutput{
		AccountMaxReadCapacityUnits:  &accountMaxRCU,
		AccountMaxWriteCapacityUnits: &accountMaxWCU,
		TableMaxReadCapacityUnits:    &tableMaxRCU,
		TableMaxWriteCapacityUnits:   &tableMaxWCU,
	}, nil
}

// --- DescribeEndpoints ---

// DescribeEndpoints returns hardcoded regional DynamoDB endpoint information.
func (db *InMemoryDB) DescribeEndpoints(
	ctx context.Context,
	_ *dynamodb.DescribeEndpointsInput,
) (*dynamodb.DescribeEndpointsOutput, error) {
	region := getRegionFromContext(ctx, db)
	address := fmt.Sprintf("dynamodb.%s.amazonaws.com", region)
	cachePeriod := endpointCachePeriodMinutes

	return &dynamodb.DescribeEndpointsOutput{
		Endpoints: []types.Endpoint{
			{
				Address:              &address,
				CachePeriodInMinutes: cachePeriod,
			},
		},
	}, nil
}
