package dynamodb

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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

// --- CreateGlobalTable ---

// CreateGlobalTable creates a new global table entry in the backend.
func (db *InMemoryDB) CreateGlobalTable(
	_ context.Context,
	input *dynamodb.CreateGlobalTableInput,
) (*dynamodb.CreateGlobalTableOutput, error) {
	if input.GlobalTableName == nil || *input.GlobalTableName == "" {
		return nil, NewValidationException("GlobalTableName is required")
	}

	name := *input.GlobalTableName

	db.mu.Lock("CreateGlobalTable")
	defer db.mu.Unlock()

	if _, exists := db.GlobalTables[name]; exists {
		return nil, &Error{
			Type:    "com.amazonaws.dynamodb.v20120810#GlobalTableAlreadyExistsException",
			Message: fmt.Sprintf("Global table with name %s already exists", name),
		}
	}

	regions := make([]string, 0, len(input.ReplicationGroup))
	sdkReplicas := make([]types.ReplicaDescription, 0, len(input.ReplicationGroup))

	for _, r := range input.ReplicationGroup {
		if r.RegionName == nil {
			continue
		}

		regions = append(regions, *r.RegionName)
		sdkReplicas = append(sdkReplicas, types.ReplicaDescription{
			RegionName:    r.RegionName,
			ReplicaStatus: types.ReplicaStatusActive,
		})
	}

	globalTableARN := arn.Build("dynamodb", db.defaultRegion, db.accountID, "global-table/"+name)
	now := time.Now()

	db.GlobalTables[name] = &StoredGlobalTable{
		GlobalTableName:  name,
		GlobalTableArn:   globalTableARN,
		CreationDateTime: now,
		ReplicationGroup: regions,
	}

	return &dynamodb.CreateGlobalTableOutput{
		GlobalTableDescription: &types.GlobalTableDescription{
			GlobalTableName:   &name,
			GlobalTableArn:    &globalTableARN,
			GlobalTableStatus: types.GlobalTableStatusActive,
			CreationDateTime:  &now,
			ReplicationGroup:  sdkReplicas,
		},
	}, nil
}

// --- DescribeGlobalTable ---

// DescribeGlobalTable returns the description of a global table.
func (db *InMemoryDB) DescribeGlobalTable(
	_ context.Context,
	input *dynamodb.DescribeGlobalTableInput,
) (*dynamodb.DescribeGlobalTableOutput, error) {
	if input.GlobalTableName == nil || *input.GlobalTableName == "" {
		return nil, NewValidationException("GlobalTableName is required")
	}

	name := *input.GlobalTableName

	db.mu.RLock("DescribeGlobalTable")
	gt, exists := db.GlobalTables[name]
	db.mu.RUnlock()

	if !exists {
		return nil, &Error{
			Type:    "com.amazonaws.dynamodb.v20120810#GlobalTableNotFoundException",
			Message: fmt.Sprintf("Global table with name %s not found", name),
		}
	}

	sdkReplicas := make([]types.ReplicaDescription, 0, len(gt.ReplicationGroup))
	for _, region := range gt.ReplicationGroup {
		r := region

		sdkReplicas = append(sdkReplicas, types.ReplicaDescription{
			RegionName:    &r,
			ReplicaStatus: types.ReplicaStatusActive,
		})
	}

	return &dynamodb.DescribeGlobalTableOutput{
		GlobalTableDescription: &types.GlobalTableDescription{
			GlobalTableName:   &gt.GlobalTableName,
			GlobalTableArn:    &gt.GlobalTableArn,
			GlobalTableStatus: types.GlobalTableStatusActive,
			CreationDateTime:  &gt.CreationDateTime,
			ReplicationGroup:  sdkReplicas,
		},
	}, nil
}

// --- DescribeGlobalTableSettings ---

// DescribeGlobalTableSettings returns per-replica settings for a global table.
func (db *InMemoryDB) DescribeGlobalTableSettings(
	_ context.Context,
	input *dynamodb.DescribeGlobalTableSettingsInput,
) (*dynamodb.DescribeGlobalTableSettingsOutput, error) {
	if input.GlobalTableName == nil || *input.GlobalTableName == "" {
		return nil, NewValidationException("GlobalTableName is required")
	}

	name := *input.GlobalTableName

	db.mu.RLock("DescribeGlobalTableSettings")
	gt, exists := db.GlobalTables[name]
	db.mu.RUnlock()

	if !exists {
		return nil, &Error{
			Type:    "com.amazonaws.dynamodb.v20120810#GlobalTableNotFoundException",
			Message: fmt.Sprintf("Global table with name %s not found", name),
		}
	}

	replicaSettings := make([]types.ReplicaSettingsDescription, 0, len(gt.ReplicationGroup))
	for _, region := range gt.ReplicationGroup {
		r := region
		rcu := accountMaxReadCapacityUnits
		wcu := accountMaxWriteCapacityUnits

		replicaSettings = append(replicaSettings, types.ReplicaSettingsDescription{
			RegionName:                           &r,
			ReplicaStatus:                        types.ReplicaStatusActive,
			ReplicaProvisionedReadCapacityUnits:  &rcu,
			ReplicaProvisionedWriteCapacityUnits: &wcu,
		})
	}

	return &dynamodb.DescribeGlobalTableSettingsOutput{
		GlobalTableName: &gt.GlobalTableName,
		ReplicaSettings: replicaSettings,
	}, nil
}

// --- DescribeKinesisStreamingDestination ---

// DescribeKinesisStreamingDestination returns the Kinesis streaming destinations for a table.
func (db *InMemoryDB) DescribeKinesisStreamingDestination(
	ctx context.Context,
	input *dynamodb.DescribeKinesisStreamingDestinationInput,
) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.RLock("DescribeKinesisStreamingDestination")
	destinations := make([]types.KinesisDataStreamDestination, 0, len(table.KinesisDestinations))

	for _, streamARN := range table.KinesisDestinations {
		sa := streamARN
		destinations = append(destinations, types.KinesisDataStreamDestination{
			StreamArn:         &sa,
			DestinationStatus: types.DestinationStatusActive,
		})
	}

	tableName := table.Name
	table.mu.RUnlock()

	return &dynamodb.DescribeKinesisStreamingDestinationOutput{
		TableName:                     &tableName,
		KinesisDataStreamDestinations: destinations,
	}, nil
}

// --- DisableKinesisStreamingDestination ---

// DisableKinesisStreamingDestination removes a Kinesis streaming destination from a table.
func (db *InMemoryDB) DisableKinesisStreamingDestination(
	ctx context.Context,
	input *dynamodb.DisableKinesisStreamingDestinationInput,
) (*dynamodb.DisableKinesisStreamingDestinationOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	if input.StreamArn == nil || *input.StreamArn == "" {
		return nil, NewValidationException("StreamArn is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	streamARN := *input.StreamArn
	tableName := *input.TableName

	table.mu.Lock("DisableKinesisStreamingDestination")

	found := false

	for i, sa := range table.KinesisDestinations {
		if sa == streamARN {
			table.KinesisDestinations = append(table.KinesisDestinations[:i], table.KinesisDestinations[i+1:]...)
			found = true

			break
		}
	}

	table.mu.Unlock()

	if !found {
		return nil, &Error{
			Type:    "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
			Message: fmt.Sprintf("Kinesis stream %s not found for table %s", streamARN, tableName),
		}
	}

	status := types.DestinationStatusDisabling

	return &dynamodb.DisableKinesisStreamingDestinationOutput{
		TableName:         &tableName,
		StreamArn:         &streamARN,
		DestinationStatus: status,
	}, nil
}

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

// --- DescribeContributorInsights ---

// DescribeContributorInsights returns contributor insights status for a table or GSI.
// The in-memory backend always reports contributor insights as DISABLED.
func (db *InMemoryDB) DescribeContributorInsights(
	ctx context.Context,
	input *dynamodb.DescribeContributorInsightsInput,
) (*dynamodb.DescribeContributorInsightsOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	// Validate the table exists.
	_, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	tableName := *input.TableName

	out := &dynamodb.DescribeContributorInsightsOutput{
		TableName:                   &tableName,
		ContributorInsightsStatus:   types.ContributorInsightsStatusDisabled,
		ContributorInsightsRuleList: []string{},
	}

	if input.IndexName != nil {
		out.IndexName = input.IndexName
	}

	return out, nil
}

// --- EnableKinesisStreamingDestination ---

// EnableKinesisStreamingDestination adds a Kinesis streaming destination to a table.
func (db *InMemoryDB) EnableKinesisStreamingDestination(
	ctx context.Context,
	input *dynamodb.EnableKinesisStreamingDestinationInput,
) (*dynamodb.EnableKinesisStreamingDestinationOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	if input.StreamArn == nil || *input.StreamArn == "" {
		return nil, NewValidationException("StreamArn is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	streamARN := *input.StreamArn
	tableName := *input.TableName

	table.mu.Lock("EnableKinesisStreamingDestination")

	// Idempotent: only add if not already present.
	alreadyExists := slices.Contains(table.KinesisDestinations, streamARN)

	if !alreadyExists {
		table.KinesisDestinations = append(table.KinesisDestinations, streamARN)
	}

	table.mu.Unlock()

	status := types.DestinationStatusEnabling

	return &dynamodb.EnableKinesisStreamingDestinationOutput{
		TableName:         &tableName,
		StreamArn:         &streamARN,
		DestinationStatus: status,
	}, nil
}

// --- ListGlobalTables ---

// ListGlobalTables returns global tables, optionally filtered by region, with pagination support.
func (db *InMemoryDB) ListGlobalTables(
	_ context.Context,
	input *dynamodb.ListGlobalTablesInput,
) (*dynamodb.ListGlobalTablesOutput, error) {
	db.mu.RLock("ListGlobalTables")
	defer db.mu.RUnlock()

	regionFilter := ""
	if input.RegionName != nil {
		regionFilter = *input.RegionName
	}

	startName := ""
	if input.ExclusiveStartGlobalTableName != nil {
		startName = *input.ExclusiveStartGlobalTableName
	}

	// Collect and sort names for deterministic pagination.
	names := make([]string, 0, len(db.GlobalTables))
	for name := range db.GlobalTables {
		names = append(names, name)
	}

	sort.Strings(names)

	// Apply start cursor.
	startIdx := 0

	if startName != "" {
		for i, name := range names {
			if name > startName {
				startIdx = i

				break
			}
		}
	}

	names = names[startIdx:]

	// Apply region filter.
	filtered := make([]types.GlobalTable, 0, len(names))

	for _, name := range names {
		gt := db.GlobalTables[name]

		if regionFilter != "" {
			matched := slices.Contains(gt.ReplicationGroup, regionFilter)

			if !matched {
				continue
			}
		}

		replicas := make([]types.Replica, 0, len(gt.ReplicationGroup))
		for _, region := range gt.ReplicationGroup {
			r := region
			replicas = append(replicas, types.Replica{RegionName: &r})
		}

		n := name
		filtered = append(filtered, types.GlobalTable{
			GlobalTableName:  &n,
			ReplicationGroup: replicas,
		})
	}

	// Apply limit.
	var lastEvaluated *string

	if input.Limit != nil && int(*input.Limit) < len(filtered) {
		limit := int(*input.Limit)
		lastTableName := *filtered[limit-1].GlobalTableName
		lastEvaluated = &lastTableName
		filtered = filtered[:limit]
	}

	return &dynamodb.ListGlobalTablesOutput{
		GlobalTables:                 filtered,
		LastEvaluatedGlobalTableName: lastEvaluated,
	}, nil
}

// --- GetResourcePolicy ---

// GetResourcePolicy is a stub that returns an empty policy.
// The in-memory backend does not track resource policies.
func (db *InMemoryDB) GetResourcePolicy(
	_ context.Context,
	input *dynamodb.GetResourcePolicyInput,
) (*dynamodb.GetResourcePolicyOutput, error) {
	if input == nil || input.ResourceArn == nil || *input.ResourceArn == "" {
		return nil, NewValidationException("ResourceArn is required")
	}

	return &dynamodb.GetResourcePolicyOutput{}, nil
}

// --- PutResourcePolicy ---

// PutResourcePolicy is a stub that accepts any policy document.
// The in-memory backend does not enforce or store resource policies.
func (db *InMemoryDB) PutResourcePolicy(
	_ context.Context,
	input *dynamodb.PutResourcePolicyInput,
) (*dynamodb.PutResourcePolicyOutput, error) {
	if input == nil || input.ResourceArn == nil || *input.ResourceArn == "" {
		return nil, NewValidationException("ResourceArn is required")
	}

	if input.Policy == nil || *input.Policy == "" {
		return nil, NewValidationException("Policy is required")
	}

	return &dynamodb.PutResourcePolicyOutput{}, nil
}

// --- DeleteResourcePolicy ---

// DeleteResourcePolicy is a stub that returns an empty revision ID.
// The in-memory backend does not track resource policies.
func (db *InMemoryDB) DeleteResourcePolicy(
	_ context.Context,
	input *dynamodb.DeleteResourcePolicyInput,
) (*dynamodb.DeleteResourcePolicyOutput, error) {
	if input == nil || input.ResourceArn == nil || *input.ResourceArn == "" {
		return nil, NewValidationException("ResourceArn is required")
	}

	return &dynamodb.DeleteResourcePolicyOutput{}, nil
}

// --- DescribeImport ---

// DescribeImport is a stub that returns a COMPLETED import description for any ARN.
// The in-memory backend does not perform real data imports.
func (db *InMemoryDB) DescribeImport(
	_ context.Context,
	input *dynamodb.DescribeImportInput,
) (*dynamodb.DescribeImportOutput, error) {
	if input.ImportArn == nil || *input.ImportArn == "" {
		return nil, NewValidationException("ImportArn is required")
	}

	importARN := *input.ImportArn
	now := time.Now()

	return &dynamodb.DescribeImportOutput{
		ImportTableDescription: &types.ImportTableDescription{
			ImportArn:    &importARN,
			ImportStatus: types.ImportStatusCompleted,
			EndTime:      &now,
		},
	}, nil
}
