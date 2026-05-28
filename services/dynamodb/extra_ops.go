package dynamodb

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

const (
	errGlobalTableNotFoundType = "com.amazonaws.dynamodb.v20120810#GlobalTableNotFoundException"
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

// CreateGlobalTable creates a global table, physically instantiating replica Table entries
// in each specified region. If a table named GlobalTableName already exists in a region,
// it is adopted into the global table; otherwise a new empty table is created there.
// The source schema is taken from the first region where the table already exists.
// All replicas get their Replicas field populated with the other regions, matching the
// DescribeTable output that AWS returns for global tables.
func (db *InMemoryDB) CreateGlobalTable(
	_ context.Context,
	input *dynamodb.CreateGlobalTableInput,
) (*dynamodb.CreateGlobalTableOutput, error) {
	if input.GlobalTableName == nil || *input.GlobalTableName == "" {
		return nil, NewValidationException("GlobalTableName is required")
	}

	if len(input.ReplicationGroup) == 0 {
		return nil, NewValidationException("ReplicationGroup must contain at least one region")
	}

	name := *input.GlobalTableName
	regions := collectValidRegions(input.ReplicationGroup)

	if len(regions) == 0 {
		return nil, NewValidationException("ReplicationGroup must contain at least one valid region")
	}

	db.mu.Lock("CreateGlobalTable")
	defer db.mu.Unlock()

	if _, exists := db.GlobalTables[name]; exists {
		return nil, &Error{
			Type:    "com.amazonaws.dynamodb.v20120810#GlobalTableAlreadyExistsException",
			Message: fmt.Sprintf("Global table with name %s already exists", name),
		}
	}

	source := db.findSourceTable(name, regions)

	globalTableARN := arn.Build("dynamodb", db.defaultRegion, db.accountID, "global-table/"+name)
	now := time.Now()

	allReplicas := buildAllReplicas(regions)

	db.ensureReplicaTablesLocked(name, regions, source, allReplicas, now)

	db.GlobalTables[name] = &StoredGlobalTable{
		GlobalTableName:  name,
		GlobalTableArn:   globalTableARN,
		CreationDateTime: now,
		ReplicationGroup: regions,
	}

	sdkReplicas := buildSDKReplicaDescriptions(regions)

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

// collectValidRegions extracts non-empty region names from a replication group.
func collectValidRegions(group []types.Replica) []string {
	regions := make([]string, 0, len(group))

	for _, r := range group {
		if r.RegionName != nil && *r.RegionName != "" {
			regions = append(regions, *r.RegionName)
		}
	}

	return regions
}

// findSourceTable returns the first Table named `name` found in the given regions.
// Must be called with db.mu held (at least read).
func (db *InMemoryDB) findSourceTable(name string, regions []string) *Table {
	for _, region := range regions {
		regionTables, regionExists := db.Tables[region]
		if !regionExists {
			continue
		}

		if tbl, tableExists := regionTables[name]; tableExists {
			return tbl
		}
	}

	return nil
}

// buildAllReplicas builds the complete ReplicaDescription slice for all regions.
func buildAllReplicas(regions []string) []models.ReplicaDescription {
	all := make([]models.ReplicaDescription, 0, len(regions))

	for _, r := range regions {
		all = append(all, models.ReplicaDescription{
			RegionName:    r,
			ReplicaStatus: statusActive,
		})
	}

	return all
}

// ensureReplicaTablesLocked creates or adopts a Table in each region under db.mu.
// Must be called with db.mu held (write).
func (db *InMemoryDB) ensureReplicaTablesLocked(
	name string,
	regions []string,
	source *Table,
	allReplicas []models.ReplicaDescription,
	now time.Time,
) {
	for _, region := range regions {
		if _, ok := db.Tables[region]; !ok {
			db.Tables[region] = make(map[string]*Table)
		}

		if _, exists := db.Tables[region][name]; !exists {
			replica := db.buildReplicaTable(name, region, source, now)
			replica.GlobalTableName = name
			db.Tables[region][name] = replica
		} else {
			db.Tables[region][name].GlobalTableName = name
		}
	}

	for _, region := range regions {
		db.Tables[region][name].Replicas = buildReplicasExcluding(allReplicas, region)
	}
}

// buildReplicaTable creates a new Table for use as a global table replica.
// If a source table exists it is cloned; otherwise a placeholder table is created.
func (db *InMemoryDB) buildReplicaTable(name, region string, source *Table, now time.Time) *Table {
	if source != nil {
		return cloneTableSchema(source, name, region, db.accountID)
	}

	t := &Table{
		Name:             name,
		Status:           statusActive,
		Items:            make([]map[string]any, 0),
		TableID:          uuid.New().String(),
		CreationDateTime: now,
		TableArn:         arn.Build("dynamodb", region, db.accountID, "table/"+name),
	}
	t.mu = newTableMutex(name)
	t.initializeIndexes()

	return t
}

// buildSDKReplicaDescriptions converts region names to SDK ReplicaDescription slice.
func buildSDKReplicaDescriptions(regions []string) []types.ReplicaDescription {
	out := make([]types.ReplicaDescription, 0, len(regions))

	for _, r := range regions {
		out = append(out, types.ReplicaDescription{
			RegionName:    &r,
			ReplicaStatus: types.ReplicaStatusActive,
		})
	}

	return out
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
			Type:    errGlobalTableNotFoundType,
			Message: fmt.Sprintf("Global table with name %s not found", name),
		}
	}

	sdkReplicas := make([]types.ReplicaDescription, 0, len(gt.ReplicationGroup))
	for _, region := range gt.ReplicationGroup {
		sdkReplicas = append(sdkReplicas, types.ReplicaDescription{
			RegionName:    &region,
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
			Type:    errGlobalTableNotFoundType,
			Message: fmt.Sprintf("Global table with name %s not found", name),
		}
	}

	replicaSettings := make([]types.ReplicaSettingsDescription, 0, len(gt.ReplicationGroup))
	for _, region := range gt.ReplicationGroup {
		rcu := accountMaxReadCapacityUnits
		wcu := accountMaxWriteCapacityUnits

		replicaSettings = append(replicaSettings, types.ReplicaSettingsDescription{
			RegionName:                           &region,
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
			Type:    errResourceNotFoundExceptionType,
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
// Status is tracked per-table on the in-memory backend; GSI-level status mirrors the table.
func (db *InMemoryDB) DescribeContributorInsights(
	ctx context.Context,
	input *dynamodb.DescribeContributorInsightsInput,
) (*dynamodb.DescribeContributorInsightsOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	tableName := *input.TableName

	table.mu.RLock("DescribeContributorInsights")
	enabled := table.ContributorInsightsEnabled
	table.mu.RUnlock()

	status := types.ContributorInsightsStatusDisabled
	if enabled {
		status = types.ContributorInsightsStatusEnabled
	}

	out := &dynamodb.DescribeContributorInsightsOutput{
		TableName:                   &tableName,
		ContributorInsightsStatus:   status,
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

	regionFilter := derefOrEmpty(input.RegionName)
	startName := derefOrEmpty(input.ExclusiveStartGlobalTableName)

	names := sortedGlobalTableNames(db.GlobalTables, startName)
	filtered := filterGlobalTables(db.GlobalTables, names, regionFilter)
	filtered, lastEvaluated := applyGlobalTableLimit(filtered, input.Limit)

	return &dynamodb.ListGlobalTablesOutput{
		GlobalTables:                 filtered,
		LastEvaluatedGlobalTableName: lastEvaluated,
	}, nil
}

// --- UpdateGlobalTable ---

// UpdateGlobalTable adds or removes replica regions for an existing global table.
// Create actions physically create a new Table entry in the target region (cloning the source schema).
// Delete actions remove the Table entry from the target region.
func (db *InMemoryDB) UpdateGlobalTable(
	_ context.Context,
	input *dynamodb.UpdateGlobalTableInput,
) (*dynamodb.UpdateGlobalTableOutput, error) {
	if input.GlobalTableName == nil || *input.GlobalTableName == "" {
		return nil, NewValidationException("GlobalTableName is required")
	}

	if len(input.ReplicaUpdates) == 0 {
		return nil, NewValidationException("ReplicaUpdates must contain at least one update")
	}

	name := *input.GlobalTableName

	db.mu.Lock("UpdateGlobalTable")
	defer db.mu.Unlock()

	gt, exists := db.GlobalTables[name]
	if !exists {
		return nil, &Error{
			Type:    errGlobalTableNotFoundType,
			Message: fmt.Sprintf("Global table with name %s not found", name),
		}
	}

	source := db.findSourceTableLocked(name, gt.ReplicationGroup)

	for _, update := range input.ReplicaUpdates {
		if err := db.applyGlobalTableReplicaUpdate(name, gt, update, source); err != nil {
			return nil, err
		}
	}

	// Rebuild per-replica Replicas field.
	db.rebuildGlobalTableReplicasLocked(name, gt.ReplicationGroup)

	sdkReplicas := buildSDKReplicaDescriptions(gt.ReplicationGroup)

	return &dynamodb.UpdateGlobalTableOutput{
		GlobalTableDescription: &types.GlobalTableDescription{
			GlobalTableName:   &name,
			GlobalTableArn:    &gt.GlobalTableArn,
			GlobalTableStatus: types.GlobalTableStatusActive,
			CreationDateTime:  &gt.CreationDateTime,
			ReplicationGroup:  sdkReplicas,
		},
	}, nil
}

// findSourceTableLocked returns the first existing Table for the given name across regions.
// Must be called with db.mu held for reading.
func (db *InMemoryDB) findSourceTableLocked(name string, regions []string) *Table {
	for _, region := range regions {
		regionTables, ok := db.Tables[region]
		if !ok {
			continue
		}

		if t, tableExists := regionTables[name]; tableExists {
			return t
		}
	}

	return nil
}

// applyGlobalTableReplicaUpdate processes a single Create or Delete replica update.
// Must be called with db.mu held for writing.
func (db *InMemoryDB) applyGlobalTableReplicaUpdate(
	name string,
	gt *StoredGlobalTable,
	update types.ReplicaUpdate,
	source *Table,
) error {
	switch {
	case update.Create != nil:
		return db.applyGlobalTableReplicaCreate(name, gt, derefOrEmpty(update.Create.RegionName), source)
	case update.Delete != nil:
		return db.applyGlobalTableReplicaDelete(name, gt, derefOrEmpty(update.Delete.RegionName))
	}

	return nil
}

// applyGlobalTableReplicaCreate adds a new region to a global table.
// Must be called with db.mu held for writing.
func (db *InMemoryDB) applyGlobalTableReplicaCreate(
	name string,
	gt *StoredGlobalTable,
	regionName string,
	source *Table,
) error {
	if regionName == "" {
		return NewValidationException("RegionName is required for Create action")
	}

	if !slices.Contains(gt.ReplicationGroup, regionName) {
		gt.ReplicationGroup = append(gt.ReplicationGroup, regionName)
	}

	if _, ok := db.Tables[regionName]; !ok {
		db.Tables[regionName] = make(map[string]*Table)
	}

	if _, tableExists := db.Tables[regionName][name]; !tableExists {
		replica := db.buildReplicaTableLocked(name, regionName, source)
		replica.GlobalTableName = name
		db.Tables[regionName][name] = replica
	} else {
		db.Tables[regionName][name].GlobalTableName = name
	}

	return nil
}

// applyGlobalTableReplicaDelete removes a region from a global table.
// Must be called with db.mu held for writing.
func (db *InMemoryDB) applyGlobalTableReplicaDelete(
	name string,
	gt *StoredGlobalTable,
	regionName string,
) error {
	if regionName == "" {
		return NewValidationException("RegionName is required for Delete action")
	}

	remaining := make([]string, 0, len(gt.ReplicationGroup))
	for _, r := range gt.ReplicationGroup {
		if r != regionName {
			remaining = append(remaining, r)
		}
	}

	gt.ReplicationGroup = remaining

	if regionTables, ok := db.Tables[regionName]; ok {
		delete(regionTables, name)
	}

	return nil
}

// rebuildGlobalTableReplicasLocked refreshes the Replicas field on every regional Table entry.
// Must be called with db.mu held for writing.
func (db *InMemoryDB) rebuildGlobalTableReplicasLocked(name string, regions []string) {
	allReplicas := buildAllReplicas(regions)
	for _, region := range regions {
		regionTables, ok := db.Tables[region]
		if !ok {
			continue
		}

		if t, tableExists := regionTables[name]; tableExists {
			t.Replicas = buildReplicasExcluding(allReplicas, region)
		}
	}
}

// buildReplicaTableLocked creates or returns a Table for a new global table region.
// Must be called with db.mu held (write).
func (db *InMemoryDB) buildReplicaTableLocked(name, region string, source *Table) *Table {
	if source != nil {
		return cloneTableSchema(source, name, region, db.accountID)
	}

	t := &Table{
		Name:             name,
		Status:           statusActive,
		Items:            make([]map[string]any, 0),
		TableID:          uuid.New().String(),
		CreationDateTime: time.Now(),
		TableArn:         arn.Build("dynamodb", region, db.accountID, "table/"+name),
	}
	t.mu = newTableMutex(name)
	t.initializeIndexes()

	return t
}

// sortedGlobalTableNames returns sorted global table names starting after startName.
func sortedGlobalTableNames(tables map[string]*StoredGlobalTable, startName string) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}

	sort.Strings(names)

	if startName == "" {
		return names
	}

	for i, name := range names {
		if name > startName {
			return names[i:]
		}
	}

	return nil
}

// filterGlobalTables converts stored global tables to SDK types, applying an optional region filter.
func filterGlobalTables(
	tables map[string]*StoredGlobalTable,
	names []string,
	regionFilter string,
) []types.GlobalTable {
	filtered := make([]types.GlobalTable, 0, len(names))

	for _, name := range names {
		gt := tables[name]

		if regionFilter != "" && !slices.Contains(gt.ReplicationGroup, regionFilter) {
			continue
		}

		replicas := make([]types.Replica, 0, len(gt.ReplicationGroup))
		for _, region := range gt.ReplicationGroup {
			replicas = append(replicas, types.Replica{RegionName: &region})
		}

		filtered = append(filtered, types.GlobalTable{
			GlobalTableName:  &name,
			ReplicationGroup: replicas,
		})
	}

	return filtered
}

// applyGlobalTableLimit applies an optional page size limit to the result set.
// Returns the (possibly truncated) list and an optional cursor for the next page.
func applyGlobalTableLimit(list []types.GlobalTable, limit *int32) ([]types.GlobalTable, *string) {
	if limit == nil || int(*limit) >= len(list) {
		return list, nil
	}

	n := int(*limit)
	if n <= 0 {
		return []types.GlobalTable{}, nil
	}

	last := *list[n-1].GlobalTableName

	return list[:n], &last
}

// derefOrEmpty safely dereferences a *string, returning "" if nil.
func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

// --- GetResourcePolicy ---

// GetResourcePolicy returns the resource-based policy stored on the table.
func (db *InMemoryDB) GetResourcePolicy(
	_ context.Context,
	input *dynamodb.GetResourcePolicyInput,
) (*dynamodb.GetResourcePolicyOutput, error) {
	if input == nil || input.ResourceArn == nil || *input.ResourceArn == "" {
		return nil, NewValidationException("ResourceArn is required")
	}

	table := db.getTableByARN(*input.ResourceArn)
	if table == nil {
		return nil, NewResourceNotFoundException("Table not found for ARN: " + *input.ResourceArn)
	}

	table.mu.RLock("GetResourcePolicy")
	policy := table.ResourcePolicy
	table.mu.RUnlock()

	if policy == "" {
		return &dynamodb.GetResourcePolicyOutput{}, nil
	}

	return &dynamodb.GetResourcePolicyOutput{
		Policy:     aws.String(policy),
		RevisionId: aws.String("1"),
	}, nil
}

// --- PutResourcePolicy ---

// PutResourcePolicy stores a resource-based policy on the table.
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

	table := db.getTableByARN(*input.ResourceArn)
	if table == nil {
		return nil, NewResourceNotFoundException("Table not found for ARN: " + *input.ResourceArn)
	}

	table.mu.Lock("PutResourcePolicy")
	table.ResourcePolicy = *input.Policy
	table.mu.Unlock()

	return &dynamodb.PutResourcePolicyOutput{
		RevisionId: aws.String("1"),
	}, nil
}

// --- DeleteResourcePolicy ---

// DeleteResourcePolicy removes the resource-based policy from the table.
func (db *InMemoryDB) DeleteResourcePolicy(
	_ context.Context,
	input *dynamodb.DeleteResourcePolicyInput,
) (*dynamodb.DeleteResourcePolicyOutput, error) {
	if input == nil || input.ResourceArn == nil || *input.ResourceArn == "" {
		return nil, NewValidationException("ResourceArn is required")
	}

	table := db.getTableByARN(*input.ResourceArn)
	if table == nil {
		// idempotent: nonexistent resource is a no-op
		return &dynamodb.DeleteResourcePolicyOutput{RevisionId: aws.String("1")}, nil
	}

	table.mu.Lock("DeleteResourcePolicy")
	table.ResourcePolicy = ""
	table.mu.Unlock()

	return &dynamodb.DeleteResourcePolicyOutput{
		RevisionId: aws.String("1"),
	}, nil
}

// getTableByARN looks up a table by its ARN across all regions.
// Returns nil if not found.
func (db *InMemoryDB) getTableByARN(resourceARN string) *Table {
	db.mu.RLock("getTableByARN")
	defer db.mu.RUnlock()

	for _, regionTables := range db.Tables {
		for _, table := range regionTables {
			if table.TableArn == resourceARN {
				return table
			}
		}
	}

	return nil
}

// --- DescribeImport ---

// DescribeImport returns the import description for a given import ARN.
// If the import was started via ImportTable, the stored record is returned.
// Otherwise, a synthetic COMPLETED response is returned for backwards compatibility.
func (db *InMemoryDB) DescribeImport(
	_ context.Context,
	input *dynamodb.DescribeImportInput,
) (*dynamodb.DescribeImportOutput, error) {
	if input.ImportArn == nil || *input.ImportArn == "" {
		return nil, NewValidationException("ImportArn is required")
	}

	importARN := *input.ImportArn
	now := time.Now()

	// Look up from persistent store first.
	if imp, ok := db.lookupImport(importARN); ok {
		tableARN := imp.TableArn

		return &dynamodb.DescribeImportOutput{
			ImportTableDescription: &types.ImportTableDescription{
				ImportArn:    &importARN,
				ImportStatus: types.ImportStatusCompleted,
				TableArn:     &tableARN,
				EndTime:      &now,
			},
		}, nil
	}

	// Fallback: synthetic response for unknown ARNs.
	return &dynamodb.DescribeImportOutput{
		ImportTableDescription: &types.ImportTableDescription{
			ImportArn:    &importARN,
			ImportStatus: types.ImportStatusCompleted,
			EndTime:      &now,
		},
	}, nil
}

// --- ListContributorInsights ---

// ListContributorInsights returns the set of tables whose contributor insights are enabled.
func (db *InMemoryDB) ListContributorInsights(
	_ context.Context,
	_ *dynamodb.ListContributorInsightsInput,
) (*dynamodb.ListContributorInsightsOutput, error) {
	db.mu.RLock("ListContributorInsights")
	defer db.mu.RUnlock()

	var summaries []types.ContributorInsightsSummary

	for _, regionTables := range db.Tables {
		for name, t := range regionTables {
			t.mu.RLock("ListContributorInsights")
			enabled := t.ContributorInsightsEnabled
			t.mu.RUnlock()

			if !enabled {
				continue
			}

			tableName := name
			summaries = append(summaries, types.ContributorInsightsSummary{
				TableName:                 &tableName,
				ContributorInsightsStatus: types.ContributorInsightsStatusEnabled,
			})
		}
	}

	return &dynamodb.ListContributorInsightsOutput{
		ContributorInsightsSummaries: summaries,
	}, nil
}

// --- UpdateContributorInsights ---

// UpdateContributorInsights toggles contributor insights for a table.
// The action is interpreted as ENABLE / DISABLE per AWS spec.
func (db *InMemoryDB) UpdateContributorInsights(
	ctx context.Context,
	input *dynamodb.UpdateContributorInsightsInput,
) (*dynamodb.UpdateContributorInsightsOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	enable := input.ContributorInsightsAction == types.ContributorInsightsActionEnable

	table.mu.Lock("UpdateContributorInsights")
	table.ContributorInsightsEnabled = enable
	table.mu.Unlock()

	tableName := *input.TableName

	status := types.ContributorInsightsStatusDisabled
	if enable {
		status = types.ContributorInsightsStatusEnabled
	}

	out := &dynamodb.UpdateContributorInsightsOutput{
		TableName:                 &tableName,
		ContributorInsightsStatus: status,
	}

	if input.IndexName != nil {
		out.IndexName = input.IndexName
	}

	return out, nil
}

// --- UpdateGlobalTableSettings ---

// UpdateGlobalTableSettings is a stub that validates the global table exists and
// returns the current (default) settings. The in-memory backend does not simulate
// per-replica autoscaling or billing mode changes at the global level.
func (db *InMemoryDB) UpdateGlobalTableSettings(
	_ context.Context,
	input *dynamodb.UpdateGlobalTableSettingsInput,
) (*dynamodb.UpdateGlobalTableSettingsOutput, error) {
	if input.GlobalTableName == nil || *input.GlobalTableName == "" {
		return nil, NewValidationException("GlobalTableName is required")
	}

	name := *input.GlobalTableName

	db.mu.RLock("UpdateGlobalTableSettings")
	gt, exists := db.GlobalTables[name]
	db.mu.RUnlock()

	if !exists {
		return nil, &Error{
			Type:    errGlobalTableNotFoundType,
			Message: fmt.Sprintf("Global table with name %s not found", name),
		}
	}

	replicas := make([]types.ReplicaSettingsDescription, 0, len(gt.ReplicationGroup))
	for _, region := range gt.ReplicationGroup {
		r := region

		replicas = append(replicas, types.ReplicaSettingsDescription{
			RegionName:    &r,
			ReplicaStatus: types.ReplicaStatusActive,
			ReplicaBillingModeSummary: &types.BillingModeSummary{
				BillingMode: types.BillingModePayPerRequest,
			},
		})
	}

	return &dynamodb.UpdateGlobalTableSettingsOutput{
		GlobalTableName: &name,
		ReplicaSettings: replicas,
	}, nil
}

// --- UpdateKinesisStreamingDestination ---

// UpdateKinesisStreamingDestination is a stub that validates the table and stream ARN
// and returns a synthetic ACTIVE status. The in-memory backend does not simulate
// Kinesis streaming configuration changes beyond enable/disable.
func (db *InMemoryDB) UpdateKinesisStreamingDestination(
	ctx context.Context,
	input *dynamodb.UpdateKinesisStreamingDestinationInput,
) (*dynamodb.UpdateKinesisStreamingDestinationOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	if input.StreamArn == nil || *input.StreamArn == "" {
		return nil, NewValidationException("StreamArn is required")
	}

	if _, err := db.getTable(ctx, *input.TableName); err != nil {
		return nil, err
	}

	tableName := *input.TableName
	streamARN := *input.StreamArn

	return &dynamodb.UpdateKinesisStreamingDestinationOutput{
		TableName:         &tableName,
		StreamArn:         &streamARN,
		DestinationStatus: types.DestinationStatusActive,
	}, nil
}

// autoScalingSettingsFromInput converts an UpdateTableReplicaAutoScalingInput
// into the persisted shape so the next DescribeTableReplicaAutoScaling can
// round-trip the values without simulating real scaling.
func autoScalingSettingsFromInput(
	input *dynamodb.UpdateTableReplicaAutoScalingInput,
) *autoScalingSettings {
	s := &autoScalingSettings{}

	if input.ProvisionedWriteCapacityAutoScalingUpdate != nil {
		s.Write = throughputFromUpdate(input.ProvisionedWriteCapacityAutoScalingUpdate)
	}

	if len(input.GlobalSecondaryIndexUpdates) > 0 {
		s.GlobalSecondaryIndexes = make(map[string]*autoScalingThroughput, len(input.GlobalSecondaryIndexUpdates))
		for _, g := range input.GlobalSecondaryIndexUpdates {
			if g.IndexName == nil {
				continue
			}
			s.GlobalSecondaryIndexes[*g.IndexName] = throughputFromUpdate(
				g.ProvisionedWriteCapacityAutoScalingUpdate,
			)
		}
	}

	return s
}

// throughputFromUpdate translates the SDK AutoScalingSettingsUpdate struct
// into the persisted shape. Returns nil when no fields were supplied so the
// caller can distinguish "explicitly cleared" from "untouched".
func throughputFromUpdate(u *types.AutoScalingSettingsUpdate) *autoScalingThroughput {
	if u == nil {
		return nil
	}

	out := &autoScalingThroughput{
		MinCapacity: u.MinimumUnits,
		MaxCapacity: u.MaximumUnits,
	}
	if u.AutoScalingDisabled != nil {
		out.Disabled = *u.AutoScalingDisabled
	}
	if u.ScalingPolicyUpdate != nil && u.ScalingPolicyUpdate.TargetTrackingScalingPolicyConfiguration != nil {
		out.TargetUtilizPct = u.ScalingPolicyUpdate.TargetTrackingScalingPolicyConfiguration.TargetValue
	}

	return out
}

// --- UpdateTableReplicaAutoScaling ---

// UpdateTableReplicaAutoScaling is a stub that validates the table exists and returns
// a basic autoscaling description. The in-memory backend does not simulate autoscaling.
func (db *InMemoryDB) UpdateTableReplicaAutoScaling(
	ctx context.Context,
	input *dynamodb.UpdateTableReplicaAutoScalingInput,
) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.Lock("UpdateTableReplicaAutoScaling")
	table.AutoScaling = autoScalingSettingsFromInput(input)
	tableName := table.Name
	tableStatus := table.Status
	replicas := make([]models.ReplicaDescription, len(table.Replicas))
	copy(replicas, table.Replicas)
	table.mu.Unlock()

	replicaDescs := make([]types.ReplicaAutoScalingDescription, 0, len(replicas))

	for _, r := range replicas {
		region := r.RegionName

		replicaDescs = append(replicaDescs, types.ReplicaAutoScalingDescription{
			RegionName:    &region,
			ReplicaStatus: types.ReplicaStatusActive,
		})
	}

	return &dynamodb.UpdateTableReplicaAutoScalingOutput{
		TableAutoScalingDescription: &types.TableAutoScalingDescription{
			TableName:   &tableName,
			TableStatus: types.TableStatus(tableStatus),
			Replicas:    replicaDescs,
		},
	}, nil
}

// --- ExecuteTransaction ---

// ExecuteTransaction executes a set of PartiQL statements in a single atomic transaction.
// The in-memory backend delegates each statement to the PartiQL runner and returns
// results in the same order. Atomicity is not guaranteed — like LocalStack's basic
// implementation, this is a best-effort sequential execution.
func (db *InMemoryDB) ExecuteTransaction(
	ctx context.Context,
	input *dynamodb.ExecuteTransactionInput,
) (*dynamodb.ExecuteTransactionOutput, error) {
	if len(input.TransactStatements) == 0 {
		return nil, NewValidationException("TransactStatements must contain at least one statement")
	}

	const maxTransactStatements = 100

	if len(input.TransactStatements) > maxTransactStatements {
		return nil, NewValidationException(
			fmt.Sprintf("Too many statements in transaction: maximum is %d", maxTransactStatements),
		)
	}

	runner := &partiQLRunner{backend: db}
	responses := make([]types.ItemResponse, len(input.TransactStatements))

	for i, stmt := range input.TransactStatements {
		stmtStr := ""
		if stmt.Statement != nil {
			stmtStr = *stmt.Statement
		}

		// Convert SDK AttributeValue parameters to wire format for the PartiQL runner.
		wireParams := make([]map[string]any, 0, len(stmt.Parameters))
		for _, p := range stmt.Parameters {
			wire, ok := models.FromSDKAttributeValue(p).(map[string]any)
			if !ok {
				return nil, NewValidationException("invalid parameter type in TransactStatement")
			}

			wireParams = append(wireParams, wire)
		}

		out, err := runner.executeStatement(ctx, executeStatementRequest{
			Statement:  stmtStr,
			Parameters: wireParams,
		})
		if err != nil {
			return nil, err
		}

		resp := types.ItemResponse{}
		if len(out.Items) > 0 {
			sdkItem, convErr := models.ToSDKItem(out.Items[0])
			if convErr == nil {
				resp.Item = sdkItem
			}
		}

		responses[i] = resp
	}

	return &dynamodb.ExecuteTransactionOutput{Responses: responses}, nil
}

// --- ImportTable ---

// ImportTable generates a synthetic import ARN, stores the import metadata, and returns COMPLETED status.
// The in-memory backend does not perform real S3 imports, but persists the record so that
// DescribeImport and ListImports return accurate results.
func (db *InMemoryDB) ImportTable(
	_ context.Context,
	input *dynamodb.ImportTableInput,
) (*dynamodb.ImportTableOutput, error) {
	if input.TableCreationParameters == nil {
		return nil, NewValidationException("TableCreationParameters is required")
	}

	if input.S3BucketSource == nil || input.S3BucketSource.S3Bucket == nil {
		return nil, NewValidationException("S3BucketSource.S3Bucket is required")
	}

	importARN := arn.Build("dynamodb", db.defaultRegion, db.accountID,
		"table/import/"+uuid.New().String())
	now := time.Now()

	tableARN := ""
	if input.TableCreationParameters.TableName != nil {
		tableARN = arn.Build("dynamodb", db.defaultRegion, db.accountID,
			"table/"+*input.TableCreationParameters.TableName)
	}

	bucket := aws.ToString(input.S3BucketSource.S3Bucket)
	inputFormat := string(input.InputFormat)

	db.storeImport(storedImport{
		ImportArn:    importARN,
		ImportStatus: string(types.ImportStatusCompleted),
		TableArn:     tableARN,
		S3Bucket:     bucket,
		InputFormat:  inputFormat,
	})

	return &dynamodb.ImportTableOutput{
		ImportTableDescription: &types.ImportTableDescription{
			ImportArn:    &importARN,
			ImportStatus: types.ImportStatusCompleted,
			TableArn:     &tableARN,
			StartTime:    &now,
			EndTime:      &now,
		},
	}, nil
}

// --- ListImports ---

// ListImports returns stored import records, sorted by ImportArn.
func (db *InMemoryDB) ListImports(
	_ context.Context,
	_ *dynamodb.ListImportsInput,
) (*dynamodb.ListImportsOutput, error) {
	stored := db.listImportsStored()
	summaries := make([]types.ImportSummary, 0, len(stored))

	for _, imp := range stored {
		importARN := imp.ImportArn
		tableARN := imp.TableArn
		summaries = append(summaries, types.ImportSummary{
			ImportArn:    &importARN,
			ImportStatus: types.ImportStatusCompleted,
			TableArn:     &tableARN,
		})
	}

	return &dynamodb.ListImportsOutput{
		ImportSummaryList: summaries,
	}, nil
}

// --- Global table replication helpers ---

// cloneTableSchema creates a new empty Table in the target region with the same
// key schema, attribute definitions, and throughput as the source.
// The clone gets a fresh TableID, ARN, and empty Items slice.
func cloneTableSchema(src *Table, name, region, accountID string) *Table {
	src.mu.RLock("cloneTableSchema")
	defer src.mu.RUnlock()

	keySchema := make([]models.KeySchemaElement, len(src.KeySchema))
	copy(keySchema, src.KeySchema)

	attrDefs := make([]models.AttributeDefinition, len(src.AttributeDefinitions))
	copy(attrDefs, src.AttributeDefinitions)

	gsis := make([]models.GlobalSecondaryIndex, len(src.GlobalSecondaryIndexes))
	copy(gsis, src.GlobalSecondaryIndexes)

	lsis := make([]models.LocalSecondaryIndex, len(src.LocalSecondaryIndexes))
	copy(lsis, src.LocalSecondaryIndexes)

	t := &Table{
		Name:                      name,
		Status:                    statusActive,
		Items:                     make([]map[string]any, 0),
		TableID:                   uuid.New().String(),
		CreationDateTime:          time.Now(),
		TableArn:                  arn.Build("dynamodb", region, accountID, "table/"+name),
		KeySchema:                 keySchema,
		AttributeDefinitions:      attrDefs,
		GlobalSecondaryIndexes:    gsis,
		LocalSecondaryIndexes:     lsis,
		ProvisionedThroughput:     src.ProvisionedThroughput,
		TableClass:                src.TableClass,
		DeletionProtectionEnabled: src.DeletionProtectionEnabled,
	}
	t.mu = newTableMutex(name)
	t.initializeIndexes()

	return t
}

// newTableMutex creates a new lockmetrics.RWMutex for the given table name.
func newTableMutex(name string) *lockmetrics.RWMutex {
	return lockmetrics.New("ddb.table." + name)
}

// buildReplicasExcluding returns a slice of ReplicaDescriptions from allReplicas
// excluding the one for excludeRegion (so a table lists all other regions as its replicas).
func buildReplicasExcluding(all []models.ReplicaDescription, excludeRegion string) []models.ReplicaDescription {
	result := make([]models.ReplicaDescription, 0, len(all))

	for _, r := range all {
		if r.RegionName != excludeRegion {
			result = append(result, r)
		}
	}

	return result
}

// replicateItemMutation propagates a completed item write (PUT or DELETE) to all
// sibling replicas of a global table. It is called after the primary write succeeds
// and after the primary table's mutex has been released.
//
// For PUT: finalItem is the item that was written.
// For DELETE: finalItem is the item that was deleted (used to locate it by key).
//
// This simulates DynamoDB global table eventual consistency: every replica converges
// to the same data state, with the last writer winning.
func (db *InMemoryDB) replicateItemMutation(
	tableName string,
	globalTableName string,
	currentRegion string,
	finalItem map[string]any,
	op string,
) {
	// Look up global table metadata under read lock.
	db.mu.RLock("replicateItemMutation-gt")
	gt, exists := db.GlobalTables[globalTableName]
	db.mu.RUnlock()

	if !exists {
		return
	}

	for _, region := range gt.ReplicationGroup {
		if region == currentRegion {
			continue
		}

		db.applyMutationToReplica(tableName, region, finalItem, op)
	}
}

// applyMutationToReplica applies a single item mutation (PUT or DELETE) to one
// regional replica table. Safe to call concurrently; acquires the replica's lock internally.
func (db *InMemoryDB) applyMutationToReplica(
	tableName string,
	region string,
	finalItem map[string]any,
	op string,
) {
	// Look up the replica table under a short read lock.
	db.mu.RLock("applyMutationToReplica-lookup")
	regionTables := db.Tables[region]

	var replica *Table
	if regionTables != nil {
		replica = regionTables[tableName]
	}

	db.mu.RUnlock()

	if replica == nil {
		return
	}

	replica.mu.Lock("applyMutationToReplica-mutate")

	if op == replicationOpDelete {
		db.deleteReplicaItemByKey(replica, finalItem)
	} else {
		_, matchIdx := db.findMatchForPut(replica, finalItem)
		db.doPut(replica, deepCopyItem(finalItem), matchIdx)
	}

	replica.mu.Unlock()
}

// deleteReplicaItemByKey locates an item in a replica by its key attributes and deletes it.
// Must be called with replica.mu held (write).
func (db *InMemoryDB) deleteReplicaItemByKey(replica *Table, keyItem map[string]any) {
	pkDef, skDef := getPKAndSK(replica.KeySchema)
	pkVal := BuildKeyString(keyItem, pkDef.AttributeName)

	matchIdx := db.resolveReplicaMatchIndex(replica, pkVal, skDef.AttributeName, keyItem)

	if matchIdx >= 0 {
		db.deleteItemAtIndex(replica, matchIdx)
	}
}

// resolveReplicaMatchIndex resolves the Items slice index for the given primary and
// optional sort key values in a replica table.
func (db *InMemoryDB) resolveReplicaMatchIndex(
	replica *Table,
	pkVal string,
	skAttr string,
	keyItem map[string]any,
) int {
	if skAttr != "" {
		skVal := BuildKeyString(keyItem, skAttr)
		if skMap, ok := replica.pkskIndex[pkVal]; ok {
			return skMap[skVal]
		}

		return -1
	}

	matchIdx, ok := replica.pkIndex[pkVal]
	if !ok {
		return -1
	}

	return matchIdx
}
