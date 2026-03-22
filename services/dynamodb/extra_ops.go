package dynamodb

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
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
			ReplicaStatus: "ACTIVE",
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
		Status:           "ACTIVE",
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
			Type:    "com.amazonaws.dynamodb.v20120810#GlobalTableNotFoundException",
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
			Type:    "com.amazonaws.dynamodb.v20120810#GlobalTableNotFoundException",
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
		Status:                    "ACTIVE",
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

	if op == "DELETE" {
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
