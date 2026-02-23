package dynamodb

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// getRegionFromContext extracts the region from the request context.
// Returns the default region if region is not found in context.
func getRegionFromContext(ctx context.Context, db *InMemoryDB) string {
	if region, ok := ctx.Value(regionContextKey{}).(string); ok && region != "" {
		return region
	}

	return db.defaultRegion
}

func (db *InMemoryDB) CreateTable(
	ctx context.Context,
	input *dynamodb.CreateTableInput,
) (*dynamodb.CreateTableOutput, error) {
	tableName := aws.ToString(input.TableName)
	if tableName == "" {
		return nil, NewValidationException("Table name is required")
	}

	region := getRegionFromContext(ctx, db)

	db.mu.Lock("CreateTable")
	defer db.mu.Unlock()

	// Initialize region map if it doesn't exist
	if _, exists := db.Tables[region]; !exists {
		db.Tables[region] = make(map[string]*Table)
	}

	if _, exists := db.Tables[region][tableName]; exists {
		return nil, NewResourceInUseException(fmt.Sprintf("table already exists: %s", tableName))
	}

	newTable := &Table{
		Name:                   tableName,
		KeySchema:              models.FromSDKKeySchema(input.KeySchema),
		AttributeDefinitions:   models.FromSDKAttributeDefinitions(input.AttributeDefinitions),
		GlobalSecondaryIndexes: models.FromSDKGlobalSecondaryIndexes(input.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:  models.FromSDKLocalSecondaryIndexes(input.LocalSecondaryIndexes),
		Items:                  make([]map[string]any, 0),
		mu:                     lockmetrics.New("ddb.table." + tableName),
		ProvisionedThroughput: models.ProvisionedThroughputDescription{
			ReadCapacityUnits:  models.DefaultReadCapacity,
			WriteCapacityUnits: models.DefaultWriteCapacity,
		},
	}

	if input.ProvisionedThroughput != nil {
		if input.ProvisionedThroughput.ReadCapacityUnits != nil {
			newTable.ProvisionedThroughput.ReadCapacityUnits = int(*input.ProvisionedThroughput.ReadCapacityUnits)
		}

		if input.ProvisionedThroughput.WriteCapacityUnits != nil {
			newTable.ProvisionedThroughput.WriteCapacityUnits = int(*input.ProvisionedThroughput.WriteCapacityUnits)
		}
	}
	newTable.initializeIndexes()

	// Handle StreamSpecification if provided
	if input.StreamSpecification != nil && aws.ToBool(input.StreamSpecification.StreamEnabled) {
		newTable.StreamsEnabled = true
		newTable.StreamViewType = string(input.StreamSpecification.StreamViewType)
		newTable.StreamARN = db.buildStreamARN(tableName)
	}

	db.Tables[region][tableName] = newTable

	// Convert GSIs to Description
	gsiDescs := make([]models.GlobalSecondaryIndexDescription, len(input.GlobalSecondaryIndexes))
	for i, gsi := range input.GlobalSecondaryIndexes {
		gsiDescs[i] = models.GlobalSecondaryIndexDescription{
			IndexName:  aws.ToString(gsi.IndexName),
			KeySchema:  models.FromSDKKeySchema(gsi.KeySchema),
			Projection: models.FromSDKProjection(gsi.Projection),
			ProvisionedThroughput: models.ProvisionedThroughputDescription{
				ReadCapacityUnits:  models.DefaultReadCapacity,
				WriteCapacityUnits: models.DefaultWriteCapacity,
			},
			IndexStatus: models.TableStatusActive,
			ItemCount:   0,
		}
	}

	// Convert LSIs to Description
	lsiDescs := make([]models.LocalSecondaryIndexDescription, len(input.LocalSecondaryIndexes))
	for i, lsi := range input.LocalSecondaryIndexes {
		lsiDescs[i] = models.LocalSecondaryIndexDescription{
			IndexName:      aws.ToString(lsi.IndexName),
			KeySchema:      models.FromSDKKeySchema(lsi.KeySchema),
			Projection:     models.FromSDKProjection(lsi.Projection),
			IndexSizeBytes: 0,
			ItemCount:      0,
		}
	}

	// Helper to construct SDK output
	sdkGSIs := models.ToSDKGlobalSecondaryIndexDescriptions(gsiDescs)
	sdkLSIs := models.ToSDKLocalSecondaryIndexDescriptions(lsiDescs)

	sdkKeySchema := models.ToSDKKeySchema(newTable.KeySchema)
	sdkAttrDefs := models.ToSDKAttributeDefinitions(newTable.AttributeDefinitions)

	rcu := int64(models.DefaultReadCapacity)
	wcu := int64(models.DefaultWriteCapacity)

	return &dynamodb.CreateTableOutput{
		TableDescription: &types.TableDescription{
			TableName:              input.TableName,
			TableStatus:            types.TableStatusActive,
			KeySchema:              sdkKeySchema,
			AttributeDefinitions:   sdkAttrDefs,
			GlobalSecondaryIndexes: sdkGSIs,
			LocalSecondaryIndexes:  sdkLSIs,
			ItemCount:              aws.Int64(0),
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  &rcu,
				WriteCapacityUnits: &wcu,
			},
		},
	}, nil
}

func (db *InMemoryDB) DeleteTable(
	ctx context.Context,
	input *dynamodb.DeleteTableInput,
) (*dynamodb.DeleteTableOutput, error) {
	tableName := aws.ToString(input.TableName)
	if tableName == "" {
		return nil, NewValidationException("Table name is required")
	}

	region := getRegionFromContext(ctx, db)

	db.mu.Lock("DeleteTable")
	defer db.mu.Unlock()

	regionTables, regionExists := db.Tables[region]
	if !regionExists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("table not found: %s", tableName))
	}

	table, tableExists := regionTables[tableName]
	if !tableExists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("table not found: %s", tableName))
	}

	// Move to the deleting map — the Janitor will do the final removal.
	delete(db.Tables[region], tableName)
	if _, deletingExists := db.deletingTables[region]; !deletingExists {
		db.deletingTables[region] = make(map[string]*Table)
	}
	db.deletingTables[region][tableName] = table

	// Capture state for return
	gsiDescs := make([]models.GlobalSecondaryIndexDescription, len(table.GlobalSecondaryIndexes))
	for i, gsi := range table.GlobalSecondaryIndexes {
		rc := int64(models.DefaultReadCapacity)
		wc := int64(models.DefaultWriteCapacity)
		if gsi.ProvisionedThroughput.ReadCapacityUnits != nil {
			rc = *gsi.ProvisionedThroughput.ReadCapacityUnits
		}
		if gsi.ProvisionedThroughput.WriteCapacityUnits != nil {
			wc = *gsi.ProvisionedThroughput.WriteCapacityUnits
		}
		gsiDescs[i] = models.GlobalSecondaryIndexDescription{
			IndexName:  gsi.IndexName,
			KeySchema:  gsi.KeySchema,
			Projection: gsi.Projection,
			ProvisionedThroughput: models.ProvisionedThroughputDescription{
				ReadCapacityUnits:  int(rc),
				WriteCapacityUnits: int(wc),
			},
			IndexStatus: "DELETING",
			ItemCount:   len(table.Items),
		}
	}

	sdkGSIs := models.ToSDKGlobalSecondaryIndexDescriptions(gsiDescs)
	sdkKeySchema := models.ToSDKKeySchema(table.KeySchema)
	sdkAttrDefs := models.ToSDKAttributeDefinitions(table.AttributeDefinitions)
	itemCount := int64(len(table.Items))

	return &dynamodb.DeleteTableOutput{
		TableDescription: &types.TableDescription{
			TableName:              input.TableName,
			TableStatus:            types.TableStatusDeleting,
			KeySchema:              sdkKeySchema,
			AttributeDefinitions:   sdkAttrDefs,
			GlobalSecondaryIndexes: sdkGSIs,
			ItemCount:              &itemCount,
		},
	}, nil
}

func (db *InMemoryDB) DescribeTable(
	ctx context.Context,
	input *dynamodb.DescribeTableInput,
) (*dynamodb.DescribeTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	region := getRegionFromContext(ctx, db)

	db.mu.RLock("DescribeTable")
	regionTables, exists := db.Tables[region]
	if !exists {
		db.mu.RUnlock()

		return nil, NewResourceNotFoundException(fmt.Sprintf("table not found: %s", tableName))
	}
	table, exists := regionTables[tableName]
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("table not found: %s", tableName))
	}

	// Snapshot table metadata under lock, release immediately, then build descriptions outside lock
	table.mu.RLock("DescribeTable")
	keySchema := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(keySchema, table.KeySchema)
	attrDefs := make([]models.AttributeDefinition, len(table.AttributeDefinitions))
	copy(attrDefs, table.AttributeDefinitions)
	gsiList := make([]models.GlobalSecondaryIndex, len(table.GlobalSecondaryIndexes))
	copy(gsiList, table.GlobalSecondaryIndexes)
	lsiList := make([]models.LocalSecondaryIndex, len(table.LocalSecondaryIndexes))
	copy(lsiList, table.LocalSecondaryIndexes)
	itemCount := int64(len(table.Items))
	pt := table.ProvisionedThroughput
	table.mu.RUnlock()

	// Build index descriptions outside lock
	gsiDescs := make([]models.GlobalSecondaryIndexDescription, len(gsiList))
	for i, gsi := range gsiList {
		rc := int64(models.DefaultReadCapacity)
		wc := int64(models.DefaultWriteCapacity)
		if gsi.ProvisionedThroughput.ReadCapacityUnits != nil {
			rc = *gsi.ProvisionedThroughput.ReadCapacityUnits
		}
		if gsi.ProvisionedThroughput.WriteCapacityUnits != nil {
			wc = *gsi.ProvisionedThroughput.WriteCapacityUnits
		}

		gsiDescs[i] = models.GlobalSecondaryIndexDescription{
			IndexName:  gsi.IndexName,
			KeySchema:  gsi.KeySchema,
			Projection: gsi.Projection,
			ProvisionedThroughput: models.ProvisionedThroughputDescription{
				ReadCapacityUnits:  int(rc),
				WriteCapacityUnits: int(wc),
			},
			IndexStatus: models.TableStatusActive,
			ItemCount:   int(itemCount),
		}
	}

	lsiDescs := make([]models.LocalSecondaryIndexDescription, len(lsiList))
	for i, lsi := range lsiList {
		lsiDescs[i] = models.LocalSecondaryIndexDescription{
			IndexName:      lsi.IndexName,
			KeySchema:      lsi.KeySchema,
			Projection:     lsi.Projection,
			IndexSizeBytes: 0,
			ItemCount:      0,
		}
	}

	sdkGSIs := models.ToSDKGlobalSecondaryIndexDescriptions(gsiDescs)
	sdkLSIs := models.ToSDKLocalSecondaryIndexDescriptions(lsiDescs)
	sdkKeySchema := models.ToSDKKeySchema(keySchema)
	sdkAttrDefs := models.ToSDKAttributeDefinitions(attrDefs)

	rcu := int64(pt.ReadCapacityUnits)
	wcu := int64(pt.WriteCapacityUnits)

	return &dynamodb.DescribeTableOutput{
		Table: &types.TableDescription{
			TableName:              input.TableName,
			TableStatus:            types.TableStatusActive,
			KeySchema:              sdkKeySchema,
			AttributeDefinitions:   sdkAttrDefs,
			GlobalSecondaryIndexes: sdkGSIs,
			LocalSecondaryIndexes:  sdkLSIs,
			ItemCount:              &itemCount,
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  &rcu,
				WriteCapacityUnits: &wcu,
			},
		},
	}, nil
}

// UpdateTable modifies a DynamoDB table's provisioned throughput, GSI list, and stream spec.
func (db *InMemoryDB) UpdateTable(
	ctx context.Context,
	input *dynamodb.UpdateTableInput,
) (*dynamodb.UpdateTableOutput, error) {
	tableName := aws.ToString(input.TableName)
	if tableName == "" {
		return nil, NewValidationException("Table name is required")
	}

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	table.mu.Lock("UpdateTable")
	defer table.mu.Unlock()

	// Update provisioned throughput.
	if pt := input.ProvisionedThroughput; pt != nil {
		if pt.ReadCapacityUnits != nil {
			table.ProvisionedThroughput.ReadCapacityUnits = int(*pt.ReadCapacityUnits)
		}

		if pt.WriteCapacityUnits != nil {
			table.ProvisionedThroughput.WriteCapacityUnits = int(*pt.WriteCapacityUnits)
		}
	}

	// Update attribute definitions (merge: add new, keep existing).
	if len(input.AttributeDefinitions) > 0 {
		existing := make(map[string]struct{}, len(table.AttributeDefinitions))
		for _, ad := range table.AttributeDefinitions {
			existing[ad.AttributeName] = struct{}{}
		}

		for _, sdkAD := range input.AttributeDefinitions {
			name := aws.ToString(sdkAD.AttributeName)
			if _, found := existing[name]; !found {
				table.AttributeDefinitions = append(table.AttributeDefinitions,
					models.AttributeDefinition{
						AttributeName: name,
						AttributeType: string(sdkAD.AttributeType),
					})
			}
		}
	}

	// Apply GSI updates.
	for _, u := range input.GlobalSecondaryIndexUpdates {
		switch {
		case u.Create != nil:
			idxName := aws.ToString(u.Create.IndexName)
			newGSI := models.GlobalSecondaryIndex{
				IndexName:  idxName,
				KeySchema:  models.FromSDKKeySchema(u.Create.KeySchema),
				Projection: models.FromSDKProjection(u.Create.Projection),
			}

			if u.Create.ProvisionedThroughput != nil {
				newGSI.ProvisionedThroughput = models.ProvisionedThroughput{
					ReadCapacityUnits:  u.Create.ProvisionedThroughput.ReadCapacityUnits,
					WriteCapacityUnits: u.Create.ProvisionedThroughput.WriteCapacityUnits,
				}
			}

			table.GlobalSecondaryIndexes = append(table.GlobalSecondaryIndexes, newGSI)
			table.initializeIndexes()

		case u.Update != nil:
			idxName := aws.ToString(u.Update.IndexName)
			for i, gsi := range table.GlobalSecondaryIndexes {
				if gsi.IndexName == idxName {
					if pt := u.Update.ProvisionedThroughput; pt != nil {
						table.GlobalSecondaryIndexes[i].ProvisionedThroughput = models.ProvisionedThroughput{
							ReadCapacityUnits:  pt.ReadCapacityUnits,
							WriteCapacityUnits: pt.WriteCapacityUnits,
						}
					}

					break
				}
			}

		case u.Delete != nil:
			idxName := aws.ToString(u.Delete.IndexName)
			updated := make([]models.GlobalSecondaryIndex, 0, len(table.GlobalSecondaryIndexes)-1)

			for _, gsi := range table.GlobalSecondaryIndexes {
				if gsi.IndexName != idxName {
					updated = append(updated, gsi)
				}
			}

			table.GlobalSecondaryIndexes = updated
			table.initializeIndexes()
		}
	}

	// Update stream specification.
	if ss := input.StreamSpecification; ss != nil {
		if aws.ToBool(ss.StreamEnabled) {
			table.StreamsEnabled = true
			table.StreamViewType = string(ss.StreamViewType)

			if table.StreamARN == "" {
				table.StreamARN = db.buildStreamARN(tableName)
			}
		} else {
			table.StreamsEnabled = false
			table.StreamViewType = ""
			table.StreamARN = ""
		}
	}

	// Build response — snapshot current table state.
	pt := table.ProvisionedThroughput
	rcu := int64(pt.ReadCapacityUnits)
	wcu := int64(pt.WriteCapacityUnits)

	gsiDescs := make([]types.GlobalSecondaryIndexDescription, 0, len(table.GlobalSecondaryIndexes))
	for _, gsi := range table.GlobalSecondaryIndexes {
		rc := int64(models.DefaultReadCapacity)
		wc := int64(models.DefaultWriteCapacity)

		if gsi.ProvisionedThroughput.ReadCapacityUnits != nil {
			rc = *gsi.ProvisionedThroughput.ReadCapacityUnits
		}

		if gsi.ProvisionedThroughput.WriteCapacityUnits != nil {
			wc = *gsi.ProvisionedThroughput.WriteCapacityUnits
		}

		sdkGSI := types.GlobalSecondaryIndexDescription{
			IndexName:   &gsi.IndexName,
			KeySchema:   models.ToSDKKeySchema(gsi.KeySchema),
			Projection:  models.ToSDKProjection(gsi.Projection),
			IndexStatus: types.IndexStatusActive,
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  &rc,
				WriteCapacityUnits: &wc,
			},
		}
		gsiDescs = append(gsiDescs, sdkGSI)
	}

	return &dynamodb.UpdateTableOutput{
		TableDescription: &types.TableDescription{
			TableName:              input.TableName,
			TableStatus:            types.TableStatusActive,
			KeySchema:              models.ToSDKKeySchema(table.KeySchema),
			AttributeDefinitions:   models.ToSDKAttributeDefinitions(table.AttributeDefinitions),
			GlobalSecondaryIndexes: gsiDescs,
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  &rcu,
				WriteCapacityUnits: &wcu,
			},
		},
	}, nil
}

func (db *InMemoryDB) UpdateTimeToLive(
	ctx context.Context,
	input *dynamodb.UpdateTimeToLiveInput,
) (*dynamodb.UpdateTimeToLiveOutput, error) {
	tableName := aws.ToString(input.TableName)
	if tableName == "" {
		return nil, NewValidationException("Table name is required")
	}

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	table.mu.Lock("UpdateTimeToLive")
	defer table.mu.Unlock()

	if input.TimeToLiveSpecification.Enabled != nil && *input.TimeToLiveSpecification.Enabled {
		table.TTLAttribute = aws.ToString(input.TimeToLiveSpecification.AttributeName)
	} else {
		table.TTLAttribute = ""
	}

	return &dynamodb.UpdateTimeToLiveOutput{
		TimeToLiveSpecification: input.TimeToLiveSpecification,
	}, nil
}

func (db *InMemoryDB) DescribeTimeToLive(
	ctx context.Context,
	input *dynamodb.DescribeTimeToLiveInput,
) (*dynamodb.DescribeTimeToLiveOutput, error) {
	tableName := aws.ToString(input.TableName)
	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	table.mu.RLock("DescribeTimeToLive")
	defer table.mu.RUnlock()

	if table.TTLAttribute == "" {
		return &dynamodb.DescribeTimeToLiveOutput{
			TimeToLiveDescription: &types.TimeToLiveDescription{
				TimeToLiveStatus: types.TimeToLiveStatusDisabled,
			},
		}, nil
	}

	return &dynamodb.DescribeTimeToLiveOutput{
		TimeToLiveDescription: &types.TimeToLiveDescription{
			AttributeName:    aws.String(table.TTLAttribute),
			TimeToLiveStatus: types.TimeToLiveStatusEnabled,
		},
	}, nil
}

func (db *InMemoryDB) ListTables(
	ctx context.Context,
	input *dynamodb.ListTablesInput,
) (*dynamodb.ListTablesOutput, error) {
	region := getRegionFromContext(ctx, db)

	// Snapshot table names under lock, then release immediately
	db.mu.RLock("ListTables")
	names := make([]string, 0)
	if regionTables, exists := db.Tables[region]; exists {
		for name := range regionTables {
			names = append(names, name)
		}
	}
	db.mu.RUnlock()

	// Sort outside the lock to reduce contention
	sort.Strings(names)

	startName := aws.ToString(input.ExclusiveStartTableName)
	startIndex := 0
	if startName != "" {
		found := false
		for i, name := range names {
			if name > startName {
				startIndex = i
				found = true

				break
			}
		}
		if !found {
			return &dynamodb.ListTablesOutput{
				TableNames: []string{},
			}, nil
		}
	}

	names = names[startIndex:]

	limit := int(aws.ToInt32(input.Limit))
	if limit <= 0 {
		limit = 100
	}

	var lastEvaluatedName string
	if len(names) > limit {
		lastEvaluatedName = names[limit-1]
		names = names[:limit]
	}

	out := &dynamodb.ListTablesOutput{
		TableNames: names,
	}

	if lastEvaluatedName != "" {
		out.LastEvaluatedTableName = aws.String(lastEvaluatedName)
	}

	return out, nil
}
