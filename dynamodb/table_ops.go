package dynamodb

import (
	"context"
	"fmt"
	"sort"

	"Gopherstack/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (db *InMemoryDB) CreateTable(
	_ context.Context,
	input *dynamodb.CreateTableInput,
) (*dynamodb.CreateTableOutput, error) {
	tableName := aws.ToString(input.TableName)
	if tableName == "" {
		return nil, NewValidationException("Table name is required")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Tables[tableName]; exists {
		return nil, NewResourceInUseException(fmt.Sprintf("table already exists: %s", tableName))
	}

	newTable := &Table{
		Name:                   tableName,
		KeySchema:              models.FromSDKKeySchema(input.KeySchema),
		AttributeDefinitions:   models.FromSDKAttributeDefinitions(input.AttributeDefinitions),
		GlobalSecondaryIndexes: models.FromSDKGlobalSecondaryIndexes(input.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:  models.FromSDKLocalSecondaryIndexes(input.LocalSecondaryIndexes),
		Items:                  make([]map[string]any, 0),
	}
	newTable.initializeIndexes()
	db.Tables[tableName] = newTable

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
	_ context.Context,
	input *dynamodb.DeleteTableInput,
) (*dynamodb.DeleteTableOutput, error) {
	tableName := aws.ToString(input.TableName)
	if tableName == "" {
		return nil, NewValidationException("Table name is required")
	}

	db.mu.Lock()
	table, exists := db.Tables[tableName]
	if !exists {
		db.mu.Unlock()

		return nil, NewResourceNotFoundException(fmt.Sprintf("table not found: %s", tableName))
	}
	delete(db.Tables, tableName)
	db.mu.Unlock()

	table.mu.RLock()
	defer table.mu.RUnlock()

	// Capture state for return
	gsiDescs := make([]models.GlobalSecondaryIndexDescription, len(table.GlobalSecondaryIndexes))
	for i, gsi := range table.GlobalSecondaryIndexes {
		gsiDescs[i] = models.GlobalSecondaryIndexDescription{
			IndexName:  gsi.IndexName,
			KeySchema:  gsi.KeySchema,
			Projection: gsi.Projection,
			ProvisionedThroughput: models.ProvisionedThroughputDescription{
				ReadCapacityUnits:  int(*gsi.ProvisionedThroughput.ReadCapacityUnits),
				WriteCapacityUnits: int(*gsi.ProvisionedThroughput.WriteCapacityUnits),
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
	_ context.Context,
	input *dynamodb.DescribeTableInput,
) (*dynamodb.DescribeTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	db.mu.RLock()
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("table not found: %s", tableName))
	}

	// Snapshot table metadata under lock, release immediately, then build descriptions outside lock
	table.mu.RLock()
	keySchema := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(keySchema, table.KeySchema)
	attrDefs := make([]models.AttributeDefinition, len(table.AttributeDefinitions))
	copy(attrDefs, table.AttributeDefinitions)
	gsiList := make([]models.GlobalSecondaryIndex, len(table.GlobalSecondaryIndexes))
	copy(gsiList, table.GlobalSecondaryIndexes)
	lsiList := make([]models.LocalSecondaryIndex, len(table.LocalSecondaryIndexes))
	copy(lsiList, table.LocalSecondaryIndexes)
	itemCount := int64(len(table.Items))
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

	rcu := int64(models.DefaultReadCapacity)
	wcu := int64(models.DefaultWriteCapacity)

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

func (db *InMemoryDB) UpdateTimeToLive(
	_ context.Context,
	input *dynamodb.UpdateTimeToLiveInput,
) (*dynamodb.UpdateTimeToLiveOutput, error) {
	tableName := aws.ToString(input.TableName)
	if tableName == "" {
		return nil, NewValidationException("Table name is required")
	}

	table, err := db.getTable(tableName)
	if err != nil {
		return nil, err
	}

	table.mu.Lock()
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
	_ context.Context,
	input *dynamodb.DescribeTimeToLiveInput,
) (*dynamodb.DescribeTimeToLiveOutput, error) {
	tableName := aws.ToString(input.TableName)
	table, err := db.getTable(tableName)
	if err != nil {
		return nil, err
	}

	table.mu.RLock()
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
	_ context.Context,
	input *dynamodb.ListTablesInput,
) (*dynamodb.ListTablesOutput, error) {
	// Snapshot table names under lock, then release immediately
	db.mu.RLock()
	names := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		names = append(names, name)
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
