package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (db *InMemoryDB) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
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
		KeySchema:              FromSDKKeySchema(input.KeySchema),
		AttributeDefinitions:   FromSDKAttributeDefinitions(input.AttributeDefinitions),
		GlobalSecondaryIndexes: FromSDKGlobalSecondaryIndexes(input.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:  FromSDKLocalSecondaryIndexes(input.LocalSecondaryIndexes),
		Items:                  make([]map[string]any, 0),
	}
	newTable.initializeIndexes()
	db.Tables[tableName] = newTable

	// Convert GSIs to Description
	gsiDescs := make([]GlobalSecondaryIndexDescription, len(input.GlobalSecondaryIndexes))
	for i, gsi := range input.GlobalSecondaryIndexes {
		gsiDescs[i] = GlobalSecondaryIndexDescription{
			IndexName:  aws.ToString(gsi.IndexName),
			KeySchema:  FromSDKKeySchema(gsi.KeySchema),
			Projection: FromSDKProjection(gsi.Projection),
			ProvisionedThroughput: ProvisionedThroughputDescription{
				ReadCapacityUnits:  DefaultReadCapacity,
				WriteCapacityUnits: DefaultWriteCapacity,
			},
			IndexStatus: TableStatusActive,
			ItemCount:   0,
		}
	}

	// Convert LSIs to Description
	lsiDescs := make([]LocalSecondaryIndexDescription, len(input.LocalSecondaryIndexes))
	for i, lsi := range input.LocalSecondaryIndexes {
		lsiDescs[i] = LocalSecondaryIndexDescription{
			IndexName:      aws.ToString(lsi.IndexName),
			KeySchema:      FromSDKKeySchema(lsi.KeySchema),
			Projection:     FromSDKProjection(lsi.Projection),
			IndexSizeBytes: 0,
			ItemCount:      0,
		}
	}

	// Helper to construct SDK output
	sdkGSIs := ToSDKGlobalSecondaryIndexDescriptions(gsiDescs)
	sdkLSIs := ToSDKLocalSecondaryIndexDescriptions(lsiDescs)

	sdkKeySchema := ToSDKKeySchema(newTable.KeySchema)
	sdkAttrDefs := ToSDKAttributeDefinitions(newTable.AttributeDefinitions)

	rcu := int64(DefaultReadCapacity)
	wcu := int64(DefaultWriteCapacity)

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

func (db *InMemoryDB) DeleteTable(input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
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
	gsiDescs := make([]GlobalSecondaryIndexDescription, len(table.GlobalSecondaryIndexes))
	for i, gsi := range table.GlobalSecondaryIndexes {
		gsiDescs[i] = GlobalSecondaryIndexDescription{
			IndexName:  gsi.IndexName,
			KeySchema:  gsi.KeySchema,
			Projection: gsi.Projection,
			ProvisionedThroughput: ProvisionedThroughputDescription{
				ReadCapacityUnits:  int(*gsi.ProvisionedThroughput.ReadCapacityUnits),
				WriteCapacityUnits: int(*gsi.ProvisionedThroughput.WriteCapacityUnits),
			},
			IndexStatus: "DELETING",
			ItemCount:   len(table.Items),
		}
	}

	sdkGSIs := ToSDKGlobalSecondaryIndexDescriptions(gsiDescs)
	sdkKeySchema := ToSDKKeySchema(table.KeySchema)
	sdkAttrDefs := ToSDKAttributeDefinitions(table.AttributeDefinitions)
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

func (db *InMemoryDB) DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	db.mu.RLock()
	table, exists := db.Tables[tableName]
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("table not found: %s", tableName))
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	gsiDescs := make([]GlobalSecondaryIndexDescription, len(table.GlobalSecondaryIndexes))
	for i, gsi := range table.GlobalSecondaryIndexes {
		rc := int64(DefaultReadCapacity)
		wc := int64(DefaultWriteCapacity)
		if gsi.ProvisionedThroughput.ReadCapacityUnits != nil {
			rc = *gsi.ProvisionedThroughput.ReadCapacityUnits
		}
		if gsi.ProvisionedThroughput.WriteCapacityUnits != nil {
			wc = *gsi.ProvisionedThroughput.WriteCapacityUnits
		}

		gsiDescs[i] = GlobalSecondaryIndexDescription{
			IndexName:  gsi.IndexName,
			KeySchema:  gsi.KeySchema,
			Projection: gsi.Projection,
			ProvisionedThroughput: ProvisionedThroughputDescription{
				ReadCapacityUnits:  int(rc),
				WriteCapacityUnits: int(wc),
			},
			IndexStatus: TableStatusActive,
			ItemCount:   len(table.Items),
		}
	}

	lsiDescs := make([]LocalSecondaryIndexDescription, len(table.LocalSecondaryIndexes))
	for i, lsi := range table.LocalSecondaryIndexes {
		lsiDescs[i] = LocalSecondaryIndexDescription{
			IndexName:      lsi.IndexName,
			KeySchema:      lsi.KeySchema,
			Projection:     lsi.Projection,
			IndexSizeBytes: 0,
			ItemCount:      0,
		}
	}

	sdkGSIs := ToSDKGlobalSecondaryIndexDescriptions(gsiDescs)
	sdkLSIs := ToSDKLocalSecondaryIndexDescriptions(lsiDescs)
	sdkKeySchema := ToSDKKeySchema(table.KeySchema)
	sdkAttrDefs := ToSDKAttributeDefinitions(table.AttributeDefinitions)
	itemCount := int64(len(table.Items))

	rcu := int64(DefaultReadCapacity)
	wcu := int64(DefaultWriteCapacity)

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
			TimeToLiveDescription: &types.TimeToLiveDescription{TimeToLiveStatus: types.TimeToLiveStatusDisabled},
		}, nil
	}

	return &dynamodb.DescribeTimeToLiveOutput{
		TimeToLiveDescription: &types.TimeToLiveDescription{
			AttributeName:    aws.String(table.TTLAttribute),
			TimeToLiveStatus: types.TimeToLiveStatusEnabled,
		},
	}, nil
}

func (db *InMemoryDB) ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		names = append(names, name)
	}

	// TODO: Handle Limit and key pagination if strictly needed,
	// but for now return all or just respect limit if easy.
	// InMemoryDB usually implies small scale.

	limit := int(aws.ToInt32(input.Limit))
	if limit > 0 && limit < len(names) {
		names = names[:limit]
	}

	return &dynamodb.ListTablesOutput{
		TableNames: names,
	}, nil
}
