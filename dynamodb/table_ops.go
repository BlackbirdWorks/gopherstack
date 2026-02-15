package dynamodb

import (
	"encoding/json"
	"fmt"
)

func (db *InMemoryDB) CreateTable(body []byte) (any, error) {
	var input CreateTableInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Tables[input.TableName]; exists {
		return nil, NewResourceInUseException(fmt.Sprintf("table already exists: %s", input.TableName))
	}

	newTable := &Table{
		Name:                   input.TableName,
		KeySchema:              input.KeySchema,
		AttributeDefinitions:   input.AttributeDefinitions,
		GlobalSecondaryIndexes: input.GlobalSecondaryIndexes,
		LocalSecondaryIndexes:  input.LocalSecondaryIndexes,
		Items:                  make([]map[string]any, 0),
	}
	newTable.initializeIndexes()
	db.Tables[input.TableName] = newTable

	// Convert GSIs to Description
	gsiDescs := make([]GlobalSecondaryIndexDescription, len(input.GlobalSecondaryIndexes))
	for i, gsi := range input.GlobalSecondaryIndexes {
		gsiDescs[i] = GlobalSecondaryIndexDescription{
			IndexName:  gsi.IndexName,
			KeySchema:  gsi.KeySchema,
			Projection: gsi.Projection,
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
			IndexName:      lsi.IndexName,
			KeySchema:      lsi.KeySchema,
			Projection:     lsi.Projection,
			IndexSizeBytes: 0,
			ItemCount:      0,
		}
	}

	return CreateTableOutput{
		TableDescription: TableDescription{
			TableName:              newTable.Name,
			TableStatus:            TableStatusActive,
			KeySchema:              newTable.KeySchema,
			AttributeDefinitions:   newTable.AttributeDefinitions,
			GlobalSecondaryIndexes: gsiDescs,
			LocalSecondaryIndexes:  lsiDescs,
			ItemCount:              0,
			ProvisionedThroughput: &ProvisionedThroughputDescription{
				ReadCapacityUnits:  DefaultReadCapacity,
				WriteCapacityUnits: DefaultWriteCapacity,
			},
		},
	}, nil
}

func (db *InMemoryDB) DeleteTable(body []byte) (any, error) {
	var input DeleteTableInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.Lock()
	table, exists := db.Tables[input.TableName]
	if !exists {
		db.mu.Unlock()

		return nil, NewResourceNotFoundException(fmt.Sprintf("table not found: %s", input.TableName))
	}
	delete(db.Tables, input.TableName)
	db.mu.Unlock()

	// Acquire table lock after releasing db lock to avoid holding both simultaneously.
	// In-flight item operations that already have a table pointer may still be running;
	// table.mu.RLock ensures we don't read table.Items concurrently with a write.
	table.mu.RLock()
	defer table.mu.RUnlock()

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

	return DeleteTableOutput{
		TableDescription: TableDescription{
			TableName:              table.Name,
			TableStatus:            "DELETING",
			KeySchema:              table.KeySchema,
			AttributeDefinitions:   table.AttributeDefinitions,
			GlobalSecondaryIndexes: gsiDescs,
			ItemCount:              len(table.Items),
		},
	}, nil
}

func (db *InMemoryDB) DescribeTable(body []byte) (any, error) {
	var input DescribeTableInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.RLock()
	table, exists := db.Tables[input.TableName]
	db.mu.RUnlock()

	if !exists {
		return nil, NewResourceNotFoundException(fmt.Sprintf("table not found: %s", input.TableName))
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

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
			IndexStatus: TableStatusActive,
			ItemCount:   len(table.Items), // Simplified: full table count
		}
	}

	// Convert LSIs
	lsiDescs := make([]LocalSecondaryIndexDescription, len(table.LocalSecondaryIndexes))
	for i, lsi := range table.LocalSecondaryIndexes {
		lsiDescs[i] = LocalSecondaryIndexDescription{
			IndexName:      lsi.IndexName,
			KeySchema:      lsi.KeySchema,
			Projection:     lsi.Projection,
			IndexSizeBytes: 0,
			ItemCount:      0, // Naive count relative to whole table? Valid enough for now.
		}
	}

	return DescribeTableOutput{
		Table: TableDescription{
			TableName:              table.Name,
			TableStatus:            TableStatusActive,
			KeySchema:              table.KeySchema,
			AttributeDefinitions:   table.AttributeDefinitions,
			GlobalSecondaryIndexes: gsiDescs,
			LocalSecondaryIndexes:  lsiDescs,
			ItemCount:              len(table.Items),
			ProvisionedThroughput: &ProvisionedThroughputDescription{
				ReadCapacityUnits:  DefaultReadCapacity,
				WriteCapacityUnits: DefaultWriteCapacity,
			},
		},
	}, nil
}

func (db *InMemoryDB) UpdateTimeToLive(body []byte) (any, error) {
	var input UpdateTimeToLiveInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	table, err := db.getTable(input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	if input.TimeToLiveSpecification.Enabled {
		table.TTLAttribute = input.TimeToLiveSpecification.AttributeName
	} else {
		table.TTLAttribute = ""
	}

	return UpdateTimeToLiveOutput{
		TimeToLiveSpecification: input.TimeToLiveSpecification,
	}, nil
}

func (db *InMemoryDB) DescribeTimeToLive(body []byte) (any, error) {
	var input DescribeTimeToLiveInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	table, err := db.getTable(input.TableName)
	if err != nil {
		return nil, err
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	if table.TTLAttribute == "" {
		return DescribeTimeToLiveOutput{
			TimeToLiveDescription: TimeToLiveDescription{TimeToLiveStatus: "DISABLED"},
		}, nil
	}

	return DescribeTimeToLiveOutput{
		TimeToLiveDescription: TimeToLiveDescription{
			AttributeName:    table.TTLAttribute,
			TimeToLiveStatus: "ENABLED",
		},
	}, nil
}

func (db *InMemoryDB) ListTables(body []byte) (any, error) {
	var input ListTablesInput
	if len(body) > 0 {
		_ = json.Unmarshal(body, &input)
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		names = append(names, name)
	}

	return ListTablesOutput{
		TableNames: names,
	}, nil
}
