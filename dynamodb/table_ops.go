package dynamodb

import (
	"encoding/json"
	"fmt"
)

func (db *InMemoryDB) CreateTable(body []byte) (interface{}, error) {
	var input CreateTableInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Tables[input.TableName]; exists {
		return nil, fmt.Errorf("table already exists: %s", input.TableName)
	}

	newTable := &Table{
		Name:                   input.TableName,
		KeySchema:              input.KeySchema,
		AttributeDefinitions:   input.AttributeDefinitions,
		GlobalSecondaryIndexes: input.GlobalSecondaryIndexes,
		LocalSecondaryIndexes:  input.LocalSecondaryIndexes,
		Items:                  make([]map[string]interface{}, 0),
	}
	db.Tables[input.TableName] = newTable

	// Convert GSIs to Description
	gsiDescs := make([]GlobalSecondaryIndexDescription, len(input.GlobalSecondaryIndexes))
	for i, gsi := range input.GlobalSecondaryIndexes {
		gsiDescs[i] = GlobalSecondaryIndexDescription{
			IndexName:  gsi.IndexName,
			KeySchema:  gsi.KeySchema,
			Projection: gsi.Projection,
			ProvisionedThroughput: ProvisionedThroughputDescription{
				ReadCapacityUnits:  5,
				WriteCapacityUnits: 5,
			},
			IndexStatus: "ACTIVE",
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
			TableStatus:            "ACTIVE",
			KeySchema:              newTable.KeySchema,
			AttributeDefinitions:   newTable.AttributeDefinitions,
			GlobalSecondaryIndexes: gsiDescs,
			LocalSecondaryIndexes:  lsiDescs,
			ItemCount:              0,
			ProvisionedThroughput: &ProvisionedThroughputDescription{
				ReadCapacityUnits:  5,
				WriteCapacityUnits: 5,
			},
		},
	}, nil
}

func (db *InMemoryDB) DeleteTable(body []byte) (interface{}, error) {
	var input DeleteTableInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	table, exists := db.Tables[input.TableName]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", input.TableName)
	}

	delete(db.Tables, input.TableName)

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

func (db *InMemoryDB) DescribeTable(body []byte) (interface{}, error) {
	var input DescribeTableInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.Tables[input.TableName]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", input.TableName)
	}

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
			IndexStatus: "ACTIVE",
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
			TableStatus:            "ACTIVE",
			KeySchema:              table.KeySchema,
			AttributeDefinitions:   table.AttributeDefinitions,
			GlobalSecondaryIndexes: gsiDescs,
			LocalSecondaryIndexes:  lsiDescs,
			ItemCount:              len(table.Items),
			ProvisionedThroughput: &ProvisionedThroughputDescription{
				ReadCapacityUnits:  5,
				WriteCapacityUnits: 5,
			},
		},
	}, nil
}

func (db *InMemoryDB) ListTables(body []byte) (interface{}, error) {
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
