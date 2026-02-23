package dynamodb

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/google/uuid"

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

	if _, exists := db.Tables[region]; !exists {
		db.Tables[region] = make(map[string]*Table)
	}

	if _, exists := db.Tables[region][tableName]; exists {
		return nil, NewResourceInUseException(fmt.Sprintf("table already exists: %s", tableName))
	}

	newTable := newTableFromCreateInput(tableName, input)
	newTable.TableID = uuid.New().String()
	newTable.CreationDateTime = time.Now()
	newTable.TableArn = fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", region, db.accountID, tableName)

	if input.StreamSpecification != nil && aws.ToBool(input.StreamSpecification.StreamEnabled) {
		newTable.StreamsEnabled = true
		newTable.StreamViewType = string(input.StreamSpecification.StreamViewType)
		newTable.StreamARN = db.buildStreamARN(tableName)
	}

	// Set initial table status based on createDelay setting.
	if db.createDelay > 0 {
		newTable.Status = string(types.TableStatusCreating)

		go func() {
			time.Sleep(db.createDelay)
			newTable.mu.Lock("activate")
			newTable.Status = string(types.TableStatusActive)
			newTable.mu.Unlock()
		}()
	} else {
		newTable.Status = string(types.TableStatusActive)
	}

	db.Tables[region][tableName] = newTable

	return buildCreateTableOutput(input, newTable), nil
}

// newTableFromCreateInput allocates and initialises a Table from a CreateTable request.
func newTableFromCreateInput(tableName string, input *dynamodb.CreateTableInput) *Table {
	t := &Table{
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

	if pt := input.ProvisionedThroughput; pt != nil {
		if pt.ReadCapacityUnits != nil {
			t.ProvisionedThroughput.ReadCapacityUnits = int(*pt.ReadCapacityUnits)
		}

		if pt.WriteCapacityUnits != nil {
			t.ProvisionedThroughput.WriteCapacityUnits = int(*pt.WriteCapacityUnits)
		}
	}

	t.initializeIndexes()

	return t
}

// buildCreateTableOutput constructs the wire response for CreateTable.
func buildCreateTableOutput(input *dynamodb.CreateTableInput, t *Table) *dynamodb.CreateTableOutput {
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
		}
	}

	lsiDescs := make([]models.LocalSecondaryIndexDescription, len(input.LocalSecondaryIndexes))
	for i, lsi := range input.LocalSecondaryIndexes {
		lsiDescs[i] = models.LocalSecondaryIndexDescription{
			IndexName:  aws.ToString(lsi.IndexName),
			KeySchema:  models.FromSDKKeySchema(lsi.KeySchema),
			Projection: models.FromSDKProjection(lsi.Projection),
		}
	}

	rcu := int64(t.ProvisionedThroughput.ReadCapacityUnits)
	wcu := int64(t.ProvisionedThroughput.WriteCapacityUnits)

	tableStatus := types.TableStatus(t.Status)
	if tableStatus == "" {
		tableStatus = types.TableStatusActive
	}

	return &dynamodb.CreateTableOutput{
		TableDescription: &types.TableDescription{
			TableName:              input.TableName,
			TableStatus:            tableStatus,
			KeySchema:              models.ToSDKKeySchema(t.KeySchema),
			AttributeDefinitions:   models.ToSDKAttributeDefinitions(t.AttributeDefinitions),
			GlobalSecondaryIndexes: models.ToSDKGlobalSecondaryIndexDescriptions(gsiDescs),
			LocalSecondaryIndexes:  models.ToSDKLocalSecondaryIndexDescriptions(lsiDescs),
			ItemCount:              aws.Int64(0),
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  &rcu,
				WriteCapacityUnits: &wcu,
			},
		},
	}
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

func buildGSIDescriptions(
	gsiList []models.GlobalSecondaryIndex,
	itemCount int64,
) []models.GlobalSecondaryIndexDescription {
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

	return gsiDescs
}

func buildLSIDescriptions(lsiList []models.LocalSecondaryIndex) []models.LocalSecondaryIndexDescription {
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

	return lsiDescs
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
	tableStatus := types.TableStatus(table.Status)
	if tableStatus == "" {
		tableStatus = types.TableStatusActive
	}
	tableArn := table.TableArn
	tableID := table.TableID
	creationDateTime := table.CreationDateTime

	table.mu.RUnlock()

	// Build index descriptions outside lock
	gsiDescs := buildGSIDescriptions(gsiList, itemCount)
	lsiDescs := buildLSIDescriptions(lsiList)

	sdkGSIs := models.ToSDKGlobalSecondaryIndexDescriptions(gsiDescs)
	sdkLSIs := models.ToSDKLocalSecondaryIndexDescriptions(lsiDescs)
	sdkKeySchema := models.ToSDKKeySchema(keySchema)
	sdkAttrDefs := models.ToSDKAttributeDefinitions(attrDefs)

	rcu := int64(pt.ReadCapacityUnits)
	wcu := int64(pt.WriteCapacityUnits)

	// Estimate table size: item count * average item size (400 bytes).
	const avgItemSizeBytes = 400
	tableSizeBytes := itemCount * avgItemSizeBytes

	tableDesc := &types.TableDescription{
		TableName:              input.TableName,
		TableStatus:            tableStatus,
		KeySchema:              sdkKeySchema,
		AttributeDefinitions:   sdkAttrDefs,
		GlobalSecondaryIndexes: sdkGSIs,
		LocalSecondaryIndexes:  sdkLSIs,
		ItemCount:              &itemCount,
		TableSizeBytes:         &tableSizeBytes,
		BillingModeSummary:     &types.BillingModeSummary{BillingMode: types.BillingModeProvisioned},
		ProvisionedThroughput: &types.ProvisionedThroughputDescription{
			ReadCapacityUnits:  &rcu,
			WriteCapacityUnits: &wcu,
		},
	}

	if tableArn != "" {
		tableDesc.TableArn = &tableArn
	}
	if tableID != "" {
		tableDesc.TableId = &tableID
	}
	if !creationDateTime.IsZero() {
		tableDesc.CreationDateTime = &creationDateTime
	}

	return &dynamodb.DescribeTableOutput{Table: tableDesc}, nil
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

	applyUpdateTableThroughput(table, input.ProvisionedThroughput)
	applyUpdateTableAttrDefs(table, input.AttributeDefinitions)
	applyGSIUpdates(table, input.GlobalSecondaryIndexUpdates)
	db.applyStreamSpec(table, tableName, input.StreamSpecification)

	return buildUpdateTableOutput(input, table), nil
}

// applyUpdateTableThroughput updates provisioned throughput on the table.
func applyUpdateTableThroughput(table *Table, pt *types.ProvisionedThroughput) {
	if pt == nil {
		return
	}

	if pt.ReadCapacityUnits != nil {
		table.ProvisionedThroughput.ReadCapacityUnits = int(*pt.ReadCapacityUnits)
	}

	if pt.WriteCapacityUnits != nil {
		table.ProvisionedThroughput.WriteCapacityUnits = int(*pt.WriteCapacityUnits)
	}
}

// applyUpdateTableAttrDefs merges new attribute definitions into the table (keeps existing ones).
func applyUpdateTableAttrDefs(table *Table, sdkADs []types.AttributeDefinition) {
	if len(sdkADs) == 0 {
		return
	}

	existing := make(map[string]struct{}, len(table.AttributeDefinitions))
	for _, ad := range table.AttributeDefinitions {
		existing[ad.AttributeName] = struct{}{}
	}

	for _, sdkAD := range sdkADs {
		name := aws.ToString(sdkAD.AttributeName)
		if _, found := existing[name]; !found {
			table.AttributeDefinitions = append(table.AttributeDefinitions,
				models.AttributeDefinition{AttributeName: name, AttributeType: string(sdkAD.AttributeType)})
		}
	}
}

// applyGSIUpdates applies Create / Update / Delete GSI actions.
func applyGSIUpdates(table *Table, updates []types.GlobalSecondaryIndexUpdate) {
	for _, u := range updates {
		switch {
		case u.Create != nil:
			applyGSICreate(table, u.Create)
		case u.Update != nil:
			applyGSIUpdate(table, u.Update)
		case u.Delete != nil:
			applyGSIDelete(table, u.Delete)
		}
	}
}

func applyGSICreate(table *Table, c *types.CreateGlobalSecondaryIndexAction) {
	newGSI := models.GlobalSecondaryIndex{
		IndexName:  aws.ToString(c.IndexName),
		KeySchema:  models.FromSDKKeySchema(c.KeySchema),
		Projection: models.FromSDKProjection(c.Projection),
	}

	if c.ProvisionedThroughput != nil {
		newGSI.ProvisionedThroughput = models.ProvisionedThroughput{
			ReadCapacityUnits:  c.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: c.ProvisionedThroughput.WriteCapacityUnits,
		}
	}

	table.GlobalSecondaryIndexes = append(table.GlobalSecondaryIndexes, newGSI)
	table.initializeIndexes()
}

func applyGSIUpdate(table *Table, u *types.UpdateGlobalSecondaryIndexAction) {
	idxName := aws.ToString(u.IndexName)

	for i, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName == idxName && u.ProvisionedThroughput != nil {
			table.GlobalSecondaryIndexes[i].ProvisionedThroughput = models.ProvisionedThroughput{
				ReadCapacityUnits:  u.ProvisionedThroughput.ReadCapacityUnits,
				WriteCapacityUnits: u.ProvisionedThroughput.WriteCapacityUnits,
			}

			break
		}
	}
}

func applyGSIDelete(table *Table, d *types.DeleteGlobalSecondaryIndexAction) {
	idxName := aws.ToString(d.IndexName)
	updated := make([]models.GlobalSecondaryIndex, 0, len(table.GlobalSecondaryIndexes))

	for _, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName != idxName {
			updated = append(updated, gsi)
		}
	}

	table.GlobalSecondaryIndexes = updated
	table.initializeIndexes()
}

// applyStreamSpec enables or disables streams on the table.
func (db *InMemoryDB) applyStreamSpec(table *Table, tableName string, ss *types.StreamSpecification) {
	if ss == nil {
		return
	}

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

// buildUpdateTableOutput constructs the UpdateTable response from the current table state.
func buildUpdateTableOutput(input *dynamodb.UpdateTableInput, table *Table) *dynamodb.UpdateTableOutput {
	rcu := int64(table.ProvisionedThroughput.ReadCapacityUnits)
	wcu := int64(table.ProvisionedThroughput.WriteCapacityUnits)

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

		gsiDescs = append(gsiDescs, types.GlobalSecondaryIndexDescription{
			IndexName:   &gsi.IndexName,
			KeySchema:   models.ToSDKKeySchema(gsi.KeySchema),
			Projection:  models.ToSDKProjection(gsi.Projection),
			IndexStatus: types.IndexStatusActive,
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  &rc,
				WriteCapacityUnits: &wc,
			},
		})
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
	}
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
