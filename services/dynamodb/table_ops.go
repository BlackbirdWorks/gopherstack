package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	errReplicaCreateRegionRequired = errors.New("RegionName is required for ReplicaUpdates Create action")
	errReplicaDeleteRegionRequired = errors.New("RegionName is required for ReplicaUpdates Delete action")
)

// getRegionFromContext extracts the region from the request context.
// Returns the default region if region is not found in context.
func getRegionFromContext(ctx context.Context, db *InMemoryDB) string {
	if region, ok := ctx.Value(regionContextKey{}).(string); ok && region != "" {
		return region
	}

	return db.defaultRegion
}

// throttleKey returns the throttler key for the given region and table.
func throttleKey(region, tableName string) string {
	return region + ":" + tableName
}

// CreateTableInRegion creates a DynamoDB table in the specified region, bypassing
// the HTTP-layer region extraction. The supplied region always takes precedence,
// even if the context already carries a region value. Useful for tests that need
// tables in non-default regions.
func (db *InMemoryDB) CreateTableInRegion(
	ctx context.Context,
	input *dynamodb.CreateTableInput,
	region string,
) (*dynamodb.CreateTableOutput, error) {
	return db.CreateTable(context.WithValue(ctx, regionContextKey{}, region), input)
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

	if err := validateAttributeDefinitions(input); err != nil {
		return nil, err
	}

	newTable := newTableFromCreateInput(tableName, input)
	newTable.TableID = uuid.New().String()
	newTable.CreationDateTime = time.Now()
	newTable.TableArn = arn.Build("dynamodb", region, db.accountID, "table/"+tableName)

	if input.StreamSpecification != nil && aws.ToBool(input.StreamSpecification.StreamEnabled) {
		newTable.StreamsEnabled = true
		newTable.StreamViewType = string(input.StreamSpecification.StreamViewType)
		newTable.StreamARN = db.buildStreamARN(tableName)
	}

	// Set initial table status based on createDelay setting.
	if db.createDelay > 0 {
		newTable.Status = string(types.TableStatusCreating)

		newTable.activateTimer = time.AfterFunc(db.createDelay, func() {
			newTable.mu.Lock("activate")
			newTable.Status = string(types.TableStatusActive)
			newTable.mu.Unlock()
		})
	} else {
		newTable.Status = string(types.TableStatusActive)
	}

	db.Tables[region][tableName] = newTable

	if newTable.StreamARN != "" {
		db.streamARNIndex[newTable.StreamARN] = newTable
	}

	rcu := int64(newTable.ProvisionedThroughput.ReadCapacityUnits)
	wcu := int64(newTable.ProvisionedThroughput.WriteCapacityUnits)
	db.throttler.SetTableCapacity(throttleKey(region, tableName), rcu, wcu)

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

	if input.TableClass != "" {
		t.TableClass = string(input.TableClass)
	}
	t.DeletionProtectionEnabled = aws.ToBool(input.DeletionProtectionEnabled)

	t.initializeIndexes()

	return t
}

func validateAttributeDefinitions(input *dynamodb.CreateTableInput) error {
	defs := make(map[string]struct{})
	for _, ad := range input.AttributeDefinitions {
		defs[aws.ToString(ad.AttributeName)] = struct{}{}
	}

	referenced := make(map[string]struct{})
	// Check Table KeySchema
	for _, k := range input.KeySchema {
		name := aws.ToString(k.AttributeName)
		referenced[name] = struct{}{}
		if _, ok := defs[name]; !ok {
			return NewValidationException(fmt.Sprintf("Parameter AttributeDefinitions does not contain definition for attribute %s which is used in KeySchema", name))
		}
	}

	// Check GSI KeySchema
	for _, gsi := range input.GlobalSecondaryIndexes {
		for _, k := range gsi.KeySchema {
			name := aws.ToString(k.AttributeName)
			referenced[name] = struct{}{}
			if _, ok := defs[name]; !ok {
				return NewValidationException(fmt.Sprintf("Parameter AttributeDefinitions does not contain definition for attribute %s which is used in GlobalSecondaryIndexes", name))
			}
		}
	}

	// Check LSI KeySchema
	for _, lsi := range input.LocalSecondaryIndexes {
		for _, k := range lsi.KeySchema {
			name := aws.ToString(k.AttributeName)
			referenced[name] = struct{}{}
			if _, ok := defs[name]; !ok {
				return NewValidationException(fmt.Sprintf("Parameter AttributeDefinitions does not contain definition for attribute %s which is used in LocalSecondaryIndexes", name))
			}
		}
	}

	// All defs must be referenced
	for name := range defs {
		if _, ok := referenced[name]; !ok {
			return NewValidationException(fmt.Sprintf("Parameter AttributeDefinitions contains unused attribute: %s", name))
		}
	}

	return nil
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
	// Cancel any pending activation timer. Stop() is called while db.mu is held, which
	// prevents a concurrent CreateTable from racing with us. If the timer has already fired
	// and the callback is in progress, Stop() returns false but the callback only writes
	// table.Status (guarded by table.mu) on an object that is about to move to
	// deletingTables — this is benign; the janitor will clean it up regardless.
	if table.activateTimer != nil {
		table.activateTimer.Stop()
	}

	delete(db.Tables[region], tableName)
	if _, deletingExists := db.deletingTables[region]; !deletingExists {
		db.deletingTables[region] = make(map[string]*Table)
	}
	db.deletingTables[region][tableName] = table
	db.throttler.DeleteTable(throttleKey(region, tableName))

	// Remove from stream ARN reverse index.
	if table.StreamARN != "" {
		delete(db.streamARNIndex, table.StreamARN)
	}

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

		status := gsi.IndexStatus
		if status == "" {
			status = models.TableStatusActive
		}

		gsiDescs[i] = models.GlobalSecondaryIndexDescription{
			IndexName:  gsi.IndexName,
			KeySchema:  gsi.KeySchema,
			Projection: gsi.Projection,
			ProvisionedThroughput: models.ProvisionedThroughputDescription{
				ReadCapacityUnits:  int(rc),
				WriteCapacityUnits: int(wc),
			},
			IndexStatus: status,
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

	tableDesc := buildTableDescription(input.TableName, table)

	return &dynamodb.DescribeTableOutput{Table: tableDesc}, nil
}

// tableSnapshot is a lock-free snapshot of table metadata for building SDK responses.
type tableSnapshot struct {
	creationDT     time.Time
	tableStatus    types.TableStatus
	tableArn       string
	tableID        string
	streamARN      string
	streamViewType string
	gsiList        []models.GlobalSecondaryIndex
	lsiList        []models.LocalSecondaryIndex
	replicaList    []models.ReplicaDescription
	keySchema      []models.KeySchemaElement
	attrDefs       []models.AttributeDefinition
	pt             models.ProvisionedThroughputDescription
	itemCount      int64
	itemSizeBytes             int64
	streamsEnabled            bool
	deletionProtectionEnabled bool
	tableClass                string
}

func snapshotTable(table *Table) tableSnapshot {
	table.mu.RLock("DescribeTable")
	defer table.mu.RUnlock()

	s := tableSnapshot{
		keySchema:      make([]models.KeySchemaElement, len(table.KeySchema)),
		attrDefs:       make([]models.AttributeDefinition, len(table.AttributeDefinitions)),
		gsiList:        make([]models.GlobalSecondaryIndex, len(table.GlobalSecondaryIndexes)),
		lsiList:        make([]models.LocalSecondaryIndex, len(table.LocalSecondaryIndexes)),
		replicaList:    make([]models.ReplicaDescription, len(table.Replicas)),
		itemCount:      int64(len(table.Items)),
		itemSizeBytes:  estimateTableSizeBytes(table.Items),
		pt:             table.ProvisionedThroughput,
		tableStatus:    types.TableStatus(table.Status),
		tableArn:       table.TableArn,
		tableID:        table.TableID,
		creationDT:                table.CreationDateTime,
		streamARN:                 table.StreamARN,
		streamsEnabled:            table.StreamsEnabled,
		streamViewType:            table.StreamViewType,
		deletionProtectionEnabled: table.DeletionProtectionEnabled,
		tableClass:                table.TableClass,
	}
	copy(s.keySchema, table.KeySchema)
	copy(s.attrDefs, table.AttributeDefinitions)
	copy(s.gsiList, table.GlobalSecondaryIndexes)
	copy(s.lsiList, table.LocalSecondaryIndexes)
	copy(s.replicaList, table.Replicas)

	if s.tableStatus == "" {
		s.tableStatus = types.TableStatusActive
	}

	return s
}

// buildTableDescription constructs the SDK TableDescription for a DescribeTable response.
func buildTableDescription(tableName *string, table *Table) *types.TableDescription {
	s := snapshotTable(table)

	gsiDescs := buildGSIDescriptions(s.gsiList, s.itemCount)
	lsiDescs := buildLSIDescriptions(s.lsiList)

	rcu := int64(s.pt.ReadCapacityUnits)
	wcu := int64(s.pt.WriteCapacityUnits)

	tableSizeBytes := s.itemSizeBytes

	td := &types.TableDescription{
		TableName:              tableName,
		TableStatus:            s.tableStatus,
		KeySchema:              models.ToSDKKeySchema(s.keySchema),
		AttributeDefinitions:   models.ToSDKAttributeDefinitions(s.attrDefs),
		GlobalSecondaryIndexes: models.ToSDKGlobalSecondaryIndexDescriptions(gsiDescs),
		LocalSecondaryIndexes:  models.ToSDKLocalSecondaryIndexDescriptions(lsiDescs),
		Replicas:               toSDKReplicaDescriptions(s.replicaList),
		ItemCount:              &s.itemCount,
		TableSizeBytes:         &tableSizeBytes,
		BillingModeSummary:     &types.BillingModeSummary{BillingMode: types.BillingModeProvisioned},
		ProvisionedThroughput: &types.ProvisionedThroughputDescription{
			ReadCapacityUnits:  &rcu,
			WriteCapacityUnits: &wcu,
		},
		TableClassSummary: &types.TableClassSummary{
			TableClass: types.TableClass(s.tableClass),
		},
		DeletionProtectionEnabled: &s.deletionProtectionEnabled,
	}

	if s.tableArn != "" {
		td.TableArn = &s.tableArn
	}
	if s.tableID != "" {
		td.TableId = &s.tableID
	}
	if !s.creationDT.IsZero() {
		td.CreationDateTime = &s.creationDT
	}

	applyStreamSpec(td, s.streamsEnabled, s.streamARN, s.streamViewType)

	return td
}

// applyStreamSpec fills the stream-related fields of a TableDescription when streams are enabled.
func applyStreamSpec(td *types.TableDescription, enabled bool, streamARN, viewType string) {
	if !enabled || streamARN == "" {
		return
	}

	td.LatestStreamArn = &streamARN

	// LatestStreamLabel is the last path segment of the stream ARN (the timestamp portion).
	streamLabel := streamARN
	if idx := strings.LastIndex(streamARN, "/"); idx >= 0 {
		streamLabel = streamARN[idx+1:]
	}

	td.LatestStreamLabel = &streamLabel
	td.StreamSpecification = &types.StreamSpecification{
		StreamEnabled:  aws.Bool(true),
		StreamViewType: types.StreamViewType(viewType),
	}
}

// UpdateTable modifies a DynamoDB table's provisioned throughput, GSI list, stream spec, and replicas.
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

	// Apply all table mutations under a single lock acquisition, then release
	// before updating db-level state (stream ARN index, throttler) to minimize the
	// table.mu critical section and avoid lock-ordering issues.
	var (
		oldStreamARN string
		newStreamARN string
		rcu          int64
		wcu          int64
		out          *dynamodb.UpdateTableOutput
		region       = getRegionFromContext(ctx, db)
	)

	if updateErr := func() error {
		table.mu.Lock("UpdateTable")
		defer table.mu.Unlock()

		applyUpdateTableThroughput(table, input.ProvisionedThroughput)
		applyUpdateTableAttrDefs(table, input.AttributeDefinitions)
		if len(input.GlobalSecondaryIndexUpdates) > 0 {
			db.applyGSIUpdates(table, input.GlobalSecondaryIndexUpdates)
		}
		oldStreamARN, newStreamARN = db.applyStreamSpec(table, tableName, input.StreamSpecification)

		if replicaErr := applyReplicaUpdates(table, input.ReplicaUpdates); replicaErr != nil {
			return NewValidationException(replicaErr.Error())
		}

		if input.DeletionProtectionEnabled != nil {
			table.DeletionProtectionEnabled = *input.DeletionProtectionEnabled
		}

		rcu = int64(table.ProvisionedThroughput.ReadCapacityUnits)
		wcu = int64(table.ProvisionedThroughput.WriteCapacityUnits)
		out = buildUpdateTableOutput(input, table)

		return nil
	}(); updateErr != nil {
		return nil, updateErr
	}

	// Update throttler outside table.mu: SetTableCapacity takes its own internal
	// lock, so calling it inside the table lock would unnecessarily extend the
	// critical section and increase contention with concurrent reads/writes.
	db.throttler.SetTableCapacity(throttleKey(region, tableName), rcu, wcu)

	// Update the stream ARN reverse index under db.mu (after the table lock has been
	// released — never hold both table.mu and db.mu simultaneously to prevent deadlocks).
	if oldStreamARN != newStreamARN {
		db.mu.Lock("UpdateTable.streamARNIndex")
		if oldStreamARN != "" {
			delete(db.streamARNIndex, oldStreamARN)
		}
		if newStreamARN != "" {
			db.streamARNIndex[newStreamARN] = table
		}
		db.mu.Unlock()
	}

	return out, nil
}

// applyReplicaUpdates processes Global Tables v2 replica create/delete actions.
// Replicas are metadata-only: no actual cross-region sync is performed.
// Returns an error if any update has an empty RegionName.
func applyReplicaUpdates(table *Table, updates []types.ReplicationGroupUpdate) error {
	for _, u := range updates {
		if u.Create != nil {
			regionName := aws.ToString(u.Create.RegionName)
			if regionName == "" {
				return errReplicaCreateRegionRequired
			}

			applyReplicaCreate(table, regionName)
		} else if u.Delete != nil {
			regionName := aws.ToString(u.Delete.RegionName)
			if regionName == "" {
				return errReplicaDeleteRegionRequired
			}

			applyReplicaDelete(table, regionName)
		}
	}

	return nil
}

func applyReplicaCreate(table *Table, regionName string) {
	if regionName == "" {
		return
	}

	for _, r := range table.Replicas {
		if r.RegionName == regionName {
			return
		}
	}

	table.Replicas = append(table.Replicas, models.ReplicaDescription{
		RegionName:    regionName,
		ReplicaStatus: "ACTIVE",
	})
}

func applyReplicaDelete(table *Table, regionName string) {
	if regionName == "" {
		return
	}

	updated := make([]models.ReplicaDescription, 0, len(table.Replicas))
	for _, r := range table.Replicas {
		if r.RegionName != regionName {
			updated = append(updated, r)
		}
	}

	table.Replicas = updated
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
func (db *InMemoryDB) applyGSIUpdates(table *Table, updates []types.GlobalSecondaryIndexUpdate) {
	for _, u := range updates {
		switch {
		case u.Create != nil:
			db.applyGSICreate(table, u.Create)
		case u.Update != nil:
			db.applyGSIUpdate(table, u.Update)
		case u.Delete != nil:
			db.applyGSIDelete(table, u.Delete)
		}
	}
}

func (db *InMemoryDB) applyGSICreate(table *Table, c *types.CreateGlobalSecondaryIndexAction) {
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

	// Simulated GSI lifecycle: CREATING -> ACTIVE
	// If createDelay is set, use a timer; otherwise transition immediately.
	// Since table.GlobalSecondaryIndexes is a slice and we need to update the status in-place later,
	// we use the slice index. note: this assumes indexes are not deleted while a timer is pending.
	idx := len(table.GlobalSecondaryIndexes)
	newGSI.IndexStatus = string(types.IndexStatusCreating)

	table.GlobalSecondaryIndexes = append(table.GlobalSecondaryIndexes, newGSI)

	// Re-get pointer to the added entry to setup timer
	gsiPtr := &table.GlobalSecondaryIndexes[idx]

	if delay := db.createDelay; delay > 0 {
		gsiPtr.IndexStatusTimer = time.AfterFunc(delay, func() {
			table.mu.Lock("GSIActivate")
			defer table.mu.Unlock()
			// Find the GSI again by name as slice might have moved or item deleted
			for i := range table.GlobalSecondaryIndexes {
				if table.GlobalSecondaryIndexes[i].IndexName == gsiPtr.IndexName {
					table.GlobalSecondaryIndexes[i].IndexStatus = string(types.IndexStatusActive)

					break
				}
			}
		})
	} else {
		gsiPtr.IndexStatus = string(types.IndexStatusActive)
	}

	// Rebuild (not just initialise) so existing items remain indexed after the GSI
	// definition is added. initializeIndexes() would clear the primary key index,
	// forcing O(n) scans for all subsequent primary-key queries.
	table.rebuildIndexes()
}


func (db *InMemoryDB) applyGSIUpdate(table *Table, u *types.UpdateGlobalSecondaryIndexAction) { // Changed to method
	idxName := aws.ToString(u.IndexName)

	for i, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName == idxName && u.ProvisionedThroughput != nil {
			table.GlobalSecondaryIndexes[i].ProvisionedThroughput = models.ProvisionedThroughput{
				ReadCapacityUnits:  u.ProvisionedThroughput.ReadCapacityUnits,
				WriteCapacityUnits: u.ProvisionedThroughput.WriteCapacityUnits,
			}

			return
		}
	}
}

func (db *InMemoryDB) applyGSIDelete(table *Table, d *types.DeleteGlobalSecondaryIndexAction) {
	idxName := aws.ToString(d.IndexName)

	var foundIdx = -1
	for i, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName == idxName {
			foundIdx = i

			break
		}
	}

	if foundIdx == -1 {
		return
	}

	// Simulated GSI lifecycle: DELETING -> removed
	if delay := db.createDelay; delay > 0 {
		gsiPtr := &table.GlobalSecondaryIndexes[foundIdx]
		gsiPtr.IndexStatus = string(types.IndexStatusDeleting)
		if gsiPtr.IndexStatusTimer != nil {
			gsiPtr.IndexStatusTimer.Stop()
		}
		gsiPtr.IndexStatusTimer = time.AfterFunc(delay, func() {
			table.mu.Lock("GSIRemove")
			defer table.mu.Unlock()
			// Find the GSI again by name as slice might have moved
			for i := range table.GlobalSecondaryIndexes {
				if table.GlobalSecondaryIndexes[i].IndexName == idxName {
					// Remove from slice
					table.GlobalSecondaryIndexes = append(
						table.GlobalSecondaryIndexes[:i],
						table.GlobalSecondaryIndexes[i+1:]...,
					)

					break
				}
			}
		})
	} else {
		// Immediate removal
		table.GlobalSecondaryIndexes = append(
			table.GlobalSecondaryIndexes[:foundIdx],
			table.GlobalSecondaryIndexes[foundIdx+1:]...,
		)
	}

	// Rebuild to clear the index from memory immediately
	table.rebuildIndexes()
}

// applyStreamSpec enables or disables streams on the table.
// Returns the old stream ARN (to remove from the index) and the new ARN (to add), so the caller
// can update db.streamARNIndex under db.mu after releasing the table lock.
func (db *InMemoryDB) applyStreamSpec(
	table *Table,
	tableName string,
	ss *types.StreamSpecification,
) (string, string) {
	if ss == nil {
		return "", ""
	}

	oldARN := table.StreamARN

	if aws.ToBool(ss.StreamEnabled) {
		table.StreamsEnabled = true
		table.StreamViewType = string(ss.StreamViewType)

		if table.StreamARN == "" {
			table.StreamARN = db.buildStreamARN(tableName)
		}

		return oldARN, table.StreamARN
	}

	table.StreamsEnabled = false
	table.StreamViewType = ""
	table.StreamARN = ""

	return oldARN, ""
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

		status := gsi.IndexStatus
		if status == "" {
			status = string(types.IndexStatusActive)
		}

		gsiDescs = append(gsiDescs, types.GlobalSecondaryIndexDescription{
			IndexName:   &gsi.IndexName,
			KeySchema:   models.ToSDKKeySchema(gsi.KeySchema),
			Projection:  models.ToSDKProjection(gsi.Projection),
			IndexStatus: types.IndexStatus(status),
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
			Replicas:               toSDKReplicaDescriptions(table.Replicas),
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  &rcu,
				WriteCapacityUnits: &wcu,
			},
		},
	}
}

// toSDKReplicaDescriptions converts internal replica metadata to SDK types.
func toSDKReplicaDescriptions(replicas []models.ReplicaDescription) []types.ReplicaDescription {
	if len(replicas) == 0 {
		return nil
	}

	out := make([]types.ReplicaDescription, len(replicas))
	for i, r := range replicas {
		regionName := r.RegionName
		out[i] = types.ReplicaDescription{
			RegionName:    &regionName,
			ReplicaStatus: types.ReplicaStatus(r.ReplicaStatus),
		}
	}

	return out
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
		idx, found := findStartIndex(names, startName)
		if !found {
			return &dynamodb.ListTablesOutput{
				TableNames: []string{},
			}, nil
		}

		startIndex = idx
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

// findStartIndex returns the index of the first name strictly greater than after,
// and whether such a name exists.
func findStartIndex(names []string, after string) (int, bool) {
	for i, name := range names {
		if name > after {
			return i, true
		}
	}

	return 0, false
}
