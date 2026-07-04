package dynamodb

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// consumedCapacityForScan returns a populated ConsumedCapacity when the caller
// has requested capacity reporting. Returns nil when reporting is disabled.
func consumedCapacityForScan(
	tableName string,
	req types.ReturnConsumedCapacity,
	n int,
	consistentRead bool,
) *types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}
	const halfRCU = 0.5 // each 4 KB read costs 0.5 RCU for eventually-consistent reads
	cu := float64(n) * halfRCU
	if cu < halfRCU {
		cu = halfRCU
	}
	// Strongly-consistent reads cost twice as much as eventually-consistent ones.
	cu = applyConsistentReadMultiplier(cu, consistentRead)

	return &types.ConsumedCapacity{
		TableName:         aws.String(tableName),
		CapacityUnits:     aws.Float64(cu),
		ReadCapacityUnits: aws.Float64(cu),
	}
}

func (db *InMemoryDB) Scan(
	ctx context.Context,
	input *dynamodb.ScanInput,
) (*dynamodb.ScanOutput, error) {
	return db.ScanWithContext(ctx, input)
}

func (db *InMemoryDB) ScanWithContext(
	ctx context.Context,
	input *dynamodb.ScanInput,
) (*dynamodb.ScanOutput, error) {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("scan cancelled: %w", ctx.Err())
	default:
	}

	if err := validatePositiveLimit(input.Limit); err != nil {
		return nil, err
	}

	tableName := aws.ToString(input.TableName)
	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	// Validate Segment/TotalSegments before doing any work.
	if input.TotalSegments != nil {
		if segErr := validateScanSegment(
			aws.ToInt32(input.Segment),
			aws.ToInt32(input.TotalSegments),
		); segErr != nil {
			return nil, segErr
		}
	}

	// Snapshot items and metadata under lock, release immediately.
	// A shallow slice copy is safe: writes always replace items[i] with a new map;
	// they never mutate an existing map in place, so our pointers remain valid.
	table.mu.RLock("Scan")
	itemsCopy := make([]map[string]any, len(table.Items))
	copy(itemsCopy, table.Items)
	ttlAttr := table.TTLAttribute
	keySchema := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(keySchema, table.KeySchema)
	gsiList := make([]models.GlobalSecondaryIndex, len(table.GlobalSecondaryIndexes))
	copy(gsiList, table.GlobalSecondaryIndexes)
	lsiList := make([]models.LocalSecondaryIndex, len(table.LocalSecondaryIndexes))
	copy(lsiList, table.LocalSecondaryIndexes)
	attrDefs := make([]models.AttributeDefinition, len(table.AttributeDefinitions))
	copy(attrDefs, table.AttributeDefinitions)
	billingMode := table.BillingMode
	table.mu.RUnlock()

	// Get key schema definitions (reconstruct the table temporarily for getScanKeySchema)
	snapshotTable := &Table{
		KeySchema:              keySchema,
		GlobalSecondaryIndexes: gsiList,
		LocalSecondaryIndexes:  lsiList,
		AttributeDefinitions:   attrDefs,
	}

	pkDef, skDef, projection, err := db.getScanKeySchema(snapshotTable, input)
	if err != nil {
		return nil, err
	}

	if verr := validateSelectConstraints(input.Select, aws.ToString(input.IndexName), projection); verr != nil {
		return nil, verr
	}

	// Process scan outside the lock; pass the table's own key schema separately
	// so that GSI/LSI scans can include the base-table PK in LastEvaluatedKey.
	items, lastKey, scannedCount := db.doScan(
		ctx,
		itemsCopy,
		ttlAttr,
		snapshotTable,
		input,
		pkDef,
		skDef,
		keySchema,
	)

	return db.buildScanOutput(ctx, tableName, billingMode, input, items, lastKey, scannedCount)
}

// buildScanOutput enforces read throughput and assembles the ScanOutput.
func (db *InMemoryDB) buildScanOutput(
	ctx context.Context,
	tableName, billingMode string,
	input *dynamodb.ScanInput,
	items []map[string]any,
	lastKey map[string]any,
	scannedCount int32,
) (*dynamodb.ScanOutput, error) {
	// Enforce throughput: charge RCU per scanned item.
	// Double for strongly-consistent; bypass for PAY_PER_REQUEST.
	n := int(scannedCount) // #nosec G115 -- bounded by len(table.Items) which fits in int
	region := getRegionFromContext(ctx, db)
	consistentRead := aws.ToBool(input.ConsistentRead)
	rcuUnits := applyConsistentReadMultiplier(rcuForCount(n), consistentRead)

	if !isOnDemandTable(billingMode) {
		if err := db.throttler.ConsumeRead(throttleKey(region, tableName), rcuUnits); err != nil {
			return nil, err
		}
	}

	outItems := make([]map[string]types.AttributeValue, len(items))
	for i, it := range items {
		sdkIt, _ := models.ToSDKItem(it)
		outItems[i] = sdkIt
	}

	out := &dynamodb.ScanOutput{
		Items:        outItems,
		Count:        int32(len(items)), // #nosec G115
		ScannedCount: scannedCount,
		ConsumedCapacity: consumedCapacityForScan(
			tableName,
			input.ReturnConsumedCapacity,
			int(scannedCount),
			aws.ToBool(input.ConsistentRead),
		),
	}

	if lastKey != nil {
		sdkKey, _ := models.ToSDKItem(lastKey)
		out.LastEvaluatedKey = sdkKey
	}

	return out, nil
}

func (db *InMemoryDB) getScanKeySchema(
	table *Table,
	input *dynamodb.ScanInput,
) (models.KeySchemaElement, models.KeySchemaElement, *models.Projection, error) {
	indexName := aws.ToString(input.IndexName)
	if indexName == "" {
		pk, sk := getPKAndSK(table.KeySchema)

		return pk, sk, nil, nil
	}

	for _, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName == indexName {
			if aws.ToBool(input.ConsistentRead) {
				return models.KeySchemaElement{}, models.KeySchemaElement{}, nil, NewValidationException(
					"Consistent reads are not supported on global secondary indexes",
				)
			}
			pk, sk := getPKAndSK(gsi.KeySchema)

			return pk, sk, &gsi.Projection, nil
		}
	}

	for _, lsi := range table.LocalSecondaryIndexes {
		if lsi.IndexName == indexName {
			pk, sk := getPKAndSK(lsi.KeySchema)

			return pk, sk, &lsi.Projection, nil
		}
	}

	return models.KeySchemaElement{}, models.KeySchemaElement{}, nil, NewResourceNotFoundException(
		fmt.Sprintf("Index: %s not found", indexName),
	)
}

func (db *InMemoryDB) doScan(
	ctx context.Context,
	items []map[string]any,
	ttlAttr string,
	table *Table,
	input *dynamodb.ScanInput,
	pkDef, skDef models.KeySchemaElement,
	tableKeySchema []models.KeySchemaElement,
) ([]map[string]any, map[string]any, int32) {
	_ = ctx // ctx reserved for future use (e.g., metrics, cancellation)

	eav := models.FromSDKItem(input.ExpressionAttributeValues)
	limit := int(aws.ToInt32(input.Limit))
	proj := aws.ToString(input.ProjectionExpression)
	filter := aws.ToString(input.FilterExpression)

	// Collect all non-expired items that are in the target index.
	candidate := make([]map[string]any, 0, minScanAllocationSize)
	for _, item := range items {
		if isItemExpired(item, ttlAttr) {
			continue
		}
		if isItemInIndex(item, input, pkDef, skDef) {
			candidate = append(candidate, item)
		}
	}

	// Sort candidate set by PK then SK (deterministic ordering for pagination).
	sortScanResults(candidate, pkDef, skDef, table)

	// Apply parallel-scan segment filter (Segment / TotalSegments).
	candidate = applySegmentFilter(candidate, input, pkDef)

	// Apply ExclusiveStartKey: skip items up to and including the start-key item.
	candidate = applyExclusiveStartKey(
		candidate,
		input.ExclusiveStartKey,
		pkDef,
		skDef,
		tableKeySchema,
	)

	projector, _ := ParseProjector(proj, input.ExpressionAttributeNames)

	// Pre-parse the filter expression once to avoid re-parsing per item in the hot loop.
	parsedFilter, _ := ParseConditionStr(filter)

	return scanPage(
		candidate,
		parsedFilter,
		eav,
		input.ExpressionAttributeNames,
		projector,
		pkDef,
		skDef,
		tableKeySchema,
		limit,
	)
}

// scanPage iterates candidate items up to 1MB or limit, applying filter and projection.
// tableKeySchema is the base-table primary key schema; when scanning a GSI/LSI it is used
// to include the table PK in LastEvaluatedKey so pagination tokens are unambiguous.
// parsedFilter is a pre-parsed filter expression (nil = no filter).
func scanPage(
	candidate []map[string]any,
	parsedFilter *ParsedCondition,
	eav map[string]any,
	eans map[string]string,
	projector *Projector,
	pkDef, skDef models.KeySchemaElement,
	tableKeySchema []models.KeySchemaElement,
	limit int,
) ([]map[string]any, map[string]any, int32) {
	const maxResponseSize = 1024 * 1024 // 1MB
	results := make([]map[string]any, 0)
	var lastKey map[string]any
	scannedCount := int32(0)
	totalScannedSize := 0

	for i, item := range candidate {
		scannedCount++
		itemSize, _ := CalculateItemSize(item)

		// AWS scans up to 1MB of data before applying FilterExpression and returning.
		if totalScannedSize+itemSize > maxResponseSize && i > 0 {
			scannedCount--
			lastKey = buildLastKey(candidate[i-1], pkDef, skDef, tableKeySchema)

			break
		}
		totalScannedSize += itemSize

		if parsedFilter.Evaluate(item, eav, eans) {
			results = append(results, projector.Project(item))
		}

		if limit > 0 && int(scannedCount) >= limit {
			if i < len(candidate)-1 {
				lastKey = buildLastKey(item, pkDef, skDef, tableKeySchema)
			}

			break
		}

		if totalScannedSize >= maxResponseSize && i < len(candidate)-1 {
			lastKey = buildLastKey(item, pkDef, skDef, tableKeySchema)

			break
		}
	}

	return results, lastKey, scannedCount
}

// buildLastKey creates a LastEvaluatedKey map for the given item.
// tableKeySchema is the base-table primary key schema; when scanning a GSI/LSI
// it is merged in so that the token includes both the index keys and the table
// PK, matching AWS DynamoDB's pagination behaviour.
func buildLastKey(
	item map[string]any,
	pkDef, skDef models.KeySchemaElement,
	tableKeySchema []models.KeySchemaElement,
) map[string]any {
	indexSchema := []models.KeySchemaElement{pkDef}
	if skDef.AttributeName != "" {
		indexSchema = append(indexSchema, skDef)
	}

	return extractKeyWithBase(item, indexSchema, tableKeySchema)
}

// applySegmentFilter partitions items by parallel scan segment using FNV hash on PK.
func applySegmentFilter(
	candidate []map[string]any,
	input *dynamodb.ScanInput,
	pkDef models.KeySchemaElement,
) []map[string]any {
	totalSegments := int(aws.ToInt32(input.TotalSegments))
	if totalSegments <= 1 {
		return candidate
	}

	segment := int(aws.ToInt32(input.Segment))
	filtered := candidate[:0]

	for _, item := range candidate {
		pkVal := fmt.Sprintf("%v", dynamoattr.UnwrapAttributeValue(item[pkDef.AttributeName]))
		h := fnv.New32a()
		_, _ = h.Write([]byte(pkVal))
		if int(h.Sum32())%totalSegments == segment {
			filtered = append(filtered, item)
		}
	}

	return filtered
}

func sortScanResults(
	items []map[string]any,
	pkDef, skDef models.KeySchemaElement,
	table *Table,
) {
	pkType := getAttributeType(table.AttributeDefinitions, pkDef.AttributeName, "S")
	var skType string
	if skDef.AttributeName != "" {
		skType = getAttributeType(table.AttributeDefinitions, skDef.AttributeName, "S")
	}

	sort.Slice(items, func(i, j int) bool {
		v1pk := dynamoattr.UnwrapAttributeValue(items[i][pkDef.AttributeName])
		v2pk := dynamoattr.UnwrapAttributeValue(items[j][pkDef.AttributeName])
		pkRes := compareAny(v1pk, v2pk, pkType)
		if pkRes != 0 {
			return pkRes < 0
		}

		if skDef.AttributeName != "" {
			v1sk := dynamoattr.UnwrapAttributeValue(items[i][skDef.AttributeName])
			v2sk := dynamoattr.UnwrapAttributeValue(items[j][skDef.AttributeName])
			skRes := compareAny(v1sk, v2sk, skType)

			return skRes < 0
		}

		return false
	})
}

// isItemInIndex reports whether item should be included in the scan based solely
// on index membership (i.e. whether the item has the required index keys).
// FilterExpression is intentionally NOT evaluated here so that Limit applies
// before filtering, matching real DynamoDB semantics.
func isItemInIndex(
	item map[string]any,
	input *dynamodb.ScanInput,
	pkDef, skDef models.KeySchemaElement,
) bool {
	indexName := aws.ToString(input.IndexName)

	// If it's a GSI scan, item MUST have the GSI's PK (and SK if defined)
	if indexName != "" {
		if _, ok := item[pkDef.AttributeName]; !ok {
			return false
		}
		if skDef.AttributeName != "" {
			if _, ok := item[skDef.AttributeName]; !ok {
				return false
			}
		}
	}

	return true
}

func applyExclusiveStartKey(
	candidate []map[string]any,
	exclusiveStartKey map[string]types.AttributeValue,
	pkDef, skDef models.KeySchemaElement,
	tableKeySchema []models.KeySchemaElement,
) []map[string]any {
	if len(exclusiveStartKey) == 0 {
		return candidate
	}

	startKey := models.FromSDKItem(exclusiveStartKey)
	tablePKDef, tableSKDef := getPKAndSK(tableKeySchema)

	for i, item := range candidate {
		if itemMatchesStartKeyMap(item, startKey, pkDef, skDef, tablePKDef, tableSKDef) {
			return candidate[i+1:]
		}
	}

	return candidate
}
