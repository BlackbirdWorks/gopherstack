package dynamodb

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// consumedCapacityForScan returns a populated ConsumedCapacity when the caller

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

	if err := validateProjectionParams(
		aws.ToString(input.ProjectionExpression), input.AttributesToGet,
	); err != nil {
		return nil, err
	}

	if err := applyLegacyScanParams(input); err != nil {
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
	itemsCopy, ttlAttr, keySchema, gsiList, lsiList, attrDefs, billingMode := snapshotTableForScan(table)

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

	if verr := validateSelectConstraints(
		input.Select, aws.ToString(input.IndexName), projection,
		aws.ToString(input.ProjectionExpression), input.AttributesToGet,
	); verr != nil {
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
		projection,
	)

	return db.buildScanOutput(ctx, tableName, billingMode, input, items, lastKey, scannedCount, snapshotTable)
}

// snapshotTableForScan copies the item slice under a single table.mu.RLock/defer,
// then releases the lock -- the caller does the actual scan work (which can be O(items))
// unlocked. Table schema definitions are immutable and shared directly without copying.
func snapshotTableForScan(table *Table) (
	[]map[string]any,
	string,
	[]models.KeySchemaElement,
	[]models.GlobalSecondaryIndex,
	[]models.LocalSecondaryIndex,
	[]models.AttributeDefinition,
	string,
) {
	table.mu.RLock("Scan")
	defer table.mu.RUnlock()

	itemsCopy := make([]map[string]any, len(table.Items))
	copy(itemsCopy, table.Items)

	return itemsCopy,
		table.TTLAttribute,
		table.KeySchema,
		table.GlobalSecondaryIndexes,
		table.LocalSecondaryIndexes,
		table.AttributeDefinitions,
		table.BillingMode
}

// buildScanOutput enforces read throughput and assembles the ScanOutput.
func (db *InMemoryDB) buildScanOutput(
	ctx context.Context,
	tableName, billingMode string,
	input *dynamodb.ScanInput,
	items []map[string]any,
	lastKey map[string]any,
	scannedCount int32,
	table *Table,
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

	// AWS omits Items entirely when Select=COUNT: "Returns the number of matching
	// items, rather than the matching items themselves." Count/ScannedCount still
	// reflect the matched/scanned totals.
	var outItems []map[string]types.AttributeValue
	if input.Select != types.SelectCount {
		outItems = make([]map[string]types.AttributeValue, len(items))
		for i, it := range items {
			sdkIt, _ := models.ToSDKItem(it)
			outItems[i] = sdkIt
		}
	}

	out := &dynamodb.ScanOutput{
		Items:        outItems,
		Count:        int32(len(items)), // #nosec G115
		ScannedCount: scannedCount,
		ConsumedCapacity: consumedCapacityForReadOp(
			tableName,
			input.ReturnConsumedCapacity,
			int(scannedCount),
			aws.ToBool(input.ConsistentRead),
			aws.ToString(input.IndexName),
			table,
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
	projection *models.Projection,
) ([]map[string]any, map[string]any, int32) {
	_ = ctx // ctx reserved for future use (e.g., metrics, cancellation)

	eav := models.FromSDKItem(input.ExpressionAttributeValues)
	limit := int(aws.ToInt32(input.Limit))
	proj := resolveProjection(aws.ToString(input.ProjectionExpression), input.AttributesToGet)
	filter := aws.ToString(input.FilterExpression)

	// Collect all non-expired items that are in the target index.
	candidate := make([]map[string]any, 0, len(items))
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

	indexKeySchema := []models.KeySchemaElement{pkDef}
	if skDef.AttributeName != "" {
		indexKeySchema = append(indexKeySchema, skDef)
	}

	return scanPage(
		candidate,
		parsedFilter,
		eav,
		input.ExpressionAttributeNames,
		projector,
		pkDef,
		skDef,
		tableKeySchema,
		indexKeySchema,
		projection,
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
	tableKeySchema, indexKeySchema []models.KeySchemaElement,
	projection *models.Projection,
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
			projectedItem := item
			if projection != nil {
				projectedItem = applyIndexProjection(item, *projection, tableKeySchema, indexKeySchema)
			}
			results = append(results, projector.Project(projectedItem))
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
		pkVal := BuildKeyString(item, pkDef.AttributeName)
		if int(httputils.FNV32a(pkVal))%totalSegments == segment {
			filtered = append(filtered, item)
		}
	}

	return filtered
}

type scanSortEntry struct {
	item  map[string]any
	pkStr string
	skStr string
	pkNum float64
	skNum float64
}

func populateScanSortEntry(
	item map[string]any,
	pkDef, skDef models.KeySchemaElement,
	pkType, skType string,
) scanSortEntry {
	entry := scanSortEntry{item: item}
	if pkVal, ok := item[pkDef.AttributeName]; ok {
		unwrapped := dynamoattr.UnwrapAttributeValue(pkVal)
		if pkType == "N" {
			entry.pkNum, _ = dynamoattr.ParseNumeric(unwrapped)
		} else {
			entry.pkStr = dynamoattr.ToString(unwrapped)
		}
	}
	if skDef.AttributeName != "" {
		if skVal, ok := item[skDef.AttributeName]; ok {
			unwrapped := dynamoattr.UnwrapAttributeValue(skVal)
			if skType == "N" {
				entry.skNum, _ = dynamoattr.ParseNumeric(unwrapped)
			} else {
				entry.skStr = dynamoattr.ToString(unwrapped)
			}
		}
	}

	return entry
}

func compareScanSortEntries(a, b scanSortEntry, pkType, skType string, hasSK bool) int {
	var pkRes int
	if pkType == "N" {
		pkRes = cmp.Compare(a.pkNum, b.pkNum)
	} else {
		pkRes = cmp.Compare(a.pkStr, b.pkStr)
	}
	if pkRes != 0 {
		return pkRes
	}

	if hasSK {
		if skType == "N" {
			return cmp.Compare(a.skNum, b.skNum)
		}

		return cmp.Compare(a.skStr, b.skStr)
	}

	return 0
}

func sortScanResults(
	items []map[string]any,
	pkDef, skDef models.KeySchemaElement,
	table *Table,
) {
	if len(items) <= 1 {
		return
	}

	pkType := getAttributeType(table.AttributeDefinitions, pkDef.AttributeName, "S")
	var skType string
	if skDef.AttributeName != "" {
		skType = getAttributeType(table.AttributeDefinitions, skDef.AttributeName, "S")
	}

	entries := make([]scanSortEntry, len(items))
	for i, item := range items {
		entries[i] = populateScanSortEntry(item, pkDef, skDef, pkType, skType)
	}

	hasSK := skDef.AttributeName != ""
	slices.SortFunc(entries, func(a, b scanSortEntry) int {
		return compareScanSortEntries(a, b, pkType, skType, hasSK)
	})

	for i := range entries {
		items[i] = entries[i].item
	}
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

// maxParallelScanSegments is the upper bound on TotalSegments for parallel Scan.
const maxParallelScanSegments = 1_000_000

// validateScanSegment returns a ValidationException when Segment or TotalSegments
// are out of range. AWS requires: 0 ≤ Segment < TotalSegments, 1 ≤ TotalSegments ≤ 1_000_000.
func validateScanSegment(segment, totalSegments int32) error {
	if totalSegments < 1 || totalSegments > maxParallelScanSegments {
		return NewValidationException(
			fmt.Sprintf(
				"TotalSegments must be between 1 and %d, got %d",
				maxParallelScanSegments, totalSegments,
			),
		)
	}

	if segment < 0 || segment >= totalSegments {
		return NewValidationException(
			fmt.Sprintf(
				"Segment must be between 0 and TotalSegments-1 (%d), got %d",
				totalSegments-1, segment,
			),
		)
	}

	return nil
}
