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
	return db.observedScan(ctx, input)
}

func (db *InMemoryDB) ScanWithContext(
	ctx context.Context,
	input *dynamodb.ScanInput,
) (*dynamodb.ScanOutput, error) {
	res, err := db.scanCore(ctx, input)
	if err != nil {
		return nil, err
	}

	return res.toScanOutput(), nil
}

func (db *InMemoryDB) scanCore(
	ctx context.Context,
	input *dynamodb.ScanInput,
) (*pageResult, error) {
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
	presorted := aws.ToString(input.IndexName) == ""
	snap := snapshotTableForScan(table, presorted)

	// Get key schema definitions (reconstruct the table temporarily for getScanKeySchema)
	snapshotTable := &Table{
		KeySchema:              snap.keySchema,
		GlobalSecondaryIndexes: snap.gsiList,
		LocalSecondaryIndexes:  snap.lsiList,
		AttributeDefinitions:   snap.attrDefs,
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

	if presorted && !snap.sorted {
		snap.items, snap.sizes = table.storeScanOrder(snap, pkDef, skDef)
		snap.sorted = true
	}

	// Process scan outside the lock; pass the table's own key schema separately
	// so that GSI/LSI scans can include the base-table PK in LastEvaluatedKey.
	items, lastKey, scannedCount, err := db.doScan(
		ctx,
		snap,
		snapshotTable,
		input,
		pkDef,
		skDef,
		projection,
		presorted,
	)
	if err != nil {
		return nil, err
	}

	return db.buildScanOutput(ctx, tableName, snap.billingMode, input, items, lastKey, scannedCount, snapshotTable)
}

// scanView is a table's scan inputs captured under one read lock. When sorted is
// true, items is the shared key-ordered cache (read-only) and sizes is aligned with it.
type scanView struct {
	ttlAttr     string
	billingMode string
	items       []map[string]any
	sizes       []int
	keySchema   []models.KeySchemaElement
	gsiList     []models.GlobalSecondaryIndex
	lsiList     []models.LocalSecondaryIndex
	attrDefs    []models.AttributeDefinition
	version     uint64
	sorted      bool
}

// snapshotTableForScan captures scan state under one read lock. A valid sorted cache is
// reused as-is, so a paged Scan copies nothing.
func snapshotTableForScan(table *Table, wantSorted bool) scanView {
	table.mu.RLock("Scan")
	defer table.mu.RUnlock()

	snap := scanView{
		keySchema:   table.KeySchema,
		gsiList:     table.GlobalSecondaryIndexes,
		lsiList:     table.LocalSecondaryIndexes,
		attrDefs:    table.AttributeDefinitions,
		ttlAttr:     table.TTLAttribute,
		billingMode: table.BillingMode,
		version:     table.itemsVersion,
	}

	if wantSorted {
		if c := table.scanOrder.Load(); c != nil && c.version == table.itemsVersion {
			snap.items, snap.sizes, snap.sorted = c.items, c.sizes, true

			return snap
		}

		if len(table.itemSizes) == len(table.Items) {
			snap.sizes = slices.Clone(table.itemSizes)
		}
	}

	snap.items = slices.Clone(table.Items)

	return snap
}

// scanOrderCache is the key-sorted item order of a table at one itemsVersion.
type scanOrderCache struct {
	items   []map[string]any
	sizes   []int
	version uint64
}

// itemsChanged invalidates the cached scan order; call it under table.mu.Lock
// before any mutation of t.Items.
func (t *Table) itemsChanged() {
	t.itemsVersion++
}

// storeScanOrder sorts the snapshot into base-table key order (with sizes alongside)
// and caches it at the snapshot's version. The result is shared and read-only.
func (t *Table) storeScanOrder(
	snap scanView,
	pkDef, skDef models.KeySchemaElement,
) ([]map[string]any, []int) {
	items, sizes := snap.items, snap.sizes
	sortScanResultsSized(items, sizes, pkDef, skDef, &Table{AttributeDefinitions: snap.attrDefs})
	t.scanOrder.Store(&scanOrderCache{items: items, sizes: sizes, version: snap.version})

	return items, sizes
}

// buildScanOutput enforces read throughput and assembles the scan page.
func (db *InMemoryDB) buildScanOutput(
	ctx context.Context,
	tableName, billingMode string,
	input *dynamodb.ScanInput,
	items []map[string]any,
	lastKey map[string]any,
	scannedCount int32,
	table *Table,
) (*pageResult, error) {
	// Enforce throughput: charge RCU per scanned item.
	// Double for strongly-consistent; bypass for PAY_PER_REQUEST.
	n := int(scannedCount) // #nosec G115 -- bounded by len(table.Items) which fits in int
	region := getRegionFromContext(ctx, db)
	consistentRead := aws.ToBool(input.ConsistentRead)
	rcuUnits := applyConsistentReadMultiplier(rcuForCount(n), consistentRead)

	db.emitRCU(region, tableName, rcuUnits)
	db.emitIndexRCU(region, tableName, table, aws.ToString(input.IndexName), rcuUnits)

	if !isOnDemandTable(billingMode) {
		if err := db.throttler.ConsumeRead(throttleKey(region, tableName), rcuUnits); err != nil {
			return nil, err
		}
	}

	res := &pageResult{
		lastKey:      lastKey,
		count:        int32(len(items)), // #nosec G115
		scannedCount: scannedCount,
		consumed: consumedCapacityForReadOp(
			tableName,
			input.ReturnConsumedCapacity,
			int(scannedCount),
			aws.ToBool(input.ConsistentRead),
			aws.ToString(input.IndexName),
			table,
		),
	}

	// AWS omits Items entirely for Select=COUNT; Count/ScannedCount still reflect totals.
	if input.Select != types.SelectCount {
		res.items = items
		if res.items == nil {
			res.items = []map[string]any{}
		}
	}

	return res, nil
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
	snap scanView,
	table *Table,
	input *dynamodb.ScanInput,
	pkDef, skDef models.KeySchemaElement,
	projection *models.Projection,
	presorted bool,
) ([]map[string]any, map[string]any, int32, error) {
	_ = ctx // ctx reserved for future use (e.g., metrics, cancellation)

	tableKeySchema := snap.keySchema
	eav := models.FromSDKItem(input.ExpressionAttributeValues)
	limit := int(aws.ToInt32(input.Limit))
	proj, atgNames := resolveProjection(aws.ToString(input.ProjectionExpression), input.AttributesToGet)
	filter := aws.ToString(input.FilterExpression)

	candidate, sizes := db.scanCandidates(snap, table, input, pkDef, skDef, presorted)

	projector, err := ParseProjector(proj, mergeAttrNames(input.ExpressionAttributeNames, atgNames))
	if err != nil {
		return nil, nil, 0, NewValidationException("Invalid ProjectionExpression: " + err.Error())
	}

	if filter != "" {
		if undefErr := checkUndefinedExpressionAttributeNames(
			input.ExpressionAttributeNames, "FilterExpression", filter,
		); undefErr != nil {
			return nil, nil, 0, undefErr
		}
		if undefErr := checkUndefinedExpressionAttributeValues(eav, "FilterExpression", filter); undefErr != nil {
			return nil, nil, 0, undefErr
		}
	}

	// Pre-parse the filter expression once to avoid re-parsing per item in the hot loop.
	parsedFilter, err := ParseConditionStr(filter)
	if err != nil {
		return nil, nil, 0, NewValidationException("Invalid FilterExpression: " + err.Error())
	}

	indexKeySchema := []models.KeySchemaElement{pkDef}
	if skDef.AttributeName != "" {
		indexKeySchema = append(indexKeySchema, skDef)
	}

	results, lastKey, scannedCount := scanPage(
		candidate,
		sizes,
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

	return results, lastKey, scannedCount, nil
}

// scanCandidates picks the items this Scan walks; an unfiltered base-table scan reuses the
// shared sorted cache after ExclusiveStartKey, so it costs O(log n + page). sizes may be nil.
func (db *InMemoryDB) scanCandidates(
	snap scanView,
	table *Table,
	input *dynamodb.ScanInput,
	pkDef, skDef models.KeySchemaElement,
	presorted bool,
) ([]map[string]any, []int) {
	if presorted && snap.ttlAttr == "" && aws.ToInt32(input.TotalSegments) <= 1 {
		start := locateAfterStartKey(snap, input.ExclusiveStartKey, pkDef, skDef)
		if snap.sizes != nil {
			return snap.items[start:], snap.sizes[start:]
		}

		return snap.items[start:], nil
	}

	// Collect all non-expired items that are in the target index.
	candidate := make([]map[string]any, 0, len(snap.items))
	for _, item := range snap.items {
		if isItemExpired(item, snap.ttlAttr) {
			continue
		}
		if isItemInIndex(item, input, pkDef, skDef) {
			candidate = append(candidate, item)
		}
	}

	// Sort candidate set by PK then SK (deterministic ordering for pagination).
	if !presorted {
		sortScanResults(candidate, pkDef, skDef, table)
	}

	// Apply parallel-scan segment filter (Segment / TotalSegments).
	candidate = applySegmentFilter(candidate, input, pkDef)

	// Apply ExclusiveStartKey: skip items up to and including the start-key item.
	candidate = applyExclusiveStartKey(
		candidate,
		input.ExclusiveStartKey,
		pkDef,
		skDef,
		snap.keySchema,
	)

	return candidate, nil
}

// locateAfterStartKey returns the index just past the ExclusiveStartKey item in the sorted
// snapshot (0 when absent or unmatched), using binary search with a linear fallback.
func locateAfterStartKey(
	snap scanView,
	startKey map[string]types.AttributeValue,
	pkDef, skDef models.KeySchemaElement,
) int {
	if len(startKey) == 0 {
		return 0
	}

	wireKey := models.FromSDKItem(startKey)
	tablePKDef, tableSKDef := getPKAndSK(snap.keySchema)
	pkType := getAttributeType(snap.attrDefs, pkDef.AttributeName, "S")

	var skType string
	if skDef.AttributeName != "" {
		skType = getAttributeType(snap.attrDefs, skDef.AttributeName, "S")
	}

	hasSK := skDef.AttributeName != ""
	target := populateScanSortEntry(wireKey, pkDef, skDef, pkType, skType)

	idx, _ := slices.BinarySearchFunc(snap.items, target, func(item map[string]any, t scanSortEntry) int {
		return compareScanSortEntries(
			populateScanSortEntry(item, pkDef, skDef, pkType, skType),
			t,
			pkType,
			skType,
			hasSK,
		)
	})

	matches := func(i int) bool {
		return i >= 0 && i < len(snap.items) &&
			itemMatchesStartKeyMap(snap.items[i], wireKey, pkDef, skDef, tablePKDef, tableSKDef)
	}

	if matches(idx) && !matches(idx-1) {
		return idx + 1
	}

	rest := applyExclusiveStartKey(snap.items, startKey, pkDef, skDef, snap.keySchema)

	return len(snap.items) - len(rest)
}

// scanPage iterates candidate items up to 1MB or limit, applying filter and projection.
// tableKeySchema is the base-table primary key schema; when scanning a GSI/LSI it is used
// to include the table PK in LastEvaluatedKey so pagination tokens are unambiguous.
// parsedFilter is a pre-parsed filter expression (nil = no filter).
func scanPage(
	candidate []map[string]any,
	sizes []int,
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

		itemSize := scanItemSize(sizes, i, item)

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

// scanItemSize returns the precomputed size at i when available, else computes it.
func scanItemSize(sizes []int, i int, item map[string]any) int {
	if sizes != nil {
		return sizes[i]
	}

	size, _ := CalculateItemSize(item)

	return size
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
	size  int
}

func populateScanSortEntry(
	item map[string]any,
	pkDef, skDef models.KeySchemaElement,
	pkType, skType string,
) scanSortEntry {
	// ParseNumeric/ToString already unwrap internally; unwrapping here too
	// doubled the cost of every sort (profiled hot path for large scans).
	entry := scanSortEntry{item: item}
	if pkVal, ok := item[pkDef.AttributeName]; ok {
		if pkType == "N" {
			entry.pkNum, _ = dynamoattr.ParseNumeric(pkVal)
		} else {
			entry.pkStr = dynamoattr.ToString(pkVal)
		}
	}
	if skDef.AttributeName != "" {
		if skVal, ok := item[skDef.AttributeName]; ok {
			if skType == "N" {
				entry.skNum, _ = dynamoattr.ParseNumeric(skVal)
			} else {
				entry.skStr = dynamoattr.ToString(skVal)
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
	sortScanResultsSized(items, nil, pkDef, skDef, table)
}

// sortScanResultsSized sorts items by key; when sizes is non-nil it is permuted alongside.
func sortScanResultsSized(
	items []map[string]any,
	sizes []int,
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
		if sizes != nil {
			entries[i].size = sizes[i]
		}
	}

	hasSK := skDef.AttributeName != ""
	slices.SortFunc(entries, func(a, b scanSortEntry) int {
		return compareScanSortEntries(a, b, pkType, skType, hasSK)
	})

	for i := range entries {
		items[i] = entries[i].item
		if sizes != nil {
			sizes[i] = entries[i].size
		}
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
