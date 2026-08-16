package dynamodb

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// batchWriteResponseLimit is the simulated 16 MB response size limit for BatchWriteItem.
const batchWriteResponseLimit = 16 * 1024 * 1024

// eventuallyConsistentRCU is the RCU cost per read for eventually-consistent reads (0.5 per 4KB).
const eventuallyConsistentRCU = 0.5

// canonicalKey produces a stable, comparable string for a single item key map.
// Keys only ever contain scalar key attributes (S, N, or B), so we serialize the
// sorted attribute names together with their typed scalar value.
func canonicalKey(key map[string]types.AttributeValue) string {
	names := collections.SortedKeys(key)

	var sb strings.Builder
	for _, name := range names {
		sb.WriteString(name)
		sb.WriteByte('=')

		switch v := key[name].(type) {
		case *types.AttributeValueMemberS:
			sb.WriteString("S:")
			sb.WriteString(v.Value)
		case *types.AttributeValueMemberN:
			sb.WriteString("N:")
			sb.WriteString(v.Value)
		case *types.AttributeValueMemberB:
			sb.WriteString("B:")
			sb.Write(v.Value)
		}

		sb.WriteByte('\x00')
	}

	return sb.String()
}

// validateNoDuplicateBatchKeys returns a ValidationException when a table's Keys list
// contains the same key more than once, matching AWS BatchGetItem behaviour.
func validateNoDuplicateBatchKeys(keys []map[string]types.AttributeValue) error {
	seen := make(map[string]struct{}, len(keys))

	for _, key := range keys {
		canon := canonicalKey(key)
		if _, dup := seen[canon]; dup {
			return NewValidationException(
				"Provided list of item keys contains duplicates",
			)
		}

		seen[canon] = struct{}{}
	}

	return nil
}

func (db *InMemoryDB) BatchGetItem(
	ctx context.Context,
	input *dynamodb.BatchGetItemInput,
) (*dynamodb.BatchGetItemOutput, error) {
	// Validate non-empty request (AWS rejects an empty RequestItems map).
	if len(input.RequestItems) == 0 {
		return nil, NewValidationException("No operations specified in the request")
	}

	// Validate per-table keys and projection params (no lock needed — inspects input only).
	for tableName, keysAndAttrs := range input.RequestItems {
		if len(keysAndAttrs.Keys) == 0 {
			return nil, NewValidationException(
				fmt.Sprintf("No operations specified for table: %s", tableName),
			)
		}

		projExpr := aws.ToString(keysAndAttrs.ProjectionExpression)
		if err := validateProjectionParams(projExpr, keysAndAttrs.AttributesToGet); err != nil {
			return nil, err
		}

		// AWS rejects a per-table Keys list that contains duplicate keys with a
		// ValidationException rather than returning the matching item twice.
		if err := validateNoDuplicateBatchKeys(keysAndAttrs.Keys); err != nil {
			return nil, err
		}
	}

	// Validate size limit (no lock needed — only inspects input).
	const batchSizeLimit = 100
	totalItems := 0
	for _, keysAndAttrs := range input.RequestItems {
		totalItems += len(keysAndAttrs.Keys)
	}
	if totalItems > batchSizeLimit {
		return nil, NewValidationException(
			fmt.Sprintf("Batch size limit exceeded: Max %d items per request", batchSizeLimit),
		)
	}

	// Collect table references under db.mu.RLock.
	tableRefs, tableErr := db.batchGetTableRefs(ctx, input.RequestItems)
	if tableErr != nil {
		return nil, tableErr
	}

	// Enforce throughput per table, same RCU formula as batchGetConsumedCapacity,
	// before reading any items. PAY_PER_REQUEST tables bypass throttling.
	region := getRegionFromContext(ctx, db)
	if thrErr := db.enforceBatchGetThroughput(region, input.RequestItems, tableRefs); thrErr != nil {
		return nil, thrErr
	}

	// Sort table names for deterministic processing (AWS also tends toward this).
	tableNames := collections.SortedKeys(input.RequestItems)

	return db.batchGetResponses(input, tableNames, tableRefs)
}

// enforceBatchGetThroughput charges each requested table's RCU bucket before any
// items are read, mirroring PutItem/GetItem/Query/Scan. Real DynamoDB returns
// ProvisionedThroughputExceededException from BatchGetItem exactly as it does from
// GetItem; without this, BatchGetItem silently bypassed throttling that every other
// read path enforces.
func (db *InMemoryDB) enforceBatchGetThroughput(
	region string,
	requestItems map[string]types.KeysAndAttributes,
	tables map[string]*Table,
) error {
	for tableName, keysAndAttrs := range requestItems {
		table := tables[tableName]

		table.mu.RLock("BatchGetItem.throttle")
		billingMode := table.BillingMode
		table.mu.RUnlock()

		if isOnDemandTable(billingMode) {
			continue
		}

		rcuPerKey := eventuallyConsistentRCU
		if aws.ToBool(keysAndAttrs.ConsistentRead) {
			rcuPerKey = 1.0
		}
		cu := float64(len(keysAndAttrs.Keys)) * rcuPerKey
		if cu < rcuPerKey {
			cu = rcuPerKey
		}

		if err := db.throttler.ConsumeRead(throttleKey(region, tableName), cu); err != nil {
			return err
		}
	}

	return nil
}

// batchGetResponses collects items across tables enforcing the 16MB response limit.
// Size is computed on the projected item so projection reduces the counted bytes.
func (db *InMemoryDB) batchGetResponses(
	input *dynamodb.BatchGetItemInput,
	tableNames []string,
	tableRefs map[string]*Table,
) (*dynamodb.BatchGetItemOutput, error) {
	const responseSizeLimit = 16 * 1024 * 1024
	currentSize := 0
	responses := make(map[string][]map[string]types.AttributeValue)
	unprocessedKeys := make(map[string]types.KeysAndAttributes)

	for _, tableName := range tableNames {
		keysAndAttrs := input.RequestItems[tableName]
		table := tableRefs[tableName]

		truncated, tableResults := db.batchGetTable(
			table,
			keysAndAttrs,
			tableName,
			&currentSize,
			responseSizeLimit,
			unprocessedKeys,
		)
		// Always include the table in Responses even when all keys miss — AWS returns
		// an empty list for zero-hit tables rather than omitting the key entirely.
		if tableResults == nil {
			tableResults = []map[string]types.AttributeValue{}
		}
		responses[tableName] = tableResults

		if truncated {
			for j := sort.SearchStrings(tableNames, tableName) + 1; j < len(tableNames); j++ {
				nextTable := tableNames[j]
				unprocessedKeys[nextTable] = input.RequestItems[nextTable]
			}

			break
		}
	}

	return &dynamodb.BatchGetItemOutput{
		Responses:       responses,
		UnprocessedKeys: unprocessedKeys,
		ConsumedCapacity: batchGetConsumedCapacity(
			input.ReturnConsumedCapacity,
			input.RequestItems,
		),
	}, nil
}

// batchGetTable reads items for a single table, enforcing the cumulative size limit.
// Per AWS semantics, the ProjectionExpression is applied before measuring item size —
// this means only the projected bytes count toward the 16MB response limit, not the
// full raw item. Returns (truncated, results) where truncated means the 16MB limit was reached.
func (db *InMemoryDB) batchGetTable(
	table *Table,
	keysAndAttrs types.KeysAndAttributes,
	tableName string,
	currentSize *int,
	responseSizeLimit int,
	unprocessedKeys map[string]types.KeysAndAttributes,
) (bool, []map[string]types.AttributeValue) {
	pkDef, skDef := getPKAndSK(table.KeySchema)
	proj := resolveProjection(
		aws.ToString(keysAndAttrs.ProjectionExpression),
		keysAndAttrs.AttributesToGet,
	)
	projector, _ := ParseProjector(proj, keysAndAttrs.ExpressionAttributeNames)

	wireKeys := make([]map[string]any, len(keysAndAttrs.Keys))
	for i, sdkKey := range keysAndAttrs.Keys {
		wireKeys[i] = models.FromSDKItem(sdkKey)
	}

	type matchedEntry struct {
		item     map[string]any
		keyIndex int
	}
	matched := make([]matchedEntry, 0, len(keysAndAttrs.Keys))

	func() {
		table.mu.RLock("BatchGetItem")
		defer table.mu.RUnlock()

		for i, wireKey := range wireKeys {
			item := db.lookupItem(table, wireKey, pkDef.AttributeName, skDef.AttributeName)
			if item != nil {
				matched = append(matched, matchedEntry{keyIndex: i, item: item})
			}
		}
	}()

	var tableResults []map[string]types.AttributeValue

	for _, m := range matched {
		// Project first, then measure size of the projected result (per AWS semantics).
		result := projector.Project(m.item)
		itemSize, _ := CalculateItemSize(result)

		if *currentSize+itemSize > responseSizeLimit && len(tableResults) > 0 {
			unprocessedKeys[tableName] = types.KeysAndAttributes{
				Keys:                     keysAndAttrs.Keys[m.keyIndex:],
				AttributesToGet:          keysAndAttrs.AttributesToGet,
				ConsistentRead:           keysAndAttrs.ConsistentRead,
				ExpressionAttributeNames: keysAndAttrs.ExpressionAttributeNames,
				ProjectionExpression:     keysAndAttrs.ProjectionExpression,
			}

			return true, tableResults
		}

		*currentSize += itemSize

		sdkResult, _ := models.ToSDKItem(result)
		tableResults = append(tableResults, sdkResult)
	}

	return false, tableResults
}

// batchGetTableRefs collects table references under db.mu.RLock.
func (db *InMemoryDB) batchGetTableRefs(
	ctx context.Context,
	requestItems map[string]types.KeysAndAttributes,
) (map[string]*Table, error) {
	db.mu.RLock("BatchGetItem")
	defer db.mu.RUnlock()

	region := getRegionFromContext(ctx, db)
	if len(db.tablesByRegion.Get(region)) == 0 {
		// Region might not have tables yet
		return nil, NewResourceNotFoundException("No tables found in region")
	}

	tableRefs := make(map[string]*Table, len(requestItems))

	for tableName := range requestItems {
		t, exists := db.tables.Get(tableKey(region, tableName))
		if !exists {
			return nil, NewResourceNotFoundException("Table not found: " + tableName)
		}

		tableRefs[tableName] = t
	}

	return tableRefs, nil
}

func batchGetConsumedCapacity(
	req types.ReturnConsumedCapacity,
	requestItems map[string]types.KeysAndAttributes,
) []types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}

	// Capacity is charged per requested key, not per returned item (missing items still consume RCU).
	// Strongly-consistent reads (ConsistentRead=true) cost 2× the eventually-consistent rate.
	caps := make([]types.ConsumedCapacity, 0, len(requestItems))
	for tableName, keysAndAttrs := range requestItems {
		rcuPerKey := eventuallyConsistentRCU
		if aws.ToBool(keysAndAttrs.ConsistentRead) {
			rcuPerKey = 1.0
		}
		cu := float64(len(keysAndAttrs.Keys)) * rcuPerKey
		if cu < rcuPerKey {
			cu = rcuPerKey
		}
		caps = append(caps, types.ConsumedCapacity{
			TableName:         aws.String(tableName),
			CapacityUnits:     aws.Float64(cu),
			ReadCapacityUnits: aws.Float64(cu),
		})
	}

	return caps
}

func validateAllBatchWriteRequests(
	requestItems map[string][]types.WriteRequest,
	tables map[string]*Table,
) error {
	for tableName, requests := range requestItems {
		table := tables[tableName]
		for _, req := range requests {
			if err := validateBatchWriteRequest(req, table); err != nil {
				return err
			}
		}

		// AWS rejects a BatchWriteItem whose per-table request list targets the same
		// item (by primary key) more than once — whether via two PutRequests, two
		// DeleteRequests, or a Put+Delete pair — since the outcome would be
		// order-dependent and undefined.
		if err := validateNoDuplicateBatchWriteKeys(requests, table); err != nil {
			return err
		}
	}

	return nil
}

// validateNoDuplicateBatchWriteKeys returns a ValidationException when requests
// (all WriteRequests targeting a single table) reference the same primary key more
// than once. Mirrors AWS's real BatchWriteItem behaviour, which rejects such
// requests wholesale rather than silently picking a winner.
func validateNoDuplicateBatchWriteKeys(requests []types.WriteRequest, table *Table) error {
	pkDef, skDef := getPKAndSK(table.KeySchema)
	seen := make(map[string]struct{}, len(requests))

	for _, req := range requests {
		var wireItem map[string]any

		switch {
		case req.PutRequest != nil:
			wireItem = models.FromSDKItem(req.PutRequest.Item)
		case req.DeleteRequest != nil:
			wireItem = models.FromSDKItem(req.DeleteRequest.Key)
		default:
			continue
		}

		canon := BuildKeyString(wireItem, pkDef.AttributeName)
		if skDef.AttributeName != "" {
			canon += "\x00" + BuildKeyString(wireItem, skDef.AttributeName)
		}

		if _, dup := seen[canon]; dup {
			return NewValidationException("Provided list of item keys contains duplicates")
		}

		seen[canon] = struct{}{}
	}

	return nil
}

// globalTableNameRLocked returns table.GlobalTableName under a
// defer-protected RLock.
func globalTableNameRLocked(table *Table) string {
	table.mu.RLock("BatchWriteItem.replication")
	defer table.mu.RUnlock()

	return table.GlobalTableName
}

func (db *InMemoryDB) replicateBatchWrites(
	tableNames []string,
	tables map[string]*Table,
	toProcess map[string][]types.WriteRequest,
	region string,
) {
	for _, tableName := range tableNames {
		table := tables[tableName]
		gtName := globalTableNameRLocked(table)

		if gtName == "" {
			continue
		}

		for _, req := range toProcess[tableName] {
			switch {
			case req.PutRequest != nil:
				wireItem := models.FromSDKItem(req.PutRequest.Item)
				db.replicateItemMutation(tableName, gtName, region, deepCopyItem(wireItem), "PUT")
			case req.DeleteRequest != nil:
				wireKey := models.FromSDKItem(req.DeleteRequest.Key)
				db.replicateItemMutation(tableName, gtName, region, deepCopyItem(wireKey), "DELETE")
			}
		}
	}
}

func (db *InMemoryDB) BatchWriteItem(
	ctx context.Context,
	input *dynamodb.BatchWriteItemInput,
) (*dynamodb.BatchWriteItemOutput, error) {
	if len(input.RequestItems) == 0 {
		return nil, NewValidationException("The batch write request cannot be empty")
	}

	const batchWriteLimit = 25
	totalItems := 0
	for _, requests := range input.RequestItems {
		totalItems += len(requests)
	}
	if totalItems > batchWriteLimit {
		return nil, NewValidationException("Batch size limit exceeded: Max 25 items per request")
	}

	region := getRegionFromContext(ctx, db)

	// Get table references with read lock
	tables, err := db.getRequestTablesRLocked(region, input.RequestItems)
	if err != nil {
		return nil, err
	}

	if err = validateAllBatchWriteRequests(input.RequestItems, tables); err != nil {
		return nil, err
	}

	// Split requests per table by size limit before processing.
	toProcess := make(map[string][]types.WriteRequest, len(input.RequestItems))
	unprocessedItems := make(map[string][]types.WriteRequest)

	for tableName, requests := range input.RequestItems {
		process, unprocessed := splitWriteRequestsBySize(requests, batchWriteResponseLimit)
		toProcess[tableName] = process
		if len(unprocessed) > 0 {
			unprocessedItems[tableName] = unprocessed
		}
	}

	// Enforce throughput per table, same WCU formula as batchWriteConsumedCapacity,
	// before mutating any state. PAY_PER_REQUEST tables bypass throttling.
	if thrErr := db.enforceBatchWriteThroughput(region, toProcess, tables); thrErr != nil {
		return nil, thrErr
	}

	// Process tables in sorted order (deadlock prevention)
	tableNames := collections.SortedKeys(tables)

	// Sequential processing for simplicity and deadlock prevention
	itemCollectionMetrics := make(map[string][]types.ItemCollectionMetrics)

	for _, tableName := range tableNames {
		metrics, procErr := db.processTableWriteRequests(
			tables[tableName], toProcess[tableName], input.ReturnItemCollectionMetrics,
		)
		if procErr != nil {
			return nil, procErr
		}
		if len(metrics) > 0 {
			itemCollectionMetrics[tableName] = metrics
		}
	}

	db.replicateBatchWrites(tableNames, tables, toProcess, region)

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems:      unprocessedItems,
		ConsumedCapacity:      batchWriteConsumedCapacity(input.ReturnConsumedCapacity, toProcess),
		ItemCollectionMetrics: itemCollectionMetrics,
	}, nil
}

// enforceBatchWriteThroughput charges each table's WCU bucket before any writes are
// applied, mirroring PutItem/DeleteItem/UpdateItem. Real DynamoDB returns
// ProvisionedThroughputExceededException from BatchWriteItem exactly as it does from
// PutItem; without this, BatchWriteItem silently bypassed throttling that every other
// write path enforces.
func (db *InMemoryDB) enforceBatchWriteThroughput(
	region string,
	processed map[string][]types.WriteRequest,
	tables map[string]*Table,
) error {
	for tableName, reqs := range processed {
		table := tables[tableName]

		table.mu.RLock("BatchWriteItem.throttle")
		billingMode := table.BillingMode
		table.mu.RUnlock()

		if isOnDemandTable(billingMode) {
			continue
		}

		cu := computeBatchWriteWCU(reqs)

		if err := db.throttler.ConsumeWrite(throttleKey(region, tableName), cu); err != nil {
			return err
		}
	}

	return nil
}

func batchWriteConsumedCapacity(
	req types.ReturnConsumedCapacity,
	processed map[string][]types.WriteRequest,
) []types.ConsumedCapacity {
	if req == "" || req == types.ReturnConsumedCapacityNone {
		return nil
	}

	caps := make([]types.ConsumedCapacity, 0, len(processed))
	for tableName, reqs := range processed {
		cu := computeBatchWriteWCU(reqs)
		caps = append(caps, types.ConsumedCapacity{
			TableName:          aws.String(tableName),
			CapacityUnits:      aws.Float64(cu),
			WriteCapacityUnits: aws.Float64(cu),
		})
	}

	return caps
}

// computeBatchWriteWCU sums the write capacity consumed by a slice of WriteRequests.
// PutRequests charge ceil(itemSize/1KB) WCU; DeleteRequests charge 1 WCU minimum
// (the exact cost depends on the stored item size, which the mock does not track).
func computeBatchWriteWCU(reqs []types.WriteRequest) float64 {
	cu := 0.0
	for _, req := range reqs {
		if req.PutRequest != nil {
			wireItem := models.FromSDKItem(req.PutRequest.Item)
			itemSize, err := CalculateItemSize(wireItem)
			if err != nil || itemSize <= 0 {
				cu += 1.0
			} else {
				cu += WriteCapacityUnitsFromSize(itemSize)
			}
		} else {
			cu += 1.0
		}
	}

	return cu
}

// splitWriteRequestsBySize splits write requests into those whose cumulative estimated size
// fits within sizeLimit bytes and those that exceed it. Only PutRequests contribute to size.
func splitWriteRequestsBySize(
	requests []types.WriteRequest,
	sizeLimit int,
) ([]types.WriteRequest, []types.WriteRequest) {
	accumulated := 0
	var process, unprocessed []types.WriteRequest

	for _, req := range requests {
		if req.PutRequest != nil {
			wireItem := models.FromSDKItem(req.PutRequest.Item)
			itemSize, err := CalculateItemSize(wireItem)
			if err != nil {
				// Process conservatively if size calculation fails
				process = append(process, req)

				continue
			}

			if accumulated+itemSize > sizeLimit {
				unprocessed = append(unprocessed, req)

				continue
			}

			accumulated += itemSize
		}

		process = append(process, req)
	}

	return process, unprocessed
}

// getRequestTables retrieves all requested tables from the specified region.
// The caller must hold db.mu locked for reading.
func (db *InMemoryDB) getRequestTables(
	region string,
	requestItems map[string][]types.WriteRequest,
) (map[string]*Table, error) {
	tables := make(map[string]*Table, len(requestItems))

	for tableName := range requestItems {
		table, _ := db.tables.Get(tableKey(region, tableName))

		if table == nil {
			return nil, NewResourceNotFoundException("Table not found: " + tableName)
		}
		tables[tableName] = table
	}

	return tables, nil
}

// getRequestTablesRLocked wraps getRequestTables in a defer-protected db.mu
// RLock, so a panic while resolving table references can never leave db.mu
// read-locked forever.
func (db *InMemoryDB) getRequestTablesRLocked(
	region string,
	requestItems map[string][]types.WriteRequest,
) (map[string]*Table, error) {
	db.mu.RLock("BatchWriteItem")
	defer db.mu.RUnlock()

	return db.getRequestTables(region, requestItems)
}

func (db *InMemoryDB) processTableWriteRequests(
	table *Table,
	requests []types.WriteRequest,
	rim types.ReturnItemCollectionMetrics,
) ([]types.ItemCollectionMetrics, error) {
	table.mu.Lock("BatchWriteItem")
	defer table.mu.Unlock()

	modifiedIndices, putMetrics := db.processBatchPutRequests(table, requests, rim)
	deletedIndices, deleteMetrics := db.processBatchDeleteRequests(table, requests, rim)

	if len(deletedIndices) > 0 {
		indices := make([]int, 0, len(deletedIndices))
		for idx := range deletedIndices {
			indices = append(indices, idx)
		}
		db.applyBatchDeletes(table, indices)
	} else if len(modifiedIndices) > 0 {
		db.updateBatchIndexes(table, modifiedIndices)
	}

	return append(putMetrics, deleteMetrics...), nil
}

// processBatchPutRequests applies every PutRequest in requests, returning the
// modified item indices and (when the table has an LSI and rim requests it) the
// per-item ItemCollectionMetrics -- same SizeEstimateRangeGB formula PutItem uses,
// computed just before each put is applied so it reflects the post-write state.
func (db *InMemoryDB) processBatchPutRequests(
	table *Table,
	requests []types.WriteRequest,
	rim types.ReturnItemCollectionMetrics,
) (map[int]map[string]any, []types.ItemCollectionMetrics) {
	// modifiedIndices maps each put's final item offset to its pre-write value
	// (nil for a fresh insert); updateBatchIndexes needs the pre-write value to
	// correctly retire stale GSI/LSI membership when a put changes a key
	// attribute's value.
	modifiedIndices := make(map[int]map[string]any)
	trackMetrics := rim == types.ReturnItemCollectionMetricsSize && len(table.LocalSecondaryIndexes) > 0

	var metrics []types.ItemCollectionMetrics

	pkDef, _ := getPKAndSK(table.KeySchema)

	for _, req := range requests {
		if req.PutRequest == nil {
			continue
		}

		wireItem := models.FromSDKItem(req.PutRequest.Item)

		if trackMetrics {
			_, matchIndex := db.findMatchForPut(table, wireItem)
			pkVal := BuildKeyString(wireItem, pkDef.AttributeName)
			collectionBytes := computeLSICollectionSize(table, pkVal, wireItem, matchIndex)
			if m := buildItemCollectionMetrics(
				table, rim, pkOnlyKey(table, req.PutRequest.Item), collectionBytes,
			); m != nil {
				metrics = append(metrics, *m)
			}
		}

		oldItem, idx := db.handleBatchPutWithIndex(table, wireItem)
		if idx >= 0 {
			modifiedIndices[idx] = oldItem
		}
	}

	return modifiedIndices, metrics
}

// processBatchDeleteRequests identifies which items each DeleteRequest removes,
// returning their indices and (when the table has an LSI and rim requests it) the
// per-item ItemCollectionMetrics reflecting the collection remaining after each
// delete -- mirrors buildDeleteItemOutput's single-item formula. Metrics are
// computed here, before applyBatchDeletes actually removes anything.
func (db *InMemoryDB) processBatchDeleteRequests(
	table *Table,
	requests []types.WriteRequest,
	rim types.ReturnItemCollectionMetrics,
) (map[int]bool, []types.ItemCollectionMetrics) {
	deletedIndices := make(map[int]bool)
	trackMetrics := rim == types.ReturnItemCollectionMetricsSize && len(table.LocalSecondaryIndexes) > 0

	var metrics []types.ItemCollectionMetrics

	pkDef, _ := getPKAndSK(table.KeySchema)

	for _, req := range requests {
		if req.DeleteRequest == nil {
			continue
		}

		wireKey := models.FromSDKItem(req.DeleteRequest.Key)
		_, matchIndex := db.findMatchForPut(table, wireKey)
		if matchIndex == -1 {
			continue
		}
		deletedIndices[matchIndex] = true

		if trackMetrics {
			pkVal := BuildKeyString(wireKey, pkDef.AttributeName)
			remaining := currentLSICollectionBytes(table, pkVal) - int64(table.itemSizes[matchIndex])
			if m := buildItemCollectionMetrics(
				table, rim, pkOnlyKey(table, req.DeleteRequest.Key), remaining,
			); m != nil {
				metrics = append(metrics, *m)
			}
		}
	}

	return deletedIndices, metrics
}

func (db *InMemoryDB) applyBatchDeletes(table *Table, indices []int) {
	if len(indices) == 0 {
		return
	}

	// Sort indices in descending order to delete in-place without shifting issues
	sort.Ints(indices)
	for _, v := range slices.Backward(indices) {
		idx := v
		if idx < 0 || idx >= len(table.Items) {
			continue
		}
		// Capture stream record (REMOVE)
		table.appendStreamRecord(streamEventRemove, table.Items[idx], nil, "", "")

		// Delete by swapping with last and truncating. table.itemSizes must be
		// kept in lockstep with table.Items (same swap, same truncation) so its
		// length never drifts from Items and totalItemSizeBytes stays accurate
		// for DescribeTable's TableSizeBytes.
		lastIdx := len(table.Items) - 1
		table.totalItemSizeBytes -= int64(table.itemSizes[idx])
		table.Items[idx] = table.Items[lastIdx]
		table.itemSizes[idx] = table.itemSizes[lastIdx]
		table.Items = table.Items[:lastIdx]
		table.itemSizes = table.itemSizes[:lastIdx]
	}

	// rebuildIndexes recomputes the key indexes AND itemSizes/totalItemSizeBytes
	// from the surviving Items, so the size accounting stays consistent with the
	// deletions above.
	table.rebuildIndexes()
}

// updateBatchIndexes incrementally updates every index (primary and
// GSI/LSI) for the items BatchWriteItem just put in place, without
// rebuilding the whole table (O(K) in the number of puts, not O(N) in table
// size). modifiedIndices maps each modified item's final offset to its
// pre-write value (nil for a freshly-inserted item) so secondary indexes can
// correctly retire stale GSI/LSI membership when a put changes a key
// attribute's value.
func (db *InMemoryDB) updateBatchIndexes(
	table *Table,
	modifiedIndices map[int]map[string]any,
) {
	if len(modifiedIndices) == 0 {
		return
	}

	pkDef, skDef := getPKAndSK(table.KeySchema)
	for idx, oldItem := range modifiedIndices {
		if idx >= 0 && idx < len(table.Items) {
			db.updateItemIndex(table, idx, pkDef, skDef)
			table.updateSecondaryIndexes(oldItem, idx, table.Items[idx], idx)
		}
	}
}

func (db *InMemoryDB) updateItemIndex(
	table *Table,
	idx int,
	pkDef models.KeySchemaElement,
	skDef models.KeySchemaElement,
) {
	item := table.Items[idx]
	pkVal := BuildKeyString(item, pkDef.AttributeName)

	if skDef.AttributeName != "" {
		skVal := BuildKeyString(item, skDef.AttributeName)
		if table.pkskIndex[pkVal] == nil {
			table.pkskIndex[pkVal] = make(map[string]int)
		}
		table.pkskIndex[pkVal][skVal] = idx
	} else {
		table.pkIndex[pkVal] = idx
	}
}

// validateBatchWriteRequest checks a single WriteRequest for AWS spec compliance:
// - exactly one of PutRequest / DeleteRequest must be set
// - PutRequest items must pass item-size and key-schema validation
// - DeleteRequest keys must contain all required key attributes
// KeySchema is immutable after table creation, so no lock is needed to read it.
func validateBatchWriteRequest(req types.WriteRequest, table *Table) error {
	if req.PutRequest == nil && req.DeleteRequest == nil {
		return NewValidationException(
			"Supplied AttributeValue has more than one datatypes set, " +
				"must contain exactly one of the supported datatypes",
		)
	}

	if req.PutRequest != nil {
		wireItem := models.FromSDKItem(req.PutRequest.Item)
		if err := ValidateItemSize(wireItem); err != nil {
			return err
		}
		if err := validateKeySchema(wireItem, table.KeySchema); err != nil {
			return err
		}
	}

	if req.DeleteRequest != nil {
		wireKey := models.FromSDKItem(req.DeleteRequest.Key)
		if err := validateKeySchema(wireKey, table.KeySchema); err != nil {
			return err
		}
	}

	return nil
}

// handleBatchPutWithIndex writes item into table.Items (in place if it
// matches an existing key, appended otherwise) and returns the item's
// pre-write value (nil for a fresh insert) alongside its final offset, so the
// caller can later feed both to updateBatchIndexes/updateSecondaryIndexes.
func (db *InMemoryDB) handleBatchPutWithIndex(table *Table, item map[string]any) (map[string]any, int) {
	// Reuse the same item-size calculator as PutItem (doPut) so table.itemSizes
	// and table.totalItemSizeBytes stay in lockstep with table.Items regardless
	// of which write path (PutItem vs BatchWriteItem) added the item. Without
	// this, DescribeTable's TableSizeBytes silently excludes anything written
	// via BatchWriteItem, and itemSizes/Items can drift out of length-sync.
	itemSize, _ := CalculateItemSize(item)
	oldItem, matchIndex := db.findMatchForPut(table, item)
	if matchIndex != -1 {
		// Capture stream event (MODIFY) before overwriting in place.
		table.appendStreamRecord(streamEventModify, oldItem, item, "", "")
		// Keep itemSizes/totalItemSizeBytes in step with Items so the
		// len(itemSizes) == len(Items) invariant holds for later CRUD ops.
		table.totalItemSizeBytes += int64(itemSize) - int64(table.itemSizes[matchIndex])
		table.itemSizes[matchIndex] = itemSize
		table.Items[matchIndex] = item

		return oldItem, matchIndex
	}
	// Capture stream event (INSERT) for the new item.
	table.appendStreamRecord(streamEventInsert, nil, item, "", "")
	idx := len(table.Items)
	table.Items = append(table.Items, item)
	table.itemSizes = append(table.itemSizes, itemSize)
	table.totalItemSizeBytes += int64(itemSize)

	return nil, idx
}
