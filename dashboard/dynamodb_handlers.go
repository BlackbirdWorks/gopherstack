package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"Gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	defaultTableLimit = 100
	maxSearchTables   = 1000
)

const (
	defaultCapacity   = 5
	maxS3ObjectSearch = 100
)

var (
	errAttrDefNotFound = errors.New("attribute definition not found")
)

// PageData represents common page data.
type PageData struct {
	Title     string
	ActiveTab string
}

// TableInfo represents table information for display.
type TableInfo struct {
	TableName              string
	PartitionKey           string
	PartitionKeyType       string
	SortKey                string
	SortKeyType            string
	GlobalSecondaryIndexes []IndexInfo
	LocalSecondaryIndexes  []IndexInfo
	ItemCount              int64
	GSICount               int
	LSICount               int
}

// IndexInfo represents index information.
type IndexInfo struct {
	IndexName        string
	PartitionKey     string
	PartitionKeyType string
	SortKey          string
	SortKeyType      string
	ProjectionType   string
}

// QueryResult represents query results.
type QueryResult struct {
	LastEvaluatedKey map[string]types.AttributeValue
	Items            []map[string]types.AttributeValue
	Count            int32
	ScannedCount     int32
}

// dynamoDBIndex renders the DynamoDB index page.
func (h *Handler) dynamoDBIndex(w http.ResponseWriter, _ *http.Request) {
	data := PageData{
		Title:     "DynamoDB Tables",
		ActiveTab: "dynamodb",
	}
	h.renderTemplate(w, "dynamodb/dynamodb_index.html", data)
}

// dynamoDBTableList returns the list of tables as HTML fragment.
func (h *Handler) dynamoDBTableList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := logger.Load(ctx)

	search := strings.ToLower(r.URL.Query().Get("search"))
	lastTable := r.URL.Query().Get("lastTable")
	log.DebugContext(ctx, "DynamoDB table list", "search", search, "lastTable", lastTable)

	var tableNames []string
	var nextTable *string

	if search != "" {
		// If searching, we need to scan more to find matches
		// For simplicity, we'll fetch up to 1000 tables and filter
		output, err := h.DynamoDB.ListTables(ctx, &dynamodb.ListTablesInput{
			Limit: aws.Int32(maxSearchTables),
		})
		if err != nil {
			h.handleListTablesError(w, r, err)

			return
		}
		tableNames = filterTableNames(search, output.TableNames)
	} else {
		// Regular paginated listing
		input := &dynamodb.ListTablesInput{
			Limit: aws.Int32(defaultTableLimit),
		}
		if lastTable != "" {
			input.ExclusiveStartTableName = aws.String(lastTable)
		}
		output, err := h.DynamoDB.ListTables(ctx, input)
		if err != nil {
			h.handleListTablesError(w, r, err)

			return
		}
		tableNames = output.TableNames
		nextTable = output.LastEvaluatedTableName
	}

	tableInfos := h.fetchTableInfos(ctx, tableNames)

	// Render table cards
	for _, tableInfo := range tableInfos {
		h.renderFragment(w, "table-card", tableInfo)
	}

	if nextTable != nil {
		// #nosec G705
		fmt.Fprintf(w, `
            <button class="btn btn-outline col-span-full mt-4" 
                hx-get="/dashboard/dynamodb/tables?lastTable=%s" 
                hx-target="this" 
                hx-swap="outerHTML"
                hx-indicator=".htmx-indicator">
                Load More
            </button>`, url.QueryEscape(*nextTable))
	}
}

func (h *Handler) handleListTablesError(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()
	log := logger.Load(ctx)
	log.ErrorContext(ctx, "Failed to list tables", "error", err)
	if strings.Contains(err.Error(), "ResourceNotFoundException") {
		http.NotFound(w, r)

		return
	}

	http.Error(w, "Failed to list tables", http.StatusInternalServerError)
}

func filterTableNames(search string, names []string) []string {
	if search == "" {
		return names
	}

	filtered := make([]string, 0, len(names))
	for _, name := range names {
		if strings.Contains(strings.ToLower(name), search) {
			filtered = append(filtered, name)
		}
	}

	return filtered
}

func (h *Handler) fetchTableInfos(ctx context.Context, tableNames []string) []TableInfo {
	if len(tableNames) == 0 {
		return []TableInfo{}
	}

	// Use a bounded worker pool (max 8 concurrent fetches)
	const maxConcurrent = 8
	semaphore := make(chan struct{}, maxConcurrent)

	tableInfos := make([]TableInfo, len(tableNames))
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, tableName := range tableNames {
		wg.Add(1)
		go func(idx int, tblName string) {
			defer wg.Done()

			// Acquire semaphore slot
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			desc, err := h.DynamoDB.DescribeTable(
				ctx,
				&dynamodb.DescribeTableInput{TableName: aws.String(tblName)},
			)
			if err != nil {
				log := logger.Load(ctx)
				log.ErrorContext(
					ctx,
					"Failed to describe table for list",
					"table",
					tblName,
					"error",
					err,
				)

				return
			}

			mu.Lock()
			tableInfos[idx] = buildTableListInfo(desc.Table)
			mu.Unlock()
		}(i, tableName)
	}

	wg.Wait()
	close(semaphore)

	// Filter out empty entries (failed fetches)
	result := make([]TableInfo, 0, len(tableInfos))
	for _, info := range tableInfos {
		if info.TableName != "" {
			result = append(result, info)
		}
	}

	return result
}

func buildTableListInfo(table *types.TableDescription) TableInfo {
	info := TableInfo{
		TableName: aws.ToString(table.TableName),
		ItemCount: aws.ToInt64(table.ItemCount),
		GSICount:  len(table.GlobalSecondaryIndexes),
		LSICount:  len(table.LocalSecondaryIndexes),
	}

	for _, key := range table.KeySchema {
		switch key.KeyType {
		case types.KeyTypeHash:
			info.PartitionKey = aws.ToString(key.AttributeName)
		case types.KeyTypeRange:
			info.SortKey = aws.ToString(key.AttributeName)
		}
	}

	for _, attr := range table.AttributeDefinitions {
		switch aws.ToString(attr.AttributeName) {
		case info.PartitionKey:
			info.PartitionKeyType = string(attr.AttributeType)
		case info.SortKey:
			info.SortKeyType = string(attr.AttributeType)
		}
	}

	return info
}

// dynamoDBSearch searches tables by name.
func (h *Handler) dynamoDBSearch(w http.ResponseWriter, r *http.Request) {
	// For now, we can reuse table list logic but filter in memory
	// In a real scenario with many tables, we might do client-side filtering or pagination
	h.dynamoDBTableList(w, r)
}

// dynamoDBTableDetail renders the table detail page.
func (h *Handler) dynamoDBTableDetail(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := r.Context()
	output, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	if err != nil {
		http.NotFound(w, r)

		return
	}
	info := h.extractTableInfo(output.Table)

	data := struct {
		PageData
		TableInfo
	}{
		PageData: PageData{
			Title:     tableName,
			ActiveTab: "dynamodb",
		},
		TableInfo: info,
	}

	h.renderTemplate(w, "dynamodb/table_detail.html", data)
}

// extractTableInfo extracts display information from a DescribeTable output.
func (h *Handler) extractTableInfo(table *types.TableDescription) TableInfo {
	info := TableInfo{
		TableName: aws.ToString(table.TableName),
		GSICount:  len(table.GlobalSecondaryIndexes),
		LSICount:  len(table.LocalSecondaryIndexes),
	}
	if table.ItemCount != nil {
		info.ItemCount = *table.ItemCount
	}

	// Extract keys
	for _, key := range table.KeySchema {
		switch key.KeyType {
		case types.KeyTypeHash:
			info.PartitionKey = *key.AttributeName
		case types.KeyTypeRange:
			info.SortKey = *key.AttributeName
		}
	}

	for _, attr := range table.AttributeDefinitions {
		switch *attr.AttributeName {
		case info.PartitionKey:
			info.PartitionKeyType = string(attr.AttributeType)
		case info.SortKey:
			info.SortKeyType = string(attr.AttributeType)
		}
	}

	// Add GSI information
	for _, gsi := range table.GlobalSecondaryIndexes {
		gsiInfo := h.extractIndexInfo(
			gsi.IndexName,
			gsi.KeySchema,
			gsi.Projection,
			table.AttributeDefinitions,
		)
		info.GlobalSecondaryIndexes = append(info.GlobalSecondaryIndexes, gsiInfo)
	}

	// Add LSI information
	for _, lsi := range table.LocalSecondaryIndexes {
		lsiInfo := h.extractIndexInfo(
			lsi.IndexName,
			lsi.KeySchema,
			lsi.Projection,
			table.AttributeDefinitions,
		)
		info.LocalSecondaryIndexes = append(info.LocalSecondaryIndexes, lsiInfo)
	}

	return info
}

func (h *Handler) extractIndexInfo(
	name *string,
	keySchema []types.KeySchemaElement,
	projection *types.Projection,
	attrDefs []types.AttributeDefinition,
) IndexInfo {
	idxInfo := IndexInfo{
		IndexName: *name,
	}

	if projection != nil {
		idxInfo.ProjectionType = string(projection.ProjectionType)
	} else {
		idxInfo.ProjectionType = "INCLUDE"
	}

	for _, key := range keySchema {
		switch key.KeyType {
		case types.KeyTypeHash:
			idxInfo.PartitionKey = *key.AttributeName
		case types.KeyTypeRange:
			idxInfo.SortKey = *key.AttributeName
		}
	}

	for _, attr := range attrDefs {
		switch *attr.AttributeName {
		case idxInfo.PartitionKey:
			idxInfo.PartitionKeyType = string(attr.AttributeType)
		case idxInfo.SortKey:
			idxInfo.SortKeyType = string(attr.AttributeType)
		}
	}

	return idxInfo
}

// dynamoDBQuery executes a query and returns results.
func (h *Handler) dynamoDBQuery(w http.ResponseWriter, r *http.Request, tableName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	params, ok := h.parseQueryRequest(w, r)
	if !ok {
		return
	}

	// Determine key schema from table/index
	ctx := r.Context()
	desc, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		log := logger.Load(ctx)
		log.ErrorContext(ctx, "Failed to describe table", "error", err)
		if strings.Contains(err.Error(), "ResourceNotFoundException") {
			http.NotFound(w, r)
		} else {
			http.Error(w, "Failed to describe table", http.StatusInternalServerError)
		}

		return
	}

	pkName, skName, pkType, skType := h.resolveKeySchema(desc, params.IndexName)

	keyCondExp, attrNames, attrValues := h.buildKeyCondition(params, pkName, skName, pkType, skType)

	h.executeAndRenderQuery(
		ctx,
		w,
		tableName,
		params.IndexName,
		keyCondExp,
		params.FilterExp,
		params.LimitStr,
		params.ExclusiveStartKey,
		attrNames,
		attrValues,
		pkName,
		skName,
	)
}

func (h *Handler) buildKeyCondition(
	params QueryParams,
	pkName string,
	skName string,
	pkType types.ScalarAttributeType,
	skType types.ScalarAttributeType,
) (string, map[string]string, map[string]types.AttributeValue) {
	attrNames := map[string]string{
		"#pk": pkName,
	}
	attrValues := map[string]types.AttributeValue{
		":pkval": h.toAttributeValue(params.PartitionKeyValue, pkType),
	}
	keyCondExp := "#pk = :pkval"

	if skName == "" || params.SortKeyOperator == "" || params.SortKeyValue == "" {
		return keyCondExp, attrNames, attrValues
	}

	attrNames["#sk"] = skName
	attrValues[":skval"] = h.toAttributeValue(params.SortKeyValue, skType)

	switch params.SortKeyOperator {
	case "=":
		keyCondExp += " AND #sk = :skval"
	case "<":
		keyCondExp += " AND #sk < :skval"
	case "<=":
		keyCondExp += " AND #sk <= :skval"
	case ">":
		keyCondExp += " AND #sk > :skval"
	case ">=":
		keyCondExp += " AND #sk >= :skval"
	case "begins_with":
		keyCondExp += " AND begins_with(#sk, :skval)"
	case "BETWEEN":
		if params.SortKeyValue2 != "" {
			attrValues[":skval2"] = h.toAttributeValue(params.SortKeyValue2, skType)
			keyCondExp += " AND #sk BETWEEN :skval AND :skval2"
		}
	}

	return keyCondExp, attrNames, attrValues
}

type QueryParams struct {
	IndexName         string
	PartitionKeyValue string
	SortKeyOperator   string
	SortKeyValue      string
	SortKeyValue2     string
	FilterExp         string
	LimitStr          string
	ExclusiveStartKey string
}

func (h *Handler) parseQueryRequest(w http.ResponseWriter, r *http.Request) (QueryParams, bool) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)

		return QueryParams{}, false
	}

	params := QueryParams{
		IndexName:         r.FormValue("indexName"),
		PartitionKeyValue: r.FormValue("partitionKeyValue"),
		SortKeyOperator:   r.FormValue("sortKeyOperator"),
		SortKeyValue:      r.FormValue("sortKeyValue"),
		SortKeyValue2:     r.FormValue("sortKeyValue2"),
		FilterExp:         r.FormValue("filterExpression"),
		LimitStr:          r.FormValue("limit"),
		ExclusiveStartKey: r.FormValue("exclusiveStartKey"),
	}

	if params.PartitionKeyValue == "" {
		http.Error(w, "Partition key value is required", http.StatusBadRequest)

		return QueryParams{}, false
	}

	return params, true
}

func (h *Handler) executeAndRenderQuery(
	ctx context.Context,
	w http.ResponseWriter,
	tableName, indexName, keyCondExp, filterExp, limitStr, exclusiveStartKey string,
	attrNames map[string]string,
	attrValues map[string]types.AttributeValue,
	pkName, skName string,
) {
	input := &dynamodb.QueryInput{
		TableName:                 &tableName,
		KeyConditionExpression:    aws.String(keyCondExp),
		ExpressionAttributeNames:  attrNames,
		ExpressionAttributeValues: attrValues,
	}

	if indexName != "" {
		input.IndexName = aws.String(indexName)
	}

	if filterExp != "" {
		input.FilterExpression = aws.String(filterExp)
	}

	if limitStr != "" {
		var l int32
		if _, srr := fmt.Sscanf(limitStr, "%d", &l); srr == nil && l > 0 {
			input.Limit = aws.Int32(l)
		}
	}

	if exclusiveStartKey != "" {
		var esk map[string]types.AttributeValue
		if err := json.Unmarshal([]byte(exclusiveStartKey), &esk); err == nil {
			input.ExclusiveStartKey = esk
		}
	}

	output, err := h.DynamoDB.Query(ctx, input)
	if err != nil {
		log := logger.Load(ctx)
		log.ErrorContext(ctx, "Failed to query table", "error", err)
		toastMessage := fmt.Sprintf(
			`{"showToast": {"message": "Failed to query table: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`),
		)
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	result := QueryResult{
		Items:            output.Items,
		Count:            output.Count,
		ScannedCount:     output.ScannedCount,
		LastEvaluatedKey: output.LastEvaluatedKey,
	}

	h.renderQueryResults(w, result, tableName, pkName, skName)
}

func (h *Handler) resolveKeySchema(
	desc *dynamodb.DescribeTableOutput,
	indexName string,
) (string, string, types.ScalarAttributeType, types.ScalarAttributeType) {
	var pkName, skName string
	var pkType, skType types.ScalarAttributeType

	if indexName == "" {
		for _, key := range desc.Table.KeySchema {
			if key.KeyType == types.KeyTypeHash {
				pkName = *key.AttributeName
			} else {
				skName = *key.AttributeName
			}
		}
	} else {
		pkName, skName = h.resolveIndexKeys(desc, indexName)
	}

	for _, attr := range desc.Table.AttributeDefinitions {
		if *attr.AttributeName == pkName {
			pkType = attr.AttributeType
		}

		if skName != "" && *attr.AttributeName == skName {
			skType = attr.AttributeType
		}
	}

	return pkName, skName, pkType, skType
}

func (h *Handler) resolveIndexKeys(
	desc *dynamodb.DescribeTableOutput,
	indexName string,
) (string, string) {
	for _, gsi := range desc.Table.GlobalSecondaryIndexes {
		if *gsi.IndexName == indexName {
			return h.extractKeys(gsi.KeySchema)
		}
	}

	for _, lsi := range desc.Table.LocalSecondaryIndexes {
		if *lsi.IndexName == indexName {
			return h.extractKeys(lsi.KeySchema)
		}
	}

	return "", ""
}

func (h *Handler) extractKeys(keySchema []types.KeySchemaElement) (string, string) {
	var pk, sk string
	for _, key := range keySchema {
		if key.KeyType == types.KeyTypeHash {
			pk = *key.AttributeName
		} else {
			sk = *key.AttributeName
		}
	}

	return pk, sk
}

// toAttributeValue converts a string to a typed AttributeValue.
func (h *Handler) toAttributeValue(val string, t types.ScalarAttributeType) types.AttributeValue {
	switch t {
	case types.ScalarAttributeTypeN:
		return &types.AttributeValueMemberN{Value: val}
	case types.ScalarAttributeTypeB:
		return &types.AttributeValueMemberB{Value: []byte(val)}
	case types.ScalarAttributeTypeS:
		return &types.AttributeValueMemberS{Value: val}
	default:
		return &types.AttributeValueMemberS{Value: val}
	}
}

// dynamoDBScan executes a scan and returns results.
func (h *Handler) dynamoDBScan(w http.ResponseWriter, r *http.Request, tableName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	// Get key schema for pinning columns
	desc, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		log.ErrorContext(ctx, "Failed to describe table for scan", "error", err)
	}

	var pkName, skName string
	if desc != nil && desc.Table != nil {
		for _, key := range desc.Table.KeySchema {
			if key.KeyType == types.KeyTypeHash {
				pkName = *key.AttributeName
			} else {
				skName = *key.AttributeName
			}
		}
	}

	input := &dynamodb.ScanInput{
		TableName: &tableName,
	}

	filterExp := r.FormValue("filterExpression")
	if filterExp != "" {
		input.FilterExpression = aws.String(filterExp)
	}

	projExp := r.FormValue("projectionExpression")
	if projExp != "" {
		input.ProjectionExpression = aws.String(projExp)
	}

	limit := r.FormValue("limit")
	if limit != "" {
		var l int32
		if _, errScan := fmt.Sscanf(limit, "%d", &l); errScan == nil && l > 0 {
			input.Limit = aws.Int32(l)
		}
	}

	esk := r.FormValue("exclusiveStartKey")
	if esk != "" {
		var eskMap map[string]types.AttributeValue
		if errUnmarshal := json.Unmarshal([]byte(esk), &eskMap); errUnmarshal == nil {
			input.ExclusiveStartKey = eskMap
		}
	}

	output, err := h.DynamoDB.Scan(ctx, input)
	if err != nil {
		log.ErrorContext(ctx, "Failed to scan table", "error", err)
		toastMessage := fmt.Sprintf(
			`{"showToast": {"message": "Failed to scan table: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`),
		)
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	result := QueryResult{
		Items:        output.Items,
		Count:        output.Count,
		ScannedCount: output.ScannedCount,
	}

	// Render results
	h.renderQueryResults(w, result, tableName, pkName, skName)
}

// renderQueryResults renders query/scan results as HTML.
func (h *Handler) renderQueryResults(
	w http.ResponseWriter,
	result QueryResult,
	tableName, pkName, skName string,
) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if len(result.Items) == 0 {
		fmt.Fprintf(w, `<div class="alert alert-info"><span>No items found</span></div>`)

		return
	}

	columns, items := h.prepareResultsData(result.Items, pkName, skName)
	h.renderResultsTable(w, tableName, columns, items, pkName, skName)
	h.renderResultsSummary(w, result)
}

func (h *Handler) prepareResultsData(
	items []map[string]types.AttributeValue,
	pkName, skName string,
) ([]string, []map[string]any) {
	columns := make([]string, 0)
	columnMap := make(map[string]bool)
	unmarshaledItems := make([]map[string]any, len(items))

	for i, item := range items {
		var m map[string]any
		if err := attributevalue.UnmarshalMap(item, &m); err != nil {
			m = make(map[string]any)
			for k, v := range item {
				m[k] = v
			}
		}
		unmarshaledItems[i] = m
		for k := range m {
			if !columnMap[k] {
				columnMap[k] = true
				columns = append(columns, k)
			}
		}
	}

	// Reorder columns: PK first, then SK, then the rest
	reordered := make([]string, 0, len(columns))
	if pkName != "" && columnMap[pkName] {
		reordered = append(reordered, pkName)
	}
	if skName != "" && columnMap[skName] && skName != pkName {
		reordered = append(reordered, skName)
	}

	for _, col := range columns {
		if col != pkName && col != skName {
			reordered = append(reordered, col)
		}
	}

	return reordered, unmarshaledItems
}

func (h *Handler) renderResultsTable(
	w http.ResponseWriter,
	tableName string,
	columns []string,
	items []map[string]any,
	pkName, skName string,
) {
	const tableWrapper = `<div class="wide-table-container border border-base-300 ` +
		`rounded-lg shadow-inner bg-base-100 mb-4">`
	fmt.Fprint(w, tableWrapper)
	fmt.Fprintf(w, `<table class="table table-zebra table-sm w-full table-auto">`)
	fmt.Fprintf(w, `<thead><tr>`)
	for _, col := range columns {
		label := html.EscapeString(col)
		switch col {
		case pkName:
			label += " <span class='badge badge-primary badge-xs ml-1'>PK</span>"
		case skName:
			label += " <span class='badge badge-secondary badge-xs ml-1'>SK</span>"
		}
		// #nosec G705 -- Data is escaped with html.EscapeString, false positive
		// #nosec G705
		fmt.Fprintf(w, `<th>%s</th>`, label)
	}
	fmt.Fprintf(w, `<th>Actions</th>`)
	fmt.Fprintf(w, `</tr></thead><tbody>`)

	for _, item := range items {
		fmt.Fprintf(w, `<tr>`)
		for _, col := range columns {
			val := item[col]
			if val == nil {
				fmt.Fprintf(w, `<td class="opacity-30">-</td>`)
			} else {
				jsonVal, _ := json.Marshal(val)
				jsonStr := string(jsonVal)
				fmt.Fprintf(w, `<td class="font-mono text-xs max-w-xs truncate" title="%s">%s</td>`,
					html.EscapeString(jsonStr), html.EscapeString(jsonStr))
			}
		}

		// Row actions
		h.renderItemActions(w, tableName, pkName, skName, item)

		fmt.Fprintf(w, `</tr>`)
	}

	fmt.Fprintf(w, `</tbody></table></div>`)
}

func (h *Handler) renderItemActions(
	w http.ResponseWriter,
	tableName, pkName, skName string,
	item map[string]any,
) {
	pkValBytes, _ := json.Marshal(item[pkName])
	pkValStr := string(pkValBytes)
	skValStr := ""
	if skName != "" {
		skValBytes, _ := json.Marshal(item[skName])
		skValStr = string(skValBytes)
	}

	fmt.Fprintf(w, `<td>
            <div class="flex gap-1">
                <button class="btn btn-ghost btn-xs text-info" 
                    hx-get="/dashboard/dynamodb/table/%s/item?pk=%s&sk=%s"
                    hx-target="#edit_item_modal_content"
                    onclick="edit_item_modal.showModal()">
                    Edit
                </button>
                <button class="btn btn-ghost btn-xs text-error" 
                    hx-delete="/dashboard/dynamodb/table/%s/item?pk=%s&sk=%s"
                    hx-confirm="Are you sure you want to delete this item?"
                    hx-target="closest tr" hx-swap="outerHTML">
                    Delete
                </button>
            </div>
        </td>`,
		url.PathEscape(tableName), url.QueryEscape(pkValStr), url.QueryEscape(skValStr),
		url.PathEscape(tableName), url.QueryEscape(pkValStr), url.QueryEscape(skValStr))
}

func (h *Handler) renderResultsSummary(w http.ResponseWriter, result QueryResult) {
	fmt.Fprintf(
		w,
		`<div class="mt-4 flex justify-between items-center bg-base-200 p-4 rounded-lg">`,
	)
	fmt.Fprintf(
		w,
		`<div><p class="text-sm font-medium">Count: <span class="badge badge-ghost">%d</span> `+
			`| Scanned: <span class="badge badge-ghost">%d</span></p></div>`,
		result.Count,
		result.ScannedCount,
	)

	if result.LastEvaluatedKey != nil {
		kb, _ := json.Marshal(result.LastEvaluatedKey)
		fmt.Fprintf(w, `<div class="flex items-center gap-4">`)
		fmt.Fprintf(
			w,
			`<span class="text-xs opacity-50 font-mono truncate max-w-xs" title="%s">LastEvaluatedKey: %s</span>`,
			string(kb),
			string(kb),
		)
		fmt.Fprintf(
			w,
			`<button class="btn btn-primary btn-sm" hx-include="closest form" hx-vals='{"exclusiveStartKey": %s}' `+
				`hx-post="" hx-target="closest #query-results, closest #scan-results">Next Page &rarr;</button>`,
			string(kb),
		)
		fmt.Fprintf(w, `</div>`)
	}
	fmt.Fprintf(w, `</div>`)
}

// dynamoDBCreateTable handles table creation requests.
func (h *Handler) dynamoDBCreateTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	if err := r.ParseForm(); err != nil {
		log.ErrorContext(ctx, "Failed to parse form", "error", err)
		http.Error(w, "Bad request", http.StatusBadRequest)

		return
	}

	tableName := r.FormValue("tableName")
	partitionKey := r.FormValue("partitionKey")
	partitionKeyType := types.ScalarAttributeType(r.FormValue("partitionKeyType"))
	sortKey := r.FormValue("sortKey")
	sortKeyType := types.ScalarAttributeType(r.FormValue("sortKeyType"))

	// Build AttributeDefinitions and KeySchema
	attrs := []types.AttributeDefinition{
		{AttributeName: &partitionKey, AttributeType: partitionKeyType},
	}
	keySchema := []types.KeySchemaElement{
		{AttributeName: &partitionKey, KeyType: types.KeyTypeHash},
	}

	if sortKey != "" {
		attrs = append(
			attrs,
			types.AttributeDefinition{AttributeName: &sortKey, AttributeType: sortKeyType},
		)
		keySchema = append(
			keySchema,
			types.KeySchemaElement{AttributeName: &sortKey, KeyType: types.KeyTypeRange},
		)
	}

	input := &dynamodb.CreateTableInput{
		TableName:            &tableName,
		AttributeDefinitions: attrs,
		KeySchema:            keySchema,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(defaultCapacity),
			WriteCapacityUnits: aws.Int64(defaultCapacity),
		},
	}

	_, err := h.DynamoDB.CreateTable(ctx, input)
	if err != nil {
		log.ErrorContext(ctx, "Failed to create table", "error", err)
		toastMessage := fmt.Sprintf(
			`{"showToast": {"message": "Failed to create table: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`),
		)
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	// On success, return the updated table list
	h.dynamoDBTableList(w, r)
}

// dynamoDBDeleteTable handles table deletion requests.
func (h *Handler) dynamoDBDeleteTable(w http.ResponseWriter, r *http.Request, tableName string) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	_, err := h.DynamoDB.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: &tableName,
	})

	if err != nil {
		log.ErrorContext(ctx, "Failed to delete table", "table", tableName, "error", err)
		toastMessage := fmt.Sprintf(
			`{"showToast": {"message": "Failed to delete table: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`),
		)
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	// Check if request is from list view or detail view
	// HTMX headers or just context
	// If HX-Target is #table-list, it's list view.
	// If it's detail view, we likely want to redirect.

	if r.Header.Get("Hx-Target") == "table-list" {
		// List view: return updated list
		h.dynamoDBTableList(w, r)
	} else {
		// Detail view: Redirect to index
		// HTMX handles redirects via HX-Location header
		w.Header().Set("Hx-Location", "/dashboard/dynamodb")
		w.WriteHeader(http.StatusOK)
	}
}

// dynamoDBPurge deletes all tables.
func (h *Handler) dynamoDBPurge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	output, err := h.DynamoDB.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		log.ErrorContext(ctx, "Failed to list tables for purge", "error", err)
		http.Error(w, "Failed to list tables", http.StatusInternalServerError)

		return
	}

	for _, tableName := range output.TableNames {
		_, err = h.DynamoDB.DeleteTable(ctx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			log.ErrorContext(
				ctx,
				"Failed to delete table during purge",
				"table",
				tableName,
				"error",
				err,
			)
		}
	}

	// Trigger a refresh of the table list
	w.Header().Set("Hx-Trigger", "tablesPurged")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(
		[]byte(
			`<div class="alert alert-success col-span-full"><span>All tables purged successfully.</span></div>`,
		),
	)
}

// dynamoDBDeleteItem handles item deletion.
func (h *Handler) dynamoDBDeleteItem(w http.ResponseWriter, r *http.Request, tableName string) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	// Get table description to find PK/SK names and types
	desc, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		http.Error(w, "Table not found", http.StatusNotFound)

		return
	}

	pkName, skName := h.extractKeys(desc.Table.KeySchema)

	// Get PK/SK values from request (JSON serialized in query params)
	pkValRaw := r.URL.Query().Get("pk")
	skValRaw := r.URL.Query().Get("sk")

	key := make(map[string]types.AttributeValue)

	// Helper to parse JSON value into AttributeValue based on attribute definition
	parseVal := func(name, valRaw string) (types.AttributeValue, error) {
		var ad *types.AttributeDefinition
		for i := range desc.Table.AttributeDefinitions {
			if *desc.Table.AttributeDefinitions[i].AttributeName == name {
				ad = &desc.Table.AttributeDefinitions[i]

				break
			}
		}
		if ad == nil {
			return nil, fmt.Errorf("%w for %s", errAttrDefNotFound, name)
		}

		// Unmarshal the JSON value (could be string, number, etc.)
		var v any
		if errUnmarshal := json.Unmarshal([]byte(valRaw), &v); errUnmarshal != nil {
			return nil, fmt.Errorf("failed to unmarshal key value: %w", errUnmarshal)
		}

		// Marshal into AttributeValue
		return attributevalue.Marshal(v)
	}

	pkAV, errPK := parseVal(pkName, pkValRaw)
	if errPK != nil {
		log.ErrorContext(ctx, "Failed to parse PK", "error", errPK)
		http.Error(w, errPK.Error(), http.StatusBadRequest)

		return
	}
	key[pkName] = pkAV

	if skName != "" && skValRaw != "" {
		skAV, errSK := parseVal(skName, skValRaw)
		if errSK != nil {
			log.ErrorContext(ctx, "Failed to parse SK", "error", errSK)
			http.Error(w, errSK.Error(), http.StatusBadRequest)

			return
		}
		key[skName] = skAV
	}

	_, err = h.DynamoDB.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to delete item", "table", tableName, "error", err)
		http.Error(w, "Failed to delete item", http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// dynamoDBCreateItem handles item creation.
func (h *Handler) dynamoDBCreateItem(w http.ResponseWriter, r *http.Request, tableName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	itemJSON := r.FormValue("itemJson")
	var m map[string]any
	if err := json.Unmarshal([]byte(itemJSON), &m); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)

		return
	}

	item, err := attributevalue.MarshalMap(m)
	if err != nil {
		http.Error(w, "Failed to marshal item: "+err.Error(), http.StatusInternalServerError)

		return
	}

	_, err = h.DynamoDB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to create item", "table", tableName, "error", err)
		http.Error(w, "Failed to create item: "+err.Error(), http.StatusInternalServerError)

		return
	}

	// Re-scan to show the new item
	h.dynamoDBScan(w, r, tableName)
}

// dynamoDBExportTable handles exporting table data to JSON.
func (h *Handler) dynamoDBExportTable(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := r.Context()
	log := logger.Load(ctx)

	// Scan all items
	input := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}
	output, err := h.DynamoDB.Scan(ctx, input)
	if err != nil {
		log.ErrorContext(ctx, "Failed to scan for export", "table", tableName, "error", err)
		http.Error(w, "Failed to export data", http.StatusInternalServerError)

		return
	}

	var items []map[string]any
	if errUnmarshal := attributevalue.UnmarshalListOfMaps(output.Items, &items); errUnmarshal != nil {
		http.Error(w, "Failed to unmarshal data for export", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.json", tableName))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(items)
}

// dynamoDBImportTable handles importing JSON data into a table.
func (h *Handler) dynamoDBImportTable(w http.ResponseWriter, r *http.Request, tableName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	importData := r.FormValue("importData")
	var items []map[string]any
	if err := json.Unmarshal([]byte(importData), &items); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)

		return
	}

	for _, m := range items {
		item, err := attributevalue.MarshalMap(m)
		if err != nil {
			log.ErrorContext(ctx, "Failed to marshal item during import", "error", err)

			continue
		}
		_, _ = h.DynamoDB.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
	}

	// Trigger a scan to refresh results
	h.dynamoDBScan(w, r, tableName)
}

// dynamoDBItemDetail handles getting an item for display/editing.
func (h *Handler) dynamoDBItemDetail(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := r.Context()
	log := logger.Load(ctx)

	// Get table description to find PK/SK names and types
	desc, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		http.Error(w, "Table not found", http.StatusNotFound)

		return
	}

	pkName, skName := h.extractKeys(desc.Table.KeySchema)

	// Get PK/SK values from request (JSON serialized in query params)
	pkValRaw := r.URL.Query().Get("pk")
	skValRaw := r.URL.Query().Get("sk")

	key := make(map[string]types.AttributeValue)

	parseVal := func(_, valRaw string) (types.AttributeValue, error) {
		var v any
		if errUnmarshal := json.Unmarshal([]byte(valRaw), &v); errUnmarshal != nil {
			return nil, fmt.Errorf("failed to unmarshal key value: %w", errUnmarshal)
		}

		return attributevalue.Marshal(v)
	}

	pkAV, errPK := parseVal(pkName, pkValRaw)
	if errPK != nil {
		http.Error(w, "Failed to parse PK", http.StatusBadRequest)

		return
	}
	key[pkName] = pkAV

	if skName != "" && skValRaw != "" {
		skAV, errSK := parseVal(skName, skValRaw)
		if errSK != nil {
			http.Error(w, "Failed to parse SK", http.StatusBadRequest)

			return
		}
		key[skName] = skAV
	}

	output, errGet := h.DynamoDB.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	})
	if errGet != nil {
		log.ErrorContext(ctx, "Failed to get item", "table", tableName, "error", errGet)
		http.Error(w, "Failed to get item", http.StatusInternalServerError)

		return
	}

	if output.Item == nil {
		http.NotFound(w, r)

		return
	}

	var m map[string]any
	if errUnmarshal := attributevalue.UnmarshalMap(output.Item, &m); errUnmarshal != nil {
		log.ErrorContext(ctx, "Failed to unmarshal item", "error", errUnmarshal)
		http.Error(w, "Failed to unmarshal item", http.StatusInternalServerError)

		return
	}

	itemJSON, _ := json.MarshalIndent(m, "", "  ")

	fmt.Fprintf(w, `
        <form hx-post="/dashboard/dynamodb/table/%s/item" 
            hx-on::after-request="if(event.detail.successful) edit_item_modal.close()" 
            hx-target="#scan-results"
            class="space-y-4">
            <div class="form-control">
                <label class="label">
                    <span class="label-text">Item JSON</span>
                </label>
                <textarea name="itemJson" class="textarea textarea-bordered font-mono h-64" required>%s</textarea>
            </div>
            <div class="modal-action">
                <button type="button" class="btn" onclick="edit_item_modal.close()">Cancel</button>
                <button type="submit" class="btn btn-primary">Save Changes</button>
            </div>
        </form>`, tableName, html.EscapeString(string(itemJSON)))
}
