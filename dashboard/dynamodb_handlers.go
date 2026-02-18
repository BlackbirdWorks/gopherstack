package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"strings"
	"sync"

	"Gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	defaultCapacity = 5
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
	output, err := h.DynamoDB.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		h.handleListTablesError(w, r, err)

		return
	}

	search := strings.ToLower(r.URL.Query().Get("search"))
	log.InfoContext(ctx, "DynamoDB search", "search", search, "all_tables", output.TableNames)
	tableNames := filterTableNames(search, output.TableNames)
	tableInfos := h.fetchTableInfos(ctx, tableNames)

	// Render table cards
	for _, tableInfo := range tableInfos {
		h.renderFragment(w, "table-card", tableInfo)
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

			desc, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tblName)})
			if err != nil {
				log := logger.Load(ctx)
				log.ErrorContext(ctx, "Failed to describe table for list", "table", tblName, "error", err)

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
		gsiInfo := h.extractIndexInfo(gsi.IndexName, gsi.KeySchema, gsi.Projection, table.AttributeDefinitions)
		info.GlobalSecondaryIndexes = append(info.GlobalSecondaryIndexes, gsiInfo)
	}

	// Add LSI information
	for _, lsi := range table.LocalSecondaryIndexes {
		lsiInfo := h.extractIndexInfo(lsi.IndexName, lsi.KeySchema, lsi.Projection, table.AttributeDefinitions)
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
		toastMessage := fmt.Sprintf(`{"showToast": {"message": "Failed to query table: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`))
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

	h.renderQueryResults(w, result)
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

func (h *Handler) resolveIndexKeys(desc *dynamodb.DescribeTableOutput, indexName string) (string, string) {
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
		if _, err := fmt.Sscanf(limit, "%d", &l); err == nil && l > 0 {
			input.Limit = aws.Int32(l)
		}
	}

	esk := r.FormValue("exclusiveStartKey")
	if esk != "" {
		var eskMap map[string]types.AttributeValue
		if err := json.Unmarshal([]byte(esk), &eskMap); err == nil {
			input.ExclusiveStartKey = eskMap
		}
	}

	output, err := h.DynamoDB.Scan(ctx, input)
	if err != nil {
		log.ErrorContext(ctx, "Failed to scan table", "error", err)
		toastMessage := fmt.Sprintf(`{"showToast": {"message": "Failed to scan table: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`))
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
	h.renderQueryResults(w, result)
}

// renderQueryResults renders query/scan results as HTML.
func (h *Handler) renderQueryResults(w http.ResponseWriter, result QueryResult) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if len(result.Items) == 0 {
		fmt.Fprintf(w, `<div class="alert alert-info"><span>No items found</span></div>`)

		return
	}

	columns, items := h.prepareResultsData(result.Items)
	h.renderResultsTable(w, columns, items)
	h.renderResultsSummary(w, result)
}

func (h *Handler) prepareResultsData(items []map[string]types.AttributeValue) ([]string, []map[string]any) {
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

	return columns, unmarshaledItems
}

func (h *Handler) renderResultsTable(w http.ResponseWriter, columns []string, items []map[string]any) {
	fmt.Fprintf(w, `<div class="overflow-x-auto"><table class="table table-zebra table-sm">`)
	fmt.Fprintf(w, `<thead><tr>`)
	for _, col := range columns {
		fmt.Fprintf(w, `<th>%s</th>`, html.EscapeString(col)) //nolint:gosec // G705: Data is properly escaped with html.EscapeString
	}
	fmt.Fprintf(w, `</tr></thead><tbody>`)

	for _, item := range items {
		fmt.Fprintf(w, `<tr>`)
		for _, col := range columns {
			val := item[col]
			if val == nil {
				fmt.Fprintf(w, `<td class="opacity-30">-</td>`)
			} else {
				jsonVal, _ := json.Marshal(val)
				fmt.Fprintf(w, `<td class="font-mono text-xs">%s</td>`, html.EscapeString(string(jsonVal)))
			}
		}
		fmt.Fprintf(w, `</tr>`)
	}

	fmt.Fprintf(w, `</tbody></table></div>`)
}

func (h *Handler) renderResultsSummary(w http.ResponseWriter, result QueryResult) {
	fmt.Fprintf(w, `<div class="mt-4 flex justify-between items-center bg-base-200 p-4 rounded-lg">`)
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
		attrs = append(attrs, types.AttributeDefinition{AttributeName: &sortKey, AttributeType: sortKeyType})
		keySchema = append(keySchema, types.KeySchemaElement{AttributeName: &sortKey, KeyType: types.KeyTypeRange})
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
		toastMessage := fmt.Sprintf(`{"showToast": {"message": "Failed to create table: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`))
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
		toastMessage := fmt.Sprintf(`{"showToast": {"message": "Failed to delete table: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`))
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
