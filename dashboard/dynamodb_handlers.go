package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
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
func (h *Handler) dynamoDBTableList(w http.ResponseWriter, _ *http.Request) {
	ctx := context.Background()
	output, err := h.DynamoDB.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		h.Logger.Error("Failed to list tables", "error", err)
		http.Error(w, "Failed to list tables", http.StatusInternalServerError)

		return
	}

	var tableInfos []TableInfo
	for _, tableName := range output.TableNames {
		var desc *dynamodb.DescribeTableOutput
		desc, err = h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
		if err != nil {
			continue
		}
		table := desc.Table

		info := TableInfo{
			TableName: *table.TableName,
			ItemCount: *table.ItemCount,
			GSICount:  len(table.GlobalSecondaryIndexes),
			LSICount:  len(table.LocalSecondaryIndexes),
		}

		// Extract keys from KeySchema and AttributeDefinitions
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

		tableInfos = append(tableInfos, info)
	}

	// Render table cards
	for _, tableInfo := range tableInfos {
		h.renderFragment(w, "table-card", tableInfo)
	}
}

// dynamoDBSearch searches tables by name.
func (h *Handler) dynamoDBSearch(w http.ResponseWriter, r *http.Request) {
	// For now, we can reuse table list logic but filter in memory
	// In a real scenario with many tables, we might do client-side filtering or pagination
	h.dynamoDBTableList(w, r)
}

// dynamoDBTableDetail renders the table detail page.
func (h *Handler) dynamoDBTableDetail(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := context.Background()
	output, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
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
		TableName: *table.TableName,
		ItemCount: *table.ItemCount,
		GSICount:  len(table.GlobalSecondaryIndexes),
		LSICount:  len(table.LocalSecondaryIndexes),
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
		IndexName:      *name,
		ProjectionType: string(projection.ProjectionType),
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
		h.Logger.ErrorContext(ctx, "Failed to describe table", "error", err)
		http.Error(w, "Failed to describe table", http.StatusInternalServerError)

		return
	}

	pkName, skName, pkType, skType := h.resolveKeySchema(desc, params.IndexName)

	// Build Expressions
	attrNames := map[string]string{
		"#pk": pkName,
	}
	attrValues := map[string]types.AttributeValue{
		":pkval": h.toAttributeValue(params.PartitionKeyValue, pkType),
	}

	keyCondExp := "#pk = :pkval"

	if skName != "" && params.SortKeyOperator != "" && params.SortKeyValue != "" {
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
	}

	h.executeAndRenderQuery(
		ctx,
		w,
		tableName,
		params.IndexName,
		keyCondExp,
		params.FilterExp,
		params.LimitStr,
		attrNames,
		attrValues,
	)
}

type QueryParams struct {
	IndexName         string
	PartitionKeyValue string
	SortKeyOperator   string
	SortKeyValue      string
	SortKeyValue2     string
	FilterExp         string
	LimitStr          string
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
	tableName, indexName, keyCondExp, filterExp, limitStr string,
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

	output, err := h.DynamoDB.Query(ctx, input)
	if err != nil {
		h.Logger.ErrorContext(ctx, "Failed to query table", "error", err)
		toastMessage := fmt.Sprintf(`{"showToast": {"message": "Failed to query table: %s", "type": "error"}}`,
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

	ctx := context.Background()
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

	output, err := h.DynamoDB.Scan(ctx, input)
	if err != nil {
		h.Logger.Error("Failed to scan table", "error", err)
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

	// Start table
	fmt.Fprintf(w, `<div class="overflow-x-auto"><table class="table table-zebra">`)
	fmt.Fprintf(w, `<thead><tr><th>Item</th></tr></thead><tbody>`)

	// Render each item as JSON
	for _, item := range result.Items {
		// Convert DynamoDB JSON to standard JSON for display
		// This is tricky with the SDK types. We might want a helper.
		// For now, let's just marshal the AttributeValue map directly.
		// A better approach would be to use attributevalue.UnmarshalMap
		// but that requires additional packages.
		// Let's stick to simple marshalling for now, acknowledging it will show types metadata.
		jsonBytes, _ := json.MarshalIndent(item, "", "  ")
		fmt.Fprintf(w, `<tr><td><pre class="json-viewer">%s</pre></td></tr>`, string(jsonBytes))
	}

	fmt.Fprintf(w, `</tbody></table></div>`)
	fmt.Fprintf(w, `<div class="mt-4"><p>Count: %d | Scanned: %d</p></div>`, result.Count, result.ScannedCount)
}

// dynamoDBCreateTable handles table creation requests.
func (h *Handler) dynamoDBCreateTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	if err := r.ParseForm(); err != nil {
		h.Logger.Error("Failed to parse form", "error", err)
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

	_, err := h.DynamoDB.CreateTable(context.Background(), input)
	if err != nil {
		h.Logger.Error("Failed to create table", "error", err)
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

	ctx := context.Background()
	_, err := h.DynamoDB.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: &tableName,
	})

	if err != nil {
		h.Logger.Error("Failed to delete table", "table", tableName, "error", err)
		// Return error alert
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// If HTMX request, we can handle error display gracefully
		// For now, simpler error
		http.Error(w, fmt.Sprintf("Failed to delete table: %v", err), http.StatusInternalServerError)

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
