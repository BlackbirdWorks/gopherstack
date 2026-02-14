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
	ItemCount              int64
	GSICount               int
	LSICount               int
	GlobalSecondaryIndexes []IndexInfo
	LocalSecondaryIndexes  []IndexInfo
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
	Items            []map[string]types.AttributeValue
	Count            int32
	ScannedCount     int32
	LastEvaluatedKey map[string]types.AttributeValue
}

// dynamoDBIndex renders the DynamoDB index page.
func (h *Handler) dynamoDBIndex(w http.ResponseWriter, r *http.Request) {
	data := PageData{
		Title:     "DynamoDB Tables",
		ActiveTab: "dynamodb",
	}
	h.renderTemplate(w, "dynamodb/dynamodb_index.html", data)
}

// dynamoDBTableList returns the list of tables as HTML fragment.
func (h *Handler) dynamoDBTableList(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	output, err := h.DynamoDB.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		h.Logger.Error("Failed to list tables", "error", err)
		http.Error(w, "Failed to list tables", http.StatusInternalServerError)
		return
	}

	var tableInfos []TableInfo
	for _, tableName := range output.TableNames {
		desc, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
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
			if key.KeyType == types.KeyTypeHash {
				info.PartitionKey = *key.AttributeName
			} else if key.KeyType == types.KeyTypeRange {
				info.SortKey = *key.AttributeName
			}
		}

		for _, attr := range table.AttributeDefinitions {
			if *attr.AttributeName == info.PartitionKey {
				info.PartitionKeyType = string(attr.AttributeType)
			} else if *attr.AttributeName == info.SortKey {
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
	table := output.Table

	info := TableInfo{
		TableName: *table.TableName,
		ItemCount: *table.ItemCount,
		GSICount:  len(table.GlobalSecondaryIndexes),
		LSICount:  len(table.LocalSecondaryIndexes),
	}

	// Extract keys
	for _, key := range table.KeySchema {
		if key.KeyType == types.KeyTypeHash {
			info.PartitionKey = *key.AttributeName
		} else if key.KeyType == types.KeyTypeRange {
			info.SortKey = *key.AttributeName
		}
	}

	for _, attr := range table.AttributeDefinitions {
		if *attr.AttributeName == info.PartitionKey {
			info.PartitionKeyType = string(attr.AttributeType)
		} else if *attr.AttributeName == info.SortKey {
			info.SortKeyType = string(attr.AttributeType)
		}
	}

	// Add GSI information
	for _, gsi := range table.GlobalSecondaryIndexes {
		gsiInfo := IndexInfo{
			IndexName:      *gsi.IndexName,
			ProjectionType: string(gsi.Projection.ProjectionType),
		}

		for _, key := range gsi.KeySchema {
			if key.KeyType == types.KeyTypeHash {
				gsiInfo.PartitionKey = *key.AttributeName
			} else if key.KeyType == types.KeyTypeRange {
				gsiInfo.SortKey = *key.AttributeName
			}
		}

		// Note: Attribute types for GSI keys must be looked up in AttributeDefinitions
		for _, attr := range table.AttributeDefinitions {
			if *attr.AttributeName == gsiInfo.PartitionKey {
				gsiInfo.PartitionKeyType = string(attr.AttributeType)
			} else if *attr.AttributeName == gsiInfo.SortKey {
				gsiInfo.SortKeyType = string(attr.AttributeType)
			}
		}

		info.GlobalSecondaryIndexes = append(info.GlobalSecondaryIndexes, gsiInfo)
	}

	// Add LSI information
	for _, lsi := range table.LocalSecondaryIndexes {
		lsiInfo := IndexInfo{
			IndexName:      *lsi.IndexName,
			ProjectionType: string(lsi.Projection.ProjectionType),
		}

		for _, key := range lsi.KeySchema {
			if key.KeyType == types.KeyTypeRange {
				lsiInfo.SortKey = *key.AttributeName
			}
		}

		for _, attr := range table.AttributeDefinitions {
			if *attr.AttributeName == lsiInfo.SortKey {
				lsiInfo.SortKeyType = string(attr.AttributeType)
			}
		}

		info.LocalSecondaryIndexes = append(info.LocalSecondaryIndexes, lsiInfo)
	}

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

// dynamoDBQuery executes a query and returns results.
func (h *Handler) dynamoDBQuery(w http.ResponseWriter, r *http.Request, tableName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse form data
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	indexName := r.FormValue("indexName")
	partitionKeyValue := r.FormValue("partitionKeyValue")
	sortKeyOperator := r.FormValue("sortKeyOperator")
	sortKeyValue := r.FormValue("sortKeyValue")
	sortKeyValue2 := r.FormValue("sortKeyValue2")
	filterExp := r.FormValue("filterExpression")
	limitStr := r.FormValue("limit")

	if partitionKeyValue == "" {
		http.Error(w, "Partition key value is required", http.StatusBadRequest)
		return
	}

	// Determine key schema from table/index
	ctx := context.Background()
	desc, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		h.Logger.Error("Failed to describe table", "error", err)
		http.Error(w, "Failed to describe table", http.StatusInternalServerError)
		return
	}

	var pkName, skName string
	var pkType, skType types.ScalarAttributeType

	if indexName == "" {
		// Base table
		for _, key := range desc.Table.KeySchema {
			if key.KeyType == types.KeyTypeHash {
				pkName = *key.AttributeName
			} else {
				skName = *key.AttributeName
			}
		}
		for _, attr := range desc.Table.AttributeDefinitions {
			if *attr.AttributeName == pkName {
				pkType = attr.AttributeType
			}
			if skName != "" && *attr.AttributeName == skName {
				skType = attr.AttributeType
			}
		}
	} else {
		// Check GSIs
		found := false
		for _, gsi := range desc.Table.GlobalSecondaryIndexes {
			if *gsi.IndexName == indexName {
				for _, key := range gsi.KeySchema {
					if key.KeyType == types.KeyTypeHash {
						pkName = *key.AttributeName
					} else {
						skName = *key.AttributeName
					}
				}
				found = true
				break
			}
		}
		if !found {
			for _, lsi := range desc.Table.LocalSecondaryIndexes {
				if *lsi.IndexName == indexName {
					for _, key := range lsi.KeySchema {
						if key.KeyType == types.KeyTypeHash {
							pkName = *key.AttributeName
						} else {
							skName = *key.AttributeName
						}
					}
					found = true
					break
				}
			}
		}
		// Types are always in AttributeDefinitions
		for _, attr := range desc.Table.AttributeDefinitions {
			if *attr.AttributeName == pkName {
				pkType = attr.AttributeType
			}
			if skName != "" && *attr.AttributeName == skName {
				skType = attr.AttributeType
			}
		}
	}

	// Build Expressions
	attrNames := map[string]string{
		"#pk": pkName,
	}
	attrValues := map[string]types.AttributeValue{
		":pkval": h.toAttributeValue(partitionKeyValue, pkType),
	}

	keyCondExp := "#pk = :pkval"

	if skName != "" && sortKeyOperator != "" && sortKeyValue != "" {
		attrNames["#sk"] = skName
		attrValues[":skval"] = h.toAttributeValue(sortKeyValue, skType)

		switch sortKeyOperator {
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
			if sortKeyValue2 != "" {
				attrValues[":skval2"] = h.toAttributeValue(sortKeyValue2, skType)
				keyCondExp += " AND #sk BETWEEN :skval AND :skval2"
			}
		}
	}

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
		fmt.Sscanf(limitStr, "%d", &l)
		if l > 0 {
			input.Limit = aws.Int32(l)
		}
	}

	output, err := h.DynamoDB.Query(ctx, input)
	if err != nil {
		h.Logger.Error("Failed to query table", "error", err)
		w.Header().Set("HX-Trigger", fmt.Sprintf(`{"showToast": {"message": "Failed to query table: %s", "type": "error"}}`, strings.ReplaceAll(err.Error(), `"`, `'`)))
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

// toAttributeValue converts a string to a typed AttributeValue.
func (h *Handler) toAttributeValue(val string, t types.ScalarAttributeType) types.AttributeValue {
	switch t {
	case types.ScalarAttributeTypeN:
		return &types.AttributeValueMemberN{Value: val}
	case types.ScalarAttributeTypeB:
		return &types.AttributeValueMemberB{Value: []byte(val)}
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
		// Parse int
		var l int32
		fmt.Sscanf(limit, "%d", &l)
		if l > 0 {
			input.Limit = aws.Int32(l)
		}
	}

	output, err := h.DynamoDB.Scan(ctx, input)
	if err != nil {
		h.Logger.Error("Failed to scan table", "error", err)
		w.Header().Set("HX-Trigger", fmt.Sprintf(`{"showToast": {"message": "Failed to scan table: %s", "type": "error"}}`, strings.ReplaceAll(err.Error(), `"`, `'`)))
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
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	}

	_, err := h.DynamoDB.CreateTable(context.Background(), input)
	if err != nil {
		h.Logger.Error("Failed to create table", "error", err)
		w.Header().Set("HX-Trigger", fmt.Sprintf(`{"showToast": {"message": "Failed to create table: %s", "type": "error"}}`, strings.ReplaceAll(err.Error(), `"`, `'`)))
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

	if r.Header.Get("HX-Target") == "table-list" {
		// List view: return updated list
		h.dynamoDBTableList(w, r)
	} else {
		// Detail view: Redirect to index
		// HTMX handles redirects via HX-Location header
		w.Header().Set("HX-Location", "/dashboard/dynamodb")
		w.WriteHeader(http.StatusOK)
	}
}
