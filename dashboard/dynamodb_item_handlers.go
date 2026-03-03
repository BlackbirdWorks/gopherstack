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

	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// dynamoDBQuery executes a query and returns results.
func (h *DashboardHandler) dynamoDBQuery(w http.ResponseWriter, r *http.Request, tableName string) {
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

func (h *DashboardHandler) buildKeyCondition(
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

func (h *DashboardHandler) parseQueryRequest(w http.ResponseWriter, r *http.Request) (QueryParams, bool) {
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

func (h *DashboardHandler) executeAndRenderQuery(
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

func (h *DashboardHandler) resolveKeySchema(
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

func (h *DashboardHandler) resolveIndexKeys(
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

func (h *DashboardHandler) extractKeys(keySchema []types.KeySchemaElement) (string, string) {
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
func (h *DashboardHandler) toAttributeValue(val string, t types.ScalarAttributeType) types.AttributeValue {
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
func (h *DashboardHandler) dynamoDBScan(w http.ResponseWriter, r *http.Request, tableName string) {
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
func (h *DashboardHandler) renderQueryResults(
	w http.ResponseWriter,
	result QueryResult,
	tableName, pkName, skName string,
) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if len(result.Items) == 0 {
		fmt.Fprintf(
			w,
			`
<div class="flex flex-col items-center justify-center p-12 bg-white dark:bg-gray-800 border-2 border-dashed border-gray-200 dark:border-gray-700 rounded-xl">
    <svg class="w-16 h-16 text-gray-300 dark:text-gray-600 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path>
    </svg>
    <h3 class="text-lg font-medium text-gray-900 dark:text-white">No items found</h3>
    <p class="mt-1 text-sm text-gray-500 dark:text-gray-400 max-w-sm text-center">There are no items matching your query or scan parameters in this table.</p>
    <div class="mt-6">
        <button data-modal-target="new_item_modal" data-modal-toggle="new_item_modal" class="inline-flex items-center px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 focus:ring-4 focus:outline-none focus:ring-blue-300 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800">
            <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"></path></svg>
            Create Item
        </button>
    </div>
</div>`,
		)

		return
	}

	columns, items := h.prepareResultsData(result.Items, pkName, skName)
	h.renderResultsTable(w, tableName, columns, items, pkName, skName)
	h.renderResultsSummary(w, result)
}

func (h *DashboardHandler) prepareResultsData(
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

func (h *DashboardHandler) renderResultsTable(
	w http.ResponseWriter,
	tableName string,
	columns []string,
	items []map[string]any,
	pkName, skName string,
) {
	const tableWrapper = `<div class="relative overflow-x-auto border border-gray-200 ` +
		`rounded-lg shadow dark:border-gray-700 mb-4">`
	fmt.Fprint(w, tableWrapper)
	fmt.Fprintf(w, `<table class="w-full text-sm text-left text-gray-500 dark:text-gray-400">`)
	fmt.Fprintf(w, `<thead class="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-gray-700 dark:text-gray-400"><tr>`)
	for _, col := range columns {
		label := html.EscapeString(col)
		switch col {
		case pkName:
			label += " <span class='bg-blue-100 text-blue-800 text-[10px] font-medium px-1.5 py-0.5 " +
				"rounded dark:bg-blue-900 dark:text-blue-300 ml-1'>PK</span>"
		case skName:
			label += " <span class='bg-purple-100 text-purple-800 text-[10px] font-medium px-1.5 py-0.5 " +
				"rounded dark:bg-purple-900 dark:text-purple-300 ml-1'>SK</span>"
		}
		// #nosec G705 -- Data is escaped with html.EscapeString, false positive
		// #nosec G705
		fmt.Fprintf(w, `<th scope="col" class="px-4 py-3">%s</th>`, label)
	}
	fmt.Fprintf(w, `<th scope="col" class="px-4 py-3">Actions</th>`)
	fmt.Fprintf(w, `</tr></thead><tbody>`)

	for _, item := range items {
		fmt.Fprintf(w, `<tr class="bg-white border-b dark:bg-gray-800 dark:border-gray-700">`)
		for _, col := range columns {
			val := item[col]
			if val == nil {
				fmt.Fprintf(w, `<td class="px-4 py-2 opacity-30">-</td>`)
			} else {
				jsonVal, _ := json.Marshal(val)
				jsonStr := string(jsonVal)
				// #nosec G705
				fmt.Fprintf(w, `<td class="px-4 py-2 font-mono text-xs max-w-xs truncate" title="%s">%s</td>`,
					html.EscapeString(jsonStr), html.EscapeString(jsonStr))
			}
		}

		// Row actions
		h.renderItemActions(w, tableName, pkName, skName, item)

		fmt.Fprintf(w, `</tr>`)
	}

	fmt.Fprintf(w, `</tbody></table></div>`)
}

func (h *DashboardHandler) renderItemActions(
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

	fmt.Fprintf(w, `<td class="px-4 py-2">
            <div class="flex gap-1">
                <button class="text-blue-600 dark:text-blue-500 hover:underline text-xs font-medium"
                    hx-get="/dashboard/dynamodb/table/%s/item?pk=%s&sk=%s"
                    hx-target="#edit_item_modal_content"
                    data-modal-target="edit_item_modal" data-modal-toggle="edit_item_modal"
                    onclick="const m=document.getElementById('edit_item_modal');if(m){`+
		`m.classList.remove('hidden');m.classList.add('flex')}">
                    Edit
                </button>
                <button class="text-red-600 dark:text-red-500 hover:underline text-xs font-medium"
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

func (h *DashboardHandler) renderResultsSummary(w http.ResponseWriter, result QueryResult) {
	fmt.Fprintf(
		w,
		`<div class="mt-4 flex justify-between items-center bg-gray-50 dark:bg-gray-800 p-4 `+
			`rounded-lg border border-gray-200 dark:border-gray-700">`,
	)
	fmt.Fprintf(
		w,
		`<div><p class="text-sm font-medium text-gray-700 dark:text-gray-300">Count: %d `+
			`| Scanned: %d</p></div>`,
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
			`<button class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 `+
				`font-medium rounded-lg text-xs px-3 py-1.5 dark:bg-blue-600 dark:hover:bg-blue-700 `+
				`dark:focus:ring-blue-800" hx-include="closest form" hx-vals='{"exclusiveStartKey": %s}' `+
				`hx-post="" hx-target="closest #query-results, closest #scan-results">Next Page &rarr;</button>`,
			string(kb),
		)
		fmt.Fprintf(w, `</div>`)
	}
	fmt.Fprintf(w, `</div>`)
}

// dynamoDBDeleteItem handles item deletion.
func (h *DashboardHandler) dynamoDBDeleteItem(w http.ResponseWriter, r *http.Request, tableName string) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	key, errKey := h.parseItemKey(ctx, tableName, r.URL.Query().Get("pk"), r.URL.Query().Get("sk"))
	if errKey != nil {
		log.ErrorContext(ctx, "Failed to parse key", "error", errKey)

		var rnf *types.ResourceNotFoundException
		if errors.As(errKey, &rnf) {
			http.Error(w, "Table not found", http.StatusNotFound)

			return
		}

		http.Error(w, errKey.Error(), http.StatusBadRequest)

		return
	}

	_, err := h.DynamoDB.DeleteItem(ctx, &dynamodb.DeleteItemInput{
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
func (h *DashboardHandler) dynamoDBCreateItem(w http.ResponseWriter, r *http.Request, tableName string) {
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
func (h *DashboardHandler) dynamoDBExportTable(w http.ResponseWriter, r *http.Request, tableName string) {
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
func (h *DashboardHandler) dynamoDBImportTable(w http.ResponseWriter, r *http.Request, tableName string) {
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
func (h *DashboardHandler) dynamoDBItemDetail(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := r.Context()
	log := logger.Load(ctx)

	pkVal := r.URL.Query().Get("pk")
	skVal := r.URL.Query().Get("sk")
	log.DebugContext(ctx, "dynamoDBItemDetail request", "table", tableName, "pk", pkVal, "sk", skVal)

	key, errKey := h.parseItemKey(ctx, tableName, pkVal, skVal)
	if errKey != nil {
		log.ErrorContext(ctx, "Failed to parse item key", "error", errKey)

		return
	}

	output, _ := h.DynamoDB.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	})
	log.DebugContext(ctx, "dynamoDBItemDetail GetItem success", "table", tableName)
	if output.Item == nil {
		log.WarnContext(ctx, "Item not found", "table", tableName)
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

	h.renderEditItemForm(w, tableName, string(itemJSON))
}

// parseItemKey parses PK/SK from request query params into a DynamoDB key map.
func (h *DashboardHandler) parseItemKey(
	ctx context.Context,
	tableName, pkValRaw, skValRaw string,
) (map[string]types.AttributeValue, error) {
	desc, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe table: %w", err)
	}

	pkName, skName := h.extractKeys(desc.Table.KeySchema)
	key := make(map[string]types.AttributeValue)

	parseVal := func(name, valRaw string) (types.AttributeValue, error) {
		ad := findAttrDef(desc.Table.AttributeDefinitions, name)
		if ad == nil {
			return nil, fmt.Errorf("%w for %s", errAttrDefNotFound, name)
		}

		var v any
		if errUnmarshal := json.Unmarshal([]byte(valRaw), &v); errUnmarshal != nil {
			return nil, fmt.Errorf("failed to unmarshal key value: %w", errUnmarshal)
		}

		return attributevalue.Marshal(v)
	}

	pkAV, errPK := parseVal(pkName, pkValRaw)
	if errPK != nil {
		return nil, errPK
	}
	key[pkName] = pkAV

	if skName != "" && skValRaw != "" {
		skAV, errSK := parseVal(skName, skValRaw)
		if errSK != nil {
			return nil, errSK
		}
		key[skName] = skAV
	}

	return key, nil
}

// renderEditItemForm renders the HTML form for editing a DynamoDB item.
func (h *DashboardHandler) renderEditItemForm(w http.ResponseWriter, tableName, itemJSON string) {
	fmt.Fprintf(w, `
        <form hx-post="/dashboard/dynamodb/table/%s/item"
            hx-on::after-request="if(event.detail.successful) { `+
		`const m = document.getElementById('edit_item_modal'); m.classList.add('hidden'); m.classList.remove('flex'); }"
            hx-target="#scan-results"
            class="space-y-4">
            <div>
                <label for="editItemJson" `+
		`class="block mb-2 text-sm font-medium text-gray-900 dark:text-white">Item JSON</label>
                <textarea name="itemJson" id="editItemJson" rows="12"
                    class="block p-2.5 w-full text-sm text-gray-900 bg-gray-50 rounded-lg border border-gray-300 `+
		`focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-600 dark:border-gray-500 `+
		`dark:placeholder-gray-400 dark:text-white font-mono"
                    required>%s</textarea>
            </div>
            <div class="flex justify-end gap-2 pt-4">
                <button type="button"
                    class="text-gray-500 bg-white hover:bg-gray-100 focus:ring-4 focus:outline-none `+
		`focus:ring-gray-200 rounded-lg border border-gray-200 text-sm font-medium px-5 py-2.5 `+
		`hover:text-gray-900 focus:z-10 dark:bg-gray-700 dark:text-gray-300 dark:border-gray-500 `+
		`dark:hover:text-white dark:hover:bg-gray-600 dark:focus:ring-gray-600"
                    data-modal-hide="edit_item_modal"
                    onclick="const m=document.getElementById('edit_item_modal');`+
		`m.classList.add('hidden');m.classList.remove('flex')">Cancel</button>
                <button type="submit"
                    class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:outline-none `+
		`focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 text-center dark:bg-blue-600 `+
		`dark:hover:bg-blue-700 dark:focus:ring-blue-800">Save Changes</button>
            </div>
        </form>`, tableName, html.EscapeString(itemJSON))
}
