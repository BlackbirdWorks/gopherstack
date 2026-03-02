package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// dynamoDBIndex renders the DynamoDB index page.
func (h *DashboardHandler) dynamoDBIndex(w http.ResponseWriter, _ *http.Request) {
	data := PageData{
		Title:     "DynamoDB Tables",
		ActiveTab: "dynamodb",
	}
	h.renderTemplate(w, "dynamodb/dynamodb_index.html", data)
}

// dynamoDBTableList returns the list of tables as HTML fragment.
func (h *DashboardHandler) dynamoDBTableList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := logger.Load(ctx)

	search := strings.ToLower(r.URL.Query().Get("search"))
	if search == "" {
		// Try 'q' as used in the template
		search = strings.ToLower(r.URL.Query().Get("q"))
	}

	offset := 0
	if offStr := r.URL.Query().Get("offset"); offStr != "" {
		offset, _ = strconv.Atoi(offStr)
	}

	limit := defaultTableLimit
	if limStr := r.URL.Query().Get("limit"); limStr != "" {
		if l, e := strconv.Atoi(limStr); e == nil && l > 0 {
			limit = l
		}
	} else {
		// Use a smaller default for UI cards if not specified
		limit = 12
	}

	log.DebugContext(ctx, "DynamoDB table list", "search", search, "offset", offset, "limit", limit)

	// In Gopherstack (local dev), we probably have few enough tables to fetch all and filter/paginate in memory
	// This ensures "search searches all tables" as requested.
	allNames, err := h.fetchAllTableNames(ctx)
	if err != nil {
		h.handleListTablesError(w, r, err)

		return
	}

	filteredNames := filterTableNames(search, allNames)
	totalFiltered := len(filteredNames)

	end := min(offset+limit, totalFiltered)

	var tableNames []string
	if offset < totalFiltered {
		tableNames = filteredNames[offset:end]
	}

	tableInfos := h.fetchTableInfos(ctx, tableNames)

	// Render table cards
	for _, tableInfo := range tableInfos {
		h.renderFragment(w, "table-card", tableInfo)
	}

	// Render pagination if needed
	if totalFiltered > limit || offset > 0 {
		pagination := PaginationInfo{
			TotalItems:   totalFiltered,
			Offset:       offset,
			Limit:        limit,
			CurrentPage:  (offset / limit) + 1,
			TotalPages:   (totalFiltered + limit - 1) / limit,
			HasPrev:      offset > 0,
			HasNext:      end < totalFiltered,
			PrevOffset:   max(0, offset-limit),
			NextOffset:   end,
			SearchQuery:  search,
			BaseEndpoint: "/dashboard/dynamodb/tables",
			TargetID:     "#table-list",
		}
		h.renderFragment(w, "pagination", pagination)
	}
}

func (h *DashboardHandler) fetchAllTableNames(ctx context.Context) ([]string, error) {
	var allNames []string
	var lastEvaluatedTable *string

	for {
		input := &dynamodb.ListTablesInput{}
		if lastEvaluatedTable != nil {
			input.ExclusiveStartTableName = lastEvaluatedTable
		}

		output, err := h.DynamoDB.ListTables(ctx, input)
		if err != nil {
			return nil, err
		}

		allNames = append(allNames, output.TableNames...)

		if output.LastEvaluatedTableName == nil {
			return allNames, nil
		}

		lastEvaluatedTable = output.LastEvaluatedTableName
	}
}

func (h *DashboardHandler) handleListTablesError(w http.ResponseWriter, r *http.Request, err error) {
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

func (h *DashboardHandler) fetchTableInfos(ctx context.Context, tableNames []string) []TableInfo {
	if len(tableNames) == 0 {
		return []TableInfo{}
	}

	// Use a bounded worker pool (max 8 concurrent fetches)
	const maxConcurrent = 8
	semaphore := make(chan struct{}, maxConcurrent)

	tableInfos := make([]TableInfo, len(tableNames))
	var wg sync.WaitGroup
	mu := lockmetrics.New("dashboard.tables")
	defer mu.Close()

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

			mu.Lock("fetchTableInfos")
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
func (h *DashboardHandler) dynamoDBSearch(w http.ResponseWriter, r *http.Request) {
	// For now, we can reuse table list logic but filter in memory
	// In a real scenario with many tables, we might do client-side filtering or pagination
	h.dynamoDBTableList(w, r)
}

// dynamoDBTableDetail renders the table detail page.
func (h *DashboardHandler) dynamoDBTableDetail(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := r.Context()
	output, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	if err != nil {
		http.NotFound(w, r)

		return
	}
	// Fetch TTL info if available
	ttlDesc, _ := h.DynamoDB.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: &tableName,
	})

	info := h.extractTableInfo(output.Table)
	if ttlDesc != nil && ttlDesc.TimeToLiveDescription != nil {
		info.TTLStatus = string(ttlDesc.TimeToLiveDescription.TimeToLiveStatus)
		info.TTLAttribute = aws.ToString(ttlDesc.TimeToLiveDescription.AttributeName)
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

// extractTableInfo extracts display information from a DescribeTable output.
func (h *DashboardHandler) extractTableInfo(table *types.TableDescription) TableInfo {
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

func (h *DashboardHandler) extractIndexInfo(
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

// dynamoDBCreateTable handles table creation requests.
func (h *DashboardHandler) dynamoDBCreateTable(w http.ResponseWriter, r *http.Request) {
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
func (h *DashboardHandler) dynamoDBDeleteTable(w http.ResponseWriter, r *http.Request, tableName string) {
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
func (h *DashboardHandler) dynamoDBPurge(w http.ResponseWriter, r *http.Request) {
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

	// Return the refreshed (empty) table list so the UI updates immediately.
	h.dynamoDBTableList(w, r)
}

// dynamoDBUpdateTTL handles updating TTL configuration for a table.
func (h *DashboardHandler) dynamoDBUpdateTTL(w http.ResponseWriter, r *http.Request, tableName string) {
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

	attributeName := r.FormValue("attributeName")
	enabled := r.FormValue("enabled") == "on"

	input := &dynamodb.UpdateTimeToLiveInput{
		TableName: &tableName,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: &attributeName,
			Enabled:       aws.Bool(enabled),
		},
	}

	_, err := h.DynamoDB.UpdateTimeToLive(ctx, input)
	if err != nil {
		log.ErrorContext(ctx, "Failed to update TTL", "table", tableName, "error", err)
		toastMessage := fmt.Sprintf(
			`{"showToast": {"message": "Failed to update TTL: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`),
		)
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	// Success toast
	toastMessage := fmt.Sprintf(
		`{"showToast": {"message": "TTL %s successfully", "type": "success"}}`,
		func() string {
			if enabled {
				return "enabled"
			}

			return "disabled"
		}(),
	)
	w.Header().Set("Hx-Trigger", toastMessage)

	// Fetch updated data for the fragment
	desc, err := h.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		h.Logger.ErrorContext(ctx, "Failed to fetch table for partial reload", "table", tableName, "error", err)

		return
	}

	ttlDesc, err := h.DynamoDB.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: &tableName})
	if err != nil {
		h.Logger.ErrorContext(ctx, "Failed to fetch TTL for partial reload", "table", tableName, "error", err)

		return
	}

	data := h.extractTableInfo(desc.Table)
	if ttlDesc.TimeToLiveDescription != nil {
		data.TTLStatus = string(ttlDesc.TimeToLiveDescription.TimeToLiveStatus)
		data.TTLAttribute = aws.ToString(ttlDesc.TimeToLiveDescription.AttributeName)
	}

	// Re-render table overview fragment
	h.renderTableDetailFragment(w, "table-overview", data)
}
