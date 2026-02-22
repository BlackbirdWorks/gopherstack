package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// dynamoDBUpdateStreams handles enabling/disabling DynamoDB Streams on a table.
func (h *DashboardHandler) dynamoDBUpdateStreams(w http.ResponseWriter, r *http.Request, tableName string) {
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

	enabled := r.FormValue("enabled") == "on"
	viewType := r.FormValue("viewType")

	db := h.DDBOps.Backend

	streamsBackend, ok := db.(ddbbackend.StreamsBackend)
	if !ok {
		http.Error(w, "Streams not supported by backend", http.StatusInternalServerError)

		return
	}

	var opErr error

	if enabled {
		opErr = streamsBackend.EnableStream(ctx, tableName, viewType)
	} else {
		opErr = streamsBackend.DisableStream(ctx, tableName)
	}

	if opErr != nil {
		log.ErrorContext(ctx, "Failed to update streams", "table", tableName, "error", opErr)
		toastMessage := fmt.Sprintf(
			`{"showToast": {"message": "Failed to update Streams: %s", "type": "error"}}`,
			strings.ReplaceAll(opErr.Error(), `"`, `'`),
		)
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	label := "disabled"
	if enabled {
		label = "enabled"
	}

	toastMessage := fmt.Sprintf(
		`{"showToast": {"message": "Streams %s successfully", "type": "success"}}`,
		label,
	)
	w.Header().Set("Hx-Trigger", toastMessage)

	// Re-render the overview fragment
	info, err := h.fetchTableInfoWithStreams(ctx, tableName)
	if err != nil {
		log.ErrorContext(ctx, "Failed to reload table for streams partial", "table", tableName, "error", err)

		return
	}

	h.renderPageFragment(w, "dynamodb/table_detail.html", "table-overview", info)
}

// dynamoDBStreamEvents returns the stream event log as an HTMX fragment.
func (h *DashboardHandler) dynamoDBStreamEvents(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := r.Context()

	info, err := h.fetchTableInfoWithStreams(ctx, tableName)
	if err != nil {
		http.Error(w, "table not found", http.StatusNotFound)

		return
	}

	h.renderPageFragment(w, "dynamodb/table_detail.html", "stream-events", info)
}

// fetchTableInfoWithStreams builds a TableInfo that includes stream state from the in-memory backend.
func (h *DashboardHandler) fetchTableInfoWithStreams(_ context.Context, tableName string) (TableInfo, error) {
	// Directly read from the in-memory backend for stream state.
	memDB, ok := h.DDBOps.Backend.(streamStateReader)
	if !ok {
		return TableInfo{TableName: tableName}, nil
	}

	table, exists := memDB.GetTable(tableName)
	if !exists {
		return TableInfo{}, ddbbackend.NewResourceNotFoundException(fmt.Sprintf("table not found: %s", tableName))
	}

	return buildTableInfoFromInMemory(table), nil
}

// streamStateReader is satisfied by *ddbbackend.InMemoryDB.
type streamStateReader interface {
	GetTable(name string) (*ddbbackend.Table, bool)
}

// buildTableInfoFromInMemory converts in-memory table state to a dashboard TableInfo.
func buildTableInfoFromInMemory(table *ddbbackend.Table) TableInfo {
	info := TableInfo{
		TableName:      table.Name,
		ItemCount:      int64(len(table.Items)),
		GSICount:       len(table.GlobalSecondaryIndexes),
		LSICount:       len(table.LocalSecondaryIndexes),
		StreamsEnabled: table.StreamsEnabled,
		StreamViewType: table.StreamViewType,
		StreamARN:      table.StreamARN,
	}

	// Extract key schema
	for _, k := range table.KeySchema {
		switch k.KeyType {
		case "HASH":
			info.PartitionKey = k.AttributeName
		case "RANGE":
			info.SortKey = k.AttributeName
		}
	}

	for _, attr := range table.AttributeDefinitions {
		switch attr.AttributeName {
		case info.PartitionKey:
			info.PartitionKeyType = attr.AttributeType
		case info.SortKey:
			info.SortKeyType = attr.AttributeType
		}
	}

	// Last 50 stream events (newest first)
	const maxDisplayEvents = 50

	events := table.StreamRecords
	if len(events) > maxDisplayEvents {
		events = events[len(events)-maxDisplayEvents:]
	}

	rows := make([]StreamEventRow, 0, len(events))
	for i := len(events) - 1; i >= 0; i-- {
		rows = append(rows, StreamEventRow{
			EventID:   events[i].EventID,
			EventName: events[i].EventName,
			Timestamp: events[i].ApproximateCreationDateTime,
		})
	}
	info.StreamEvents = rows

	return info
}
