package dashboard

import (
	"encoding/json"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// dynamoDBPartiQL handles the PartiQL query tab on the table detail page.
func (h *DashboardHandler) dynamoDBPartiQL(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := r.Context()
	log := logger.Load(ctx)

	if r.Method != http.MethodPost {
		h.renderTableDetailFragment(w, "partiql-form", map[string]any{
			"TableName": tableName,
			"ActiveTab": "dynamodb",
		})

		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxFormBodySize)

	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid form", http.StatusBadRequest)

		return
	}

	statement := r.FormValue("statement")
	if statement == "" {
		http.Error(w, "Statement is required", http.StatusBadRequest)

		return
	}

	result, execErr := h.DynamoDB.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement: aws.String(statement),
	})
	if execErr != nil {
		log.ErrorContext(ctx, "PartiQL execute failed", "table", tableName, "error", execErr)
		http.Error(w, "Query failed: "+execErr.Error(), http.StatusBadRequest)

		return
	}

	// Convert DynamoDB AttributeValue items to native Go maps for human-readable JSON.
	var nativeItems []map[string]any
	if unmarshalErr := attributevalue.UnmarshalListOfMaps(result.Items, &nativeItems); unmarshalErr != nil {
		http.Error(w, "Failed to unmarshal results: "+unmarshalErr.Error(), http.StatusInternalServerError)

		return
	}

	itemsJSON, marshalErr := json.MarshalIndent(nativeItems, "", "  ")
	if marshalErr != nil {
		http.Error(w, "Failed to marshal results", http.StatusInternalServerError)

		return
	}

	h.renderTableDetailFragment(w, "partiql-results", map[string]any{
		"TableName":  tableName,
		"ActiveTab":  "dynamodb",
		"ResultJSON": string(itemsJSON),
		"ItemCount":  len(result.Items),
	})
}
