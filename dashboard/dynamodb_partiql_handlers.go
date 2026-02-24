package dashboard

import (
	"encoding/json"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// dynamoDBPartiQL handles the PartiQL query tab on the table detail page.
func (h *DashboardHandler) dynamoDBPartiQL(w http.ResponseWriter, r *http.Request, tableName string) {
	ctx := r.Context()
	log := logger.Load(ctx)

	if r.Method == http.MethodPost {
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

		// Convert items to JSON for display
		itemsJSON, marshalErr := json.MarshalIndent(result.Items, "", "  ")
		if marshalErr != nil {
			http.Error(w, "Failed to marshal results", http.StatusInternalServerError)

			return
		}

		h.renderPageFragment(w, "dynamodb/table_detail.html", "partiql-results", map[string]interface{}{
			"TableName":  tableName,
			"ActiveTab":  "dynamodb",
			"ResultJSON": string(itemsJSON),
			"ItemCount":  len(result.Items),
		})

		return
	}

	h.renderPageFragment(w, "dynamodb/table_detail.html", "partiql-form", map[string]interface{}{
		"TableName": tableName,
		"ActiveTab": "dynamodb",
	})
}
