package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
)

// ErrInvalidStatement is returned when a PartiQL statement cannot be parsed.
var ErrInvalidStatement = errors.New("invalid PartiQL statement")

// fromClauseRegex extracts the table name from a SELECT ... FROM "tableName" PartiQL statement.
// Supports DynamoDB table names: alphanumeric, hyphen, dot, and underscore.
var fromClauseRegex = regexp.MustCompile(`(?i)FROM\s+"([\w.\-]+)"`)

// executeStatementRequest is the wire format for ExecuteStatement.
type executeStatementRequest struct {
	Statement  string           `json:"Statement"`
	NextToken  string           `json:"NextToken,omitempty"`
	Parameters []map[string]any `json:"Parameters,omitempty"`
}

// executeStatementResponse is the wire response for ExecuteStatement.
// Items uses the DynamoDB wire format (map[string]any with {"S":…}, {"N":…} etc.)
// so that the AWS SDK can deserialise it correctly.
type executeStatementResponse struct {
	NextToken string           `json:"NextToken,omitempty"`
	Items     []map[string]any `json:"Items"`
}

// batchStatementRequest is one statement entry inside BatchExecuteStatement.
type batchStatementRequest struct {
	Statement  string           `json:"Statement"`
	Parameters []map[string]any `json:"Parameters,omitempty"`
}

// batchExecuteStatementRequest is the wire format for BatchExecuteStatement.
type batchExecuteStatementRequest struct {
	Statements []batchStatementRequest `json:"Statements"`
}

// batchStatementResponse is one result entry inside BatchExecuteStatement response.
type batchStatementResponse struct {
	Item  map[string]any       `json:"Item,omitempty"`
	Error *batchStatementError `json:"Error,omitempty"`
}

type batchStatementError struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

// batchExecuteStatementResponse is the wire response for BatchExecuteStatement.
type batchExecuteStatementResponse struct {
	Responses []batchStatementResponse `json:"Responses"`
}

// handleExecuteStatement handles a single PartiQL SELECT statement via a full table scan.
func (h *DynamoDBHandler) handleExecuteStatement(ctx context.Context, body []byte) (any, error) {
	var req executeStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	tableName, err := extractTableNameFromStatement(req.Statement)
	if err != nil {
		return nil, err
	}

	out, err := h.Backend.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, err
	}

	// Convert SDK attribute values to DynamoDB wire format ({"S":…}, {"N":…}, etc.)
	// so the AWS SDK client can correctly deserialise the response.
	wireItems := make([]map[string]any, 0, len(out.Items))
	for _, item := range out.Items {
		wireItems = append(wireItems, models.FromSDKItem(item))
	}

	return &executeStatementResponse{Items: wireItems}, nil
}

// handleBatchExecuteStatement handles multiple PartiQL statements via individual scans.
func (h *DynamoDBHandler) handleBatchExecuteStatement(ctx context.Context, body []byte) (any, error) {
	var req batchExecuteStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	responses := make([]batchStatementResponse, 0, len(req.Statements))

	for _, stmt := range req.Statements {
		stmtBody, _ := json.Marshal(executeStatementRequest{
			Statement:  stmt.Statement,
			Parameters: stmt.Parameters,
		})

		result, err := h.handleExecuteStatement(ctx, stmtBody)
		if err != nil {
			responses = append(responses, batchStatementResponse{
				Error: &batchStatementError{
					Code:    "StatementError",
					Message: err.Error(),
				},
			})

			continue
		}

		execResp, ok := result.(*executeStatementResponse)
		if !ok || len(execResp.Items) == 0 {
			responses = append(responses, batchStatementResponse{})

			continue
		}

		// Return the first item for each statement (BatchExecuteStatement returns one item per statement).
		responses = append(responses, batchStatementResponse{Item: execResp.Items[0]})
	}

	return &batchExecuteStatementResponse{Responses: responses}, nil
}

// extractTableNameFromStatement extracts the table name from a SELECT PartiQL statement.
func extractTableNameFromStatement(statement string) (string, error) {
	const minMatchLen = 2 // full match + first capture group

	matches := fromClauseRegex.FindStringSubmatch(statement)
	if len(matches) < minMatchLen {
		return "", fmt.Errorf("%w: %q", ErrInvalidStatement, statement)
	}

	return matches[1], nil
}
