package athena

import (
	"encoding/json"
	"fmt"
)

// batchGetPreparedStatementInput's StatementNames field is tagged
// "PreparedStatementNames" (not "StatementNames") to match the real Athena
// wire shape: aws-sdk-go-v2 serializes BatchGetPreparedStatementInput's
// PreparedStatementNames member under that exact key, so a mismatched tag
// here would silently drop every requested name from a real SDK client.
type batchGetPreparedStatementInput struct {
	WorkGroup      string   `json:"WorkGroup"`
	StatementNames []string `json:"PreparedStatementNames"`
}

type createPreparedStatementInput struct {
	StatementName  string `json:"StatementName"`
	Description    string `json:"Description"`
	WorkGroup      string `json:"WorkGroup"`
	QueryStatement string `json:"QueryStatement"`
}

type deletePreparedStatementInput struct {
	StatementName string `json:"StatementName"`
	WorkGroup     string `json:"WorkGroup"`
}

type getPreparedStatementInput struct {
	StatementName string `json:"StatementName"`
	WorkGroup     string `json:"WorkGroup"`
}

type listPreparedStatementsInput struct {
	WorkGroup  string `json:"WorkGroup"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type updatePreparedStatementInput struct {
	StatementName  string `json:"StatementName"`
	WorkGroup      string `json:"WorkGroup"`
	QueryStatement string `json:"QueryStatement"`
	Description    string `json:"Description"`
}

func (h *Handler) preparedStatementOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreatePreparedStatement": func(b []byte) (any, error) {
			var input createPreparedStatementInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CreatePreparedStatement(
				input.StatementName, input.Description, input.WorkGroup, input.QueryStatement,
			)
		},
		"BatchGetPreparedStatement": func(b []byte) (any, error) {
			var input batchGetPreparedStatementInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			const maxBatchGetPreparedStatement = 25
			if len(input.StatementNames) > maxBatchGetPreparedStatement {
				return nil, fmt.Errorf(
					"%w: BatchGetPreparedStatement accepts at most 25 names",
					ErrValidation,
				)
			}

			found, unprocessed := h.Backend.BatchGetPreparedStatement(
				input.WorkGroup,
				input.StatementNames,
			)

			return map[string]any{
				"PreparedStatements":                found,
				"UnprocessedPreparedStatementNames": unprocessed,
			}, nil
		},
		"DeletePreparedStatement": func(b []byte) (any, error) {
			var input deletePreparedStatementInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeletePreparedStatement(
				input.StatementName,
				input.WorkGroup,
			)
		},
		"GetPreparedStatement": func(b []byte) (any, error) {
			var input getPreparedStatementInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			ps, err := h.Backend.GetPreparedStatement(input.StatementName, input.WorkGroup)
			if err != nil {
				return nil, err
			}

			return map[string]any{"PreparedStatement": ps}, nil
		},
		"ListPreparedStatements": func(b []byte) (any, error) {
			var input listPreparedStatementsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			stmts, nextToken, err := h.Backend.ListPreparedStatements(
				input.WorkGroup,
				input.NextToken,
				input.MaxResults,
			)
			if err != nil {
				return nil, err
			}

			resp := map[string]any{"PreparedStatements": stmts}
			if nextToken != "" {
				resp["NextToken"] = nextToken
			}

			return resp, nil
		},
		"UpdatePreparedStatement": func(b []byte) (any, error) {
			var input updatePreparedStatementInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdatePreparedStatement(
				input.StatementName, input.WorkGroup, input.QueryStatement, input.Description,
			)
		},
	}
}
