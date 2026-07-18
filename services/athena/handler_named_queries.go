package athena

import (
	"encoding/json"
	"fmt"
)

type createNamedQueryInput struct {
	Name        string `json:"Name"`
	Description string `json:"Description"`
	Database    string `json:"Database"`
	QueryString string `json:"QueryString"`
	WorkGroup   string `json:"WorkGroup"`
}

type getNamedQueryInput struct {
	NamedQueryID string `json:"NamedQueryId"`
}

type listNamedQueriesInput struct {
	WorkGroup  string `json:"WorkGroup"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type batchGetNamedQueryInput struct {
	NamedQueryIDs []string `json:"NamedQueryIds"`
}

type deleteNamedQueryInput struct {
	NamedQueryID string `json:"NamedQueryId"`
}

type updateNamedQueryInput struct {
	NamedQueryID string `json:"NamedQueryId"`
	Name         string `json:"Name"`
	Description  string `json:"Description"`
	QueryString  string `json:"QueryString"`
}

func (h *Handler) namedQueryOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreateNamedQuery": func(b []byte) (any, error) {
			var input createNamedQueryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, err := h.Backend.CreateNamedQuery(
				input.Name, input.Description, input.Database, input.QueryString, input.WorkGroup,
			)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NamedQueryId": id}, nil
		},
		"GetNamedQuery": func(b []byte) (any, error) {
			var input getNamedQueryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			q, err := h.Backend.GetNamedQuery(input.NamedQueryID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NamedQuery": q}, nil
		},
		"ListNamedQueries": func(b []byte) (any, error) {
			var input listNamedQueriesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			ids, nextToken, err := h.Backend.ListNamedQueries(
				input.WorkGroup,
				input.NextToken,
				input.MaxResults,
			)
			if err != nil {
				return nil, err
			}

			resp := map[string]any{"NamedQueryIds": ids}
			if nextToken != "" {
				resp["NextToken"] = nextToken
			}

			return resp, nil
		},
		"BatchGetNamedQuery": func(b []byte) (any, error) {
			var input batchGetNamedQueryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			const maxBatchGetNamedQuery = 50
			if len(input.NamedQueryIDs) > maxBatchGetNamedQuery {
				return nil, fmt.Errorf(
					"%w: BatchGetNamedQuery accepts at most 50 IDs",
					ErrValidation,
				)
			}

			found, unprocessed := h.Backend.BatchGetNamedQuery(input.NamedQueryIDs)

			return map[string]any{
				"NamedQueries":             found,
				"UnprocessedNamedQueryIds": unprocessed,
			}, nil
		},
		"DeleteNamedQuery": func(b []byte) (any, error) {
			var input deleteNamedQueryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteNamedQuery(input.NamedQueryID)
		},
	}
}

// namedQueryUpdateOps is kept separate from namedQueryOps to keep both
// functions' cognitive complexity low.
func (h *Handler) namedQueryUpdateOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"UpdateNamedQuery": func(b []byte) (any, error) {
			var input updateNamedQueryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateNamedQuery(
				input.NamedQueryID,
				input.Name,
				input.Description,
				input.QueryString,
			)
		},
	}
}
