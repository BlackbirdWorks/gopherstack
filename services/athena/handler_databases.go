package athena

import "encoding/json"

type getDatabaseInput struct {
	CatalogName  string `json:"CatalogName"`
	DatabaseName string `json:"DatabaseName"`
}

type listDatabasesInput struct {
	NextToken   string `json:"NextToken"`
	CatalogName string `json:"CatalogName"`
	MaxResults  int    `json:"MaxResults"`
}

type getTableMetadataInput struct {
	CatalogName  string `json:"CatalogName"`
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

type listTableMetadataInput struct {
	NextToken    string `json:"NextToken"`
	CatalogName  string `json:"CatalogName"`
	DatabaseName string `json:"DatabaseName"`
	Expression   string `json:"Expression"`
	MaxResults   int    `json:"MaxResults"`
}

func (h *Handler) databaseOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"GetDatabase": func(b []byte) (any, error) {
			var input getDatabaseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			db, err := h.Backend.GetDatabase(input.CatalogName, input.DatabaseName)
			if err != nil {
				return nil, err
			}

			return map[string]any{"Database": db}, nil
		},
		"ListDatabases": func(b []byte) (any, error) {
			var input listDatabasesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			dbs, err := h.Backend.ListDatabases(input.CatalogName)
			if err != nil {
				return nil, err
			}

			page, next, pageErr := pageByKey(
				h.tokens,
				dbs,
				func(d Database) string { return d.Name },
				input.MaxResults,
				input.NextToken,
			)
			if pageErr != nil {
				return nil, pageErr
			}

			return withNextToken(map[string]any{"DatabaseList": page}, next), nil
		},
		"GetTableMetadata": func(b []byte) (any, error) {
			var input getTableMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			t, err := h.Backend.GetTableMetadata(input.CatalogName, input.DatabaseName, input.TableName)
			if err != nil {
				return nil, err
			}

			return map[string]any{"TableMetadata": t}, nil
		},
		"ListTableMetadata": func(b []byte) (any, error) {
			var input listTableMetadataInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			tables, err := h.Backend.ListTableMetadata(input.CatalogName, input.DatabaseName, input.Expression)
			if err != nil {
				return nil, err
			}

			page, next, pageErr := pageByKey(
				h.tokens,
				tables,
				func(t TableMetadata) string { return t.Name },
				input.MaxResults,
				input.NextToken,
			)
			if pageErr != nil {
				return nil, pageErr
			}

			return withNextToken(map[string]any{"TableMetadataList": page}, next), nil
		},
	}
}
