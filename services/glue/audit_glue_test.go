package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAuditGlue_GetPartitions_Expression tests GetPartitions expression filtering.
func TestAuditGlue_GetPartitions_Expression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		partitions [][]string
		expr       string
		name       string
		wantValues [][]string
		wantCode   int
	}{
		{
			name:       "eq_match",
			partitions: [][]string{{"2023", "01"}, {"2023", "02"}, {"2024", "01"}},
			expr:       "year = '2023'",
			wantValues: [][]string{{"2023", "01"}, {"2023", "02"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "eq_no_match",
			partitions: [][]string{{"2023", "01"}, {"2024", "01"}},
			expr:       "year = '2025'",
			wantValues: [][]string{},
			wantCode:   http.StatusOK,
		},
		{
			name:       "neq",
			partitions: [][]string{{"2023", "01"}, {"2024", "01"}},
			expr:       "year <> '2023'",
			wantValues: [][]string{{"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "and",
			partitions: [][]string{{"2023", "01"}, {"2023", "02"}, {"2024", "01"}},
			expr:       "year = '2023' AND month = '01'",
			wantValues: [][]string{{"2023", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "or",
			partitions: [][]string{{"2023", "01"}, {"2023", "02"}, {"2024", "01"}},
			expr:       "month = '01' OR month = '02'",
			wantValues: [][]string{{"2023", "01"}, {"2023", "02"}, {"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "in_list",
			partitions: [][]string{{"2023", "01"}, {"2023", "06"}, {"2024", "01"}},
			expr:       "year IN ('2023', '2024')",
			wantValues: [][]string{{"2023", "01"}, {"2023", "06"}, {"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "not_in_list",
			partitions: [][]string{{"2023", "01"}, {"2023", "06"}, {"2024", "01"}},
			expr:       "year NOT IN ('2023')",
			wantValues: [][]string{{"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "gt",
			partitions: [][]string{{"2021", "01"}, {"2022", "01"}, {"2023", "01"}},
			expr:       "year > '2021'",
			wantValues: [][]string{{"2022", "01"}, {"2023", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "gte",
			partitions: [][]string{{"2021", "01"}, {"2022", "01"}, {"2023", "01"}},
			expr:       "year >= '2022'",
			wantValues: [][]string{{"2022", "01"}, {"2023", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "lt",
			partitions: [][]string{{"2021", "01"}, {"2022", "01"}, {"2023", "01"}},
			expr:       "year < '2023'",
			wantValues: [][]string{{"2021", "01"}, {"2022", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "lte",
			partitions: [][]string{{"2021", "01"}, {"2022", "01"}, {"2023", "01"}},
			expr:       "year <= '2022'",
			wantValues: [][]string{{"2021", "01"}, {"2022", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "like_prefix",
			partitions: [][]string{{"2023", "01"}, {"2023", "12"}, {"2024", "01"}},
			expr:       "year LIKE '202%'",
			wantValues: [][]string{{"2023", "01"}, {"2023", "12"}, {"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "like_exact",
			partitions: [][]string{{"2023", "01"}, {"2024", "01"}},
			expr:       "year LIKE '2023'",
			wantValues: [][]string{{"2023", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "no_expression_returns_all",
			partitions: [][]string{{"2023", "01"}, {"2024", "01"}},
			expr:       "",
			wantValues: [][]string{{"2023", "01"}, {"2024", "01"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "compound_and_or",
			partitions: [][]string{{"2023", "01"}, {"2023", "06"}, {"2024", "03"}},
			expr:       "(year = '2023' AND month = '06') OR year = '2024'",
			wantValues: [][]string{{"2023", "06"}, {"2024", "03"}},
			wantCode:   http.StatusOK,
		},
		{
			name:     "invalid_expression_returns_error",
			expr:     "BADBADBAD ~~~ ~~~",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			dbName := "exprdb_" + tc.name
			tableName := "exprtbl_" + tc.name

			rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
				"DatabaseInput": map[string]any{"Name": dbName},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doGlueRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": dbName,
				"TableInput": map[string]any{
					"Name": tableName,
					"PartitionKeys": []map[string]any{
						{"Name": "year", "Type": "string"},
						{"Name": "month", "Type": "string"},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			for _, vals := range tc.partitions {
				rec = doGlueRequest(t, h, "CreatePartition", map[string]any{
					"DatabaseName": dbName,
					"TableName":    tableName,
					"PartitionInput": map[string]any{
						"Values": vals,
						"StorageDescriptor": map[string]any{
							"Location": "s3://bucket/" + tableName,
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{
				"DatabaseName": dbName,
				"TableName":    tableName,
			}
			if tc.expr != "" {
				body["Expression"] = tc.expr
			}

			rec = doGlueRequest(t, h, "GetPartitions", body)
			assert.Equal(t, tc.wantCode, rec.Code, "response body: %s", rec.Body.String())

			if tc.wantCode != http.StatusOK {
				return
			}

			var out struct {
				Partitions []struct {
					Values []string `json:"Values"`
				} `json:"Partitions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			got := make([][]string, len(out.Partitions))
			for i, p := range out.Partitions {
				got[i] = p.Values
			}

			if len(tc.wantValues) == 0 {
				assert.Empty(t, got)
			} else {
				assert.ElementsMatch(t, tc.wantValues, got)
			}
		})
	}
}

// TestAuditGlue_GetTables_Expression tests GetTables regex expression filtering.
func TestAuditGlue_GetTables_Expression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tableNames []string
		expr       string
		name       string
		wantNames  []string
		wantCode   int
	}{
		{
			name:       "prefix_match",
			tableNames: []string{"sales_2023", "sales_2024", "inventory_2023"},
			expr:       "^sales_.*",
			wantNames:  []string{"sales_2023", "sales_2024"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "exact_match",
			tableNames: []string{"orders", "order_items", "customers"},
			expr:       "^orders$",
			wantNames:  []string{"orders"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "no_match",
			tableNames: []string{"sales_2023", "inventory"},
			expr:       "^nonexistent.*",
			wantNames:  []string{},
			wantCode:   http.StatusOK,
		},
		{
			name:       "no_expression_returns_all",
			tableNames: []string{"alpha", "beta"},
			expr:       "",
			wantNames:  []string{"alpha", "beta"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "pattern_suffix",
			tableNames: []string{"raw_2023", "processed_2023", "raw_2024"},
			expr:       ".*_2023$",
			wantNames:  []string{"raw_2023", "processed_2023"},
			wantCode:   http.StatusOK,
		},
		{
			name:     "invalid_regex_returns_error",
			expr:     "[invalid",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			dbName := "tblexprdb_" + tc.name

			rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
				"DatabaseInput": map[string]any{"Name": dbName},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			for _, name := range tc.tableNames {
				rec = doGlueRequest(t, h, "CreateTable", map[string]any{
					"DatabaseName": dbName,
					"TableInput":   map[string]any{"Name": name},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{
				"DatabaseName": dbName,
			}
			if tc.expr != "" {
				body["Expression"] = tc.expr
			}

			rec = doGlueRequest(t, h, "GetTables", body)
			assert.Equal(t, tc.wantCode, rec.Code, "response body: %s", rec.Body.String())

			if tc.wantCode != http.StatusOK {
				return
			}

			var out struct {
				TableList []struct {
					Name string `json:"Name"`
				} `json:"TableList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			gotNames := make([]string, len(out.TableList))
			for i, tbl := range out.TableList {
				gotNames[i] = tbl.Name
			}

			if len(tc.wantNames) == 0 {
				assert.Empty(t, gotNames)
			} else {
				assert.ElementsMatch(t, tc.wantNames, gotNames)
			}
		})
	}
}
