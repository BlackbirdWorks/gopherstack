package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchTables_SortCriteria(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		criteria []map[string]any
		catalog  string
		want     []string
		wantErr  bool
	}{
		{name: "asc", criteria: []map[string]any{{"FieldName": "Name", "Sort": "ASC"}}, want: []string{"a", "b", "c"}},
		{
			name:     "desc",
			criteria: []map[string]any{{"FieldName": "Name", "Sort": "DESC"}},
			want:     []string{"c", "b", "a"},
		},
		{name: "default_asc", criteria: []map[string]any{{"FieldName": "Name"}}, want: []string{"a", "b", "c"}},
		{
			name: "second_key_breaks_tie",
			criteria: []map[string]any{
				{"FieldName": "TableType", "Sort": "ASC"},
				{"FieldName": "Name", "Sort": "DESC"},
			},
			want: []string{"c", "b", "a"},
		},
		{name: "bad_field", criteria: []map[string]any{{"FieldName": "Nope"}}, wantErr: true},
		{name: "bad_direction", criteria: []map[string]any{{"FieldName": "Name", "Sort": "UP"}}, wantErr: true},
		{name: "other_catalog", catalog: "999999999999", want: []string{}},
		{name: "own_catalog", catalog: testAccountID, want: []string{"a", "b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateDatabase", map[string]any{
				"DatabaseInput": map[string]any{"Name": "db"},
			}).Code)

			type seed struct{ name, typ string }

			for _, tbl := range []seed{{"b", "VIRTUAL_VIEW"}, {"a", "VIRTUAL_VIEW"}, {"c", "EXTERNAL_TABLE"}} {
				require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateTable", map[string]any{
					"DatabaseName": "db", "TableInput": map[string]any{"Name": tbl.name, "TableType": tbl.typ},
				}).Code)
			}

			body := map[string]any{"SortCriteria": tt.criteria}
			if tt.catalog != "" {
				body["CatalogId"] = tt.catalog
			}

			rec := doGlueRequest(t, h, "SearchTables", body)
			if tt.wantErr {
				require.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				TableList []struct {
					Name string `json:"Name"`
				} `json:"TableList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			got := make([]string, 0, len(out.TableList))
			for _, tbl := range out.TableList {
				got = append(got, tbl.Name)
			}

			if tt.criteria == nil {
				assert.ElementsMatch(t, tt.want, got)

				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
