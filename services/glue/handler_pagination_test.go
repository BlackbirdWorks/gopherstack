package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPagination_GetDatabases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults  any
		name        string
		nextToken   string
		dbNames     []string
		wantCount   int
		wantStatus  int
		wantHasNext bool
	}{
		{
			name:        "all results when no MaxResults",
			dbNames:     []string{"a", "b", "c"},
			wantCount:   3,
			wantHasNext: false,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "first page",
			dbNames:     []string{"a", "b", "c"},
			maxResults:  2,
			wantCount:   2,
			wantHasNext: true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "second page via NextToken",
			dbNames:     []string{"a", "b", "c"},
			maxResults:  2,
			nextToken:   "2",
			wantCount:   1,
			wantHasNext: false,
			wantStatus:  http.StatusOK,
		},
		{
			name:       "MaxResults=0 is invalid",
			dbNames:    []string{"a"},
			maxResults: 0,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "MaxResults=101 exceeds limit",
			dbNames:    []string{"a"},
			maxResults: 101,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, name := range tc.dbNames {
				rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
					"DatabaseInput": map[string]any{"Name": name},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			req := map[string]any{}
			if tc.maxResults != nil {
				req["MaxResults"] = tc.maxResults
			}
			if tc.nextToken != "" {
				req["NextToken"] = tc.nextToken
			}

			rec := doGlueRequest(t, h, "GetDatabases", req)
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantStatus != http.StatusOK {
				return
			}

			var out struct {
				NextToken    string `json:"NextToken"`
				DatabaseList []any  `json:"DatabaseList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.DatabaseList, tc.wantCount)
			if tc.wantHasNext {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

func TestPagination_GetTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults  any
		name        string
		nextToken   string
		tableNames  []string
		wantCount   int
		wantStatus  int
		wantHasNext bool
	}{
		{
			name:        "all results when no MaxResults",
			tableNames:  []string{"t1", "t2", "t3"},
			wantCount:   3,
			wantHasNext: false,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "first page",
			tableNames:  []string{"t1", "t2", "t3"},
			maxResults:  2,
			wantCount:   2,
			wantHasNext: true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "second page via NextToken",
			tableNames:  []string{"t1", "t2", "t3"},
			maxResults:  2,
			nextToken:   "2",
			wantCount:   1,
			wantHasNext: false,
			wantStatus:  http.StatusOK,
		},
		{
			name:       "MaxResults=0 is invalid",
			tableNames: []string{"t1"},
			maxResults: 0,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "MaxResults=101 exceeds limit",
			tableNames: []string{"t1"},
			maxResults: 101,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
				"DatabaseInput": map[string]any{"Name": "pgdb"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			for _, name := range tc.tableNames {
				r := doGlueRequest(t, h, "CreateTable", map[string]any{
					"DatabaseName": "pgdb",
					"TableInput":   map[string]any{"Name": name},
				})
				require.Equal(t, http.StatusOK, r.Code)
			}

			req := map[string]any{"DatabaseName": "pgdb"}
			if tc.maxResults != nil {
				req["MaxResults"] = tc.maxResults
			}
			if tc.nextToken != "" {
				req["NextToken"] = tc.nextToken
			}

			rec = doGlueRequest(t, h, "GetTables", req)
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantStatus != http.StatusOK {
				return
			}

			var out struct {
				NextToken string `json:"NextToken"`
				TableList []any  `json:"TableList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.TableList, tc.wantCount)
			if tc.wantHasNext {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

func TestPagination_GetPartitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults  any
		name        string
		nextToken   string
		partitions  []string
		wantCount   int
		wantStatus  int
		wantHasNext bool
	}{
		{
			name:        "all results when no MaxResults",
			partitions:  []string{"2020", "2021", "2022"},
			wantCount:   3,
			wantHasNext: false,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "first page",
			partitions:  []string{"2020", "2021", "2022"},
			maxResults:  2,
			wantCount:   2,
			wantHasNext: true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "second page via NextToken",
			partitions:  []string{"2020", "2021", "2022"},
			maxResults:  2,
			nextToken:   "2",
			wantCount:   1,
			wantHasNext: false,
			wantStatus:  http.StatusOK,
		},
		{
			name:       "MaxResults=0 is invalid",
			partitions: []string{"2020"},
			maxResults: 0,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "MaxResults=1001 exceeds limit",
			partitions: []string{"2020"},
			maxResults: 1001,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestDB(t, h, "pgdb2", "pgtbl2")
			for _, year := range tc.partitions {
				createTestPartition(t, h, "pgdb2", "pgtbl2", []string{year})
			}

			req := map[string]any{
				"DatabaseName": "pgdb2",
				"TableName":    "pgtbl2",
			}
			if tc.maxResults != nil {
				req["MaxResults"] = tc.maxResults
			}
			if tc.nextToken != "" {
				req["NextToken"] = tc.nextToken
			}

			rec := doGlueRequest(t, h, "GetPartitions", req)
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantStatus != http.StatusOK {
				return
			}

			var out struct {
				NextToken  string `json:"NextToken"`
				Partitions []any  `json:"Partitions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Partitions, tc.wantCount)
			if tc.wantHasNext {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}
