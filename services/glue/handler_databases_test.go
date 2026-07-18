package glue_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestNameLengthLimits tests max 255-char name enforcement.
func TestNameLengthLimits(t *testing.T) {
	t.Parallel()

	longName := strings.Repeat("a", 256)
	okName := strings.Repeat("a", 255)

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "database_name_too_long",
			action: "CreateDatabase",
			body: map[string]any{
				"DatabaseInput": map[string]any{"Name": longName},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "database_name_max_ok",
			action: "CreateDatabase",
			body: map[string]any{
				"DatabaseInput": map[string]any{"Name": okName},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "crawler_name_too_long",
			action: "CreateCrawler",
			body: map[string]any{
				"Name": longName,
				"Role": "r",
				"Targets": map[string]any{
					"S3Targets": []map[string]any{{"Path": "s3://b/p"}},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "job_name_too_long",
			action: "CreateJob",
			body: map[string]any{
				"Name":    longName,
				"Role":    "r",
				"Command": map[string]any{"Name": "glueetl"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "job_name_max_ok",
			action: "CreateJob",
			body: map[string]any{
				"Name":    okName,
				"Role":    "r",
				"Command": map[string]any{"Name": "glueetl"},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestValidation tests input validation across operations.
func TestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*glue.Handler)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "create_database_empty_name",
			action:   "CreateDatabase",
			body:     map[string]any{"DatabaseInput": map[string]string{"Name": ""}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_job_empty_name",
			action:   "CreateJob",
			body:     map[string]any{"Name": "", "Role": "r"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_job_empty_role",
			action:   "CreateJob",
			body:     map[string]any{"Name": "j", "Role": ""},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "create_crawler_empty_name",
			setup: func(h *glue.Handler) {
				doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			},
			action:   "CreateCrawler",
			body:     map[string]any{"Name": "", "Role": "r", "DatabaseName": "db"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "create_crawler_empty_role",
			setup: func(h *glue.Handler) {
				doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			},
			action:   "CreateCrawler",
			body:     map[string]any{"Name": "c", "Role": "", "DatabaseName": "db"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_dq_ruleset_empty_name",
			action:   "CreateDataQualityRuleset",
			body:     map[string]any{"Name": "", "Ruleset": "Rules=[]"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_dq_ruleset_empty_ruleset",
			action:   "CreateDataQualityRuleset",
			body:     map[string]any{"Name": "r", "Ruleset": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doGlueRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestTimestamps tests that resources have timestamps populated.
func TestTimestamps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*glue.Handler)
		name        string
		action      string
		body        map[string]any
		resourceKey string
		fields      []string
	}{
		{
			name: "job_timestamps",
			setup: func(h *glue.Handler) {
				doGlueRequest(
					t,
					h,
					"CreateJob",
					map[string]any{"Name": "j", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
				)
			},
			action:      "GetJob",
			body:        map[string]any{"JobName": "j"},
			resourceKey: "Job",
			fields:      []string{"CreatedOn", "LastModifiedOn"},
		},
		{
			name: "database_timestamps",
			setup: func(h *glue.Handler) {
				doGlueRequest(
					t,
					h,
					"CreateDatabase",
					map[string]any{"DatabaseInput": map[string]string{"Name": "ts-db"}},
				)
			},
			action:      "GetDatabase",
			body:        map[string]any{"Name": "ts-db"},
			resourceKey: "Database",
			fields:      []string{"CreateTime"},
		},
		{
			name: "table_timestamps",
			setup: func(h *glue.Handler) {
				doGlueRequest(
					t,
					h,
					"CreateDatabase",
					map[string]any{"DatabaseInput": map[string]string{"Name": "ts-db2"}},
				)
				doGlueRequest(t, h, "CreateTable", map[string]any{
					"DatabaseName": "ts-db2",
					"TableInput":   map[string]string{"Name": "ts-tbl"},
				})
			},
			action:      "GetTable",
			body:        map[string]any{"DatabaseName": "ts-db2", "Name": "ts-tbl"},
			resourceKey: "Table",
			fields:      []string{"CreateTime", "UpdateTime"},
		},
		{
			name: "crawler_timestamps",
			setup: func(h *glue.Handler) {
				doGlueRequest(
					t,
					h,
					"CreateDatabase",
					map[string]any{"DatabaseInput": map[string]string{"Name": "ts-db3"}},
				)
				doGlueRequest(t, h, "CreateCrawler", map[string]any{
					"Name": "ts-crawler", "Role": "r", "DatabaseName": "ts-db3",
				})
			},
			action:      "GetCrawler",
			body:        map[string]any{"Name": "ts-crawler"},
			resourceKey: "Crawler",
			fields:      []string{"CreationTime", "LastUpdated"},
		},
		{
			name: "dq_ruleset_timestamps",
			setup: func(h *glue.Handler) {
				doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
					"Name": "ts-rs", "Ruleset": "Rules = [ RowCount > 0 ]",
				})
			},
			action:      "GetDataQualityRuleset",
			body:        map[string]any{"Name": "ts-rs"},
			resourceKey: "",
			fields:      []string{"CreatedOn"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doGlueRequest(t, h, tt.action, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			resource := out
			if tt.resourceKey != "" {
				resource = out[tt.resourceKey].(map[string]any)
			}
			for _, field := range tt.fields {
				assert.NotZero(t, resource[field], "field %s should not be zero", field)
			}
		})
	}
}
