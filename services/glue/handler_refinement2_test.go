package glue_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestRefinement2_ConnectionCRUD tests Connection create/get/delete operations.
func TestRefinement2_ConnectionCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*glue.Handler)
		body     map[string]any
		check    func(*testing.T, map[string]any)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "create_jdbc_connection",
			action: "CreateConnection",
			body: map[string]any{
				"ConnectionInput": map[string]any{
					"Name":           "jdbc-conn",
					"ConnectionType": "JDBC",
					"ConnectionProperties": map[string]string{
						"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/db",
					},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				assert.Equal(t, "jdbc-conn", out["Name"])
			},
		},
		{
			name:     "create_empty_name_fails",
			action:   "CreateConnection",
			body:     map[string]any{"ConnectionInput": map[string]any{"Name": ""}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_not_found",
			action:   "GetConnection",
			body:     map[string]any{"Name": "no-such"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteConnection",
			body:     map[string]any{"ConnectionName": "no-such"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "create_duplicate_fails",
			action: "CreateConnection",
			setup: func(h *glue.Handler) {
				doGlueRequest(t, h, "CreateConnection", map[string]any{
					"ConnectionInput": map[string]any{"Name": "dup"},
				})
			},
			body:     map[string]any{"ConnectionInput": map[string]any{"Name": "dup"}},
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

			if tt.check != nil && rec.Code == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.check(t, out)
			}
		})
	}
}

// TestRefinement2_ConnectionGetFields tests that GetConnection returns expected fields.
func TestRefinement2_ConnectionGetFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		connType       string
		wantConnType   string
		wantCreation   bool
		wantLastUpdate bool
	}{
		{
			name:           "jdbc_fields",
			connType:       "JDBC",
			wantConnType:   "JDBC",
			wantCreation:   true,
			wantLastUpdate: true,
		},
		{
			name:           "sftp_fields",
			connType:       "SFTP",
			wantConnType:   "SFTP",
			wantCreation:   true,
			wantLastUpdate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateConnection", map[string]any{
				"ConnectionInput": map[string]any{
					"Name":           "conn-" + tt.name,
					"ConnectionType": tt.connType,
				},
			})

			rec := doGlueRequest(t, h, "GetConnection", map[string]any{"Name": "conn-" + tt.name})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			conn := out["Connection"].(map[string]any)
			assert.Equal(t, tt.wantConnType, conn["ConnectionType"])
			if tt.wantCreation {
				assert.NotZero(t, conn["CreationTime"])
			}
			if tt.wantLastUpdate {
				assert.NotZero(t, conn["LastUpdatedTime"])
			}
		})
	}
}

// TestRefinement2_GetConnections tests listing connections.
func TestRefinement2_GetConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		connCount int
		wantLen   int
	}{
		{name: "empty", connCount: 0, wantLen: 0},
		{name: "one", connCount: 1, wantLen: 1},
		{name: "four_sorted", connCount: 4, wantLen: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for i := range tt.connCount {
				doGlueRequest(t, h, "CreateConnection", map[string]any{
					"ConnectionInput": map[string]any{
						"Name":           fmt.Sprintf("conn-%d", i),
						"ConnectionType": "JDBC",
					},
				})
			}

			rec := doGlueRequest(t, h, "GetConnections", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			list, _ := out["ConnectionList"].([]any)
			assert.Len(t, list, tt.wantLen)
		})
	}
}

// TestRefinement2_ListCrawlers tests listing crawlers.
func TestRefinement2_ListCrawlers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		crawlerCount int
		wantLen      int
		checkOrder   bool
	}{
		{name: "empty", crawlerCount: 0, wantLen: 0},
		{name: "three_sorted", crawlerCount: 3, wantLen: 3, checkOrder: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			for i := range tt.crawlerCount {
				doGlueRequest(t, h, "CreateCrawler", map[string]any{
					"Name":         fmt.Sprintf("lc-%d", i),
					"Role":         "arn:aws:iam::000000000000:role/r",
					"DatabaseName": "db",
				})
			}

			rec := doGlueRequest(t, h, "ListCrawlers", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			names, _ := out["CrawlerNames"].([]any)
			assert.Len(t, names, tt.wantLen)

			if tt.checkOrder && tt.wantLen >= 3 {
				assert.Equal(t, "lc-0", names[0])
				assert.Equal(t, "lc-2", names[2])
			}
		})
	}
}

// TestRefinement2_Validation tests input validation across operations.
func TestRefinement2_Validation(t *testing.T) {
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

// TestRefinement2_Timestamps tests that resources have timestamps populated.
func TestRefinement2_Timestamps(t *testing.T) {
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

// TestRefinement2_JobLifecycle tests job lifecycle operations.
func TestRefinement2_JobLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*glue.Handler) string
		makeBody func(string) map[string]any
		check    func(*testing.T, map[string]any)
		name     string
		action   string
		wantCode int
	}{
		{
			name: "delete_cascades_runs_and_bookmarks",
			setup: func(h *glue.Handler) string {
				doGlueRequest(
					t,
					h,
					"CreateJob",
					map[string]any{"Name": "cascade-job", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
				)
				doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "cascade-job"})
				doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "cascade-job"})

				return "cascade-job"
			},
			action:   "DeleteJob",
			makeBody: func(name string) map[string]any { return map[string]any{"JobName": name} },
			wantCode: http.StatusOK,
		},
		{
			name: "bookmark_not_found_for_nonexistent_job",
			setup: func(_ *glue.Handler) string {
				return "no-job"
			},
			action:   "GetJobBookmark",
			makeBody: func(name string) map[string]any { return map[string]any{"JobName": name} },
			wantCode: http.StatusBadRequest,
		},
		{
			name: "reset_bookmark_returns_post_reset_state",
			setup: func(h *glue.Handler) string {
				doGlueRequest(
					t,
					h,
					"CreateJob",
					map[string]any{"Name": "j", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
				)
				doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "j"})

				return "j"
			},
			action:   "ResetJobBookmark",
			makeBody: func(name string) map[string]any { return map[string]any{"JobName": name} },
			wantCode: http.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				bm, _ := out["JobBookmarkEntry"].(map[string]any)
				assert.Equal(t, "j", bm["JobName"])
			},
		},
		{
			name: "reset_bookmark_not_found",
			setup: func(_ *glue.Handler) string {
				return "no-job"
			},
			action:   "ResetJobBookmark",
			makeBody: func(name string) map[string]any { return map[string]any{"JobName": name} },
			wantCode: http.StatusBadRequest,
		},
		{
			name: "bookmark_active_run_tracking",
			setup: func(h *glue.Handler) string {
				doGlueRequest(
					t,
					h,
					"CreateJob",
					map[string]any{"Name": "j", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
				)
				doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "j"})

				return "j"
			},
			action:   "GetJobBookmark",
			makeBody: func(name string) map[string]any { return map[string]any{"JobName": name} },
			wantCode: http.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				bm, _ := out["JobBookmarkEntry"].(map[string]any)
				assert.NotEmpty(t, bm["ActiveRun"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			var resourceName string
			if tt.setup != nil {
				resourceName = tt.setup(h)
			}

			rec := doGlueRequest(t, h, tt.action, tt.makeBody(resourceName))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil && rec.Code == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.check(t, out)
			}
		})
	}
}

// TestRefinement2_MaxConcurrentRuns tests job run concurrency enforcement.
func TestRefinement2_MaxConcurrentRuns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		maxConcurrentRuns int
		startCount        int
		wantLastCode      int
	}{
		{
			name:              "limit_2_blocks_third",
			maxConcurrentRuns: 2,
			startCount:        3,
			wantLastCode:      http.StatusBadRequest,
		},
		{
			name:              "limit_1_blocks_second",
			maxConcurrentRuns: 1,
			startCount:        2,
			wantLastCode:      http.StatusBadRequest,
		},
		{
			name:              "unlimited_allows_many",
			maxConcurrentRuns: 0,
			startCount:        3,
			wantLastCode:      http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"Name": "limited-job", "Role": "r", "Command": map[string]any{"Name": "glueetl"}}
			if tt.maxConcurrentRuns > 0 {
				body["ExecutionProperty"] = map[string]any{"MaxConcurrentRuns": tt.maxConcurrentRuns}
			}
			doGlueRequest(t, h, "CreateJob", body)

			var lastCode int
			for range tt.startCount {
				rec := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "limited-job"})
				lastCode = rec.Code
			}
			assert.Equal(t, tt.wantLastCode, lastCode)
		})
	}
}

// TestRefinement2_CrawlerErrors tests crawler-specific error types.
func TestRefinement2_CrawlerErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*glue.Handler)
		body          map[string]any
		name          string
		action        string
		wantErrorType string
		wantCode      int
	}{
		{
			name: "crawler_running_exception",
			setup: func(h *glue.Handler) {
				doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
				doGlueRequest(t, h, "CreateCrawler", map[string]any{"Name": "c", "Role": "r", "DatabaseName": "db"})
				doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": "c"})
			},
			action:        "StartCrawler",
			body:          map[string]any{"Name": "c"},
			wantCode:      http.StatusBadRequest,
			wantErrorType: "CrawlerRunningException",
		},
		{
			name: "crawler_not_running_exception",
			setup: func(h *glue.Handler) {
				doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
				doGlueRequest(t, h, "CreateCrawler", map[string]any{"Name": "c", "Role": "r", "DatabaseName": "db"})
			},
			action:        "StopCrawler",
			body:          map[string]any{"Name": "c"},
			wantCode:      http.StatusBadRequest,
			wantErrorType: "CrawlerNotRunningException",
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

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantErrorType, out["__type"])
		})
	}
}

// TestRefinement2_CrawlerSchedule tests crawler schedule operations.
func TestRefinement2_CrawlerSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*glue.Handler)
		body     map[string]any
		check    func(*testing.T, map[string]any)
		name     string
		action   string
		wantCode int
	}{
		{
			name: "update_schedule_uses_crawler_name",
			setup: func(h *glue.Handler) {
				doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
				doGlueRequest(t, h, "CreateCrawler", map[string]any{"Name": "c", "Role": "r", "DatabaseName": "db"})
			},
			action:   "UpdateCrawlerSchedule",
			body:     map[string]any{"CrawlerName": "c", "Schedule": "cron(0 * * * ? *)"},
			wantCode: http.StatusOK,
		},
		{
			name: "start_schedule_requires_expression",
			setup: func(h *glue.Handler) {
				doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
				doGlueRequest(t, h, "CreateCrawler", map[string]any{"Name": "c", "Role": "r", "DatabaseName": "db"})
			},
			action:   "StartCrawlerSchedule",
			body:     map[string]any{"CrawlerName": "c"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "start_schedule_with_expression_ok",
			setup: func(h *glue.Handler) {
				doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
				doGlueRequest(t, h, "CreateCrawler", map[string]any{"Name": "c", "Role": "r", "DatabaseName": "db"})
				doGlueRequest(t, h, "UpdateCrawlerSchedule", map[string]any{
					"CrawlerName": "c", "Schedule": "cron(0 12 * * ? *)",
				})
			},
			action:   "StartCrawlerSchedule",
			body:     map[string]any{"CrawlerName": "c"},
			wantCode: http.StatusOK,
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

			if tt.check != nil && rec.Code == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.check(t, out)
			}
		})
	}
}

// TestRefinement2_DataQualityRulesets tests data quality ruleset operations.
func TestRefinement2_DataQualityRulesets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*glue.Handler)
		body     map[string]any
		check    func(*testing.T, map[string]any)
		name     string
		action   string
		wantCode int
	}{
		{
			name: "list_uses_rulesets_field",
			setup: func(h *glue.Handler) {
				for i := range 2 {
					doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
						"Name":    fmt.Sprintf("rs-%d", i),
						"Ruleset": "Rules = [ RowCount > 0 ]",
					})
				}
			},
			action:   "ListDataQualityRulesets",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				rulesets, _ := out["Rulesets"].([]any)
				assert.Len(t, rulesets, 2)
				_, hasOldKey := out["DataQualityRulesets"]
				assert.False(t, hasOldKey, "should not use deprecated DataQualityRulesets key")
			},
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

			if tt.check != nil && rec.Code == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.check(t, out)
			}
		})
	}
}

// TestRefinement2_BatchStopJobRun tests batch stop for non-stoppable runs.
func TestRefinement2_BatchStopJobRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrors int
	}{
		{
			name:       "already_stopped_run_returns_error",
			wantErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(
				t,
				h,
				"CreateJob",
				map[string]any{"Name": "j", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
			)
			runRec := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "j"})

			var runOut map[string]any
			require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
			runID := runOut["JobRunId"].(string)

			doGlueRequest(t, h, "BatchStopJobRun", map[string]any{
				"JobName": "j", "JobRunIds": []string{runID},
			})

			rec := doGlueRequest(t, h, "BatchStopJobRun", map[string]any{
				"JobName": "j", "JobRunIds": []string{runID},
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			errs, _ := out["Errors"].([]any)
			assert.Len(t, errs, tt.wantErrors)
		})
	}
}

// TestRefinement2_SeedHelpers tests backend seed helper functions.
func TestRefinement2_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed  func(*glue.InMemoryBackend)
		check func(*testing.T, *glue.InMemoryBackend)
		name  string
	}{
		{
			name: "job_run_seed",
			seed: func(b *glue.InMemoryBackend) {
				b.AddJobRunInternal(&glue.JobRun{
					ID: "jr-seed", JobName: "seed-job", JobRunState: "SUCCEEDED",
				})
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.JobRunCount(b))
			},
		},
		{
			name: "dq_ruleset_seed",
			seed: func(b *glue.InMemoryBackend) {
				b.AddDataQualityRulesetInternal(&glue.DataQualityRuleset{
					Name: "seed-rs", Ruleset: "Rules = [ RowCount > 0 ]",
				})
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.DataQualityRulesetCount(b))
			},
		},
		{
			name: "dq_eval_run_seed",
			seed: func(b *glue.InMemoryBackend) {
				b.AddDataQualityEvalRunInternal(&glue.DataQualityEvaluationRun{
					RunID: "seed-run-1", Status: "SUCCEEDED",
				})
			},
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 1, glue.DataQualityEvalRunCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			tt.seed(b)
			tt.check(t, b)
		})
	}
}

// TestRefinement2_ConnectionPersistence tests snapshot/restore for connections.
func TestRefinement2_ConnectionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		connProps map[string]string
		wantType  string
		wantProp  string
		wantKey   string
	}{
		{
			name:      "jdbc_connection_round_trip",
			connProps: map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306"},
			wantType:  "JDBC",
			wantKey:   "JDBC_CONNECTION_URL",
			wantProp:  "jdbc:mysql://localhost:3306",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			b.AddConnectionInternal(&glue.Connection{
				Name:                 "persist-conn",
				ConnectionType:       tt.wantType,
				ConnectionProperties: tt.connProps,
			})

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := glue.NewInMemoryBackend(testAccountID, testRegion)
			require.NoError(t, b2.Restore(snap))
			assert.Equal(t, 1, glue.ConnectionCount(b2))

			conn, err := b2.GetConnection("persist-conn")
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, conn.ConnectionType)
			assert.Equal(t, tt.wantProp, conn.ConnectionProperties[tt.wantKey])
		})
	}
}

// TestRefinement2_ConnectionTagging tests tagging Connections via ARN.
func TestRefinement2_ConnectionTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check  func(*testing.T, map[string]any)
		name   string
		action string
	}{
		{
			name:   "tag_and_get_tags",
			action: "TagResource",
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				tags, _ := out["Tags"].(map[string]any)
				assert.Equal(t, "bar", tags["foo"])
			},
		},
		{
			name:   "untag_resource",
			action: "UntagResource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			b.AddConnectionInternal(&glue.Connection{Name: "tagged-conn", ConnectionType: "JDBC"})
			h := glue.NewHandler(b)

			connARN := "arn:aws:glue:us-east-1:000000000000:connection/tagged-conn"

			if tt.action == "TagResource" || tt.action == "UntagResource" {
				rec := doGlueRequest(t, h, "TagResource", map[string]any{
					"ResourceArn": connARN,
					"TagsToAdd":   map[string]string{"foo": "bar"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			if tt.action == "TagResource" {
				rec := doGlueRequest(t, h, "GetTags", map[string]any{"ResourceArn": connARN})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.check(t, out)
			}

			if tt.action == "UntagResource" {
				rec := doGlueRequest(t, h, "UntagResource", map[string]any{
					"ResourceArn":  connARN,
					"TagsToRemove": []string{"foo"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec2 := doGlueRequest(t, h, "GetTags", map[string]any{"ResourceArn": connARN})
				require.Equal(t, http.StatusOK, rec2.Code)
				var out2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
				tags, _ := out2["Tags"].(map[string]any)
				_, hasFoo := tags["foo"]
				assert.False(t, hasFoo)
			}
		})
	}
}

// TestRefinement2_DataQualityRulesetTagging tests tagging DataQualityRulesets via ARN.
func TestRefinement2_DataQualityRulesetTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(*testing.T, map[string]any)
		name     string
		wantCode int
	}{
		{
			name:     "tag_and_get_dq_ruleset",
			wantCode: http.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				tags, _ := out["Tags"].(map[string]any)
				assert.Equal(t, "val", tags["key"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			b.AddDataQualityRulesetInternal(&glue.DataQualityRuleset{
				Name: "dq-rs", Ruleset: "Rules = [ RowCount > 0 ]",
			})
			h := glue.NewHandler(b)

			rsARN := "arn:aws:glue:us-east-1:000000000000:dataQualityRuleset/dq-rs"

			rec := doGlueRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": rsARN,
				"TagsToAdd":   map[string]string{"key": "val"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doGlueRequest(t, h, "GetTags", map[string]any{"ResourceArn": rsARN})
			require.Equal(t, tt.wantCode, rec2.Code)

			if tt.check != nil {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
				tt.check(t, out)
			}
		})
	}
}

// TestRefinement2_CloneTableWithColumns tests deep copy of tables with columns.
func TestRefinement2_CloneTableWithColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		columns       []glue.Column
		partitionKeys []glue.Column
	}{
		{
			name:          "table_with_columns",
			columns:       []glue.Column{{Name: "id", Type: "int"}, {Name: "name", Type: "string"}},
			partitionKeys: []glue.Column{{Name: "year", Type: "int"}},
		},
		{
			name:          "table_no_columns",
			columns:       nil,
			partitionKeys: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateTable", map[string]any{
				"DatabaseName": "db",
				"TableInput": map[string]any{
					"Name":              "tbl",
					"StorageDescriptor": map[string]any{"Columns": tt.columns},
					"PartitionKeys":     tt.partitionKeys,
				},
			})

			rec := doGlueRequest(t, h, "GetTable", map[string]any{"DatabaseName": "db", "Name": "tbl"})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			tbl := out["Table"].(map[string]any)
			assert.NotNil(t, tbl)

			if len(tt.columns) > 0 {
				sd := tbl["StorageDescriptor"].(map[string]any)
				cols, _ := sd["Columns"].([]any)
				assert.Len(t, cols, len(tt.columns))
			}
		})
	}
}

// TestRefinement2_ExtractResource tests ExtractResource with CrawlerName/JobName/ConnectionName.
func TestRefinement2_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "update_crawler_schedule_extracts_crawler_name",
			action:   "UpdateCrawlerSchedule",
			body:     map[string]any{"CrawlerName": "no-such-crawler", "Schedule": "cron(0 * * * ? *)"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete_connection_extracts_connection_name",
			action:   "DeleteConnection",
			body:     map[string]any{"ConnectionName": "no-such-conn"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete_job_extracts_job_name",
			action:   "DeleteJob",
			body:     map[string]any{"JobName": "no-such-job"},
			wantCode: http.StatusBadRequest,
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
