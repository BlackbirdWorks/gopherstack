package glue_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// Test_Crawler_CreateOptions_RoundTrip verifies CreateCrawler/UpdateCrawler
// accept and persist Schedule, Classifiers, Configuration and TablePrefix —
// fields AWS's CreateCrawlerRequest/UpdateCrawlerRequest both support that
// were previously silently discarded (CreateCrawler only accepted
// name/role/database/targets/tags). Also verifies JDBC and catalog crawler
// targets round-trip alongside S3 targets.
func Test_Crawler_CreateOptions_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name":          "crawler1",
		"Role":          "role1",
		"Schedule":      "cron(0 12 * * ? *)",
		"Classifiers":   []string{"my-classifier"},
		"Configuration": `{"Version":1.0}`,
		"TablePrefix":   "raw_",
		"Targets": map[string]any{
			"S3Targets":      []map[string]any{{"Path": "s3://bucket/data/"}},
			"JdbcTargets":    []map[string]any{{"ConnectionName": "conn1", "Path": "db/%"}},
			"CatalogTargets": []map[string]any{{"DatabaseName": "db1", "Tables": []string{"t1"}}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "crawler1"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Crawler struct {
			Schedule struct {
				ScheduleExpression string `json:"ScheduleExpression"`
			} `json:"Schedule"`
			Configuration string   `json:"Configuration"`
			TablePrefix   string   `json:"TablePrefix"`
			Classifiers   []string `json:"Classifiers"`
			Targets       struct {
				S3Targets []struct {
					Path string `json:"Path"`
				} `json:"S3Targets"`
				JdbcTargets []struct {
					ConnectionName string `json:"ConnectionName"`
					Path           string `json:"Path"`
				} `json:"JdbcTargets"`
				CatalogTargets []struct {
					DatabaseName string   `json:"DatabaseName"`
					Tables       []string `json:"Tables"`
				} `json:"CatalogTargets"`
			} `json:"Targets"`
		} `json:"Crawler"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, "cron(0 12 * * ? *)", out.Crawler.Schedule.ScheduleExpression)
	assert.Equal(t, `{"Version":1.0}`, out.Crawler.Configuration)
	assert.Equal(t, "raw_", out.Crawler.TablePrefix)
	assert.Equal(t, []string{"my-classifier"}, out.Crawler.Classifiers)
	require.Len(t, out.Crawler.Targets.JdbcTargets, 1)
	assert.Equal(t, "conn1", out.Crawler.Targets.JdbcTargets[0].ConnectionName)
	require.Len(t, out.Crawler.Targets.CatalogTargets, 1)
	assert.Equal(t, "db1", out.Crawler.Targets.CatalogTargets[0].DatabaseName)

	// UpdateCrawler should also accept and persist the same options.
	rec = doGlueRequest(t, h, "UpdateCrawler", map[string]any{
		"Name":        "crawler1",
		"Role":        "role1",
		"TablePrefix": "curated_",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "crawler1"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "curated_", out.Crawler.TablePrefix)
}

func TestBatch3_Crawler_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		check       func(t *testing.T, body string)
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name: "create",
			op:   "CreateCrawler",
			body: map[string]any{
				"Name":    "cr1",
				"Role":    "arn:aws:iam::123456789012:role/GlueRole",
				"Targets": map[string]any{"S3Targets": []map[string]any{{"Path": "s3://bucket/prefix"}}},
			},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
		},
		{
			name:     "create-duplicate",
			op:       "CreateCrawler",
			body:     map[string]any{"Name": "cr1", "Role": "arn:aws:iam::123:role/R"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get",
			op:       "GetCrawler",
			body:     map[string]any{"Name": "cr1"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "READY")
			},
		},
		{
			name:     "get-missing",
			op:       "GetCrawler",
			body:     map[string]any{"Name": "no-cr"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-crawlers",
			op:       "GetCrawlers",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "list-crawlers",
			op:       "ListCrawlers",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "update",
			op:   "UpdateCrawler",
			body: map[string]any{
				"Name":    "cr1",
				"Role":    "arn:aws:iam::123456789012:role/UpdatedRole",
				"Targets": map[string]any{},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "update-missing",
			op:       "UpdateCrawler",
			body:     map[string]any{"Name": "no-cr", "Role": "r"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "batch-get",
			op:       "BatchGetCrawlers",
			body:     map[string]any{"CrawlerNames": []string{"cr1", "no-cr"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete",
			op:       "DeleteCrawler",
			body:     map[string]any{"Name": "cr1"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateCrawler", map[string]any{
					"Name":    "cr1",
					"Role":    "arn:aws:iam::123456789012:role/GlueRole",
					"Targets": map[string]any{"S3Targets": []map[string]any{{"Path": "s3://bucket/prefix"}}},
				})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestBatch3_Crawler_Lifecycle_States(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantState string
		ops       []string
		wantCode  int
	}{
		{
			name:      "start-crawler",
			ops:       []string{"StartCrawler"},
			wantState: "RUNNING",
			wantCode:  http.StatusOK,
		},
		{
			name:     "start-already-running",
			ops:      []string{"StartCrawler", "StartCrawler"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:      "stop-running-crawler",
			ops:       []string{"StartCrawler", "StopCrawler"},
			wantState: "STOPPING",
			wantCode:  http.StatusOK,
		},
		{
			name:     "stop-not-running",
			ops:      []string{"StopCrawler"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name": "cr-lifecycle",
				"Role": "arn:aws:iam::123:role/R",
			})

			var lastCode int
			for _, op := range tt.ops {
				r := doGlueRequest(t, h, op, map[string]any{"Name": "cr-lifecycle"})
				lastCode = r.Code
			}

			assert.Equal(t, tt.wantCode, lastCode)
			if tt.wantState != "" && tt.wantCode == http.StatusOK {
				getRec := doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "cr-lifecycle"})
				var out map[string]any
				require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
				cr := out["Crawler"].(map[string]any)
				assert.Equal(t, tt.wantState, cr["State"])
			}
		})
	}
}

func TestBatch3_Crawler_Schedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "cr-sched",
		"Role": "arn:aws:iam::123:role/R",
	})

	updateSchedRec := doGlueRequest(t, h, "UpdateCrawlerSchedule", map[string]any{
		"CrawlerName": "cr-sched",
		"Schedule":    "cron(0 0 * * ? *)",
	})
	assert.Equal(t, http.StatusOK, updateSchedRec.Code)

	startSchedRec := doGlueRequest(t, h, "StartCrawlerSchedule", map[string]any{"CrawlerName": "cr-sched"})
	assert.Equal(t, http.StatusOK, startSchedRec.Code)

	stopSchedRec := doGlueRequest(t, h, "StopCrawlerSchedule", map[string]any{"CrawlerName": "cr-sched"})
	assert.Equal(t, http.StatusOK, stopSchedRec.Code)

	metricsRec := doGlueRequest(t, h, "GetCrawlerMetrics", map[string]any{"CrawlerNameList": []string{"cr-sched"}})
	assert.Equal(t, http.StatusOK, metricsRec.Code)
}

// TestAudit2_DeleteCrawler_WhileRunning tests that deleting a running crawler fails.
func TestAudit2_DeleteCrawler_WhileRunning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantError string
		wantCode  int
		startIt   bool
	}{
		{
			name:     "delete_ready_crawler_succeeds",
			startIt:  false,
			wantCode: http.StatusOK,
		},
		{
			name:      "delete_running_crawler_fails",
			startIt:   true,
			wantCode:  http.StatusBadRequest,
			wantError: "CrawlerRunningException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name": "dc-crawler",
				"Role": "arn:aws:iam::000000000000:role/GlueCrawlerRole",
				"Targets": map[string]any{
					"S3Targets": []map[string]any{{"Path": "s3://bucket/data/"}},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.startIt {
				rec = doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": "dc-crawler"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec = doGlueRequest(t, h, "DeleteCrawler", map[string]any{"Name": "dc-crawler"})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestAudit2_CreateCrawler_TagValidation tests tag validation on CreateCrawler.
func TestAudit2_CreateCrawler_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid_tags",
			tags:     map[string]string{"env": "test"},
			wantCode: http.StatusOK,
		},
		{
			name:     "key_too_long",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value_too_long",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name": "tagged-crawler-" + tt.name,
				"Role": "r",
				"Targets": map[string]any{
					"S3Targets": []map[string]any{{"Path": "s3://b/p"}},
				},
				"Tags": tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_HTTPBatchGetCrawlers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create database and crawler.
	postGlue(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	})
	postGlue(t, h, "CreateCrawler", map[string]any{
		"Name":         "crawler1",
		"Role":         "role",
		"DatabaseName": "db1",
	})

	rec := postGlue(t, h, "BatchGetCrawlers", map[string]any{
		"CrawlerNames": []string{"crawler1", "notexist"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	crawlers, ok := out["Crawlers"].([]any)
	require.True(t, ok)
	assert.Len(t, crawlers, 1)
	missing, ok := out["CrawlersNotFound"].([]any)
	require.True(t, ok)
	assert.Len(t, missing, 1)
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

// TestListCrawls_ReturnsRealCrawlHistory verifies ListCrawls surfaces the crawl
// history recorded by StartCrawler rather than always returning an empty list.
func TestListCrawls_ReturnsRealCrawlHistory(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "db"}})
	doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "my-crawler",
		"Role": "arn:aws:iam::000000000000:role/GlueCrawlerRole",
		"Targets": map[string]any{
			"S3Targets": []map[string]any{{"Path": "s3://my-bucket/data/"}},
		},
		"DatabaseName": "db",
	})

	rec := doGlueRequest(t, h, "ListCrawls", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "CrawlerName is required")

	rec = doGlueRequest(t, h, "ListCrawls", map[string]any{"CrawlerName": "no-such-crawler"})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "unknown crawler")

	rec = doGlueRequest(t, h, "ListCrawls", map[string]any{"CrawlerName": "my-crawler"})
	require.Equal(t, http.StatusOK, rec.Code)

	var before struct {
		Crawls []map[string]any `json:"Crawls"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &before))
	assert.Empty(t, before.Crawls, "no crawl has started yet")

	rec = doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": "my-crawler"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "ListCrawls", map[string]any{"CrawlerName": "my-crawler"})
	require.Equal(t, http.StatusOK, rec.Code)

	var after struct {
		Crawls []map[string]any `json:"Crawls"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &after))
	require.Len(t, after.Crawls, 1)
	assert.NotEmpty(t, after.Crawls[0]["CrawlId"])
	assert.NotEmpty(t, after.Crawls[0]["StartTime"])
}

func TestAccuracy_CreateCrawler_WithoutDatabase(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "my-crawler",
		"Role": "arn:aws:iam::000000000000:role/GlueCrawlerRole",
		"Targets": map[string]any{
			"S3Targets": []map[string]any{
				{"Path": "s3://my-bucket/data/"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerCrawlerSchedule_StartCrawler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		crawlerName string
		wantCode    int
	}{
		{
			name:        "success_ready_to_running",
			crawlerName: "my-crawler",
			wantCode:    http.StatusOK,
		},
		{
			name:        "already_running_error",
			crawlerName: "my-crawler",
			wantCode:    http.StatusBadRequest,
		},
		{
			name:        "not_found",
			crawlerName: "no-such-crawler",
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name":         "my-crawler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"DatabaseName": "db",
			})

			if tt.name == "already_running_error" {
				rec := doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": "my-crawler"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": tt.crawlerName})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerCrawlerSchedule_StopCrawler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		crawlerName string
		makeRunning bool
		wantCode    int
	}{
		{
			name:        "success_running_to_stopping",
			crawlerName: "my-crawler",
			makeRunning: true,
			wantCode:    http.StatusOK,
		},
		{
			name:        "not_running_error",
			crawlerName: "my-crawler",
			makeRunning: false,
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name":         "my-crawler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"DatabaseName": "db",
			})

			if tt.makeRunning {
				rec := doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": "my-crawler"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "StopCrawler", map[string]any{"Name": tt.crawlerName})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerCrawlerSchedule_UpdateCrawlerSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		crawlerName string
		schedule    string
		wantCode    int
	}{
		{
			name:        "success",
			crawlerName: "my-crawler",
			schedule:    "cron(0 12 * * ? *)",
			wantCode:    http.StatusOK,
		},
		{
			name:        "not_found",
			crawlerName: "no-such-crawler",
			schedule:    "cron(0 12 * * ? *)",
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name":         "my-crawler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"DatabaseName": "db",
			})

			rec := doGlueRequest(t, h, "UpdateCrawlerSchedule", map[string]any{
				"CrawlerName": tt.crawlerName,
				"Schedule":    tt.schedule,
			})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerCrawlerSchedule_StartCrawlerSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		crawlerName string
		preSchedule bool
		wantCode    int
	}{
		{
			name:        "success",
			crawlerName: "my-crawler",
			wantCode:    http.StatusOK,
		},
		{
			name:        "already_scheduled",
			crawlerName: "my-crawler",
			preSchedule: true,
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name":         "my-crawler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"DatabaseName": "db",
			})
			doGlueRequest(t, h, "UpdateCrawlerSchedule", map[string]any{
				"CrawlerName": "my-crawler",
				"Schedule":    "cron(0 12 * * ? *)",
			})

			if tt.preSchedule {
				rec := doGlueRequest(t, h, "StartCrawlerSchedule", map[string]any{"CrawlerName": "my-crawler"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "StartCrawlerSchedule", map[string]any{"CrawlerName": tt.crawlerName})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerCrawlerSchedule_StopCrawlerSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		crawlerName string
		wantCode    int
	}{
		{
			name:        "success",
			crawlerName: "my-crawler",
			wantCode:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name":         "my-crawler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"DatabaseName": "db",
			})

			rec := doGlueRequest(t, h, "StopCrawlerSchedule", map[string]any{"CrawlerName": tt.crawlerName})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
