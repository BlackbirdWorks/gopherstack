package glue_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAudit2_TagValidation tests tag validation constraints on TagResource and resource creation.
func TestAudit2_TagValidation(t *testing.T) {
	t.Parallel()

	longKey := strings.Repeat("k", 129)
	longValue := strings.Repeat("v", 257)
	maxTags := make(map[string]string, 51)
	for i := range 51 {
		maxTags[strings.Repeat("a", 1)+string(rune('a'+i%26))+string(rune('a'+i/26))] = "val"
	}

	tests := []struct {
		setup    func(h interface{ createJob() })
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "tag_key_too_long",
			tags:     map[string]string{longKey: "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag_value_too_long",
			tags:     map[string]string{"k": longValue},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "too_many_tags",
			tags:     maxTags,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid_tags",
			tags:     map[string]string{"env": "prod", "team": "data"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
				"DatabaseInput": map[string]any{"Name": "tagdb-" + tt.name},
				"Tags":          tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestAudit2_TagResource_Validation tests TagResource validation.
func TestAudit2_TagResource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
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
		{
			name:     "valid_tag",
			tags:     map[string]string{"key": "val"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler(t)
			doGlueRequest(t, h2, "CreateDatabase", map[string]any{
				"DatabaseInput": map[string]any{"Name": "tagresdb2"},
			})
			rec := doGlueRequest(t, h2, "GetDatabase", map[string]any{"Name": "tagresdb2"})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doGlueRequest(t, h2, "TagResource", map[string]any{
				"ResourceArn": "arn:aws:glue:us-east-1:000000000000:database/tagresdb2",
				"TagsToAdd":   tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
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

// TestAudit2_CreateJob_CommandNameRequired tests that CreateJob requires Command.Name.
func TestAudit2_CreateJob_CommandNameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_command_rejected",
			body:     map[string]any{"Name": "j1", "Role": "r"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_command_name_rejected",
			body:     map[string]any{"Name": "j2", "Role": "r", "Command": map[string]any{"Name": ""}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid_command_name_accepted",
			body:     map[string]any{"Name": "j3", "Role": "r", "Command": map[string]any{"Name": "glueetl"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "pythonshell_accepted",
			body:     map[string]any{"Name": "j4", "Role": "r", "Command": map[string]any{"Name": "pythonshell"}},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateJob", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestAudit2_CreateJob_MaxRetries tests MaxRetries bounds validation.
func TestAudit2_CreateJob_MaxRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxRetries int
		wantCode   int
	}{
		{
			name:       "zero_retries_ok",
			maxRetries: 0,
			wantCode:   http.StatusOK,
		},
		{
			name:       "max_retries_ok",
			maxRetries: 10,
			wantCode:   http.StatusOK,
		},
		{
			name:       "eleven_retries_rejected",
			maxRetries: 11,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "negative_retries_rejected",
			maxRetries: -1,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CreateJob", map[string]any{
				"Name":       "retry-job-" + tt.name,
				"Role":       "r",
				"Command":    map[string]any{"Name": "glueetl"},
				"MaxRetries": tt.maxRetries,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestAudit2_UpdateJob_MaxRetries tests MaxRetries bounds on UpdateJob.
func TestAudit2_UpdateJob_MaxRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxRetries int
		wantCode   int
	}{
		{
			name:       "valid_retries",
			maxRetries: 5,
			wantCode:   http.StatusOK,
		},
		{
			name:       "too_many_retries",
			maxRetries: 11,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateJob", map[string]any{
				"Name":    "upjob-" + tt.name,
				"Role":    "r",
				"Command": map[string]any{"Name": "glueetl"},
			})

			rec := doGlueRequest(t, h, "UpdateJob", map[string]any{
				"JobName": "upjob-" + tt.name,
				"JobUpdate": map[string]any{
					"Role":       "r",
					"MaxRetries": tt.maxRetries,
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestAudit2_NameLengthLimits tests max 255-char name enforcement.
func TestAudit2_NameLengthLimits(t *testing.T) {
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

// TestAudit2_BatchCreatePartition_Limit tests the 100-partition-per-call limit.
func TestAudit2_BatchCreatePartition_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		partCount int
		wantCode  int
	}{
		{
			name:      "exactly_100_ok",
			partCount: 100,
			wantCode:  http.StatusOK,
		},
		{
			name:      "101_partitions_rejected",
			partCount: 101,
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestDB(t, h, "bcp-db-"+tt.name, "bcp-tbl")

			parts := make([]map[string]any, tt.partCount)
			for i := range tt.partCount {
				parts[i] = map[string]any{
					"Values": []string{string(rune('a' + i%26))},
				}
			}

			rec := doGlueRequest(t, h, "BatchCreatePartition", map[string]any{
				"DatabaseName":       "bcp-db-" + tt.name,
				"TableName":          "bcp-tbl",
				"PartitionInputList": parts,
			})
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

// TestAudit2_CreateJob_TagValidation tests tag validation on CreateJob.
func TestAudit2_CreateJob_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid_tags",
			tags:     map[string]string{"env": "prod"},
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
			rec := doGlueRequest(t, h, "CreateJob", map[string]any{
				"Name":    "tagged-job-" + tt.name,
				"Role":    "r",
				"Command": map[string]any{"Name": "glueetl"},
				"Tags":    tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
