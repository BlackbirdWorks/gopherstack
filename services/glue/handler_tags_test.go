package glue_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestInMemoryBackend_TaggedResources covers the enumeration method cli.go's
// wireTaggingGlue registers with the Resource Groups Tagging API (gopherstack-3xne).
// Glue has the widest resource-kind spread of any service wired this pass (databases,
// crawlers, jobs, data quality rulesets, connections, triggers, workflows), and unlike
// ECS/Athena/ECR it keeps tags inline on each typed resource rather than in a shared
// ARN-keyed side map -- so this proves the walk-every-collection enumeration picks up
// more than one kind, and that an untagged resource is excluded.
func TestInMemoryBackend_TaggedResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *glue.InMemoryBackend) map[string]map[string]string
		name  string
	}{
		{
			name: "database_and_job_tagged",
			setup: func(t *testing.T, b *glue.InMemoryBackend) map[string]map[string]string {
				t.Helper()

				db, err := b.CreateDatabase(glue.DatabaseInput{Name: "db1"}, map[string]string{"env": "prod"})
				require.NoError(t, err)

				job, err := b.CreateJob(glue.Job{
					Name:    "job1",
					Command: glue.JobCommand{Name: "glueetl", ScriptLocation: "s3://bucket/script.py"},
					Role:    "arn:aws:iam::123456789012:role/glue-role",
					Tags:    map[string]string{"team": "data"},
				})
				require.NoError(t, err)

				return map[string]map[string]string{
					db.ARN:  {"env": "prod"},
					job.ARN: {"team": "data"},
				}
			},
		},
		{
			name: "untagged_database_excluded",
			setup: func(t *testing.T, b *glue.InMemoryBackend) map[string]map[string]string {
				t.Helper()

				_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db-untagged"}, nil)
				require.NoError(t, err)

				return map[string]map[string]string{}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend("123456789012", "us-east-1")
			want := tt.setup(t, b)

			got := b.TaggedResources()
			gotMap := make(map[string]map[string]string, len(got))

			for _, e := range got {
				gotMap[e.ARN] = e.Tags
			}

			assert.Equal(t, want, gotMap)
		})
	}
}

// TestTagValidation tests tag validation constraints on TagResource and resource creation.
func TestTagValidation(t *testing.T) {
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

// TestTagResource_Validation tests TagResource validation.
func TestTagResource_Validation(t *testing.T) {
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
