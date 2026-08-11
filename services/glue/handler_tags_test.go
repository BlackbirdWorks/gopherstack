package glue_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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

// TestTagResource_BlueprintDevEndpointMLTransformUDF covers TagResource/
// UntagResource/GetTags dispatch for the four resource kinds tagResource's
// ARN dispatcher previously did not recognize (gopherstack-dol3): Blueprint,
// DevEndpoint, MLTransform, UserDefinedFunction. Also proves creation-time
// tags survive: for MLTransform/UDF they previously vanished entirely (no
// Tags field existed on either struct, and the internal tagResource(ARN, ...)
// call made from CreateMLTransformWithOptions/CreateUserDefinedFunction
// silently failed against the un-dispatched ARN).
func TestTagResource_BlueprintDevEndpointMLTransformUDF(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, b *glue.InMemoryBackend) string
		name   string
	}{
		{
			name: "blueprint",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				bp, err := b.CreateBlueprint("bp1", "s3://bucket/bp", "", map[string]string{"env": "prod"})
				require.NoError(t, err)

				return arn.Build("glue", b.Region(), b.AccountID(), "blueprint/"+bp.Name)
			},
		},
		{
			name: "devendpoint",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				dep, err := b.CreateDevEndpoint("dep1", glue.DevEndpointInput{}, "arn:aws:iam::000000000000:role/R",
					map[string]string{"env": "prod"})
				require.NoError(t, err)

				return dep.ARN
			},
		},
		{
			name: "mltransform",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				m, err := b.CreateMLTransformWithOptions("mt1", "", "arn:aws:iam::000000000000:role/R", nil,
					glue.MLTransformParameter{}, map[string]string{"env": "prod"}, glue.MLTransformOptions{})
				require.NoError(t, err)

				return arn.Build("glue", b.Region(), b.AccountID(), "mlTransform/"+m.TransformID)
			},
		},
		{
			name: "udf",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				_, err := b.CreateDatabase(glue.DatabaseInput{Name: "udfdb"}, nil)
				require.NoError(t, err)

				u, err := b.CreateUserDefinedFunction("udfdb", glue.UserDefinedFunction{FunctionName: "fn1"},
					map[string]string{"env": "prod"})
				require.NoError(t, err)

				return u.FunctionARN
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			resourceARN := tt.create(t, b)

			got, err := b.GetTags(resourceARN)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, got)

			require.NoError(t, b.TagResource(resourceARN, map[string]string{"team": "data"}))

			got, err = b.GetTags(resourceARN)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "prod", "team": "data"}, got)

			require.NoError(t, b.UntagResource(resourceARN, []string{"env"}))

			got, err = b.GetTags(resourceARN)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"team": "data"}, got)
		})
	}
}

// TestTagResource_SurvivesUpdate covers a second bug found alongside the ARN
// dispatch gap (gopherstack-dol3): UpdateMLTransform/UpdateUserDefinedFunction
// replace the whole stored record with the caller-supplied input, and neither
// UpdateMLTransformRequest nor UpdateUserDefinedFunctionInput carries Tags on
// the real wire (AWS updates tags only via TagResource/UntagResource) -- so
// without explicitly carrying the existing Tags forward, any Update call
// silently wiped a resource's tags.
func TestTagResource_SurvivesUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *glue.InMemoryBackend) (resourceARN string, doUpdate func(t *testing.T))
		name  string
	}{
		{
			name: "mltransform",
			setup: func(t *testing.T, b *glue.InMemoryBackend) (string, func(t *testing.T)) {
				t.Helper()

				m, err := b.CreateMLTransformWithOptions("mt-upd", "", "arn:aws:iam::000000000000:role/R", nil,
					glue.MLTransformParameter{}, map[string]string{"env": "prod"}, glue.MLTransformOptions{})
				require.NoError(t, err)

				resourceARN := arn.Build("glue", b.Region(), b.AccountID(), "mlTransform/"+m.TransformID)

				return resourceARN, func(t *testing.T) {
					t.Helper()
					require.NoError(t, b.UpdateMLTransform(m.TransformID, glue.MLTransform{Description: "updated"}))
				}
			},
		},
		{
			name: "udf",
			setup: func(t *testing.T, b *glue.InMemoryBackend) (string, func(t *testing.T)) {
				t.Helper()

				_, err := b.CreateDatabase(glue.DatabaseInput{Name: "udfdb-upd"}, nil)
				require.NoError(t, err)

				u, err := b.CreateUserDefinedFunction("udfdb-upd", glue.UserDefinedFunction{FunctionName: "fn1"},
					map[string]string{"env": "prod"})
				require.NoError(t, err)

				return u.FunctionARN, func(t *testing.T) {
					t.Helper()
					require.NoError(t, b.UpdateUserDefinedFunction("udfdb-upd", "fn1",
						glue.UserDefinedFunction{FunctionName: "fn1", ClassName: "com.example.Fn"}))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			resourceARN, doUpdate := tt.setup(t, b)

			doUpdate(t)

			got, err := b.GetTags(resourceARN)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, got)
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
