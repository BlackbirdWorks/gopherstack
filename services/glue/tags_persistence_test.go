package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestInMemoryBackend_TagsSurviveRestore covers gopherstack-x6y9e: Tags is
// json:"-" on all eleven taggable Glue structs -- it is never part of a real
// Get*/Describe* wire shape (tags come back only via GetTags), so
// store.Table's per-row JSON persistence silently dropped it on restart.
// Each subtest tags a resource, removes one of those tags, snapshots,
// restores into a fresh backend, and checks GetTags reflects both the
// surviving tag and the removal.
func TestInMemoryBackend_TagsSurviveRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, b *glue.InMemoryBackend) string
		name   string
	}{
		{
			name: "database",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				db, err := b.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
				require.NoError(t, err)

				return db.ARN
			},
		},
		{
			name: "crawler",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				c, err := b.CreateCrawler("crawler1", "role1", "", glue.CrawlerTarget{}, nil)
				require.NoError(t, err)

				return c.ARN
			},
		},
		{
			name: "job",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				j, err := b.CreateJob(glue.Job{
					Name:    "job1",
					Command: glue.JobCommand{Name: "glueetl", ScriptLocation: "s3://bucket/script.py"},
					Role:    "arn:aws:iam::000000000000:role/glue-role",
				})
				require.NoError(t, err)

				return j.ARN
			},
		},
		{
			name: "dataqualityruleset",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				r, err := b.CreateDataQualityRuleset("ruleset1", "Rules = []", nil)
				require.NoError(t, err)

				return r.ARN
			},
		},
		{
			name: "connection",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				conn, err := b.CreateConnection("conn1", "JDBC", nil, nil)
				require.NoError(t, err)

				return conn.ARN
			},
		},
		{
			name: "trigger",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				trig, err := b.CreateTrigger(glue.Trigger{Name: "trig1"}, nil)
				require.NoError(t, err)

				return trig.ARN
			},
		},
		{
			name: "workflow",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				w, err := b.CreateWorkflow(glue.Workflow{Name: "wf1"}, nil)
				require.NoError(t, err)

				return w.ARN
			},
		},
		{
			name: "blueprint",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				bp, err := b.CreateBlueprint("bp1", "s3://bucket/bp", "", nil)
				require.NoError(t, err)

				return arn.Build("glue", b.Region(), b.AccountID(), "blueprint/"+bp.Name)
			},
		},
		{
			name: "devendpoint",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				dep, err := b.CreateDevEndpoint(
					"dep1", glue.DevEndpointInput{}, "arn:aws:iam::000000000000:role/R", nil,
				)
				require.NoError(t, err)

				return dep.ARN
			},
		},
		{
			name: "mltransform",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				m, err := b.CreateMLTransformWithOptions("mt1", "", "arn:aws:iam::000000000000:role/R", nil,
					glue.MLTransformParameter{}, nil, glue.MLTransformOptions{})
				require.NoError(t, err)

				return arn.Build("glue", b.Region(), b.AccountID(), "mlTransform/"+m.TransformID)
			},
		},
		{
			name: "userdefinedfunction",
			create: func(t *testing.T, b *glue.InMemoryBackend) string {
				t.Helper()

				_, err := b.CreateDatabase(glue.DatabaseInput{Name: "udfdb"}, nil)
				require.NoError(t, err)

				u, err := b.CreateUserDefinedFunction("udfdb", glue.UserDefinedFunction{FunctionName: "fn1"}, nil)
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

			require.NoError(t, b.TagResource(resourceARN, map[string]string{"env": "prod", "team": "data"}))
			require.NoError(t, b.UntagResource(resourceARN, []string{"team"}))

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			restored := glue.NewInMemoryBackend(testAccountID, testRegion)
			require.NoError(t, restored.Restore(t.Context(), snap))

			got, err := restored.GetTags(resourceARN)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, got)
		})
	}
}
