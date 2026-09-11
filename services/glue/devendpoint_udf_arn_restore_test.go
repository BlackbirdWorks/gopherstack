package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestSnapshotRestore_DevEndpointARN_UDFFunctionARN_Survive pins
// gopherstack-evpmb: DevEndpoint.ARN and UserDefinedFunction.FunctionARN are
// json:"-" (real Glue's types.DevEndpoint/types.UserDefinedFunction expose
// no ARN on the wire -- aws-sdk-go-v2 service/glue/types/types.go), so
// neither round-trips through a snapshot on its own. Restore must recompute
// both from the same deterministic devEndpointARN/udfARN helpers
// CreateDevEndpoint/CreateUserDefinedFunction use (persistence.go's
// restoreDeterministicARNs), or every pre-existing dev endpoint/UDF loses
// its ARN across a restart.
func TestSnapshotRestore_DevEndpointARN_UDFFunctionARN_Survive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, b *glue.InMemoryBackend)
		name   string
	}{
		{
			name: "devendpoint_arn",
			verify: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()

				dep, err := b.GetDevEndpoint("dep1")
				require.NoError(t, err)
				assert.NotEmpty(t, dep.ARN)
				assert.Equal(t, "arn:aws:glue:us-east-1:123456789012:devEndpoint/dep1", dep.ARN)
			},
		},
		{
			name: "udf_function_arn",
			verify: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()

				udf, err := b.GetUserDefinedFunction("db1", "udf1")
				require.NoError(t, err)
				assert.NotEmpty(t, udf.FunctionARN)
				assert.Equal(t,
					"arn:aws:glue:us-east-1:123456789012:userDefinedFunction/db1/udf1",
					udf.FunctionARN,
				)
			},
		},
		{
			name: "tagged_resources",
			verify: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()

				byARN := make(map[string]map[string]string)
				for _, e := range b.TaggedResources() {
					byARN[e.ARN] = e.Tags
				}

				depTags, ok := byARN["arn:aws:glue:us-east-1:123456789012:devEndpoint/dep1"]
				require.True(t, ok, "dev endpoint ARN missing from TaggedResources")
				assert.Equal(t, "dep-tag-value", depTags["dep-tag-key"])

				_, emptyKeyPresent := byARN[""]
				assert.False(t, emptyKeyPresent, "a resource reported an empty ARN")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			orig := glue.NewInMemoryBackend("123456789012", "us-east-1")

			_, err := orig.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
			require.NoError(t, err)

			_, err = orig.CreateDevEndpoint(
				"dep1", glue.DevEndpointInput{}, "arn:aws:iam::123456789012:role/dep-role",
				map[string]string{"dep-tag-key": "dep-tag-value"},
			)
			require.NoError(t, err)

			_, err = orig.CreateUserDefinedFunction(
				"db1", glue.UserDefinedFunction{FunctionName: "udf1"}, nil,
			)
			require.NoError(t, err)

			snap := orig.Snapshot(t.Context())
			require.NotNil(t, snap)

			restored := glue.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, restored.Restore(t.Context(), snap))

			tc.verify(t, restored)
		})
	}
}

// TestUpdateUserDefinedFunction_AfterRestore_PreservesFunctionARN pins the
// second live read site: UpdateUserDefinedFunction carries existing.FunctionARN
// forward into the updated record (user_defined_functions.go). Before the
// fix, a restore-then-update sequence would permanently overwrite a UDF's
// FunctionARN with "" instead of merely reading it back empty once.
func TestUpdateUserDefinedFunction_AfterRestore_PreservesFunctionARN(t *testing.T) {
	t.Parallel()

	orig := glue.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := orig.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)
	_, err = orig.CreateUserDefinedFunction("db1", glue.UserDefinedFunction{FunctionName: "udf1"}, nil)
	require.NoError(t, err)

	snap := orig.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := glue.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	require.NoError(t, restored.UpdateUserDefinedFunction(
		"db1", "udf1", glue.UserDefinedFunction{FunctionName: "udf1", ClassName: "com.example.Fn"},
	))

	udf, err := restored.GetUserDefinedFunction("db1", "udf1")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:glue:us-east-1:123456789012:userDefinedFunction/db1/udf1", udf.FunctionARN)
}
