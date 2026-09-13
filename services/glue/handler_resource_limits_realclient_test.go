package glue_test

import (
	"testing"

	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestSDKRoundTrip_ResourceNumberLimitExceeded drives CreateDatabase through
// the real aws-sdk-go-v2 client against httptest, past a lowered
// WithResourceLimits cap, and proves the resulting
// ResourceNumberLimitExceededException round-trips through the real SDK
// deserializer (types.ResourceNumberLimitExceededException, HTTP 400 --
// FaultClient per aws-sdk-go-v2/service/glue/types/errors.go) rather than
// only through the hand-rolled JSON assertions in
// handler_resource_limits_test.go. Covers gopherstack-qd3.5.
func TestSDKRoundTrip_ResourceNumberLimitExceeded(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion).
		WithResourceLimits(glue.ResourceLimits{Databases: testResourceCap})
	h := glue.NewHandler(backend)
	client := newTestGlueClient(t, h)

	for i := range testResourceCap {
		name := "sdk-rnle-db-" + string(rune('a'+i))
		_, err := client.CreateDatabase(t.Context(), &gluesdk.CreateDatabaseInput{
			DatabaseInput: &types.DatabaseInput{Name: &name},
		})
		require.NoError(t, err, "database %d should succeed under the limit", i)
	}

	overLimitName := "sdk-rnle-db-over-limit"
	_, err := client.CreateDatabase(t.Context(), &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: &overLimitName},
	})
	require.Error(t, err)

	var rnle *types.ResourceNumberLimitExceededException
	require.ErrorAs(t, err, &rnle, "CreateDatabase over the limit must decode as ResourceNumberLimitExceededException")
}
