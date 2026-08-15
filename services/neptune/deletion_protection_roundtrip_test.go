package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/aws-sdk-go-v2/service/neptune/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// TestDeleteGlobalCluster_DeletionProtectionRoundTrip proves ModifyGlobalCluster's
// DeletionProtection has an effect on DeleteGlobalCluster, mirroring the sibling fix
// already present in rds and the identical fix just made in docdb for the same
// concept. DeleteGlobalCluster's own deserializer (neptune@v1.48.4
// deserializers.go:2905-2911) models InvalidGlobalClusterStateFault as a typed error
// for this op -- before the fix, the field was stored on the global cluster and read
// only by Describe/serialization code, so DeleteGlobalCluster always succeeded
// regardless of the setting.
func TestDeleteGlobalCluster_DeletionProtectionRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		id        string
		protected bool
		wantErr   bool
	}{
		{"protected blocks delete", "dp-rt-protected", true, true},
		{"unprotected allows delete", "dp-rt-unprotected", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := neptune.NewInMemoryBackend("000000000000", testRegion)
			h := neptune.NewHandler(backend)
			client := newTestNeptuneClient(t, h)
			ctx := t.Context()

			_, err := client.CreateGlobalCluster(ctx, &neptunesdk.CreateGlobalClusterInput{
				GlobalClusterIdentifier: aws.String(tt.id),
			})
			require.NoError(t, err)

			_, err = client.ModifyGlobalCluster(ctx, &neptunesdk.ModifyGlobalClusterInput{
				GlobalClusterIdentifier: aws.String(tt.id),
				DeletionProtection:      aws.Bool(tt.protected),
			})
			require.NoError(t, err)

			_, err = client.DeleteGlobalCluster(ctx, &neptunesdk.DeleteGlobalClusterInput{
				GlobalClusterIdentifier: aws.String(tt.id),
			})

			if tt.wantErr {
				require.Error(t, err)

				var invalidState *types.InvalidGlobalClusterStateFault
				require.ErrorAs(t, err, &invalidState,
					"expected a typed InvalidGlobalClusterStateFault, got %v", err)

				return
			}

			require.NoError(t, err)
		})
	}
}
