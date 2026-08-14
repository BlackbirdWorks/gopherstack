package docdb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

// TestDeleteGlobalCluster_DeletionProtectionRoundTrip proves ModifyGlobalCluster's
// DeletionProtection has an effect on DeleteGlobalCluster, mirroring the enforcement
// this package's DeleteDBCluster/DeleteDBInstance already have. DeleteGlobalCluster's
// own deserializer (docdb@v1.51.4 deserializers.go:2261) models
// InvalidGlobalClusterStateFault as a typed error for this op -- before the fix, the
// field was stored on the global cluster and read only by Describe/serialization code,
// so DeleteGlobalCluster always succeeded regardless of the setting. (The test sets the
// flag via ModifyGlobalCluster rather than at CreateGlobalCluster time because the
// handler's CreateGlobalCluster path doesn't parse the request's DeletionProtection
// member at all -- a separate, pre-existing gap out of scope for this fix.)
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

			backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
			h := docdb.NewHandler(backend)
			client := newTestDocDBClient(t, h)
			ctx := t.Context()

			_, err := client.CreateGlobalCluster(ctx, &docdbsdk.CreateGlobalClusterInput{
				GlobalClusterIdentifier: aws.String(tt.id),
				Engine:                  aws.String("docdb"),
			})
			require.NoError(t, err)

			_, err = client.ModifyGlobalCluster(ctx, &docdbsdk.ModifyGlobalClusterInput{
				GlobalClusterIdentifier: aws.String(tt.id),
				DeletionProtection:      aws.Bool(tt.protected),
			})
			require.NoError(t, err)

			_, err = client.DeleteGlobalCluster(ctx, &docdbsdk.DeleteGlobalClusterInput{
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
