package detective_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	detectivesdk "github.com/aws/aws-sdk-go-v2/service/detective"
	detectivetypes "github.com/aws/aws-sdk-go-v2/service/detective/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

func TestListDatasourcePackages_LastIngestStateChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pkg  detectivetypes.DatasourcePackage
	}{
		{name: "detective_core", pkg: detectivetypes.DatasourcePackageDetectiveCore},
		{name: "eks_audit", pkg: detectivetypes.DatasourcePackageEksAudit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := detective.NewInMemoryBackend("000000000000", detectiveRTRegion)
			client := newTestDetectiveSDKClient(t, detective.NewHandler(backend))

			graph, err := client.CreateGraph(t.Context(), &detectivesdk.CreateGraphInput{})
			require.NoError(t, err)

			_, err = client.UpdateDatasourcePackages(t.Context(), &detectivesdk.UpdateDatasourcePackagesInput{
				GraphArn:           graph.GraphArn,
				DatasourcePackages: []detectivetypes.DatasourcePackage{tt.pkg},
			})
			require.NoError(t, err)

			out, err := client.ListDatasourcePackages(t.Context(), &detectivesdk.ListDatasourcePackagesInput{
				GraphArn: aws.String(aws.ToString(graph.GraphArn)),
			})
			require.NoError(t, err)

			detail, ok := out.DatasourcePackages[string(tt.pkg)]
			require.True(t, ok)

			ts, ok := detail.LastIngestStateChange[string(detail.DatasourcePackageIngestState)]
			require.True(t, ok, "LastIngestStateChange keyed by the current ingest state")
			require.NotNil(t, ts.Timestamp)
			assert.False(t, ts.Timestamp.IsZero())
		})
	}
}
