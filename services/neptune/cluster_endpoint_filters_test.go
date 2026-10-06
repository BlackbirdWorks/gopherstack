package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/aws-sdk-go-v2/service/neptune/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// TestDescribeDBClusterEndpoints_Filters covers the documented filter names
// (neptune@v1.48.4 api_op_DescribeDBClusterEndpoints.go:42-51).
func TestDescribeDBClusterEndpoints_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{name: "none", want: []string{"ep-a", "ep-b"}},
		{
			name:    "id",
			filters: []types.Filter{{Name: aws.String("db-cluster-endpoint-id"), Values: []string{"ep-b"}}},
			want:    []string{"ep-b"},
		},
		{
			name: "type_or",
			filters: []types.Filter{
				{Name: aws.String("db-cluster-endpoint-type"), Values: []string{"reader", "writer"}},
			},
			want: []string{"ep-a"},
		},
		{
			name:    "status_miss",
			filters: []types.Filter{{Name: aws.String("db-cluster-endpoint-status"), Values: []string{"deleting"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := neptune.NewInMemoryBackend("000000000000", testRegion)
			client := newTestNeptuneClient(t, neptune.NewHandler(backend))

			_, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("c1"), Engine: aws.String("neptune"),
			})
			require.NoError(t, err)

			for id, typ := range map[string]string{"ep-a": "READER", "ep-b": "CUSTOM"} {
				_, err = client.CreateDBClusterEndpoint(t.Context(), &neptunesdk.CreateDBClusterEndpointInput{
					DBClusterIdentifier:         aws.String("c1"),
					DBClusterEndpointIdentifier: aws.String(id),
					EndpointType:                aws.String(typ),
				})
				require.NoError(t, err)
			}

			out, err := client.DescribeDBClusterEndpoints(t.Context(), &neptunesdk.DescribeDBClusterEndpointsInput{
				Filters: tt.filters,
			})
			require.NoError(t, err)

			got := make([]string, 0)
			for _, e := range out.DBClusterEndpoints {
				got = append(got, aws.ToString(e.DBClusterEndpointIdentifier))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
