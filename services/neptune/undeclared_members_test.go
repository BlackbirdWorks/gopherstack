package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_DBClusterRoundTripAfterTrim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		port int32
	}{
		{name: "default-port", port: 0},
		{name: "custom-port", port: 8183},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newMembersClient(t)

			cl := mustCreateCluster(t, c, "rt-"+tc.name, func(in *neptunesdk.CreateDBClusterInput) {
				if tc.port != 0 {
					in.Port = aws.Int32(tc.port)
				}
			})
			assert.Equal(t, "rt-"+tc.name, aws.ToString(cl.DBClusterIdentifier))
			assert.NotEmpty(t, aws.ToString(cl.DBClusterArn))
			assert.Equal(t, "neptune", aws.ToString(cl.Engine))
			assert.NotZero(t, aws.ToInt32(cl.Port))

			if tc.port != 0 {
				assert.Equal(t, tc.port, aws.ToInt32(cl.Port))
			}

			got, err := c.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
				DBClusterIdentifier: cl.DBClusterIdentifier,
			})
			require.NoError(t, err)
			require.Len(t, got.DBClusters, 1)
			assert.Equal(t, aws.ToString(cl.DBClusterArn), aws.ToString(got.DBClusters[0].DBClusterArn))
		})
	}
}
