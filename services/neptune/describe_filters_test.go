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

func TestDescribeDBClustersAndInstancesFilters_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filters      func(arnA string) []types.Filter
		wantClusters []string
		wantInsts    []string
	}{
		{
			name: "cluster id",
			filters: func(string) []types.Filter {
				return []types.Filter{{Name: aws.String("db-cluster-id"), Values: []string{"fc-a"}}}
			},
			wantClusters: []string{"fc-a"},
			wantInsts:    []string{"fc-a-i"},
		},
		{
			name: "cluster arn",
			filters: func(arn string) []types.Filter {
				return []types.Filter{{Name: aws.String("db-cluster-id"), Values: []string{arn}}}
			},
			wantClusters: []string{"fc-a"},
			wantInsts:    []string{"fc-a-i"},
		},
		{
			name: "multiple values or",
			filters: func(string) []types.Filter {
				return []types.Filter{{Name: aws.String("db-cluster-id"), Values: []string{"fc-a", "fc-b"}}}
			},
			wantClusters: []string{"fc-a", "fc-b"},
			wantInsts:    []string{"fc-a-i", "fc-b-i"},
		},
		{
			name: "engine match",
			filters: func(string) []types.Filter {
				return []types.Filter{{Name: aws.String("engine"), Values: []string{"neptune"}}}
			},
			wantClusters: []string{"fc-a", "fc-b"},
			wantInsts:    []string{"fc-a-i", "fc-b-i"},
		},
		{
			name: "engine no match",
			filters: func(string) []types.Filter {
				return []types.Filter{{Name: aws.String("engine"), Values: []string{"docdb"}}}
			},
			wantClusters: []string{},
			wantInsts:    []string{},
		},
		{
			name: "filters and",
			filters: func(string) []types.Filter {
				return []types.Filter{
					{Name: aws.String("db-cluster-id"), Values: []string{"fc-a"}},
					{Name: aws.String("engine"), Values: []string{"docdb"}},
				}
			},
			wantClusters: []string{},
			wantInsts:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := neptune.NewInMemoryBackend("000000000000", testRegion)
			client := newTestNeptuneClient(t, neptune.NewHandler(backend))

			var arnA string

			for _, id := range []string{"fc-a", "fc-b"} {
				out, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String(id),
					Engine:              aws.String("neptune"),
				})
				require.NoError(t, err)

				if id == "fc-a" {
					arnA = aws.ToString(out.DBCluster.DBClusterArn)
				}

				_, err = client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String(id + "-i"),
					DBClusterIdentifier:  aws.String(id),
					DBInstanceClass:      aws.String("db.r5.large"),
					Engine:               aws.String("neptune"),
				})
				require.NoError(t, err)
			}

			clusters, err := client.DescribeDBClusters(
				t.Context(),
				&neptunesdk.DescribeDBClustersInput{Filters: tt.filters(arnA)},
			)
			require.NoError(t, err)

			gotC := make([]string, 0)
			for _, c := range clusters.DBClusters {
				gotC = append(gotC, aws.ToString(c.DBClusterIdentifier))
			}

			assert.ElementsMatch(t, tt.wantClusters, gotC)

			insts, err := client.DescribeDBInstances(
				t.Context(),
				&neptunesdk.DescribeDBInstancesInput{Filters: tt.filters(arnA)},
			)
			require.NoError(t, err)

			gotI := make([]string, 0)
			for _, i := range insts.DBInstances {
				gotI = append(gotI, aws.ToString(i.DBInstanceIdentifier))
			}

			assert.ElementsMatch(t, tt.wantInsts, gotI)
		})
	}
}
