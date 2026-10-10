package neptune_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func apiError(t *testing.T, err error) (string, string) {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode(), apiErr.ErrorMessage()
}

func TestSDK_CreateValidation(t *testing.T) {
	t.Parallel()

	createCluster := func(id, engine string) func(context.Context, *neptunesdk.Client) error {
		return func(ctx context.Context, c *neptunesdk.Client) error {
			_, err := c.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String(id), Engine: aws.String(engine),
			})

			return err
		}
	}

	tests := []struct {
		run      func(context.Context, *neptunesdk.Client) error
		name     string
		wantCode string
	}{
		{name: "valid", run: createCluster("c-1", "neptune")},
		{name: "wrong_engine", run: createCluster("c-1", "docdb"), wantCode: "InvalidParameterValue"},
		{
			name: "instance_class", wantCode: "InvalidParameterValue",
			run: func(ctx context.Context, c *neptunesdk.Client) error {
				if err := createCluster("c-1", "neptune")(ctx, c); err != nil {
					return err
				}
				_, err := c.CreateDBInstance(ctx, &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("i-1"), DBInstanceClass: aws.String("db.bogus"),
					Engine: aws.String("neptune"), DBClusterIdentifier: aws.String("c-1"),
				})

				return err
			},
		},
		{
			name: "subnet_group_name", wantCode: "InvalidParameterValue",
			run: func(ctx context.Context, c *neptunesdk.Client) error {
				_, err := c.CreateDBSubnetGroup(ctx, &neptunesdk.CreateDBSubnetGroupInput{
					DBSubnetGroupName: aws.String("bad!name"), DBSubnetGroupDescription: aws.String("d"),
					SubnetIds: []string{"s1", "s2"},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.run(t.Context(), newTestNeptuneClient(t, newTestHandler(t)))
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			code, msg := apiError(t, err)
			assert.Equal(t, tt.wantCode, code)
			assert.NotContains(t, msg, code+":")
		})
	}
}

func TestSDK_ResourceIDsAndNotFound(t *testing.T) {
	t.Parallel()

	client := newTestNeptuneClient(t, newTestHandler(t))
	ctx := t.Context()

	cl, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("c-1"), Engine: aws.String("neptune"),
	})
	require.NoError(t, err)
	assert.Regexp(t, `^cluster-[A-Z2-7]{26}$`, aws.ToString(cl.DBCluster.DbClusterResourceId))

	inst, err := client.CreateDBInstance(ctx, &neptunesdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("i-1"), DBInstanceClass: aws.String("db.r5.large"),
		Engine: aws.String("neptune"), DBClusterIdentifier: aws.String("c-1"),
	})
	require.NoError(t, err)
	assert.Regexp(t, `^db-[A-Z2-7]{26}$`, aws.ToString(inst.DBInstance.DbiResourceId))

	_, err = client.DescribeDBClusters(
		ctx,
		&neptunesdk.DescribeDBClustersInput{DBClusterIdentifier: aws.String("nope")},
	)
	code, msg := apiError(t, err)
	assert.Equal(t, "DBClusterNotFoundFault", code)
	assert.Equal(t, "DBCluster not found: nope", msg)
}

func TestSDK_DescribePagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		marker     *string
		maxRecords *int32
		name       string
		wantErr    bool
	}{
		{name: "bad_marker", marker: aws.String("garbage"), wantErr: true},
		{name: "max_too_large", maxRecords: aws.Int32(101), wantErr: true},
		{name: "ok", maxRecords: aws.Int32(2)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestNeptuneClient(t, newTestHandler(t))
			for i := range 5 {
				_, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String(fmt.Sprintf("pg-%d", i)), Engine: aws.String("neptune"),
				})
				require.NoError(t, err)
			}

			out, err := client.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
				Marker: tt.marker, MaxRecords: tt.maxRecords,
			})
			if tt.wantErr {
				code, _ := apiError(t, err)
				assert.Equal(t, "InvalidParameterValue", code)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.DBClusters, 2)
			require.NotNil(t, out.Marker)

			next, err := client.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
				Marker: out.Marker, MaxRecords: tt.maxRecords,
			})
			require.NoError(t, err)
			assert.Len(t, next.DBClusters, 2)
		})
	}
}
