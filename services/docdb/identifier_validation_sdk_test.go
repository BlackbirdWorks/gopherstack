package docdb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func apiErrorCode(t *testing.T, err error) (string, string) {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode(), apiErr.ErrorMessage()
}

func TestSDK_CreateValidation(t *testing.T) {
	t.Parallel()

	createCluster := func(id, engine, user string) func(context.Context, *docdbsdk.Client) error {
		return func(ctx context.Context, c *docdbsdk.Client) error {
			_, err := c.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String(id), Engine: aws.String(engine),
				MasterUsername: aws.String(user), MasterUserPassword: aws.String("password-123"),
			})

			return err
		}
	}

	tests := []struct {
		run      func(context.Context, *docdbsdk.Client) error
		name     string
		wantCode string
	}{
		{name: "valid", run: createCluster("c-1", "docdb", "admin")},
		{name: "id_digit_first", run: createCluster("1bad", "docdb", "admin"), wantCode: "InvalidParameterValue"},
		{name: "id_trailing_hyphen", run: createCluster("bad-", "docdb", "admin"), wantCode: "InvalidParameterValue"},
		{name: "id_double_hyphen", run: createCluster("bad--id", "docdb", "admin"), wantCode: "InvalidParameterValue"},
		{name: "wrong_engine", run: createCluster("c-1", "mysql", "admin"), wantCode: "InvalidParameterValue"},
		{name: "username_digit_first", run: createCluster("c-1", "docdb", "1admin"), wantCode: "InvalidParameterValue"},
		{
			name: "instance_class", wantCode: "InvalidParameterValue",
			run: func(ctx context.Context, c *docdbsdk.Client) error {
				if err := createCluster("c-1", "docdb", "admin")(ctx, c); err != nil {
					return err
				}
				_, err := c.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("i-1"), DBInstanceClass: aws.String("db.bogus"),
					Engine: aws.String("docdb"), DBClusterIdentifier: aws.String("c-1"),
				})

				return err
			},
		},
		{
			name: "subnet_group_name", wantCode: "InvalidParameterValue",
			run: func(ctx context.Context, c *docdbsdk.Client) error {
				_, err := c.CreateDBSubnetGroup(ctx, &docdbsdk.CreateDBSubnetGroupInput{
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

			err := tt.run(t.Context(), newRealClient(t))
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			code, msg := apiErrorCode(t, err)
			assert.Equal(t, tt.wantCode, code)
			assert.NotContains(t, msg, code+":")
		})
	}
}

func TestSDK_NotFoundWording(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run      func(context.Context, *docdbsdk.Client) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "cluster", wantCode: "DBClusterNotFoundFault", wantMsg: "DBCluster not found: nope",
			run: func(ctx context.Context, c *docdbsdk.Client) error {
				_, err := c.DescribeDBClusters(
					ctx,
					&docdbsdk.DescribeDBClustersInput{DBClusterIdentifier: aws.String("nope")},
				)

				return err
			},
		},
		{
			name: "instance", wantCode: "DBInstanceNotFound", wantMsg: "DBInstance nope not found.",
			run: func(ctx context.Context, c *docdbsdk.Client) error {
				_, err := c.DescribeDBInstances(
					ctx,
					&docdbsdk.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String("nope")},
				)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, msg := apiErrorCode(t, tt.run(t.Context(), newRealClient(t)))
			assert.Equal(t, tt.wantCode, code)
			assert.Equal(t, tt.wantMsg, msg)
		})
	}
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
		{name: "max_zero", maxRecords: aws.Int32(0), wantErr: true},
		{name: "ok", maxRecords: aws.Int32(2)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			for i := range 5 {
				_, err := client.CreateDBCluster(t.Context(), &docdbsdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String(fmt.Sprintf("pg-%d", i)), Engine: aws.String("docdb"),
					MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("password-123"),
				})
				require.NoError(t, err)
			}

			out, err := client.DescribeDBClusters(t.Context(), &docdbsdk.DescribeDBClustersInput{
				Marker: tt.marker, MaxRecords: tt.maxRecords,
			})
			if tt.wantErr {
				code, _ := apiErrorCode(t, err)
				assert.Equal(t, "InvalidParameterValue", code)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.DBClusters, 2)
			require.NotNil(t, out.Marker)

			next, err := client.DescribeDBClusters(t.Context(), &docdbsdk.DescribeDBClustersInput{
				Marker: out.Marker, MaxRecords: tt.maxRecords,
			})
			require.NoError(t, err)
			assert.Len(t, next.DBClusters, 2)
		})
	}
}
