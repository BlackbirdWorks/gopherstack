package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3tablesclientsdk "github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_S3Tables_TableBucketLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3TablesClient(t)
	ctx := t.Context()

	bucketName := "integ-s3tb-" + uuid.NewString()[:8]

	tests := []struct {
		name       string
		bucketName string
		wantErr    bool
	}{
		{
			name:       "create_and_get_bucket",
			bucketName: bucketName,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// CreateTableBucket
			createOut, err := client.CreateTableBucket(ctx, &s3tablesclientsdk.CreateTableBucketInput{
				Name: aws.String(tt.bucketName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, createOut.Arn)
			assert.NotEmpty(t, aws.ToString(createOut.Arn))

			bucketARN := aws.ToString(createOut.Arn)

			// GetTableBucket
			getOut, err := client.GetTableBucket(ctx, &s3tablesclientsdk.GetTableBucketInput{
				TableBucketARN: aws.String(bucketARN),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.bucketName, aws.ToString(getOut.Name))

			// ListTableBuckets
			listOut, err := client.ListTableBuckets(ctx, &s3tablesclientsdk.ListTableBucketsInput{})
			require.NoError(t, err)

			found := false
			for _, b := range listOut.TableBuckets {
				if aws.ToString(b.Arn) == bucketARN {
					found = true

					break
				}
			}

			assert.True(t, found, "created bucket should appear in ListTableBuckets")

			// DeleteTableBucket
			_, err = client.DeleteTableBucket(ctx, &s3tablesclientsdk.DeleteTableBucketInput{
				TableBucketARN: aws.String(bucketARN),
			})
			require.NoError(t, err)

			// Verify deletion via GetTableBucket (should return 404)
			_, err = client.GetTableBucket(ctx, &s3tablesclientsdk.GetTableBucketInput{
				TableBucketARN: aws.String(bucketARN),
			})
			require.Error(t, err)
		})
	}
}

func TestIntegration_S3Tables_NamespaceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3TablesClient(t)
	ctx := t.Context()

	bucketName := "integ-ns-bucket-" + uuid.NewString()[:8]
	nsName := "integ-ns-" + uuid.NewString()[:8]

	// Create a table bucket first
	bucketOut, createBucketErr := client.CreateTableBucket(ctx, &s3tablesclientsdk.CreateTableBucketInput{
		Name: aws.String(bucketName),
	})
	require.NoError(t, createBucketErr)
	bucketARN := aws.ToString(bucketOut.Arn)

	tests := []struct {
		name    string
		nsName  string
		wantErr bool
	}{
		{
			name:    "create_and_get_namespace",
			nsName:  nsName,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// CreateNamespace
			createNsOut, err := client.CreateNamespace(ctx, &s3tablesclientsdk.CreateNamespaceInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      []string{tt.nsName},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, []string{tt.nsName}, createNsOut.Namespace)

			// GetNamespace
			getNsOut, err := client.GetNamespace(ctx, &s3tablesclientsdk.GetNamespaceInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(tt.nsName),
			})
			require.NoError(t, err)
			assert.Equal(t, []string{tt.nsName}, getNsOut.Namespace)

			// ListNamespaces
			listNsOut, err := client.ListNamespaces(ctx, &s3tablesclientsdk.ListNamespacesInput{
				TableBucketARN: aws.String(bucketARN),
			})
			require.NoError(t, err)

			found := false
			for _, ns := range listNsOut.Namespaces {
				if len(ns.Namespace) > 0 && ns.Namespace[0] == tt.nsName {
					found = true

					break
				}
			}

			assert.True(t, found, "created namespace should appear in ListNamespaces")

			// DeleteNamespace
			_, err = client.DeleteNamespace(ctx, &s3tablesclientsdk.DeleteNamespaceInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(tt.nsName),
			})
			require.NoError(t, err)
		})
	}
}

func TestIntegration_S3Tables_TableLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3TablesClient(t)
	ctx := t.Context()

	bucketName := "integ-tbl-bucket-" + uuid.NewString()[:8]
	nsName := "integ-tbl-ns-" + uuid.NewString()[:8]
	tableName := "integ-table-" + uuid.NewString()[:8]

	// Create table bucket
	tblBucketOut, tblBucketErr := client.CreateTableBucket(ctx, &s3tablesclientsdk.CreateTableBucketInput{
		Name: aws.String(bucketName),
	})
	require.NoError(t, tblBucketErr)
	bucketARN := aws.ToString(tblBucketOut.Arn)

	// Create namespace
	_, nsErr := client.CreateNamespace(ctx, &s3tablesclientsdk.CreateNamespaceInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      []string{nsName},
	})
	require.NoError(t, nsErr)

	tests := []struct {
		name      string
		tableName string
		wantErr   bool
	}{
		{
			name:      "create_and_get_table",
			tableName: tableName,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// CreateTable
			createTblOut, err := client.CreateTable(ctx, &s3tablesclientsdk.CreateTableInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(nsName),
				Name:           aws.String(tt.tableName),
				Format:         "ICEBERG",
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(createTblOut.TableARN))

			// GetTable
			getTblOut, err := client.GetTable(ctx, &s3tablesclientsdk.GetTableInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(nsName),
				Name:           aws.String(tt.tableName),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.tableName, aws.ToString(getTblOut.Name))

			// ListTables
			listTblOut, err := client.ListTables(ctx, &s3tablesclientsdk.ListTablesInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(nsName),
			})
			require.NoError(t, err)

			found := false
			for _, tbl := range listTblOut.Tables {
				if aws.ToString(tbl.Name) == tt.tableName {
					found = true

					break
				}
			}

			assert.True(t, found, "created table should appear in ListTables")

			// UpdateTableMetadataLocation
			updateOut, err := client.UpdateTableMetadataLocation(
				ctx,
				&s3tablesclientsdk.UpdateTableMetadataLocationInput{
					TableBucketARN:   aws.String(bucketARN),
					Namespace:        aws.String(nsName),
					Name:             aws.String(tt.tableName),
					MetadataLocation: aws.String("s3://bucket/path/metadata.json"),
					VersionToken:     getTblOut.VersionToken,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "s3://bucket/path/metadata.json", aws.ToString(updateOut.MetadataLocation))

			// DeleteTable
			_, err = client.DeleteTable(ctx, &s3tablesclientsdk.DeleteTableInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(nsName),
				Name:           aws.String(tt.tableName),
			})
			require.NoError(t, err)

			// Verify deletion via GetTable (should return error)
			_, err = client.GetTable(ctx, &s3tablesclientsdk.GetTableInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(nsName),
				Name:           aws.String(tt.tableName),
			})
			require.Error(t, err)
		})
	}
}

func TestIntegration_S3Tables_TableBucket_Policy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3TablesClient(t)
	ctx := t.Context()

	bucketName := "integ-pol-bucket-" + uuid.NewString()[:8]

	bucketOut, createErr := client.CreateTableBucket(ctx, &s3tablesclientsdk.CreateTableBucketInput{
		Name: aws.String(bucketName),
	})
	require.NoError(t, createErr)

	bucketARN := aws.ToString(bucketOut.Arn)
	policy := `{"Version":"2012-10-17","Statement":[]}`

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "put_get_delete_bucket_policy", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// PutTableBucketPolicy
			_, err := client.PutTableBucketPolicy(ctx, &s3tablesclientsdk.PutTableBucketPolicyInput{
				TableBucketARN: aws.String(bucketARN),
				ResourcePolicy: aws.String(policy),
			})
			require.NoError(t, err)

			// GetTableBucketPolicy
			getOut, err := client.GetTableBucketPolicy(ctx, &s3tablesclientsdk.GetTableBucketPolicyInput{
				TableBucketARN: aws.String(bucketARN),
			})
			require.NoError(t, err)
			assert.Equal(t, policy, aws.ToString(getOut.ResourcePolicy))

			// DeleteTableBucketPolicy
			_, err = client.DeleteTableBucketPolicy(ctx, &s3tablesclientsdk.DeleteTableBucketPolicyInput{
				TableBucketARN: aws.String(bucketARN),
			})
			require.NoError(t, err)

			// GetTableBucketPolicy after delete should return an error
			_, err = client.GetTableBucketPolicy(ctx, &s3tablesclientsdk.GetTableBucketPolicyInput{
				TableBucketARN: aws.String(bucketARN),
			})
			require.Error(t, err)
		})
	}
}

func TestIntegration_S3Tables_Table_Policy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3TablesClient(t)
	ctx := t.Context()

	bucketName := "integ-tpol-bucket-" + uuid.NewString()[:8]
	nsName := "integ-tpol-ns-" + uuid.NewString()[:8]
	tableName := "integ-tpol-tbl-" + uuid.NewString()[:8]

	bucketOut, setupErr := client.CreateTableBucket(ctx, &s3tablesclientsdk.CreateTableBucketInput{
		Name: aws.String(bucketName),
	})
	require.NoError(t, setupErr)

	bucketARN := aws.ToString(bucketOut.Arn)

	_, setupErr = client.CreateNamespace(ctx, &s3tablesclientsdk.CreateNamespaceInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      []string{nsName},
	})
	require.NoError(t, setupErr)

	_, setupErr = client.CreateTable(ctx, &s3tablesclientsdk.CreateTableInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String(nsName),
		Name:           aws.String(tableName),
		Format:         "ICEBERG",
	})
	require.NoError(t, setupErr)

	policy := `{"Version":"2012-10-17","Statement":[]}`

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "put_get_delete_table_policy", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				err    error
				getOut *s3tablesclientsdk.GetTablePolicyOutput
			)

			// PutTablePolicy
			_, err = client.PutTablePolicy(ctx, &s3tablesclientsdk.PutTablePolicyInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(nsName),
				Name:           aws.String(tableName),
				ResourcePolicy: aws.String(policy),
			})
			require.NoError(t, err)

			// GetTablePolicy
			getOut, err = client.GetTablePolicy(ctx, &s3tablesclientsdk.GetTablePolicyInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(nsName),
				Name:           aws.String(tableName),
			})
			require.NoError(t, err)
			assert.Equal(t, policy, aws.ToString(getOut.ResourcePolicy))

			// DeleteTablePolicy
			_, err = client.DeleteTablePolicy(ctx, &s3tablesclientsdk.DeleteTablePolicyInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(nsName),
				Name:           aws.String(tableName),
			})
			require.NoError(t, err)

			// GetTablePolicy after delete should return an error
			_, err = client.GetTablePolicy(ctx, &s3tablesclientsdk.GetTablePolicyInput{
				TableBucketARN: aws.String(bucketARN),
				Namespace:      aws.String(nsName),
				Name:           aws.String(tableName),
			})
			require.Error(t, err)
		})
	}
}

func TestIntegration_S3Tables_MaintenanceConfig(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3TablesClient(t)
	ctx := t.Context()

	bucketName := "integ-maint-bucket-" + uuid.NewString()[:8]
	nsName := "integ-maint-ns-" + uuid.NewString()[:8]
	tableName := "integ-maint-tbl-" + uuid.NewString()[:8]

	bucketOut, setupErr := client.CreateTableBucket(ctx, &s3tablesclientsdk.CreateTableBucketInput{
		Name: aws.String(bucketName),
	})
	require.NoError(t, setupErr)

	bucketARN := aws.ToString(bucketOut.Arn)

	_, setupErr = client.CreateNamespace(ctx, &s3tablesclientsdk.CreateNamespaceInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      []string{nsName},
	})
	require.NoError(t, setupErr)

	_, setupErr = client.CreateTable(ctx, &s3tablesclientsdk.CreateTableInput{
		TableBucketARN: aws.String(bucketARN),
		Namespace:      aws.String(nsName),
		Name:           aws.String(tableName),
		Format:         "ICEBERG",
	})
	require.NoError(t, setupErr)

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "bucket_and_table_maintenance_config", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				err               error
				getBucketMaintOut *s3tablesclientsdk.GetTableBucketMaintenanceConfigurationOutput
				getTableMaintOut  *s3tablesclientsdk.GetTableMaintenanceConfigurationOutput
			)

			// GetTableBucketMaintenanceConfiguration (default empty config)
			getBucketMaintOut, err = client.GetTableBucketMaintenanceConfiguration(
				ctx,
				&s3tablesclientsdk.GetTableBucketMaintenanceConfigurationInput{
					TableBucketARN: aws.String(bucketARN),
				},
			)
			require.NoError(t, err)
			assert.NotNil(t, getBucketMaintOut)

			// PutTableBucketMaintenanceConfiguration
			_, err = client.PutTableBucketMaintenanceConfiguration(
				ctx,
				&s3tablesclientsdk.PutTableBucketMaintenanceConfigurationInput{
					TableBucketARN: aws.String(bucketARN),
					Type:           types.TableBucketMaintenanceTypeIcebergUnreferencedFileRemoval,
					Value: &types.TableBucketMaintenanceConfigurationValue{
						Status: types.MaintenanceStatusEnabled,
					},
				},
			)
			require.NoError(t, err)

			// GetTableMaintenanceConfiguration (default empty config)
			getTableMaintOut, err = client.GetTableMaintenanceConfiguration(
				ctx,
				&s3tablesclientsdk.GetTableMaintenanceConfigurationInput{
					TableBucketARN: aws.String(bucketARN),
					Namespace:      aws.String(nsName),
					Name:           aws.String(tableName),
				},
			)
			require.NoError(t, err)
			assert.NotNil(t, getTableMaintOut)

			// PutTableMaintenanceConfiguration
			_, err = client.PutTableMaintenanceConfiguration(
				ctx,
				&s3tablesclientsdk.PutTableMaintenanceConfigurationInput{
					TableBucketARN: aws.String(bucketARN),
					Namespace:      aws.String(nsName),
					Name:           aws.String(tableName),
					Type:           types.TableMaintenanceTypeIcebergCompaction,
					Value: &types.TableMaintenanceConfigurationValue{
						Status: types.MaintenanceStatusEnabled,
					},
				},
			)
			require.NoError(t, err)
		})
	}
}

func TestIntegration_S3Tables_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3TablesClient(t)
	ctx := t.Context()

	nonExistentARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/does-not-exist"

	tests := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{
			name: "GetTableBucket_NotFound",
			fn: func(t *testing.T) {
				t.Helper()

				_, err := client.GetTableBucket(ctx, &s3tablesclientsdk.GetTableBucketInput{
					TableBucketARN: aws.String(nonExistentARN),
				})
				require.Error(t, err)
			},
		},
		{
			name: "GetNamespace_NotFound",
			fn: func(t *testing.T) {
				t.Helper()

				_, err := client.GetNamespace(ctx, &s3tablesclientsdk.GetNamespaceInput{
					TableBucketARN: aws.String(nonExistentARN),
					Namespace:      aws.String("no-such-ns"),
				})
				require.Error(t, err)
			},
		},
		{
			name: "GetTable_NotFound",
			fn: func(t *testing.T) {
				t.Helper()

				_, err := client.GetTable(ctx, &s3tablesclientsdk.GetTableInput{
					TableBucketARN: aws.String(nonExistentARN),
					Namespace:      aws.String("no-such-ns"),
					Name:           aws.String("no-such-table"),
				})
				require.Error(t, err)
			},
		},
		{
			name: "DeleteTableBucket_NotFound",
			fn: func(t *testing.T) {
				t.Helper()

				_, err := client.DeleteTableBucket(ctx, &s3tablesclientsdk.DeleteTableBucketInput{
					TableBucketARN: aws.String(nonExistentARN),
				})
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fn(t)
		})
	}
}
