package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3tablesclientsdk "github.com/aws/aws-sdk-go-v2/service/s3tables"
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
