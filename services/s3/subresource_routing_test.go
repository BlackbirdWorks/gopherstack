package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetadataTableFamily_ReachableViaRealClient is a regression suite for
// gopherstack-zr2u: the router keyed CreateBucketMetadataTableConfiguration,
// UpdateBucketMetadataInventoryTableConfiguration and
// UpdateBucketMetadataJournalTableConfiguration on ?metadataTableConfiguration
// / ?metadataInventoryTableConfiguration / ?metadataJournalTableConfiguration.
// The real SDK sends ?metadataTable / ?metadataInventoryTable /
// ?metadataJournalTable (s3@v1.106.5 serializers.go: SplitURI calls in
// awsRestxml_serializeOpCreateBucketMetadataTableConfiguration and the two
// Update ops). CreateBucketMetadataConfiguration and
// CreateBucketMetadataTableConfiguration were also wired to PUT when the real
// SDK sends POST for both (same file, request.Method = "POST"). A real typed
// client calling any of these four never reached its handler: the two
// Creates fell through routeBucketPost's default arm to MethodNotAllowed,
// and the two Updates fell through routeBucketPut's default arm to
// createBucket, which returned BucketAlreadyOwnedByYou for the pre-existing
// bucket the test just created.
func TestMetadataTableFamily_ReachableViaRealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call   func(t *testing.T, client *sdk_s3.Client, bucket string) error
		name   string
		bucket string
	}{
		{
			name:   "create_bucket_metadata_configuration",
			bucket: "metadata-family-create-config",
			call: func(t *testing.T, client *sdk_s3.Client, bucket string) error {
				t.Helper()

				_, err := client.CreateBucketMetadataConfiguration(
					t.Context(), &sdk_s3.CreateBucketMetadataConfigurationInput{
						Bucket: aws.String(bucket),
						MetadataConfiguration: &types.MetadataConfiguration{
							JournalTableConfiguration: &types.JournalTableConfiguration{
								RecordExpiration: &types.RecordExpiration{
									Expiration: types.ExpirationStateDisabled,
								},
							},
						},
					})

				return err
			},
		},
		{
			name:   "create_bucket_metadata_table_configuration",
			bucket: "metadata-family-create-table",
			call: func(t *testing.T, client *sdk_s3.Client, bucket string) error {
				t.Helper()

				_, err := client.CreateBucketMetadataTableConfiguration(
					t.Context(), &sdk_s3.CreateBucketMetadataTableConfigurationInput{
						Bucket: aws.String(bucket),
						MetadataTableConfiguration: &types.MetadataTableConfiguration{
							S3TablesDestination: &types.S3TablesDestination{
								TableBucketArn: aws.String("arn:aws:s3tables:us-east-1:000000000000:bucket/dest"),
								TableName:      aws.String("metadata-table"),
							},
						},
					})

				return err
			},
		},
		{
			name:   "update_bucket_metadata_inventory_table_configuration",
			bucket: "metadata-family-update-inventory",
			call: func(t *testing.T, client *sdk_s3.Client, bucket string) error {
				t.Helper()

				_, err := client.UpdateBucketMetadataInventoryTableConfiguration(
					t.Context(), &sdk_s3.UpdateBucketMetadataInventoryTableConfigurationInput{
						Bucket: aws.String(bucket),
						InventoryTableConfiguration: &types.InventoryTableConfigurationUpdates{
							ConfigurationState: types.InventoryConfigurationStateEnabled,
						},
					})

				return err
			},
		},
		{
			name:   "update_bucket_metadata_journal_table_configuration",
			bucket: "metadata-family-update-journal",
			call: func(t *testing.T, client *sdk_s3.Client, bucket string) error {
				t.Helper()

				_, err := client.UpdateBucketMetadataJournalTableConfiguration(
					t.Context(), &sdk_s3.UpdateBucketMetadataJournalTableConfigurationInput{
						Bucket: aws.String(bucket),
						JournalTableConfiguration: &types.JournalTableConfigurationUpdates{
							RecordExpiration: &types.RecordExpiration{
								Expiration: types.ExpirationStateDisabled,
							},
						},
					})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)

			_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(tt.bucket)})
			require.NoError(t, err)

			err = tt.call(t, client, tt.bucket)
			require.NoError(t, err, "operation must reach its own handler, not fall through to a different op")
		})
	}
}

// TestDeleteBucketMetadataTableConfiguration_DoesNotDeleteBucket is a
// regression test for gopherstack-zr2u's most severe finding: the router
// keyed DeleteBucketMetadataTableConfiguration on ?metadataTableConfiguration,
// but the real SDK sends ?metadataTable (s3@v1.106.5 serializers.go:
// awsRestxml_serializeOpDeleteBucketMetadataTableConfiguration). A real
// client's DELETE fell through every case in routeBucketDeleteExtra to the
// default arm, which is deleteBucket -- so a caller asking only to remove
// the metadata table configuration from an (empty) bucket instead got the
// whole bucket deleted, with a 204 success response indistinguishable from
// the intended op succeeding.
func TestDeleteBucketMetadataTableConfiguration_DoesNotDeleteBucket(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "metadata-table-delete-real-bucket"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.CreateBucketMetadataTableConfiguration(
		t.Context(), &sdk_s3.CreateBucketMetadataTableConfigurationInput{
			Bucket: aws.String(bucket),
			MetadataTableConfiguration: &types.MetadataTableConfiguration{
				S3TablesDestination: &types.S3TablesDestination{
					TableBucketArn: aws.String("arn:aws:s3tables:us-east-1:000000000000:bucket/dest"),
					TableName:      aws.String("metadata-table"),
				},
			},
		})
	require.NoError(t, err)

	_, err = client.DeleteBucketMetadataTableConfiguration(
		t.Context(), &sdk_s3.DeleteBucketMetadataTableConfigurationInput{
			Bucket: aws.String(bucket),
		})
	require.NoError(t, err)

	// The bucket itself must still exist: only its metadata table
	// configuration should have been removed. ListBuckets (unlike this
	// backend's HeadBucket, which has its own unrelated gap: it doesn't
	// check DeletePending) filters out a bucket mid-deletion, so it reliably
	// tells the two cases apart. Before the fix this failed because the
	// DELETE fell through to DeleteBucket and marked the bucket pending.
	listOut, err := client.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
	require.NoError(t, err)

	names := make([]string, 0, len(listOut.Buckets))
	for _, b := range listOut.Buckets {
		names = append(names, aws.ToString(b.Name))
	}
	assert.Contains(t, names, bucket,
		"bucket should still exist: DeleteBucketMetadataTableConfiguration must not fall through to DeleteBucket")
}

// TestGetBucketMetadataTableConfiguration_KeyedOnRealQueryParam is a
// regression test for the Get side of the same key mismatch: the router
// checked ?metadataTableConfiguration where the real SDK's
// GetBucketMetadataTableConfiguration sends ?metadataTable (s3@v1.106.5
// serializers.go). This can't be proven through the typed client's response
// alone -- gopherstack's Get handler echoes back the raw stored Create body
// rather than the real GetBucketMetadataTableConfigurationResult shape, so a
// mis-routed ListObjects fallthrough and the correctly-routed handler both
// decode as an empty, error-free result to the SDK; the wrong answer is
// silent, exactly the failure mode this bug class produces. Verified at the
// wire instead: only the real key must return the stored config body, not a
// bucket listing.
func TestGetBucketMetadataTableConfiguration_KeyedOnRealQueryParam(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	bucket := "metadata-table-get-real-key-bucket"
	mustCreateBucket(t, backend, bucket)

	const marker = "arn:aws:s3tables:::bucket/marker-table"
	createXML := `<MetadataTableConfiguration><S3TablesDestination>` +
		`<TableBucketArn>` + marker + `</TableBucketArn></S3TablesDestination></MetadataTableConfiguration>`

	createReq := httptest.NewRequest(http.MethodPost, "/"+bucket+"?metadataTable", strings.NewReader(createXML))
	createRec := httptest.NewRecorder()
	serveS3Handler(handler, createRec, createReq)
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	getReq := httptest.NewRequest(http.MethodGet, "/"+bucket+"?metadataTable", nil)
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)

	require.Equal(t, http.StatusOK, getRec.Code, getRec.Body.String())
	assert.Contains(t, getRec.Body.String(), marker,
		"the real ?metadataTable key must reach GetBucketMetadataTableConfiguration, not fall through to ListObjects")
	assert.NotContains(t, getRec.Body.String(), "ListBucketResult")
}

// TestWriteGetObjectResponse_RealPathReachable is a regression test for
// gopherstack-zr2u: WriteGetObjectResponse targets the literal path
// "/WriteGetObjectResponse" with no query string at all (s3@v1.106.5
// serializers.go: awsRestxml_serializeOpWriteGetObjectResponse,
// httpbinding.SplitURI("/WriteGetObjectResponse")). The router instead
// checked a ?writeGetObjectResponse query parameter the real SDK never
// sends, and worse, a real request never reached that check in the first
// place: resolveBucketAndKey parsed the path segment "WriteGetObjectResponse"
// as a bucket name, which fails IsValidBucketName (uppercase letters aren't
// legal in a bucket name), so every real request was rejected with 400
// InvalidBucketName before routing logic ever ran. A POST to the real path
// with an (unmatched) X-Amz-Request-Token must now reach
// handleWriteGetObjectResponse, which responds 200 as a no-op for a token it
// doesn't recognize -- not 400 InvalidBucketName.
func TestWriteGetObjectResponse_RealPathReachable(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/WriteGetObjectResponse", strings.NewReader("transformed body"))
	req.Header.Set("X-Amz-Request-Token", "no-such-pending-request")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "InvalidBucketName")
}
