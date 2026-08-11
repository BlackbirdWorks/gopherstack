package glacier_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// newTestHandlerWithS3 builds a Glacier handler with an S3 backend wired via
// SetS3Backend, mirroring cli.go's wireGlacierS3.
func newTestHandlerWithS3(t *testing.T) (*glacier.Handler, *s3backend.InMemoryBackend) {
	t.Helper()

	bk := glacier.NewInMemoryBackend()
	glacier.SetRetrievalDelay(bk, 0)

	s3Bk := s3backend.NewInMemoryBackend(nil)
	bk.SetS3Backend(s3Bk)

	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h, s3Bk
}

// testOutputBucket is the S3 bucket every test in this file writes to and reads
// back from.
const testOutputBucket = "results-bucket"

// getS3Object fetches and reads an object's full body from testOutputBucket,
// failing the test if either step errors.
func getS3Object(t *testing.T, s3Bk *s3backend.InMemoryBackend, key string) string {
	t.Helper()

	out, err := s3Bk.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(testOutputBucket), Key: aws.String(key),
	})
	require.NoError(t, err, "expected object at key %q", key)

	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return string(data)
}

// TestGetJobOutput_Select_WritesRealS3OutputLocation verifies that, once an S3
// backend is wired, a completed select job's real output lands in S3 under the
// documented key layout (job.txt/results/1/result_manifest.txt under
// <prefix>/<jobID>/) -- see glacier-select.md's "S3 Glacier Select Output"
// section, cited in select_output.go.
func TestGetJobOutput_Select_WritesRealS3OutputLocation(t *testing.T) {
	t.Parallel()

	h, s3Bk := newTestHandlerWithS3(t)

	ctx := context.Background()
	_, err := s3Bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testOutputBucket)})
	require.NoError(t, err)

	createVault(t, h, "select-s3-vault")
	archiveID := uploadArchiveData(t, h, "select-s3-vault", []byte(selectTestArchive))

	jobID := initiateJobWithBody(t, h, "select-s3-vault",
		basicSelectBody(archiveID, "SELECT * FROM archive WHERE _3 > 28"))

	base := "out/" + jobID + "/"

	jobTxt := getS3Object(t, s3Bk, base+"job.txt")
	assert.Contains(t, jobTxt, jobID)
	assert.Contains(t, jobTxt, `"Action":"Select"`)

	results := getS3Object(t, s3Bk, base+"results/1")
	assert.Equal(t, "1,alice,30\n3,carol,40\n", results)

	manifest := getS3Object(t, s3Bk, base+"result_manifest.txt")
	assert.Contains(t, manifest, base+"results/1")

	_, err = s3Bk.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(testOutputBucket), Key: aws.String(base + "error_manifest.txt"),
	})
	assert.Error(t, err, "a successful query must not also write an error manifest")
}

// TestDescribeJob_Select_WritesRealS3OutputLocation verifies DescribeJob (not just
// GetJobOutput) also triggers materialization, since real AWS's async retrieval
// window means an operator could poll status before ever calling GetJobOutput.
func TestDescribeJob_Select_WritesRealS3OutputLocation(t *testing.T) {
	t.Parallel()

	h, s3Bk := newTestHandlerWithS3(t)

	ctx := context.Background()
	_, err := s3Bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testOutputBucket)})
	require.NoError(t, err)

	createVault(t, h, "select-s3-describe-vault")
	archiveID := uploadArchiveData(t, h, "select-s3-describe-vault", []byte(selectTestArchive))

	jobID := initiateJobWithBody(t, h, "select-s3-describe-vault",
		basicSelectBody(archiveID, "SELECT * FROM archive"))

	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/select-s3-describe-vault/jobs/"+jobID, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	base := "out/" + jobID + "/"
	assert.Equal(t, selectTestArchive, getS3Object(t, s3Bk, base+"results/1"))
}

// TestGetJobOutput_Select_S3WriteFailureDoesNotBreakDelivery verifies that when
// the OutputLocation bucket does not exist in the wired S3 backend, GetJobOutput
// still serves the correctly computed result rather than failing the request --
// the S3 write-back is best-effort supplementary delivery, not the source of truth.
func TestGetJobOutput_Select_S3WriteFailureDoesNotBreakDelivery(t *testing.T) {
	t.Parallel()

	h, s3Bk := newTestHandlerWithS3(t)
	_ = s3Bk // deliberately no CreateBucket("results-bucket") call

	createVault(t, h, "select-s3-missing-bucket-vault")
	archiveID := uploadArchiveData(t, h, "select-s3-missing-bucket-vault", []byte(selectTestArchive))

	jobID := initiateJobWithBody(t, h, "select-s3-missing-bucket-vault",
		basicSelectBody(archiveID, "SELECT * FROM archive"))

	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/select-s3-missing-bucket-vault/jobs/"+jobID+"/output", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, selectTestArchive, rec.Body.String())
}

// TestGetJobOutput_Select_S3WriteOnceNotDuplicated verifies that calling
// GetJobOutput twice does not re-write (or duplicate-write) the S3 output -- real
// AWS's job.txt is documented as "written once, never updated".
func TestGetJobOutput_Select_S3WriteOnceNotDuplicated(t *testing.T) {
	t.Parallel()

	h, s3Bk := newTestHandlerWithS3(t)

	ctx := context.Background()
	_, err := s3Bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testOutputBucket)})
	require.NoError(t, err)

	createVault(t, h, "select-s3-once-vault")
	archiveID := uploadArchiveData(t, h, "select-s3-once-vault", []byte(selectTestArchive))

	jobID := initiateJobWithBody(t, h, "select-s3-once-vault",
		basicSelectBody(archiveID, "SELECT * FROM archive"))

	base := "out/" + jobID + "/"

	for range 2 {
		rec := doRequest(t, h, http.MethodGet,
			"/"+testAccountID+"/vaults/select-s3-once-vault/jobs/"+jobID+"/output", "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	}

	out, err := s3Bk.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(testOutputBucket), Key: aws.String(base + "results/1"),
	})
	require.NoError(t, err)

	data, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	assert.Equal(t, selectTestArchive, string(data))
}

// TestGetJobOutput_Select_S3ErrorManifestOnQueryFailure verifies a select job
// whose InputSerialization.Csv is malformed in a way InitiateJob's syntax-only
// Expression validation cannot catch (FieldDelimiter == Comments, which
// encoding/csv rejects only once it actually reads archive bytes) writes to the
// errors/ prefix and error_manifest.txt instead of results/ and
// result_manifest.txt, per glacier-select.md's "Error Handling" section.
func TestGetJobOutput_Select_S3ErrorManifestOnQueryFailure(t *testing.T) {
	t.Parallel()

	h, s3Bk := newTestHandlerWithS3(t)

	ctx := context.Background()
	_, err := s3Bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testOutputBucket)})
	require.NoError(t, err)

	createVault(t, h, "select-s3-error-vault")
	archiveID := uploadArchiveData(t, h, "select-s3-error-vault", []byte(selectTestArchive))

	body, err := json.Marshal(map[string]any{
		"Type":      "select",
		"ArchiveId": archiveID,
		"SelectParameters": map[string]any{
			"Expression":     "SELECT * FROM archive",
			"ExpressionType": "SQL",
			"InputSerialization": map[string]any{
				"Csv": map[string]any{"FieldDelimiter": "#", "Comments": "#"},
			},
			"OutputSerialization": map[string]any{"Csv": map[string]any{}},
		},
		"OutputLocation": map[string]any{
			"S3": map[string]any{"BucketName": testOutputBucket, "Prefix": "out/"},
		},
	})
	require.NoError(t, err)

	jobID := initiateJobWithBody(t, h, "select-s3-error-vault", string(body))

	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/select-s3-error-vault/jobs/"+jobID+"/output", "")
	require.Equal(t, http.StatusBadRequest, rec.Code)

	base := "out/" + jobID + "/"
	manifest := getS3Object(t, s3Bk, base+"error_manifest.txt")
	assert.Contains(t, manifest, base+"errors/1")

	_, err = s3Bk.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(testOutputBucket), Key: aws.String(base + "results/1"),
	})
	assert.Error(t, err, "a failed query must not also write a results object")
}
