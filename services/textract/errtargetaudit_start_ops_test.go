package textract_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	textractsdk "github.com/aws/aws-sdk-go-v2/service/textract"
	textracttypes "github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

// TestStartOps_TrimmedBeforeReadback_RealClient drives the real
// aws-sdk-go-v2 textract client against the Start* async job operations
// with maxJobs=0, which forces trimJobsIfNeeded to evict the job that was
// just stored before the post-write readback runs (gopherstack-0ho6's race
// window). None of these four operations' own deserializeOpError<Op>
// switches (textract@v1.43.4 deserializers.go) declare
// InvalidJobIdException -- only the Get* ops do -- so the typed error a
// real client can decode here must be InternalServerError, not
// InvalidJobIdException.
func TestStartOps_TrimmedBeforeReadback_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("start document analysis", func(t *testing.T) {
		t.Parallel()

		client := newTestTextractClient(t, textract.NewHandler(textract.NewInMemoryBackendWithCap(0)))
		_, err := client.StartDocumentAnalysis(t.Context(), &textractsdk.StartDocumentAnalysisInput{
			DocumentLocation: &textracttypes.DocumentLocation{
				S3Object: &textracttypes.S3Object{Bucket: aws.String("bucket"), Name: aws.String("doc.pdf")},
			},
			FeatureTypes: []textracttypes.FeatureType{textracttypes.FeatureTypeTables},
		}, func(o *textractsdk.Options) { o.RetryMaxAttempts = 1 })
		assertInternalServerError(t, err)
	})

	t.Run("start document text detection", func(t *testing.T) {
		t.Parallel()

		client := newTestTextractClient(t, textract.NewHandler(textract.NewInMemoryBackendWithCap(0)))
		_, err := client.StartDocumentTextDetection(t.Context(), &textractsdk.StartDocumentTextDetectionInput{
			DocumentLocation: &textracttypes.DocumentLocation{
				S3Object: &textracttypes.S3Object{Bucket: aws.String("bucket"), Name: aws.String("doc.pdf")},
			},
		}, func(o *textractsdk.Options) { o.RetryMaxAttempts = 1 })
		assertInternalServerError(t, err)
	})

	t.Run("start expense analysis", func(t *testing.T) {
		t.Parallel()

		client := newTestTextractClient(t, textract.NewHandler(textract.NewInMemoryBackendWithCap(0)))
		_, err := client.StartExpenseAnalysis(t.Context(), &textractsdk.StartExpenseAnalysisInput{
			DocumentLocation: &textracttypes.DocumentLocation{
				S3Object: &textracttypes.S3Object{Bucket: aws.String("bucket"), Name: aws.String("doc.pdf")},
			},
		}, func(o *textractsdk.Options) { o.RetryMaxAttempts = 1 })
		assertInternalServerError(t, err)
	})

	t.Run("start lending analysis", func(t *testing.T) {
		t.Parallel()

		client := newTestTextractClient(t, textract.NewHandler(textract.NewInMemoryBackendWithCap(0)))
		_, err := client.StartLendingAnalysis(t.Context(), &textractsdk.StartLendingAnalysisInput{
			DocumentLocation: &textracttypes.DocumentLocation{
				S3Object: &textracttypes.S3Object{Bucket: aws.String("bucket"), Name: aws.String("doc.pdf")},
			},
		}, func(o *textractsdk.Options) { o.RetryMaxAttempts = 1 })
		assertInternalServerError(t, err)
	})
}

func assertInternalServerError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)

	var jobIDErr *textracttypes.InvalidJobIdException
	require.NotErrorAs(t, err, &jobIDErr,
		"Start ops' own deserializer switch has no InvalidJobIdException case; got: %v", err)

	var internalErr *textracttypes.InternalServerError
	require.ErrorAs(t, err, &internalErr,
		"expected a real InternalServerError from the SDK deserializer, got: %v", err)
}
