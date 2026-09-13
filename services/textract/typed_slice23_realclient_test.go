package textract_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	textractsdk "github.com/aws/aws-sdk-go-v2/service/textract"
	textracttypes "github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

// TestTypedSlice23RealClient drives textract's remaining typed-coverage-
// blind ops (gopherstack-n3zi slice 23) through the real aws-sdk-go-v2
// client: AnalyzeExpense, AnalyzeID, DeleteAdapter, DeleteAdapterVersion,
// GetAdapter, GetAdapterVersion, GetDocumentAnalysis, GetExpenseAnalysis,
// GetLendingAnalysis, GetLendingAnalysisSummary, ListAdapters, TagResource,
// UntagResource, UpdateAdapter.
func TestTypedSlice23RealClient(t *testing.T) {
	t.Parallel()

	t.Run("adapter CRUD plus tags", func(t *testing.T) {
		t.Parallel()

		backend := textract.NewInMemoryBackend(tagsRTAccountID, tagsRTRegion)
		client := newTestTextractClient(t, textract.NewHandler(backend))
		ctx := t.Context()

		createOut, err := client.CreateAdapter(ctx, &textractsdk.CreateAdapterInput{
			AdapterName:  aws.String("s23-adapter"),
			Description:  aws.String("original description"),
			FeatureTypes: []textracttypes.FeatureType{textracttypes.FeatureTypeQueries},
			AutoUpdate:   textracttypes.AutoUpdateDisabled,
		})
		require.NoError(t, err)
		adapterID := createOut.AdapterId

		getOut, err := client.GetAdapter(ctx, &textractsdk.GetAdapterInput{AdapterId: adapterID})
		require.NoError(t, err)
		assert.Equal(t, "s23-adapter", aws.ToString(getOut.AdapterName))
		assert.Equal(t, "original description", aws.ToString(getOut.Description))
		assert.Equal(t, textracttypes.AutoUpdateDisabled, getOut.AutoUpdate)

		_, err = client.UpdateAdapter(ctx, &textractsdk.UpdateAdapterInput{
			AdapterId:   adapterID,
			Description: aws.String("updated description"),
			AutoUpdate:  textracttypes.AutoUpdateEnabled,
		})
		require.NoError(t, err)

		getAfterUpdate, err := client.GetAdapter(ctx, &textractsdk.GetAdapterInput{AdapterId: adapterID})
		require.NoError(t, err)
		assert.Equal(t, "updated description", aws.ToString(getAfterUpdate.Description))
		assert.Equal(t, textracttypes.AutoUpdateEnabled, getAfterUpdate.AutoUpdate)

		listOut, err := client.ListAdapters(ctx, &textractsdk.ListAdaptersInput{})
		require.NoError(t, err)

		ids := make([]string, 0, len(listOut.Adapters))
		for _, a := range listOut.Adapters {
			ids = append(ids, aws.ToString(a.AdapterId))
		}

		assert.Contains(t, ids, aws.ToString(adapterID))

		adapterArn := "arn:aws:textract:" + tagsRTRegion + ":" + tagsRTAccountID + ":adapter/" + aws.ToString(adapterID)

		_, err = client.TagResource(ctx, &textractsdk.TagResourceInput{
			ResourceARN: aws.String(adapterArn),
			Tags:        map[string]string{"team": "docs"},
		})
		require.NoError(t, err)

		tagsOut, err := client.ListTagsForResource(ctx, &textractsdk.ListTagsForResourceInput{
			ResourceARN: aws.String(adapterArn),
		})
		require.NoError(t, err)
		assert.Equal(t, "docs", tagsOut.Tags["team"])

		_, err = client.UntagResource(ctx, &textractsdk.UntagResourceInput{
			ResourceARN: aws.String(adapterArn),
			TagKeys:     []string{"team"},
		})
		require.NoError(t, err)

		tagsOut2, err := client.ListTagsForResource(ctx, &textractsdk.ListTagsForResourceInput{
			ResourceARN: aws.String(adapterArn),
		})
		require.NoError(t, err)
		assert.NotContains(t, tagsOut2.Tags, "team")

		versionOut, err := client.CreateAdapterVersion(ctx, &textractsdk.CreateAdapterVersionInput{
			AdapterId: adapterID,
			DatasetConfig: &textracttypes.AdapterVersionDatasetConfig{
				ManifestS3Object: &textracttypes.S3Object{
					Bucket: aws.String("manifests"),
					Name:   aws.String("m.json"),
				},
			},
			OutputConfig: &textracttypes.OutputConfig{S3Bucket: aws.String("out")},
		})
		require.NoError(t, err)

		getVersionOut, err := client.GetAdapterVersion(ctx, &textractsdk.GetAdapterVersionInput{
			AdapterId:      adapterID,
			AdapterVersion: versionOut.AdapterVersion,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(adapterID), aws.ToString(getVersionOut.AdapterId))
		assert.Equal(t, aws.ToString(versionOut.AdapterVersion), aws.ToString(getVersionOut.AdapterVersion))

		_, err = client.DeleteAdapterVersion(ctx, &textractsdk.DeleteAdapterVersionInput{
			AdapterId:      adapterID,
			AdapterVersion: versionOut.AdapterVersion,
		})
		require.NoError(t, err)

		_, err = client.GetAdapterVersion(ctx, &textractsdk.GetAdapterVersionInput{
			AdapterId:      adapterID,
			AdapterVersion: versionOut.AdapterVersion,
		})
		require.Error(t, err, "adapter version no longer exists after delete")

		_, err = client.DeleteAdapter(ctx, &textractsdk.DeleteAdapterInput{AdapterId: adapterID})
		require.NoError(t, err)

		_, err = client.GetAdapter(ctx, &textractsdk.GetAdapterInput{AdapterId: adapterID})
		require.Error(t, err, "adapter no longer exists after delete")
	})

	t.Run("AnalyzeExpense and AnalyzeID synchronous", func(t *testing.T) {
		t.Parallel()

		backend := textract.NewInMemoryBackend(tagsRTAccountID, tagsRTRegion)
		client := newTestTextractClient(t, textract.NewHandler(backend))
		ctx := t.Context()

		expenseOut, err := client.AnalyzeExpense(ctx, &textractsdk.AnalyzeExpenseInput{
			Document: &textracttypes.Document{
				S3Object: &textracttypes.S3Object{Bucket: aws.String("bucket"), Name: aws.String("invoice.pdf")},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, expenseOut.ExpenseDocuments)

		idOut, err := client.AnalyzeID(ctx, &textractsdk.AnalyzeIDInput{
			DocumentPages: []textracttypes.Document{
				{S3Object: &textracttypes.S3Object{Bucket: aws.String("bucket"), Name: aws.String("id-front.png")}},
				{S3Object: &textracttypes.S3Object{Bucket: aws.String("bucket"), Name: aws.String("id-back.png")}},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(idOut.AnalyzeIDModelVersion))
		require.NotEmpty(t, idOut.IdentityDocuments)
		require.NotNil(t, idOut.DocumentMetadata)
		assert.Equal(t, int32(2), aws.ToInt32(idOut.DocumentMetadata.Pages))
	})

	t.Run("Get* job results", func(t *testing.T) {
		t.Parallel()

		backend := textract.NewInMemoryBackendSync(tagsRTAccountID, tagsRTRegion)
		client := newTestTextractClient(t, textract.NewHandler(backend))
		ctx := t.Context()

		docJobOut, err := client.StartDocumentAnalysis(ctx, &textractsdk.StartDocumentAnalysisInput{
			DocumentLocation: &textracttypes.DocumentLocation{
				S3Object: &textracttypes.S3Object{Bucket: aws.String("bucket"), Name: aws.String("doc.pdf")},
			},
			FeatureTypes: []textracttypes.FeatureType{textracttypes.FeatureTypeTables},
		})
		require.NoError(t, err)

		getDocOut, err := client.GetDocumentAnalysis(ctx, &textractsdk.GetDocumentAnalysisInput{
			JobId: docJobOut.JobId,
		})
		require.NoError(t, err)
		assert.Equal(t, textracttypes.JobStatusSucceeded, getDocOut.JobStatus)
		require.NotEmpty(t, getDocOut.Blocks)

		expenseJobOut, err := client.StartExpenseAnalysis(ctx, &textractsdk.StartExpenseAnalysisInput{
			DocumentLocation: &textracttypes.DocumentLocation{
				S3Object: &textracttypes.S3Object{Bucket: aws.String("bucket"), Name: aws.String("invoice.pdf")},
			},
		})
		require.NoError(t, err)

		getExpenseOut, err := client.GetExpenseAnalysis(ctx, &textractsdk.GetExpenseAnalysisInput{
			JobId: expenseJobOut.JobId,
		})
		require.NoError(t, err)
		assert.Equal(t, textracttypes.JobStatusSucceeded, getExpenseOut.JobStatus)
		require.NotEmpty(t, getExpenseOut.ExpenseDocuments)

		lendingJobOut, err := client.StartLendingAnalysis(ctx, &textractsdk.StartLendingAnalysisInput{
			DocumentLocation: &textracttypes.DocumentLocation{
				S3Object: &textracttypes.S3Object{Bucket: aws.String("bucket"), Name: aws.String("loan.pdf")},
			},
		})
		require.NoError(t, err)

		getLendingOut, err := client.GetLendingAnalysis(ctx, &textractsdk.GetLendingAnalysisInput{
			JobId: lendingJobOut.JobId,
		})
		require.NoError(t, err)
		assert.Equal(t, textracttypes.JobStatusSucceeded, getLendingOut.JobStatus)

		getLendingSummaryOut, err := client.GetLendingAnalysisSummary(ctx, &textractsdk.GetLendingAnalysisSummaryInput{
			JobId: lendingJobOut.JobId,
		})
		require.NoError(t, err)
		assert.Equal(t, textracttypes.JobStatusSucceeded, getLendingSummaryOut.JobStatus)
	})
}
