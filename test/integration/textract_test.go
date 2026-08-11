package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	textractsdk "github.com/aws/aws-sdk-go-v2/service/textract"
	textracttypes "github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Textract_DetectDocumentText exercises the synchronous
// DetectDocumentText operation via the AWS SDK v2 against the in-memory
// Textract backend.
func TestIntegration_Textract_DetectDocumentText(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createTextractClient(t)
	ctx := t.Context()

	out, err := client.DetectDocumentText(ctx, &textractsdk.DetectDocumentTextInput{
		Document: &textracttypes.Document{
			S3Object: &textracttypes.S3Object{
				Bucket: aws.String("it-textract-bucket"),
				Name:   aws.String("doc.png"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.DocumentMetadata)
	assert.Equal(t, int32(1), aws.ToInt32(out.DocumentMetadata.Pages))
}

// TestIntegration_Textract_AnalyzeDocument_AdaptersConfig drives
// CreateAdapter/CreateAdapterVersion/AnalyzeDocument through a real
// aws-sdk-go-v2 client: AdaptersConfig.Adapters references are validated
// against real Adapter/AdapterVersion backend state, and an unknown adapter
// surfaces InvalidParameterException -- not ResourceNotFoundException,
// which AnalyzeDocument's real error surface has no case for at all.
func TestIntegration_Textract_AnalyzeDocument_AdaptersConfig(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createTextractClient(t)
	ctx := t.Context()

	adapterOut, err := client.CreateAdapter(ctx, &textractsdk.CreateAdapterInput{
		AdapterName:  aws.String("it-adapter"),
		FeatureTypes: []textracttypes.FeatureType{textracttypes.FeatureTypeQueries},
	})
	require.NoError(t, err)

	adapterID := aws.ToString(adapterOut.AdapterId)
	require.NotEmpty(t, adapterID)

	versionOut, err := client.CreateAdapterVersion(ctx, &textractsdk.CreateAdapterVersionInput{
		AdapterId: aws.String(adapterID),
		DatasetConfig: &textracttypes.AdapterVersionDatasetConfig{
			ManifestS3Object: &textracttypes.S3Object{
				Bucket: aws.String("it-textract-bucket"),
				Name:   aws.String("manifest.jsonl"),
			},
		},
		OutputConfig: &textracttypes.OutputConfig{
			S3Bucket: aws.String("it-textract-bucket"),
		},
	})
	require.NoError(t, err)

	adapterVersion := aws.ToString(versionOut.AdapterVersion)
	require.NotEmpty(t, adapterVersion)

	doc := &textracttypes.Document{
		S3Object: &textracttypes.S3Object{
			Bucket: aws.String("it-textract-bucket"),
			Name:   aws.String("doc.png"),
		},
	}

	t.Run("known adapter and version succeeds", func(t *testing.T) {
		t.Parallel()

		_, analyzeErr := client.AnalyzeDocument(ctx, &textractsdk.AnalyzeDocumentInput{
			Document:     doc,
			FeatureTypes: []textracttypes.FeatureType{textracttypes.FeatureTypeTables},
			AdaptersConfig: &textracttypes.AdaptersConfig{
				Adapters: []textracttypes.Adapter{
					{AdapterId: aws.String(adapterID), Version: aws.String(adapterVersion)},
				},
			},
		})
		require.NoError(t, analyzeErr)
	})

	t.Run("unknown adapter gives InvalidParameterException", func(t *testing.T) {
		t.Parallel()

		_, analyzeErr := client.AnalyzeDocument(ctx, &textractsdk.AnalyzeDocumentInput{
			Document:     doc,
			FeatureTypes: []textracttypes.FeatureType{textracttypes.FeatureTypeTables},
			AdaptersConfig: &textracttypes.AdaptersConfig{
				Adapters: []textracttypes.Adapter{
					{AdapterId: aws.String("no-such-adapter"), Version: aws.String("1")},
				},
			},
		})
		require.Error(t, analyzeErr)

		var invalidParam *textracttypes.InvalidParameterException
		require.ErrorAs(t, analyzeErr, &invalidParam)
	})
}
