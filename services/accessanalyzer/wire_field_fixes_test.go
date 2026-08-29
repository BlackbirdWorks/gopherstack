package accessanalyzer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestListAccessPreviewFindings_ChangeType_RealSDKClient proves
// AccessPreviewFinding.ChangeType (accessanalyzer@v1.51.4
// types/types.go's AccessPreviewFinding, types/enums.go:237-244) decodes as
// the real types.FindingChangeTypeNew ("NEW") member, not the non-member
// string "New" the handler previously emitted. A typed client decodes any
// string into ChangeType without error, so the wrong-but-plausible "New"
// produced no decode failure -- only a switch on the typed constant would
// silently fall through every real case.
func TestListAccessPreviewFindings_ChangeType_RealSDKClient(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)
	client := newTestAccessAnalyzerClient(t, h)
	ctx := t.Context()

	analyzer, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
		AnalyzerName: aws.String("wire-fix-changetype-analyzer"),
		Type:         aatypes.TypeAccount,
	})
	require.NoError(t, err)

	created, err := client.CreateAccessPreview(ctx, &aasdk.CreateAccessPreviewInput{
		AnalyzerArn: analyzer.Arn,
		Configurations: map[string]aatypes.Configuration{
			"arn:aws:s3:::wire-fix-changetype-bucket": &aatypes.ConfigurationMemberS3Bucket{
				Value: aatypes.S3BucketConfiguration{
					BucketPolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = b.AddFinding(
		"wire-fix-changetype-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::bucket", nil, nil, nil,
	)
	require.NoError(t, err)

	out, err := client.ListAccessPreviewFindings(ctx, &aasdk.ListAccessPreviewFindingsInput{
		AccessPreviewId: created.Id,
		AnalyzerArn:     analyzer.Arn,
	})
	require.NoError(t, err)
	require.Len(t, out.Findings, 1)
	assert.Equal(t, aatypes.FindingChangeTypeNew, out.Findings[0].ChangeType)
}
