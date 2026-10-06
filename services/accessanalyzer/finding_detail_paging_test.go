package accessanalyzer_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestGetFinding_PagingMembers checks maxResults/nextToken on GetFindingV2 and GetFindingRecommendation.
func TestGetFinding_PagingMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(c *aasdk.Client, in aasdk.GetFindingV2Input) error
		size    *int32
		name    string
		token   string
		wantErr bool
	}{
		{name: "v2_size_ok", size: aws.Int32(1), call: v2Call},
		{name: "v2_zero_size", size: aws.Int32(0), wantErr: true, call: v2Call},
		{name: "v2_bad_token", token: "!!bad", wantErr: true, call: v2Call},
		{name: "recommendation_size_ok", size: aws.Int32(5), call: recCall},
		{name: "recommendation_bad_token", token: "!!bad", wantErr: true, call: recCall},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestAccessAnalyzerClient(t, accessanalyzer.NewHandler(b))

			an, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String("pg"), Type: aatypes.TypeAccount,
			})
			require.NoError(t, err)

			f, err := b.AddFinding("pg", "AWS::S3::Bucket", "arn:aws:s3:::pg", nil, nil, nil)
			require.NoError(t, err)

			_, err = client.GenerateFindingRecommendation(t.Context(), &aasdk.GenerateFindingRecommendationInput{
				AnalyzerArn: an.Arn, Id: aws.String(f.ID),
			})
			require.NoError(t, err)

			in := aasdk.GetFindingV2Input{AnalyzerArn: an.Arn, Id: aws.String(f.ID), MaxResults: tt.size}
			if tt.token != "" {
				in.NextToken = aws.String(tt.token)
			}

			err = tt.call(client, in)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}

			require.NoError(t, err)
		})
	}
}

func v2Call(c *aasdk.Client, in aasdk.GetFindingV2Input) error {
	out, err := c.GetFindingV2(context.Background(), &in)
	if err == nil {
		if len(out.FindingDetails) != 1 || out.NextToken != nil {
			return assert.AnError
		}
	}

	return err
}

func recCall(c *aasdk.Client, in aasdk.GetFindingV2Input) error {
	_, err := c.GetFindingRecommendation(context.Background(), &aasdk.GetFindingRecommendationInput{
		AnalyzerArn: in.AnalyzerArn, Id: in.Id, MaxResults: in.MaxResults, NextToken: in.NextToken,
	})

	return err
}
