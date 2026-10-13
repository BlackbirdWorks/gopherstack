package codedeploy_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplicationRevision_RawStringRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		sha     string
	}{
		{name: "yaml", content: "version: 0.0\nResources: []", sha: "abc123"},
		{name: "json", content: `{"version":"0.0"}`, sha: "def456"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCodeDeployClient(t, newTestHandler(t))
			ctx := t.Context()

			_, err := client.CreateApplication(ctx, &codedeploysdk.CreateApplicationInput{
				ApplicationName: aws.String("app"),
				ComputePlatform: types.ComputePlatformLambda,
			})
			require.NoError(t, err)

			rev := rawStringRevision(tt.content, tt.sha)
			_, err = client.RegisterApplicationRevision(ctx, &codedeploysdk.RegisterApplicationRevisionInput{
				ApplicationName: aws.String("app"),
				Revision:        rev,
			})
			require.NoError(t, err)

			got, err := client.GetApplicationRevision(ctx, &codedeploysdk.GetApplicationRevisionInput{
				ApplicationName: aws.String("app"),
				Revision:        rev,
			})
			require.NoError(t, err)
			require.NotNil(t, got.Revision)
			content, sha, ok := rawStringOf(got.Revision)
			require.True(t, ok)
			assert.Equal(t, tt.content, content)
			assert.Equal(t, tt.sha, sha)
		})
	}
}

//nolint:staticcheck // SA1019: the deprecated String revision is the subject of this test
func rawStringRevision(content, sha string) *types.RevisionLocation {
	return &types.RevisionLocation{
		RevisionType: types.RevisionLocationTypeString,
		String_:      &types.RawString{Content: aws.String(content), Sha256: aws.String(sha)},
	}
}

//nolint:staticcheck // SA1019: the deprecated String revision is the subject of this test
func rawStringOf(r *types.RevisionLocation) (string, string, bool) {
	if r.String_ == nil {
		return "", "", false
	}

	return aws.ToString(r.String_.Content), aws.ToString(r.String_.Sha256), true
}
