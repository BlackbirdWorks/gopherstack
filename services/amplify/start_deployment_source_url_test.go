package amplify_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	amplifytypes "github.com/aws/aws-sdk-go-v2/service/amplify/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestStartDeployment_SourceURLEchoedInJobSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		srcURL   string
		srcType  amplifytypes.SourceUrlType
		wantURL  string
		wantType amplifytypes.SourceUrlType
		wantErr  bool
	}{
		{
			name: "default zip", srcURL: "https://example.com/a.zip",
			wantURL: "https://example.com/a.zip", wantType: amplifytypes.SourceUrlTypeZip,
		},
		{
			name: "bucket prefix", srcURL: "s3://bkt/pre", srcType: amplifytypes.SourceUrlTypeBucketPrefix,
			wantURL: "s3://bkt/pre", wantType: amplifytypes.SourceUrlTypeBucketPrefix,
		},
		{name: "no source", wantURL: "", wantType: ""},
		{name: "invalid type", srcURL: "x", srcType: "TAR", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestAmplifyClient(t, amplify.NewHandler(backend))
			ctx := t.Context()

			app, err := client.CreateApp(ctx, &amplifysdk.CreateAppInput{Name: aws.String("src-app")})
			require.NoError(t, err)
			_, err = client.CreateBranch(ctx, &amplifysdk.CreateBranchInput{
				AppId: app.App.AppId, BranchName: aws.String("main"),
			})
			require.NoError(t, err)

			in := &amplifysdk.StartDeploymentInput{
				AppId: app.App.AppId, BranchName: aws.String("main"), SourceUrlType: tt.srcType,
			}
			if tt.srcURL != "" {
				in.SourceUrl = aws.String(tt.srcURL)
			}

			out, err := client.StartDeployment(ctx, in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantURL, aws.ToString(out.JobSummary.SourceUrl))
			assert.Equal(t, tt.wantType, out.JobSummary.SourceUrlType)

			got, err := client.GetJob(ctx, &amplifysdk.GetJobInput{
				AppId: app.App.AppId, BranchName: aws.String("main"), JobId: out.JobSummary.JobId,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantURL, aws.ToString(got.Job.Summary.SourceUrl))
			assert.Equal(t, tt.wantType, got.Job.Summary.SourceUrlType)
		})
	}
}
