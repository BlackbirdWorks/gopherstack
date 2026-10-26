package cloudformation_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cgrsdk "github.com/aws/aws-sdk-go-v2/service/codegurureviewer"
	cgrtypes "github.com/aws/aws-sdk-go-v2/service/codegurureviewer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cgrbackend "github.com/blackbirdworks/gopherstack/services/codegurureviewer"
)

func newCodeGuruEnv(t *testing.T) (familyStackEnv, *cgrsdk.Client) {
	t.Helper()

	handler := cgrbackend.NewHandler(cgrbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	env := newFamilyStackEnv(t, handler, func(b *cloudformation.ServiceBackends) { b.CodeGuruReviewer = handler })

	return env, cgrsdk.NewFromConfig(env.cfg, func(o *cgrsdk.Options) { o.BaseEndpoint = aws.String(env.url) })
}

func codeGuruTemplate(t *testing.T, props map[string]any) string {
	t.Helper()

	return stackTemplate(t, map[string]any{
		"Assoc": cfnRes("AWS::CodeGuruReviewer::RepositoryAssociation", props),
	}, map[string]any{"Ref": cfnRef("Assoc"), "Arn": cfnGetAtt("Assoc", "AssociationArn")})
}

func TestCreateStack_CodeGuruRepositoryAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, r *cgrtypes.RepositoryAssociation)
		props    map[string]any
		name     string
		provider cgrtypes.ProviderType
	}{
		{
			name:     "codecommit",
			provider: cgrtypes.ProviderTypeCodeCommit,
			props: map[string]any{
				"Name": "repo-a", "Type": "CodeCommit", "Tags": cfnTags(map[string]string{"env": "emu"}),
			},
			check: func(t *testing.T, r *cgrtypes.RepositoryAssociation) {
				t.Helper()
				assert.Equal(t, "repo-a", aws.ToString(r.Name))
			},
		},
		{
			name:     "s3 bucket",
			provider: cgrtypes.ProviderTypeS3Bucket,
			props:    map[string]any{"Name": "repo-s3", "Type": "S3Bucket", "BucketName": "codeguru-reviewer-bkt"},
			check: func(t *testing.T, r *cgrtypes.RepositoryAssociation) {
				t.Helper()
				require.NotNil(t, r.S3RepositoryDetails)
				assert.Equal(t, "codeguru-reviewer-bkt", aws.ToString(r.S3RepositoryDetails.BucketName))
			},
		},
		{
			name:     "bitbucket",
			provider: cgrtypes.ProviderTypeBitbucket,
			props: map[string]any{
				"Name": "repo-bb", "Type": "Bitbucket", "Owner": "acme",
				"ConnectionArn": "arn:aws:codestar-connections:us-east-1:000000000000:connection/abc",
			},
			check: func(t *testing.T, r *cgrtypes.RepositoryAssociation) {
				t.Helper()
				assert.Equal(t, "acme", aws.ToString(r.Owner))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, cgr := newCodeGuruEnv(t)
			stack := "cgr-" + strings.ReplaceAll(tc.name, " ", "-")
			out := createStackAndGetOutputs(t, env.cfn, stack, codeGuruTemplate(t, tc.props))
			assert.Equal(t, out["Arn"], out["Ref"])

			got, err := cgr.DescribeRepositoryAssociation(t.Context(), &cgrsdk.DescribeRepositoryAssociationInput{
				AssociationArn: aws.String(out["Arn"]),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.provider, got.RepositoryAssociation.ProviderType)
			tc.check(t, got.RepositoryAssociation)

			env.deleteStack(t, stack)

			after, err := cgr.DescribeRepositoryAssociation(t.Context(), &cgrsdk.DescribeRepositoryAssociationInput{
				AssociationArn: aws.String(out["Arn"]),
			})
			require.NoError(t, err)
			assert.Contains(t, []cgrtypes.RepositoryAssociationState{
				cgrtypes.RepositoryAssociationStateDisassociating, cgrtypes.RepositoryAssociationStateDisassociated,
			}, after.RepositoryAssociation.State)
		})
	}
}
