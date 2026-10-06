package rekognition_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"
	rekognitiontypes "github.com/aws/aws-sdk-go-v2/service/rekognition/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_CreateTagsAndKmsAreApplied(t *testing.T) {
	t.Parallel()

	tags := map[string]string{"env": "dev", "team": "ml"}

	tests := []struct {
		// create returns the ARN of the resource carrying tags.
		create func(t *testing.T, c *rekognitionsdk.Client) string
		name   string
	}{
		{
			name: "project",
			create: func(t *testing.T, c *rekognitionsdk.Client) string {
				t.Helper()

				out, err := c.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
					ProjectName: aws.String("tagged"), Tags: tags,
				})
				require.NoError(t, err)

				return aws.ToString(out.ProjectArn)
			},
		},
		{
			name: "dataset",
			create: func(t *testing.T, c *rekognitionsdk.Client) string {
				t.Helper()

				p, err := c.CreateProject(
					t.Context(),
					&rekognitionsdk.CreateProjectInput{ProjectName: aws.String("dsp")},
				)
				require.NoError(t, err)

				out, err := c.CreateDataset(t.Context(), &rekognitionsdk.CreateDatasetInput{
					ProjectArn:  p.ProjectArn,
					DatasetType: rekognitiontypes.DatasetTypeTrain,
					Tags:        tags,
				})
				require.NoError(t, err)

				return aws.ToString(out.DatasetArn)
			},
		},
		{
			name: "copied project version",
			create: func(t *testing.T, c *rekognitionsdk.Client) string {
				t.Helper()

				src, err := c.CreateProject(
					t.Context(),
					&rekognitionsdk.CreateProjectInput{ProjectName: aws.String("src")},
				)
				require.NoError(t, err)

				dst, err := c.CreateProject(
					t.Context(),
					&rekognitionsdk.CreateProjectInput{ProjectName: aws.String("dst")},
				)
				require.NoError(t, err)

				ver, err := c.CreateProjectVersion(t.Context(), &rekognitionsdk.CreateProjectVersionInput{
					ProjectArn:  src.ProjectArn,
					VersionName: aws.String("v1"),
					OutputConfig: &rekognitiontypes.OutputConfig{
						S3Bucket: aws.String("b"),
					},
				})
				require.NoError(t, err)

				cp, err := c.CopyProjectVersion(t.Context(), &rekognitionsdk.CopyProjectVersionInput{
					SourceProjectArn:        src.ProjectArn,
					SourceProjectVersionArn: ver.ProjectVersionArn,
					DestinationProjectArn:   dst.ProjectArn,
					VersionName:             aws.String("v2"),
					OutputConfig:            &rekognitiontypes.OutputConfig{S3Bucket: aws.String("b")},
					KmsKeyId:                aws.String("kms-1"),
					Tags:                    tags,
				})
				require.NoError(t, err)

				desc, err := c.DescribeProjectVersions(t.Context(), &rekognitionsdk.DescribeProjectVersionsInput{
					ProjectArn: dst.ProjectArn,
				})
				require.NoError(t, err)
				require.Len(t, desc.ProjectVersionDescriptions, 1)
				assert.Equal(t, "kms-1", aws.ToString(desc.ProjectVersionDescriptions[0].KmsKeyId))

				return aws.ToString(cp.ProjectVersionArn)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			arn := tc.create(t, c)

			out, err := c.ListTagsForResource(t.Context(), &rekognitionsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(arn),
			})
			require.NoError(t, err)
			assert.Equal(t, tags, out.Tags)
		})
	}
}

func TestSDK_ClientRequestTokenIdempotency(t *testing.T) {
	t.Parallel()

	video := func(name string) *rekognitiontypes.Video {
		return &rekognitiontypes.Video{
			S3Object: &rekognitiontypes.S3Object{Bucket: aws.String("b"), Name: aws.String(name)},
		}
	}

	tests := []struct {
		// start issues the call with (token, variant); variant changes a parameter.
		start func(c *rekognitionsdk.Client, token string, variant bool) (string, error)
		name  string
	}{
		{
			name: "label detection",
			start: func(c *rekognitionsdk.Client, token string, variant bool) (string, error) {
				n := "a.mp4"
				if variant {
					n = "other.mp4"
				}

				out, err := c.StartLabelDetection(t.Context(), &rekognitionsdk.StartLabelDetectionInput{
					Video: video(n), ClientRequestToken: aws.String(token),
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.JobId), nil
			},
		},
		{
			name: "text detection",
			start: func(c *rekognitionsdk.Client, token string, variant bool) (string, error) {
				tag := "x"
				if variant {
					tag = "y"
				}

				out, err := c.StartTextDetection(t.Context(), &rekognitionsdk.StartTextDetectionInput{
					Video: video("a.mp4"), JobTag: aws.String(tag), ClientRequestToken: aws.String(token),
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.JobId), nil
			},
		},
		{
			name: "media analysis",
			start: func(c *rekognitionsdk.Client, token string, variant bool) (string, error) {
				bucket := "out"
				if variant {
					bucket = "out2"
				}

				out, err := c.StartMediaAnalysisJob(t.Context(), &rekognitionsdk.StartMediaAnalysisJobInput{
					OperationsConfig: &rekognitiontypes.MediaAnalysisOperationsConfig{},
					Input: &rekognitiontypes.MediaAnalysisInput{
						S3Object: &rekognitiontypes.S3Object{Bucket: aws.String("b"), Name: aws.String("m.json")},
					},
					OutputConfig:       &rekognitiontypes.MediaAnalysisOutputConfig{S3Bucket: aws.String(bucket)},
					ClientRequestToken: aws.String(token),
				})
				if err != nil {
					return "", err
				}

				return aws.ToString(out.JobId), nil
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)

			first, err := tc.start(c, "tok-1", false)
			require.NoError(t, err)

			again, err := tc.start(c, "tok-1", false)
			require.NoError(t, err)
			assert.Equal(t, first, again)

			fresh, err := tc.start(c, "tok-2", false)
			require.NoError(t, err)
			assert.NotEqual(t, first, fresh)

			_, err = tc.start(c, "tok-1", true)
			var mismatch *rekognitiontypes.IdempotentParameterMismatchException
			require.ErrorAs(t, err, &mismatch)
		})
	}
}

func TestSDK_FaceLivenessTokenReturnsSameSession(t *testing.T) {
	t.Parallel()

	c := newTestHandlerAndClient(t)

	a, err := c.CreateFaceLivenessSession(t.Context(), &rekognitionsdk.CreateFaceLivenessSessionInput{
		ClientRequestToken: aws.String("t"),
	})
	require.NoError(t, err)

	b, err := c.CreateFaceLivenessSession(t.Context(), &rekognitionsdk.CreateFaceLivenessSessionInput{
		ClientRequestToken: aws.String("t"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(a.SessionId), aws.ToString(b.SessionId))

	n, err := c.CreateFaceLivenessSession(t.Context(), &rekognitionsdk.CreateFaceLivenessSessionInput{})
	require.NoError(t, err)
	assert.NotEqual(t, aws.ToString(a.SessionId), aws.ToString(n.SessionId))
}

func TestSDK_MediaAnalysisKmsKeyRoundTrip(t *testing.T) {
	t.Parallel()

	c := newTestHandlerAndClient(t)

	out, err := c.StartMediaAnalysisJob(t.Context(), &rekognitionsdk.StartMediaAnalysisJobInput{
		OperationsConfig: &rekognitiontypes.MediaAnalysisOperationsConfig{},
		Input: &rekognitiontypes.MediaAnalysisInput{
			S3Object: &rekognitiontypes.S3Object{Bucket: aws.String("b"), Name: aws.String("m.json")},
		},
		OutputConfig: &rekognitiontypes.MediaAnalysisOutputConfig{S3Bucket: aws.String("out")},
		KmsKeyId:     aws.String("kms-9"),
	})
	require.NoError(t, err)

	got, err := c.GetMediaAnalysisJob(t.Context(), &rekognitionsdk.GetMediaAnalysisJobInput{JobId: out.JobId})
	require.NoError(t, err)
	assert.Equal(t, "kms-9", aws.ToString(got.KmsKeyId))
}

func TestSDK_UserOpsClientRequestToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token *string
		name  string
		want  bool
	}{
		{name: "same token replays success", token: aws.String("tok"), want: true},
		{name: "no token conflicts", token: nil, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)

			_, err := c.CreateCollection(
				t.Context(),
				&rekognitionsdk.CreateCollectionInput{CollectionId: aws.String("c")},
			)
			require.NoError(t, err)

			newIn := func(user string) *rekognitionsdk.CreateUserInput {
				return &rekognitionsdk.CreateUserInput{
					CollectionId: aws.String("c"), UserId: aws.String(user), ClientRequestToken: tc.token,
				}
			}

			_, err = c.CreateUser(t.Context(), newIn("u"))
			require.NoError(t, err)

			_, err = c.CreateUser(t.Context(), newIn("u"))
			if tc.want {
				require.NoError(t, err)

				_, err = c.CreateUser(t.Context(), newIn("other"))

				var m *rekognitiontypes.IdempotentParameterMismatchException
				require.ErrorAs(t, err, &m)

				return
			}

			var conflict *rekognitiontypes.ConflictException
			require.ErrorAs(t, err, &conflict)
		})
	}
}

func TestSDK_DeleteUserTokenReplaysSuccess(t *testing.T) {
	t.Parallel()

	c := newTestHandlerAndClient(t)

	_, err := c.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{CollectionId: aws.String("c")})
	require.NoError(t, err)

	_, err = c.CreateUser(
		t.Context(),
		&rekognitionsdk.CreateUserInput{CollectionId: aws.String("c"), UserId: aws.String("u")},
	)
	require.NoError(t, err)

	for range 2 {
		_, err = c.DeleteUser(t.Context(), &rekognitionsdk.DeleteUserInput{
			CollectionId: aws.String("c"), UserId: aws.String("u"), ClientRequestToken: aws.String("t"),
		})
		require.NoError(t, err)
	}

	_, err = c.DeleteUser(t.Context(), &rekognitionsdk.DeleteUserInput{
		CollectionId: aws.String("c"), UserId: aws.String("u"), ClientRequestToken: aws.String("fresh"),
	})

	var nf *rekognitiontypes.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
}

func TestSDK_ProjectVersionFeatureFollowsProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		feature rekognitiontypes.CustomizationFeature
	}{
		{name: "custom labels", feature: rekognitiontypes.CustomizationFeatureCustomLabels},
		{name: "content moderation", feature: rekognitiontypes.CustomizationFeatureContentModeration},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)

			p, err := c.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
				ProjectName: aws.String("p"), Feature: tc.feature,
			})
			require.NoError(t, err)

			_, err = c.CreateProjectVersion(t.Context(), &rekognitionsdk.CreateProjectVersionInput{
				ProjectArn: p.ProjectArn, VersionName: aws.String("v"),
				OutputConfig: &rekognitiontypes.OutputConfig{S3Bucket: aws.String("b")},
			})
			require.NoError(t, err)

			out, err := c.DescribeProjectVersions(
				t.Context(),
				&rekognitionsdk.DescribeProjectVersionsInput{ProjectArn: p.ProjectArn},
			)
			require.NoError(t, err)
			require.Len(t, out.ProjectVersionDescriptions, 1)
			assert.Equal(t, tc.feature, out.ProjectVersionDescriptions[0].Feature)
		})
	}
}
