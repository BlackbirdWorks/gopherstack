package elasticbeanstalk_test

import (
	"archive/zip"
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const composeBucket = "eb-bundles"

func zipWithManifest(t *testing.T, manifest string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	if manifest != "" {
		w, err := zw.Create("env.yaml")
		require.NoError(t, err)
		_, err = w.Write([]byte(manifest))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func newComposeClient(t *testing.T) (*ebsdk.Client, *s3backend.S3Handler) {
	t.Helper()

	s3h := s3backend.NewHandler(s3backend.NewInMemoryBackend(nil))
	_, err := s3h.Backend.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String(composeBucket)})
	require.NoError(t, err)

	backend := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
	backend.SetS3Backend(s3h.Backend)

	return newTestEBClient(t, elasticbeanstalk.NewHandler(backend)), s3h
}

func putBundle(t *testing.T, client *ebsdk.Client, s3h *s3backend.S3Handler, label string, data []byte) {
	t.Helper()

	key := label + ".zip"
	_, err := s3h.Backend.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String(composeBucket), Key: aws.String(key), Body: bytes.NewReader(data),
	})
	require.NoError(t, err)

	_, err = client.CreateApplicationVersion(t.Context(), &ebsdk.CreateApplicationVersionInput{
		ApplicationName: aws.String("compose-app"),
		VersionLabel:    aws.String(label),
		SourceBundle:    &ebtypes.S3Location{S3Bucket: aws.String(composeBucket), S3Key: aws.String(key)},
	})
	require.NoError(t, err)
}

const (
	frontManifest = `EnvironmentName: front+
SolutionStack: 64bit Amazon Linux 2023 v4.0.0 running Python 3.11
EnvironmentLinks:
  WORKERQUEUE: worker+
OptionSettings:
  aws:elasticbeanstalk:application:environment:
    STAGE: dev
`
	workerManifest = `EnvironmentName: worker+
SolutionStack: 64bit Amazon Linux 2023 v4.0.0 running Python 3.11
EnvironmentTier:
  Name: Worker
  Type: SQS/HTTP
`
)

func TestComposeEnvironments(t *testing.T) {
	t.Parallel()

	t.Run("creates grouped environments with links", func(t *testing.T) {
		t.Parallel()

		client, s3h := newComposeClient(t)
		ctx := t.Context()

		_, err := client.CreateApplication(
			ctx,
			&ebsdk.CreateApplicationInput{ApplicationName: aws.String("compose-app")},
		)
		require.NoError(t, err)

		putBundle(t, client, s3h, "front-v1", zipWithManifest(t, frontManifest))
		putBundle(t, client, s3h, "worker-v1", zipWithManifest(t, workerManifest))

		out, err := client.ComposeEnvironments(ctx, &ebsdk.ComposeEnvironmentsInput{
			ApplicationName: aws.String("compose-app"),
			GroupName:       aws.String("dev"),
			VersionLabels:   []string{"front-v1", "worker-v1"},
		})
		require.NoError(t, err)
		require.Len(t, out.Environments, 2)

		byName := map[string]ebtypes.EnvironmentDescription{}
		for _, e := range out.Environments {
			byName[aws.ToString(e.EnvironmentName)] = e
		}

		front := byName["front-dev"]
		assert.Equal(t, "front-v1", aws.ToString(front.VersionLabel))
		require.Len(t, front.EnvironmentLinks, 1)
		assert.Equal(t, "WORKERQUEUE", aws.ToString(front.EnvironmentLinks[0].LinkName))
		assert.Equal(t, "worker-dev", aws.ToString(front.EnvironmentLinks[0].EnvironmentName))
		assert.Equal(t, "Worker", aws.ToString(byName["worker-dev"].Tier.Name))

		described, err := client.DescribeEnvironments(ctx, &ebsdk.DescribeEnvironmentsInput{
			ApplicationName: aws.String("compose-app"),
		})
		require.NoError(t, err)
		assert.Len(t, described.Environments, 2)

		again, err := client.ComposeEnvironments(ctx, &ebsdk.ComposeEnvironmentsInput{
			ApplicationName: aws.String("compose-app"),
			GroupName:       aws.String("dev"),
			VersionLabels:   []string{"front-v1"},
		})
		require.NoError(t, err)
		assert.Len(t, again.Environments, 1, "an existing environment is updated, not recreated")
	})

	errCases := []struct {
		name     string
		manifest string
		group    string
		label    string
	}{
		{name: "missing version", label: "nope", group: "dev"},
		{name: "no manifest", label: "empty-v1", group: "dev"},
		{name: "group required", manifest: frontManifest, label: "front-v1"},
		{name: "invalid yaml", manifest: "EnvironmentName: [", label: "bad-v1", group: "dev"},
	}

	for _, tc := range errCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, s3h := newComposeClient(t)
			ctx := t.Context()

			_, err := client.CreateApplication(ctx, &ebsdk.CreateApplicationInput{
				ApplicationName: aws.String("compose-app"),
			})
			require.NoError(t, err)

			if tc.label != "nope" {
				putBundle(t, client, s3h, tc.label, zipWithManifest(t, tc.manifest))
			}

			in := &ebsdk.ComposeEnvironmentsInput{
				ApplicationName: aws.String("compose-app"),
				VersionLabels:   []string{tc.label},
			}
			if tc.group != "" {
				in.GroupName = aws.String(tc.group)
			}

			_, err = client.ComposeEnvironments(ctx, in)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "InvalidParameterValue")
		})
	}
}
