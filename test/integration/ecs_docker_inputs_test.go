package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ECS_DockerEnvironmentFilesAndRegistryAuth drives environmentFiles (S3) and
// repositoryCredentials through the ecs, s3 and secretsmanager SDK clients against a docker-runtime server.
// Skipped unless GOPHERSTACK_TEST_ECS_DOCKER_ENDPOINT is set.
func TestIntegration_ECS_DockerEnvironmentFilesAndRegistryAuth(t *testing.T) {
	t.Parallel()

	endpoint := os.Getenv("GOPHERSTACK_TEST_ECS_DOCKER_ENDPOINT")
	if endpoint == "" {
		t.Skip("skipping docker-runtime ECS test: GOPHERSTACK_TEST_ECS_DOCKER_ENDPOINT not set")
	}

	ctx := t.Context()
	suffix := uuid.NewString()[:8]

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := ecs.NewFromConfig(cfg, func(o *ecs.Options) { o.BaseEndpoint = &endpoint })
	sm := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) { o.BaseEndpoint = &endpoint })
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = &endpoint; o.UsePathStyle = true })

	bucket := "ecs-env-" + suffix
	_, err = s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("app.env"),
		Body: strings.NewReader("# comment\nFROM_FILE=file-value\nOVERRIDE=file-loses\n"),
	})
	require.NoError(t, err)

	clusterName := "docker-inputs-" + suffix
	_, err = client.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String(clusterName)})
	require.NoError(t, err)

	hostDir := t.TempDir()
	require.NoError(t, os.Chmod(hostDir, 0o777))

	volumes := []ecstypes.Volume{{
		Name: aws.String("shared"), Host: &ecstypes.HostVolumeProperties{SourcePath: aws.String(hostDir)},
	}}

	register := func(name string, cd ecstypes.ContainerDefinition) string {
		out, rerr := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
			Family:               aws.String(name + "-" + suffix),
			Volumes:              volumes,
			ContainerDefinitions: []ecstypes.ContainerDefinition{cd},
		})
		require.NoError(t, rerr)

		return aws.ToString(out.TaskDefinition.TaskDefinitionArn)
	}

	run := func(td string) string {
		out, rerr := client.RunTask(ctx, &ecs.RunTaskInput{
			Cluster: aws.String(clusterName), TaskDefinition: aws.String(td),
		})
		require.NoError(t, rerr)
		require.Len(t, out.Tasks, 1)

		arn := aws.ToString(out.Tasks[0].TaskArn)

		t.Cleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			defer cancel()

			_, _ = client.StopTask(stopCtx, &ecs.StopTaskInput{Cluster: aws.String(clusterName), Task: aws.String(arn)})
		})

		return arn
	}

	stoppedReason := func(arn string) string {
		var reason string

		require.Eventually(t, func() bool {
			out, derr := client.DescribeTasks(ctx, &ecs.DescribeTasksInput{
				Cluster: aws.String(clusterName), Tasks: []string{arn},
			})
			if derr != nil || len(out.Tasks) != 1 || aws.ToString(out.Tasks[0].LastStatus) != "STOPPED" {
				return false
			}

			reason = aws.ToString(out.Tasks[0].StoppedReason)

			return true
		}, time.Minute, 500*time.Millisecond)

		return reason
	}

	const image = "busybox:1.36"

	t.Run("environment_file_applied", func(t *testing.T) {
		t.Parallel()

		run(register("envfile", ecstypes.ContainerDefinition{
			Name: aws.String("app"), Image: aws.String(image), Essential: aws.Bool(true),
			Command: []string{"sh", "-c", `echo "$FROM_FILE $OVERRIDE" > /data/env-out; sleep 300`},
			Environment: []ecstypes.KeyValuePair{
				{Name: aws.String("OVERRIDE"), Value: aws.String("env-wins")},
			},
			EnvironmentFiles: []ecstypes.EnvironmentFile{{
				Type: ecstypes.EnvironmentFileTypeS3, Value: aws.String("arn:aws:s3:::" + bucket + "/app.env"),
			}},
			MountPoints: []ecstypes.MountPoint{
				{SourceVolume: aws.String("shared"), ContainerPath: aws.String("/data")},
			},
			StopTimeout: aws.Int32(2),
		}))

		require.Eventually(t, func() bool {
			b, rerr := os.ReadFile(filepath.Join(hostDir, "env-out"))

			return rerr == nil && strings.TrimSpace(string(b)) == "file-value env-wins"
		}, time.Minute, 500*time.Millisecond)
	})

	t.Run("missing_environment_file_fails_task", func(t *testing.T) {
		t.Parallel()

		arn := run(register("envmissing", ecstypes.ContainerDefinition{
			Name: aws.String("app"), Image: aws.String(image), Essential: aws.Bool(true),
			EnvironmentFiles: []ecstypes.EnvironmentFile{{
				Type: ecstypes.EnvironmentFileTypeS3, Value: aws.String("arn:aws:s3:::" + bucket + "/absent.env"),
			}},
		}))

		assert.Contains(t, stoppedReason(arn), "ResourceInitializationError: failed to download env files")
	})

	t.Run("missing_registry_secret_fails_task", func(t *testing.T) {
		t.Parallel()

		arn := run(register("regmissing", ecstypes.ContainerDefinition{
			Name: aws.String("app"), Image: aws.String(image), Essential: aws.Bool(true),
			RepositoryCredentials: &ecstypes.RepositoryCredentials{
				CredentialsParameter: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:nope-AbCdEf"),
			},
		}))

		assert.Contains(t, stoppedReason(arn), "ResourceInitializationError: unable to pull secrets or registry auth")
	})

	t.Run("malformed_registry_secret_fails_task", func(t *testing.T) {
		t.Parallel()

		secret, cerr := sm.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
			Name: aws.String("regcreds-" + suffix), SecretString: aws.String(`{"user":"x"}`),
		})
		require.NoError(t, cerr)

		arn := run(register("regbad", ecstypes.ContainerDefinition{
			Name: aws.String("app"), Image: aws.String(image), Essential: aws.Bool(true),
			RepositoryCredentials: &ecstypes.RepositoryCredentials{CredentialsParameter: secret.ARN},
		}))

		reason := stoppedReason(arn)
		assert.Contains(t, reason, "unable to pull secrets or registry auth")
	})
}
