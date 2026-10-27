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
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ECS_DockerContainerDefinition runs real busybox containers against a server started with
// GOPHERSTACK_ECS_RUNTIME=docker and checks that container-definition parameters are honoured: a Secrets Manager
// secret, a bind volume shared by two containers, dependsOn HEALTHY gating, health reporting, and
// essential-container exit. Skipped unless GOPHERSTACK_TEST_ECS_DOCKER_ENDPOINT is set.
func TestIntegration_ECS_DockerContainerDefinition(t *testing.T) {
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

	secret, err := sm.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String("ecs-docker-" + suffix),
		SecretString: aws.String(`{"password":"hunter2"}`),
	})
	require.NoError(t, err)

	clusterName := "docker-def-" + suffix
	_, err = client.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String(clusterName)})
	require.NoError(t, err)

	hostDir := t.TempDir()
	require.NoError(t, os.Chmod(hostDir, 0o777))

	reg, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String("docker-def-" + suffix),
		Volumes: []ecstypes.Volume{{
			Name: aws.String("shared"),
			Host: &ecstypes.HostVolumeProperties{SourcePath: aws.String(hostDir)},
		}},
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{
				Name:      aws.String("app"),
				Image:     aws.String("busybox:latest"),
				Essential: aws.Bool(true),
				Command:   []string{"sh", "-c", `echo "$DB_PASS" > /data/app-secret; sleep 300`},
				Secrets: []ecstypes.Secret{{
					Name: aws.String("DB_PASS"), ValueFrom: aws.String(aws.ToString(secret.ARN) + ":password::"),
				}},
				MountPoints: []ecstypes.MountPoint{
					{SourceVolume: aws.String("shared"), ContainerPath: aws.String("/data")},
				},
				DependsOn: []ecstypes.ContainerDependency{{
					ContainerName: aws.String("db"), Condition: ecstypes.ContainerConditionHealthy,
				}},
			},
			{
				Name:      aws.String("db"),
				Image:     aws.String("busybox:latest"),
				Essential: aws.Bool(true),
				Command:   []string{"sh", "-c", "touch /data/db-ready; sleep 300"},
				HealthCheck: &ecstypes.HealthCheck{
					Command:  []string{"CMD-SHELL", "test -f /data/db-ready"},
					Interval: aws.Int32(5), Timeout: aws.Int32(3), Retries: aws.Int32(3),
				},
				StopTimeout: aws.Int32(2),
				MountPoints: []ecstypes.MountPoint{
					{SourceVolume: aws.String("shared"), ContainerPath: aws.String("/data")},
				},
			},
		},
	})
	require.NoError(t, err)

	run, err := client.RunTask(ctx, &ecs.RunTaskInput{
		Cluster: aws.String(clusterName), TaskDefinition: reg.TaskDefinition.TaskDefinitionArn,
	})
	require.NoError(t, err)
	require.Len(t, run.Tasks, 1)

	taskArn := aws.ToString(run.Tasks[0].TaskArn)

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()

		_, _ = client.StopTask(stopCtx, &ecs.StopTaskInput{Cluster: aws.String(clusterName), Task: aws.String(taskArn)})
	})

	describe := func() ecstypes.Task {
		out, derr := client.DescribeTasks(ctx, &ecs.DescribeTasksInput{
			Cluster: aws.String(clusterName), Tasks: []string{taskArn},
		})
		require.NoError(t, derr)
		require.Len(t, out.Tasks, 1)

		return out.Tasks[0]
	}

	require.Equal(t, "RUNNING", aws.ToString(describe().LastStatus), "RunTask returns once dependsOn gates passed")

	require.Eventually(t, func() bool {
		return describe().HealthStatus == ecstypes.HealthStatusHealthy
	}, time.Minute, time.Second, "task health follows the essential db container")

	byName := map[string]ecstypes.Container{}
	for _, c := range describe().Containers {
		byName[aws.ToString(c.Name)] = c
	}

	assert.Equal(t, ecstypes.HealthStatusHealthy, byName["db"].HealthStatus)

	require.Eventually(t, func() bool {
		b, rerr := os.ReadFile(filepath.Join(hostDir, "app-secret"))

		return rerr == nil && strings.TrimSpace(string(b)) == "hunter2"
	}, 30*time.Second, 250*time.Millisecond, "app saw the secret and wrote it to the shared bind volume")

	assert.FileExists(t, filepath.Join(hostDir, "db-ready"))

	// An essential container exiting stops the task.
	exitReg, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String("docker-exit-" + suffix),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{
				Name: aws.String("main"), Image: aws.String("busybox:latest"), Essential: aws.Bool(true),
				Command: []string{"sh", "-c", "exit 3"},
			},
			{
				Name: aws.String("side"), Image: aws.String("busybox:latest"), Essential: aws.Bool(false),
				Command: []string{"sleep", "300"},
			},
		},
	})
	require.NoError(t, err)

	exitRun, err := client.RunTask(ctx, &ecs.RunTaskInput{
		Cluster: aws.String(clusterName), TaskDefinition: exitReg.TaskDefinition.TaskDefinitionArn,
	})
	require.NoError(t, err)

	exitArn := aws.ToString(exitRun.Tasks[0].TaskArn)

	require.Eventually(t, func() bool {
		out, derr := client.DescribeTasks(
			ctx,
			&ecs.DescribeTasksInput{Cluster: aws.String(clusterName), Tasks: []string{exitArn}},
		)
		if derr != nil || len(out.Tasks) != 1 || aws.ToString(out.Tasks[0].LastStatus) != "STOPPED" {
			return false
		}

		got := out.Tasks[0]
		assert.Equal(t, ecstypes.TaskStopCodeEssentialContainerExited, got.StopCode)
		assert.Equal(t, "Essential container in task exited", aws.ToString(got.StoppedReason))

		for _, c := range got.Containers {
			if aws.ToString(c.Name) == "main" {
				require.NotNil(t, c.ExitCode)
				assert.EqualValues(t, 3, *c.ExitCode)
			}
		}

		return true
	}, time.Minute, 500*time.Millisecond)

	// A secret that does not exist fails the task before any container starts.
	missing, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String("docker-missing-" + suffix),
		ContainerDefinitions: []ecstypes.ContainerDefinition{{
			Name: aws.String("x"), Image: aws.String("busybox:latest"), Essential: aws.Bool(true),
			Secrets: []ecstypes.Secret{{
				Name:      aws.String("S"),
				ValueFrom: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:nope-AbCdEf"),
			}},
		}},
	})
	require.NoError(t, err)

	missRun, err := client.RunTask(ctx, &ecs.RunTaskInput{
		Cluster: aws.String(clusterName), TaskDefinition: missing.TaskDefinition.TaskDefinitionArn,
	})
	require.NoError(t, err)

	out, err := client.DescribeTasks(ctx, &ecs.DescribeTasksInput{
		Cluster: aws.String(clusterName), Tasks: []string{aws.ToString(missRun.Tasks[0].TaskArn)},
	})
	require.NoError(t, err)
	require.Len(t, out.Tasks, 1)
	assert.Equal(t, "STOPPED", aws.ToString(out.Tasks[0].LastStatus))
	assert.Equal(t, ecstypes.TaskStopCodeTaskFailedToStart, out.Tasks[0].StopCode)
	assert.Contains(t, aws.ToString(out.Tasks[0].StoppedReason), "ResourceInitializationError")
}
