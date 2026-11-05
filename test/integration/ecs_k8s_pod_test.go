package integration_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	pkglogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

const (
	ecsK8sBudget = 12 * time.Minute
	ecsK8sNginx  = "public.ecr.aws/docker/library/nginx:1.27-alpine"
	ecsK8sBusy   = "public.ecr.aws/docker/library/busybox:1.36"
)

const ecsK8sProbeScript = `for i in $(seq 1 60); do
  wget -q -O- http://127.0.0.1:80/ && echo "probe-ok $GREETING" && exit 0
  sleep 1
done
exit 3`

type ecsK8sLogs struct {
	backend *cwlogsbackend.InMemoryBackend
}

func (l ecsK8sLogs) EnsureLogGroupAndStream(group, stream string) error {
	if _, err := l.backend.CreateLogGroup(context.Background(), group, "", ""); err != nil &&
		!errors.Is(err, cwlogsbackend.ErrLogGroupAlreadyExists) {
		return err
	}

	if _, err := l.backend.CreateLogStream(context.Background(), group, stream); err != nil &&
		!errors.Is(err, cwlogsbackend.ErrLogStreamAlreadyExist) {
		return err
	}

	return nil
}

func (l ecsK8sLogs) PutLogLines(group, stream string, messages []string) error {
	events := make([]cwlogsbackend.InputLogEvent, len(messages))
	now := time.Now().UnixMilli()

	for i, m := range messages {
		events[i] = cwlogsbackend.InputLogEvent{Message: m, Timestamp: now}
	}

	_, err := l.backend.PutLogEvents(context.Background(), group, stream, "", events)

	return err
}

// TestECS_KubernetesPodTasks runs gopherstack in-process against a k3s cluster and runs ECS tasks through the SDK:
// a two-container task (nginx plus a busybox sidecar that fetches it over the shared Pod network) and a task whose
// essential container exits. It checks the lifecycle, exit codes, stop codes, CloudWatch Logs and pods/exec.
func TestECS_KubernetesPodTasks(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	if testing.Short() {
		t.Skip("k3s ECS pod test skipped in -short")
	}

	ctx, cancel := context.WithTimeout(t.Context(), ecsK8sBudget)
	defer cancel()

	cluster := startK3sPods(ctx, t, k3sPodsConfig{
		prefix: "k8secstest", portLo: 41400, portHi: 41500, images: []string{ecsK8sNginx, ecsK8sBusy},
	})

	t.Setenv("CONTAINER_RUNTIME", "kubernetes")
	t.Setenv("GOPHERSTACK_ECS_RUNTIME", "docker")
	t.Setenv(container.EnvK8sKubeconfig, cluster.kubeconfig)
	t.Setenv(container.EnvK8sNamespace, k8sPodNS)

	provider := &ecsbackend.Provider{}
	reg, err := provider.Init(&service.AppContext{Logger: slog.Default(), JanitorCtx: ctx})
	require.NoError(t, err)

	handler, ok := reg.(*ecsbackend.Handler)
	require.True(t, ok)

	backend, ok := handler.Backend.(*ecsbackend.InMemoryBackend)
	require.True(t, ok)

	t.Cleanup(backend.Close)

	logs := cwlogsbackend.NewInMemoryBackend()
	backend.SetCWLogsBackend(ecsK8sLogs{backend: logs})

	e := echo.New()
	e.Pre(pkglogger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := ecs.NewFromConfig(cfg, func(o *ecs.Options) { o.BaseEndpoint = aws.String(server.URL) })

	_, err = client.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("k8s")})
	require.NoError(t, err)

	awslogs := func(prefix string) *ecstypes.LogConfiguration {
		return &ecstypes.LogConfiguration{
			LogDriver: ecstypes.LogDriverAwslogs,
			Options:   map[string]string{"awslogs-group": "/ecs/k8s", "awslogs-stream-prefix": prefix},
		}
	}

	register := func(family string, defs ...ecstypes.ContainerDefinition) string {
		out, regErr := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
			Family: aws.String(family), NetworkMode: ecstypes.NetworkModeAwsvpc,
			RequiresCompatibilities: []ecstypes.Compatibility{ecstypes.CompatibilityFargate},
			Cpu:                     aws.String("256"), Memory: aws.String("512"),
			Volumes:              []ecstypes.Volume{{Name: aws.String("scratch")}},
			ContainerDefinitions: defs,
		})
		require.NoError(t, regErr)

		return aws.ToString(out.TaskDefinition.TaskDefinitionArn)
	}

	run := func(td string) string {
		out, runErr := client.RunTask(ctx, &ecs.RunTaskInput{
			Cluster: aws.String("k8s"), TaskDefinition: aws.String(td), LaunchType: ecstypes.LaunchTypeFargate,
			NetworkConfiguration: &ecstypes.NetworkConfiguration{AwsvpcConfiguration: &ecstypes.AwsVpcConfiguration{
				Subnets: []string{"subnet-1"},
			}},
		})
		require.NoError(t, runErr)
		require.Len(t, out.Tasks, 1)

		return aws.ToString(out.Tasks[0].TaskArn)
	}

	describe := func(arn string) ecstypes.Task {
		out, descErr := client.DescribeTasks(ctx, &ecs.DescribeTasksInput{
			Cluster: aws.String("k8s"), Tasks: []string{arn},
		})
		require.NoError(t, descErr)
		require.Len(t, out.Tasks, 1)

		return out.Tasks[0]
	}

	containers := func(task ecstypes.Task) map[string]ecstypes.Container {
		m := map[string]ecstypes.Container{}
		for _, c := range task.Containers {
			m[aws.ToString(c.Name)] = c
		}

		return m
	}

	podCount := func() int {
		l, listErr := cluster.cs.CoreV1().
			Pods(k8sPodNS).
			List(ctx, metav1.ListOptions{LabelSelector: container.LabelManaged + "=true"})
		require.NoError(t, listErr)

		return len(l.Items)
	}

	twoContainers := register("k8s-web",
		ecstypes.ContainerDefinition{
			Name:         aws.String("web"),
			Image:        aws.String(ecsK8sNginx),
			Essential:    aws.Bool(true),
			PortMappings: []ecstypes.PortMapping{{ContainerPort: aws.Int32(80)}},
			MountPoints: []ecstypes.MountPoint{
				{SourceVolume: aws.String("scratch"), ContainerPath: aws.String("/scratch")},
			},
			StopTimeout:      aws.Int32(2),
			LogConfiguration: awslogs("web"),
			Environment:      []ecstypes.KeyValuePair{{Name: aws.String("GREETING"), Value: aws.String("hello-pod")}},
		},
		ecstypes.ContainerDefinition{
			Name:      aws.String("probe"),
			Image:     aws.String(ecsK8sBusy),
			Essential: aws.Bool(false),
			Command: []string{
				"sh",
				"-c",
				ecsK8sProbeScript,
			},
			Environment:      []ecstypes.KeyValuePair{{Name: aws.String("GREETING"), Value: aws.String("hello-pod")}},
			LogConfiguration: awslogs("probe"),
		},
	)

	arn := run(twoContainers)

	require.Eventually(t, func() bool {
		return aws.ToString(describe(arn).LastStatus) == "RUNNING"
	}, 5*time.Minute, time.Second, "the task reaches RUNNING once its Pod runs")

	assert.Equal(t, 1, podCount(), "one Pod per task")

	require.Eventually(t, func() bool {
		c := containers(describe(arn))["probe"]

		return c.ExitCode != nil && aws.ToString(c.LastStatus) == "STOPPED"
	}, 3*time.Minute, time.Second, "the sidecar exits on its own")

	got := describe(arn)
	assert.Equal(t, int32(0), aws.ToInt32(containers(got)["probe"].ExitCode))
	assert.Equal(t, "RUNNING", aws.ToString(got.LastStatus), "a non-essential exit leaves the task running")
	assert.Equal(t, "RUNNING", aws.ToString(containers(got)["web"].LastStatus))

	require.Eventually(t, func() bool {
		evs, _, _, getErr := logs.GetLogEvents(
			ctx,
			"/ecs/k8s",
			"probe/probe/"+arn[strings.LastIndex(arn, "/")+1:],
			nil,
			nil,
			100,
			"",
			true,
		)
		if getErr != nil {
			return false
		}

		for _, ev := range evs {
			if strings.Contains(ev.Message, "probe-ok hello-pod") {
				return true
			}
		}

		return false
	}, time.Minute, time.Second, "the sidecar reached nginx over the Pod network and its logs are in CloudWatch Logs")

	pods, err := cluster.cs.CoreV1().Pods(k8sPodNS).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, pods.Items, 1)

	rt, err := container.NewRuntime(container.Config{
		Runtime: container.RuntimeKubernetes, Kubernetes: &container.KubernetesConfig{Kubeconfig: cluster.kubeconfig},
	})
	require.NoError(t, err)

	k, isK8s := rt.(*container.KubernetesRuntime)
	require.True(t, isK8s)

	shell, err := k.ExecInteractive(ctx, pods.Items[0].Name, "web", []string{"sh", "-c", "echo exec-$GREETING"}, 80, 24)
	require.NoError(t, err)

	execOut, _ := io.ReadAll(shell)
	_ = shell.Close()

	assert.Contains(t, string(execOut), "exec-hello-pod", "pods/exec runs in the named container")

	_, err = client.StopTask(
		ctx,
		&ecs.StopTaskInput{Cluster: aws.String("k8s"), Task: aws.String(arn), Reason: aws.String("done")},
	)
	require.NoError(t, err)

	stopped := describe(arn)
	assert.Equal(t, "STOPPED", aws.ToString(stopped.LastStatus))
	assert.Equal(t, ecstypes.TaskStopCodeUserInitiated, stopped.StopCode)
	require.Eventually(t, func() bool { return podCount() == 0 }, time.Minute, time.Second, "StopTask removes the Pod")

	essential := register("k8s-essential",
		ecstypes.ContainerDefinition{
			Name: aws.String("main"), Image: aws.String(ecsK8sBusy), Essential: aws.Bool(true),
			Command: []string{"sh", "-c", "echo bye; exit 7"},
		},
		ecstypes.ContainerDefinition{
			Name: aws.String("web"), Image: aws.String(ecsK8sNginx), Essential: aws.Bool(false),
		},
	)

	arn2 := run(essential)

	require.Eventually(t, func() bool {
		return aws.ToString(describe(arn2).LastStatus) == "STOPPED"
	}, 5*time.Minute, time.Second, "an essential container exiting stops the task")

	done := describe(arn2)
	assert.Equal(t, ecstypes.TaskStopCodeEssentialContainerExited, done.StopCode)
	require.NotNil(t, containers(done)["main"].ExitCode)
	assert.Equal(t, int32(7), aws.ToInt32(containers(done)["main"].ExitCode))
	require.Eventually(
		t,
		func() bool { return podCount() == 0 },
		time.Minute,
		time.Second,
		"the stopped task's Pod is removed",
	)
}
