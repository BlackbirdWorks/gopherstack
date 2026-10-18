package kafkaconnect_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/kafka"
	"github.com/blackbirdworks/gopherstack/services/kafkaconnect"
)

// bridgeHost is reachable from containers on the default docker bridge and from the host.
const bridgeHost = "172.17.0.1"

func TestWorkerEngine_RealConnectDocker(t *testing.T) {
	t.Parallel()

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	pctx, pcancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer pcancel()

	if err = rt.Ping(pctx); err != nil {
		t.Skipf("container runtime not reachable: %v", err)
	}

	for _, img := range []string{kafka.BrokerImage, kafkaconnect.WorkerImage} {
		if ok, herr := rt.HasImage(pctx, img); herr != nil || !ok {
			t.Skipf("image %s not present", img)
		}
	}

	msk := kafka.NewInMemoryBackend("123456789012", "us-east-1")
	msk.EnableBrokers(kafka.BrokerConfig{Runtime: rt, Host: bridgeHost, StartTimeout: 3 * time.Minute})
	t.Cleanup(msk.Close)

	cluster, err := msk.CreateCluster(t.Context(), "conn-e2e", "3.9.1", 1, kafka.BrokerNodeGroupInfo{}, nil, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, derr := msk.DescribeCluster(t.Context(), cluster.ClusterArn)

		return derr == nil && got.State == kafka.ClusterStateActive
	}, 4*time.Minute, 500*time.Millisecond)

	servers, _ := msk.ManagedBootstrap(cluster.ClusterArn)
	require.Len(t, servers, 1)

	producer, err := kgo.NewClient(kgo.SeedBrokers(servers[0]), kgo.AllowAutoTopicCreation())
	require.NoError(t, err)

	defer producer.Close()

	require.NoError(
		t,
		producer.ProduceSync(t.Context(), &kgo.Record{Topic: "sink-topic", Value: []byte("hello-connect")}).FirstErr(),
	)

	cb := kafkaconnect.NewInMemoryBackend()
	cb.EnableWorkers(kafkaconnect.WorkerConfig{Runtime: rt, StartTimeout: 4 * time.Minute})
	t.Cleanup(cb.Close)

	c, err := cb.CreateConnector("123456789012", "us-east-1", kafkaconnect.ConnectorSpec{
		Name: "sink",
		ConnectorConfiguration: map[string]string{
			"connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
			"tasks.max":       "1",
			"topics":          "sink-topic",
			"file":            "/tmp/out.txt",
			"value.converter": "org.apache.kafka.connect.storage.StringConverter",
		},
		ServiceExecutionRoleArn: "arn:aws:iam::123456789012:role/r",
		Capacity: kafkaconnect.Capacity{
			Provisioned: &kafkaconnect.ProvisionedCapacity{McuCount: 1, WorkerCount: 1},
		},
		ApacheKafkaCluster: kafkaconnect.ApacheKafkaCluster{BootstrapServers: servers[0]},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, derr := cb.DescribeConnector(c.ARN)

		return derr == nil && got.State != "CREATING"
	}, 5*time.Minute, time.Second)

	got, err := cb.DescribeConnector(c.ARN)
	require.NoError(t, err)

	if desc := cb.ConnectorStateDescription(got.ARN); desc != nil {
		t.Logf("state description: %s", desc.Message)
	}

	require.Equal(t, "RUNNING", got.State)

	require.Eventually(t, func() bool {
		ids, _ := exec.CommandContext(t.Context(), "docker", "ps", "-q", "--filter", "name=gopherstack-mskc-sink").
			Output()

		id := strings.TrimSpace(string(ids))
		if id == "" {
			return false
		}

		out, _ := exec.CommandContext(t.Context(), "docker", "exec", id, "cat", "/tmp/out.txt").Output()

		return strings.Contains(string(out), "hello-connect")
	}, 2*time.Minute, time.Second)

	_, err = cb.DeleteConnector(c.ARN, "")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		ids, _ := exec.CommandContext(t.Context(), "docker", "ps", "-aq", "--filter", "name=gopherstack-mskc-sink").
			Output()

		return strings.TrimSpace(string(ids)) == ""
	}, time.Minute, time.Second)
}
