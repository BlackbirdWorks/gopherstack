package ecs

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	cmRegistryA = "arn:aws:servicediscovery:us-east-1:123456789012:service/srv-a"
	cmRegistryB = "arn:aws:servicediscovery:us-east-1:123456789012:service/srv-b"
)

var errCMUnknown = errors.New("unknown registry")

type fakeCloudMap struct {
	instances map[string]map[string]string
	mu        sync.Mutex
}

func newFakeCloudMap() *fakeCloudMap { return &fakeCloudMap{instances: map[string]map[string]string{}} }

func (f *fakeCloudMap) ValidateRegistry(_ context.Context, arn string) error {
	if arn == cmRegistryA || arn == cmRegistryB {
		return nil
	}

	return errCMUnknown
}

func (f *fakeCloudMap) RegisterInstance(_ context.Context, arn, id string, attrs map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.instances[arn+"|"+id] = attrs

	return nil
}

func (f *fakeCloudMap) DeregisterInstance(_ context.Context, arn, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.instances, arn+"|"+id)

	return nil
}

func (f *fakeCloudMap) count(arn string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	n := 0

	for k := range f.instances {
		if len(k) > len(arn) && k[:len(arn)] == arn {
			n++
		}
	}

	return n
}

func newCloudMapService(t *testing.T, b *InMemoryBackend, regs []ServiceRegistry) fargateLBService {
	t.Helper()

	const cluster = "cl-cloudmap"

	_, err := b.CreateCluster(CreateClusterInput{ClusterName: cluster})
	require.NoError(t, err)

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "cm-fam", RequiresCompatibilities: []string{"FARGATE"}, NetworkMode: networkModeAwsvpc,
		CPU: "256", Memory: "512", ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	_, err = b.CreateService(CreateServiceInput{
		Cluster: cluster, ServiceName: "web", TaskDefinition: td.TaskDefinitionArn,
		LaunchType: launchTypeFargate, ServiceRegistries: regs,
	})
	require.NoError(t, err)

	return fargateLBService{cluster: cluster, service: "web", tdArn: td.TaskDefinitionArn}
}

func TestCloudMap_CreateServiceValidatesRegistry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arn     string
		wantErr bool
	}{
		{name: "known", arn: cmRegistryA},
		{name: "unknown", arn: "arn:aws:servicediscovery:us-east-1:123456789012:service/srv-zzz", wantErr: true},
		{name: "empty arn", arn: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
			b.SetCloudMapRegistrar(newFakeCloudMap())

			_, err := b.CreateCluster(CreateClusterInput{ClusterName: "c"})
			require.NoError(t, err)

			td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family: "f", ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
			})
			require.NoError(t, err)

			_, err = b.CreateService(CreateServiceInput{
				Cluster: "c", ServiceName: "s", TaskDefinition: td.TaskDefinitionArn,
				ServiceRegistries: []ServiceRegistry{{RegistryArn: tt.arn}},
			})

			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCloudMap_TaskLifecycleRegistration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want map[string]string
		name string
		regs []ServiceRegistry
	}{
		{
			name: "container port",
			regs: []ServiceRegistry{{RegistryArn: cmRegistryA, ContainerName: "app", ContainerPort: 8080}},
			want: map[string]string{
				"AWS_INSTANCE_PORT": "8080", "ECS_SERVICE_NAME": "web", "ECS_CLUSTER_NAME": "cl-cloudmap",
				"ECS_TASK_DEFINITION_FAMILY": "cm-fam", "AVAILABILITY_ZONE": "us-east-1a", "REGION": "us-east-1",
			},
		},
		{
			name: "srv port",
			regs: []ServiceRegistry{{RegistryArn: cmRegistryA, Port: 9090}},
			want: map[string]string{"AWS_INSTANCE_PORT": "9090"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
			cm := newFakeCloudMap()
			b.SetCloudMapRegistrar(cm)

			s := newCloudMapService(t, b, tt.regs)

			require.NoError(t, b.StartTaskForService(s.cluster, s.service, s.tdArn))
			require.Equal(t, 1, cm.count(cmRegistryA))

			for k, attrs := range cm.instances {
				assert.Contains(t, k, "|")
				assert.NotEmpty(t, attrs["AWS_INSTANCE_IPV4"])

				for ak, av := range tt.want {
					assert.Equal(t, av, attrs[ak], ak)
				}
			}

			require.NoError(t, b.StopOldestServiceTask(s.cluster, s.service))
			assert.Equal(t, 0, cm.count(cmRegistryA))
		})
	}
}

func TestCloudMap_UpdateMovesAndDeleteDeregisters(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
	cm := newFakeCloudMap()
	b.SetCloudMapRegistrar(cm)

	s := newCloudMapService(t, b, []ServiceRegistry{{RegistryArn: cmRegistryA}})
	require.NoError(t, b.StartTaskForService(s.cluster, s.service, s.tdArn))
	require.Equal(t, 1, cm.count(cmRegistryA))

	_, err := b.UpdateService(UpdateServiceInput{
		Cluster: s.cluster, Service: s.service, ServiceRegistries: []ServiceRegistry{{RegistryArn: cmRegistryB}},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, cm.count(cmRegistryA))
	assert.Equal(t, 1, cm.count(cmRegistryB))

	_, err = b.UpdateService(UpdateServiceInput{
		Cluster: s.cluster, Service: s.service, ServiceRegistries: []ServiceRegistry{{RegistryArn: "arn:bad"}},
	})
	require.ErrorIs(t, err, ErrInvalidParameter)
	assert.Equal(t, 1, cm.count(cmRegistryB))

	_, err = b.DeleteService(s.cluster, s.service, true)
	require.NoError(t, err)
	assert.Equal(t, 0, cm.count(cmRegistryB))
}
