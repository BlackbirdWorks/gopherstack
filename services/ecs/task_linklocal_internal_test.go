package ecs

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errAttachFailed = errors.New("attach failed")

type fakeLinkLocal struct {
	err      error
	attached []string
	mu       sync.Mutex
	active   bool
}

func (f *fakeLinkLocal) Active(context.Context) bool { return f.active }

func (f *fakeLinkLocal) Attach(_ context.Context, containerID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}

	f.attached = append(f.attached, containerID)

	return nil
}

func TestLinkLocalTaskEnv(t *testing.T) {
	t.Parallel()

	const taskArn = "arn:aws:ecs:us-west-2:123456789012:task/c/0123abcd"

	tests := []struct {
		wantKeys   map[string]string
		name       string
		wantAbsent []string
		active     bool
	}{
		{
			name: "active", active: true,
			wantKeys: map[string]string{
				"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/_gopherstack/ecs/credentials/0123abcd",
				"ECS_CONTAINER_METADATA_URI_V4":          "http://169.254.170.2/_gopherstack/ecs/metadata/0123abcd/app",
				"AWS_ENDPOINT_URL":                       "http://172.17.0.1:8123",
			},
			wantAbsent: []string{"AWS_CONTAINER_CREDENTIALS_FULL_URI"},
		},
		{
			name: "inactive",
			wantKeys: map[string]string{
				"AWS_CONTAINER_CREDENTIALS_FULL_URI": "http://172.17.0.1:8123/_gopherstack/ecs/credentials/0123abcd",
			},
			wantAbsent: []string{"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-west-2", NewNoopRunner())
			b.SetEndpointURL("http://172.17.0.1:8123")
			b.SetRoleCredentialIssuer(&stubIssuer{})
			b.SetLinkLocal(&fakeLinkLocal{active: tt.active})

			env := envMap(b.containerRuntimeEnv(&Task{TaskArn: taskArn, TaskRoleArn: "arn:aws:iam::1:role/r"}, "app"))

			for k, v := range tt.wantKeys {
				assert.Equal(t, v, env[k], k)
			}

			for _, k := range tt.wantAbsent {
				assert.NotContains(t, env, k)
			}
		})
	}
}

func TestDockerRunnerAttachesLinkLocal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attachErr    error
		name         string
		wantAttached int
		wantRemoved  int
		wantStarted  int
		wantErr      bool
	}{
		{name: "attached", wantAttached: 1, wantStarted: 1},
		{name: "attach_error", attachErr: errAttachFailed, wantRemoved: 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ll := &fakeLinkLocal{active: true, err: tt.attachErr}
			b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
			b.SetLinkLocal(ll)

			fake := &fakeDockerClient{}
			runner := newDockerRunnerWithClient(context.Background(), fake)
			runner.SetContainerAttacher(b.attachLinkLocal)

			task := &Task{TaskArn: "arn:aws:ecs:us-east-1:123456789012:task/c/abc", ClusterArn: "arn:c"}
			td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}}}

			err := runner.RunTask(task, td)
			if tt.wantErr {
				require.ErrorIs(t, err, errAttachFailed)
			} else {
				require.NoError(t, err)
			}

			assert.Len(t, ll.attached, tt.wantAttached)
			assert.Len(t, fake.removed, tt.wantRemoved)
			assert.Len(t, fake.started, tt.wantStarted)
		})
	}
}
