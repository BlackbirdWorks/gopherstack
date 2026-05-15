package ec2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testInstanceIDKey = "InstanceId.1"

// stubCompute records calls and returns canned LaunchResults so the
// handler/backend wiring can be exercised without a real Docker daemon.
type stubCompute struct {
	launchErr      error
	result         LaunchResult
	launchCalls    []LaunchRequest
	terminateCalls []string
	startCalls     []string
	stopCalls      []string
}

func (s *stubCompute) Launch(_ context.Context, req LaunchRequest) (LaunchResult, error) {
	s.launchCalls = append(s.launchCalls, req)

	return s.result, s.launchErr
}

func (s *stubCompute) Terminate(_ context.Context, _, providerID string) error {
	s.terminateCalls = append(s.terminateCalls, providerID)

	return nil
}

func (s *stubCompute) Start(_ context.Context, _, providerID string) error {
	s.startCalls = append(s.startCalls, providerID)

	return nil
}

func (s *stubCompute) Stop(_ context.Context, _, providerID string) error {
	s.stopCalls = append(s.stopCalls, providerID)

	return nil
}

func TestComputeHookLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, b *InMemoryBackend, c *stubCompute)
		assertAfter func(t *testing.T, b *InMemoryBackend, c *stubCompute)
		name        string
	}{
		{
			name: "run_terminate_lifecycle",
			setup: func(t *testing.T, _ *InMemoryBackend, c *stubCompute) {
				t.Helper()
				c.result = LaunchResult{
					ProviderID:      "ctr-abc",
					PrivateIP:       "172.20.0.5",
					PublicIPAddress: "127.0.0.1",
					SSHPort:         32100,
				}
			},
			assertAfter: func(t *testing.T, b *InMemoryBackend, c *stubCompute) {
				t.Helper()

				kp, err := b.CreateKeyPair("demo")
				require.NoError(t, err)
				assert.NotEmpty(t, kp.PublicKey, "CreateKeyPair must derive an OpenSSH public key")

				h := NewHandler(b)
				vals := map[string][]string{
					"ImageId":      {"ami-1"},
					"InstanceType": {"t3.micro"},
					"MinCount":     {"1"},
					"KeyName":      {"demo"},
				}

				resp, err := h.handleRunInstances(vals, "req-1")
				require.NoError(t, err)
				require.NotNil(t, resp)

				assert.Len(t, c.launchCalls, 1)
				assert.Equal(t, "demo", c.launchCalls[0].KeyName)
				assert.Contains(t, c.launchCalls[0].AuthorizedKey, "ssh-rsa")

				// verify the instance reflects the docker-assigned IP/port
				instances := b.DescribeInstances(nil, "")
				require.Len(t, instances, 1)
				assert.Equal(t, "ctr-abc", instances[0].ProviderID)
				assert.Equal(t, "172.20.0.5", instances[0].PrivateIP)
				assert.Equal(t, 32100, instances[0].SSHPort)

				// terminate goes through the compute hook
				_, terr := h.handleTerminateInstances(map[string][]string{
					testInstanceIDKey: {instances[0].ID},
				}, "req-2")
				require.NoError(t, terr)
				assert.Equal(t, []string{"ctr-abc"}, c.terminateCalls)
			},
		},
		{
			name:  "start_stop_dispatch",
			setup: func(_ *testing.T, _ *InMemoryBackend, _ *stubCompute) {},
			assertAfter: func(t *testing.T, b *InMemoryBackend, c *stubCompute) {
				t.Helper()
				c.result = LaunchResult{ProviderID: "ctr-xyz"}

				h := NewHandler(b)
				resp, err := h.handleRunInstances(map[string][]string{
					"ImageId": {"ami-2"}, "MinCount": {"1"},
				}, "req")
				require.NoError(t, err)
				require.NotNil(t, resp)

				instances := b.DescribeInstances(nil, "")
				require.Len(t, instances, 1)
				id := instances[0].ID

				_, err = h.handleStopInstances(map[string][]string{testInstanceIDKey: {id}}, "req")
				require.NoError(t, err)
				assert.Equal(t, []string{"ctr-xyz"}, c.stopCalls)

				_, err = h.handleStartInstances(map[string][]string{testInstanceIDKey: {id}}, "req")
				require.NoError(t, err)
				assert.Equal(t, []string{"ctr-xyz"}, c.startCalls)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			c := &stubCompute{}
			b.WithCompute(c)
			tt.setup(t, b, c)
			tt.assertAfter(t, b, c)
		})
	}
}
