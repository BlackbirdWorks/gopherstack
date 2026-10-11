package ec2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDockerLaunchInjectsMetadataEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		wantEnv  []string
	}{
		{name: "with_endpoint", endpoint: "http://172.17.0.1:4242", wantEnv: []string{
			"AWS_EC2_METADATA_SERVICE_ENDPOINT=http://172.17.0.1:4242",
		}},
		{name: "without_endpoint"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &fakeDockerAPI{containerID: "c1", containerIP: "172.17.0.7"}
			dc := newDockerComputeWithAPI(api, DockerComputeConfig{Image: defaultDockerImage})

			_, err := dc.Launch(t.Context(), LaunchRequest{InstanceID: "i-env", MetadataEndpoint: tt.endpoint})
			require.NoError(t, err)

			api.mu.Lock()
			defer api.mu.Unlock()
			require.Len(t, api.created, 1)
			assert.Equal(t, tt.wantEnv, api.created[0].Config.Env)
		})
	}
}
