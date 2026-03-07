package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPortMappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mappings []PortMapping
		wantHost int
	}{
		{
			name:     "empty mappings",
			mappings: nil,
			wantHost: 0,
		},
		{
			name: "with host port",
			mappings: []PortMapping{
				{ContainerPort: 80, HostPort: 8080, Protocol: "tcp"},
			},
			wantHost: 1,
		},
		{
			name: "default protocol is tcp",
			mappings: []PortMapping{
				{ContainerPort: 443, HostPort: 443},
			},
			wantHost: 1,
		},
		{
			name: "no host port means no binding",
			mappings: []PortMapping{
				{ContainerPort: 8080, Protocol: "tcp"},
			},
			wantHost: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			portBindings, exposedPorts := buildPortMappings(tt.mappings)
			assert.Len(t, portBindings, tt.wantHost)
			assert.Len(t, exposedPorts, len(tt.mappings))
		})
	}
}

func TestBuildEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kvs  []KeyValuePair
		want []string
	}{
		{
			name: "empty",
			kvs:  nil,
			want: []string{},
		},
		{
			name: "single pair",
			kvs:  []KeyValuePair{{Name: "FOO", Value: "bar"}},
			want: []string{"FOO=bar"},
		},
		{
			name: "multiple pairs",
			kvs: []KeyValuePair{
				{Name: "A", Value: "1"},
				{Name: "B", Value: "2"},
			},
			want: []string{"A=1", "B=2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := buildEnv(tt.kvs)
			require.Len(t, got, len(tt.want))

			for i, expected := range tt.want {
				assert.Equal(t, expected, got[i])
			}
		})
	}
}

func TestClusterKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{
			input: "my-cluster",
			want:  "my-cluster",
		},
		{
			input: "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster",
			want:  "my-cluster",
		},
		{
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, clusterKey(tt.input))
		})
	}
}

func TestServiceKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{
			input: "my-service",
			want:  "my-service",
		},
		{
			input: "arn:aws:ecs:us-east-1:000000000000:service/cluster/my-service",
			want:  "my-service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, serviceKey(tt.input))
		})
	}
}
