package ecs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestHandlerClusterCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		want   int
	}{
		{name: "home region", region: "us-east-1", want: 2},
		{name: "other region", region: "eu-west-1", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend("123456789012", "us-east-1", ecs.NewNoopRunner())
			h := ecs.NewHandler(b)

			for _, name := range []string{"a", "b"} {
				_, err := b.CreateCluster(ecs.CreateClusterInput{ClusterName: name})
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, h.ClusterCount(tt.region))
		})
	}
}
