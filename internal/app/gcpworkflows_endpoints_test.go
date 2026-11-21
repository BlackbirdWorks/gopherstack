package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWorkflowEndpointsBaseURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		eps  workflowEndpoints
		name string
		svc  string
		want string
	}{
		{workflowEndpoints{}, "unwired tasks", "cloudtasks", ""},
		{workflowEndpoints{}, "unknown service", "nope", ""},
		{workflowEndpoints{}, "unwired storage", "storage", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.eps.BaseURL(tt.svc))
		})
	}
}
