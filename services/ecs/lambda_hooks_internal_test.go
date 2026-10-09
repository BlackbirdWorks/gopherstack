package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHookInvocation_TrafficWeights(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTest map[string]int
		wantProd map[string]int
		name     string
		stage    string
	}{
		{map[string]int{}, map[string]int{}, "pre_scale_up", "PRE_SCALE_UP"},
		{map[string]int{"new": 100, "old": 0}, map[string]int{}, "test_shift", stageTestTrafficShift},
		{map[string]int{}, map[string]int{"new": 0, "old": 100}, "pre_production", stagePreProductionShift},
		{map[string]int{}, map[string]int{"new": 100, "old": 0}, "production", stageProductionTrafficShift},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inv := &hookInvocation{rawStage: tt.stage, revisionArn: "new", sourceArns: []string{"old"}}
			test, prod := inv.trafficWeights()

			assert.Equal(t, tt.wantTest, test)
			assert.Equal(t, tt.wantProd, prod)
		})
	}
}
