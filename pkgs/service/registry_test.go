package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegistry_Setters(t *testing.T) {
	t.Parallel()

	mockRec := &mockRecorder{}

	type args struct {
		recorder  *mockRecorder
		latencyMs int
	}
	type wants struct {
		recorder  *mockRecorder
		latencyMs int
	}

	tests := []struct {
		name  string
		wants wants
		args  args
	}{
		{
			name: "set latency",
			args: args{
				latencyMs: 100,
			},
			wants: wants{
				latencyMs: 100,
			},
		},
		{
			name: "set recorder",
			args: args{
				recorder: mockRec,
			},
			wants: wants{
				recorder: mockRec,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := NewRegistry()
			assert.NotNil(t, r)

			if tc.args.latencyMs != 0 {
				r.SetLatencyMs(tc.args.latencyMs)
			}
			if tc.args.recorder != nil {
				r.SetCloudTrailRecorder(tc.args.recorder)
			}

			if tc.wants.latencyMs != 0 {
				assert.Equal(t, tc.wants.latencyMs, r.latencyMs)
			}
			if tc.wants.recorder != nil {
				assert.Equal(t, tc.wants.recorder, r.cloudTrailRecorder)
			}
		})
	}
}
