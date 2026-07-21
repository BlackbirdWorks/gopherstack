package service_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestRegistry_Setters(t *testing.T) {
	t.Parallel()

	mockRec := &mockRecorder{}

	tests := []struct {
		setup    func(*service.Registry)
		validate func(*testing.T, *service.Registry)
		name     string
	}{
		{
			func(r *service.Registry) { r.SetLatencyMs(100) },
			func(t *testing.T, r *service.Registry) { t.Helper(); assert.Equal(t, 100, r.LatencyMs()) },
			"LatencyMs",
		},
		{
			func(r *service.Registry) { r.SetCloudTrailRecorder(mockRec) },
			func(t *testing.T, r *service.Registry) { t.Helper(); assert.Equal(t, mockRec, r.CloudTrailRecorder()) },
			"CloudTrailRecorder",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := service.NewRegistry()
			assert.NotNil(t, r)

			tt.setup(r)
			tt.validate(t, r)
		})
	}
}
