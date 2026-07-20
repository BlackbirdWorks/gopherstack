package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegistry_Setters(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	assert.NotNil(t, r)

	r.SetLatencyMs(100)
	assert.Equal(t, 100, r.latencyMs)

	mockRec := &mockRecorder{}
	r.SetCloudTrailRecorder(mockRec)
	assert.Equal(t, mockRec, r.cloudTrailRecorder)
}
