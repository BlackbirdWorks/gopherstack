package comprehend_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Flywheel iteration field shapes ---

func TestFlywheelIterationFieldShapes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	created := request(t, h, "CreateFlywheel", map[string]any{"FlywheelName": "audit-fw"})
	fwArn := created["FlywheelArn"].(string)

	startResp := request(t, h, "StartFlywheelIteration", map[string]any{"FlywheelArn": fwArn})
	iterID, ok := startResp["FlywheelIterationId"].(string)
	require.True(t, ok, "StartFlywheelIteration must return FlywheelIterationId")
	assert.NotEmpty(t, iterID)

	getResp := request(t, h, "GetFlywheelIteration", map[string]any{"FlywheelIterationId": iterID})
	props, ok := getResp["FlywheelIterationProperties"].(map[string]any)
	require.True(t, ok, "GetFlywheelIteration must return FlywheelIterationProperties")
	assert.NotEmpty(t, props["FlywheelArn"], "iteration properties must have FlywheelArn")
	assert.NotEmpty(t, props["FlywheelIterationId"], "iteration properties must have FlywheelIterationId")
	assert.NotEmpty(t, props["FlywheelIterationStatus"], "iteration properties must have FlywheelIterationStatus")
	assert.NotEmpty(t, props["CreationTime"], "iteration properties must have CreationTime")

	histResp := request(t, h, "ListFlywheelIterationHistory", map[string]any{"FlywheelArn": fwArn})
	hist, ok := histResp["FlywheelIterationPropertiesList"].([]any)
	require.True(t, ok, "ListFlywheelIterationHistory must return FlywheelIterationPropertiesList")
	assert.Len(t, hist, 1)
}
