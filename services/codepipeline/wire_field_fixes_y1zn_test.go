package codepipeline_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetPipelineState_EnabledDisabledReasonKeys_RealClient covers
// gopherstack-y1zn. handleGetPipelineState emitted "disabled"/"reason" for a
// stage's inbound/outbound transition state; types.TransitionState
// (codepipeline@v1.49.4 deserializers.go's
// awsAwsjson11_deserializeDocumentTransitionState) declares "enabled"
// (inverted polarity from "disabled") and "disabledReason" (not "reason").
func TestGetPipelineState_EnabledDisabledReasonKeys_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("y1zn-trans-pipeline"), nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "DisableStageTransition", map[string]any{
		"pipelineName":   "y1zn-trans-pipeline",
		"stageName":      "Source",
		"transitionType": "Inbound",
		"reason":         "y1zn testing",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec2 := doRequest(t, h, "GetPipelineState", map[string]any{"name": "y1zn-trans-pipeline"})
	require.Equal(t, http.StatusOK, rec2.Code, rec2.Body.String())

	body := rec2.Body.String()
	assert.NotContains(t, body, `"disabled"`,
		"types.TransitionState has no disabled member; the real member is enabled (inverted)")
	assert.NotContains(t, body, `"reason"`,
		"types.TransitionState has no reason member; the real member is disabledReason")
	assert.Contains(t, body, `"enabled":false`,
		"a disabled transition must decode as enabled=false on a real client")
	assert.Contains(t, body, `"disabledReason"`,
		"types.TransitionState's real member is disabledReason")
}
