package rekognition_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// CreateFaceLivenessSession confidence
// =============================================================================

func TestFaceLiveness_ConfidenceRange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create multiple sessions and verify confidence stays in [75, 100)
	for i := range 10 {
		_ = i
		rec := doRequest(t, h, "CreateFaceLivenessSession", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var createResp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
		sessionID := createResp["SessionId"].(string)

		rec = doRequest(t, h, "GetFaceLivenessSessionResults", map[string]any{"SessionId": sessionID})
		require.Equal(t, http.StatusOK, rec.Code)

		var resultResp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resultResp))
		confidence := resultResp["Confidence"].(float64)
		assert.GreaterOrEqual(t, confidence, float64(75), "confidence must be >= 75")
		assert.Less(t, confidence, float64(100), "confidence must be < 100")
	}
}

func TestFaceLiveness_TwoSessionsDifferentConfidence(t *testing.T) { //nolint:paralleltest // stateful
	h := newTestHandler(t)

	getConfidence := func(sessionID string) float64 {
		t.Helper()
		rec := doRequest(t, h, "GetFaceLivenessSessionResults", map[string]any{"SessionId": sessionID})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		return resp["Confidence"].(float64)
	}

	// Create many sessions; at least some should differ in confidence.
	confidences := make(map[float64]bool)
	for range 20 {
		rec := doRequest(t, h, "CreateFaceLivenessSession", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		c := getConfidence(resp["SessionId"].(string))
		confidences[c] = true
	}

	assert.Greater(t, len(confidences), 1, "expected varied confidence values across sessions")
}

func TestFaceLiveness(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateFaceLivenessSession returns session ID",
			action:   "CreateFaceLivenessSession",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["SessionId"])
			},
		},
	}

	var sessionID string

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}

			if tc.action == "CreateFaceLivenessSession" && rec.Code == http.StatusOK {
				var resp map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)
				sessionID = resp["SessionId"].(string)
			}
		})
	}

	t.Run("GetFaceLivenessSessionResults returns result", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "GetFaceLivenessSessionResults", map[string]any{"SessionId": sessionID})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "SUCCEEDED", resp["Status"])
		assert.Equal(t, sessionID, resp["SessionId"])
	})

	t.Run( //nolint:paralleltest // existing issue.
		"GetFaceLivenessSessionResults unknown session returns error",
		func(t *testing.T) {
			rec := doRequest(t, h, "GetFaceLivenessSessionResults", map[string]any{
				"SessionId": "00000000-0000-0000-0000-000000000000",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		},
	)

	t.Run( //nolint:paralleltest // existing issue.
		"GetFaceLivenessSessionResults missing ID returns error",
		func(t *testing.T) {
			rec := doRequest(t, h, "GetFaceLivenessSessionResults", map[string]any{})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		},
	)
}
