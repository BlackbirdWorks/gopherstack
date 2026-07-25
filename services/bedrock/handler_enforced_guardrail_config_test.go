package bedrock_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_EnforcedGuardrailConfig_PutListDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// A referenced guardrail must exist -- PutEnforcedGuardrailConfiguration
	// validates guardrailIdentifier against real guardrails.
	gRec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{"name": "egc-guardrail"})
	require.Equal(t, http.StatusOK, gRec.Code)

	var gOut map[string]any
	mustUnmarshal(t, gRec, &gOut)
	guardrailID := gOut["guardrailId"].(string)

	// List empty.
	listRec := doRequest(t, h, http.MethodGet, "/enforcedGuardrailsConfiguration", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	mustUnmarshal(t, listRec, &listOut)
	assert.Empty(t, listOut["guardrailsConfig"])

	// Put.
	putRec := doRequest(t, h, http.MethodPut, "/enforcedGuardrailsConfiguration", map[string]any{
		"guardrailInferenceConfig": map[string]any{
			"guardrailIdentifier": guardrailID,
			"guardrailVersion":    "DRAFT",
			"inputTags":           "HONOR",
			"modelEnforcement": map[string]any{
				"includedModels": []string{"anthropic.claude-v2"},
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	var putOut map[string]any
	mustUnmarshal(t, putRec, &putOut)
	configID, _ := putOut["configId"].(string)
	require.NotEmpty(t, configID)
	assert.NotEmpty(t, putOut["updatedAt"])
	assert.NotEmpty(t, putOut["updatedBy"])

	// List has one, with the full real-shape fields populated.
	listRec2 := doRequest(t, h, http.MethodGet, "/enforcedGuardrailsConfiguration", nil)
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listOut2 map[string]any
	mustUnmarshal(t, listRec2, &listOut2)
	configs := listOut2["guardrailsConfig"].([]any)
	require.Len(t, configs, 1)

	cfg := configs[0].(map[string]any)
	assert.Equal(t, configID, cfg["configId"])
	assert.Equal(t, guardrailID, cfg["guardrailId"])
	assert.Equal(t, "DRAFT", cfg["guardrailVersion"])
	assert.Equal(t, "HONOR", cfg["inputTags"])
	assert.Equal(t, "ACCOUNT", cfg["owner"])
	assert.NotEmpty(t, cfg["guardrailArn"])
	assert.NotEmpty(t, cfg["createdAt"])
	assert.NotEmpty(t, cfg["createdBy"])
	modelEnforcement := cfg["modelEnforcement"].(map[string]any)
	assert.Equal(t, []any{"anthropic.claude-v2"}, modelEnforcement["includedModels"])

	// Delete by ConfigId path param.
	delRec := doRequest(t, h, http.MethodDelete, "/enforcedGuardrailsConfiguration/"+configID, nil)
	assert.Equal(t, http.StatusOK, delRec.Code)

	// List empty again.
	listRec3 := doRequest(t, h, http.MethodGet, "/enforcedGuardrailsConfiguration", nil)
	var listOut3 map[string]any
	mustUnmarshal(t, listRec3, &listOut3)
	assert.Empty(t, listOut3["guardrailsConfig"])
}

func TestHandler_EnforcedGuardrailConfig_PutWithExistingConfigIDUpdatesInPlace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	gRec := doRequest(t, h, http.MethodPost, "/guardrails", map[string]any{"name": "egc-update-guardrail"})
	require.Equal(t, http.StatusOK, gRec.Code)

	var gOut map[string]any
	mustUnmarshal(t, gRec, &gOut)
	guardrailID := gOut["guardrailId"].(string)

	putRec1 := doRequest(t, h, http.MethodPut, "/enforcedGuardrailsConfiguration", map[string]any{
		"guardrailInferenceConfig": map[string]any{
			"guardrailIdentifier": guardrailID,
			"guardrailVersion":    "DRAFT",
			"inputTags":           "HONOR",
		},
	})
	require.Equal(t, http.StatusOK, putRec1.Code)

	var putOut1 map[string]any
	mustUnmarshal(t, putRec1, &putOut1)
	configID := putOut1["configId"].(string)

	// Update the SAME configId with different inputTags -- should not create a
	// second entry.
	putRec2 := doRequest(t, h, http.MethodPut, "/enforcedGuardrailsConfiguration", map[string]any{
		"configId": configID,
		"guardrailInferenceConfig": map[string]any{
			"guardrailIdentifier": guardrailID,
			"guardrailVersion":    "DRAFT",
			"inputTags":           "IGNORE",
		},
	})
	require.Equal(t, http.StatusOK, putRec2.Code)

	var putOut2 map[string]any
	mustUnmarshal(t, putRec2, &putOut2)
	assert.Equal(t, configID, putOut2["configId"])

	listRec := doRequest(t, h, http.MethodGet, "/enforcedGuardrailsConfiguration", nil)
	var listOut map[string]any
	mustUnmarshal(t, listRec, &listOut)
	configs := listOut["guardrailsConfig"].([]any)
	require.Len(t, configs, 1, "same configId PUT should update in place, not duplicate")
	assert.Equal(t, "IGNORE", configs[0].(map[string]any)["inputTags"])
}

func TestHandler_EnforcedGuardrailConfig_PutUnknownGuardrailNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPut, "/enforcedGuardrailsConfiguration", map[string]any{
		"guardrailInferenceConfig": map[string]any{
			"guardrailIdentifier": "no-such-guardrail",
			"guardrailVersion":    "DRAFT",
			"inputTags":           "HONOR",
		},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_EnforcedGuardrailConfig_PutMissingRequiredFieldsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing guardrailInferenceConfig entirely", body: map[string]any{}},
		{
			name: "missing inputTags",
			body: map[string]any{
				"guardrailInferenceConfig": map[string]any{
					"guardrailIdentifier": "g-1",
					"guardrailVersion":    "DRAFT",
				},
			},
		},
		{
			name: "invalid inputTags value",
			body: map[string]any{
				"guardrailInferenceConfig": map[string]any{
					"guardrailIdentifier": "g-1",
					"guardrailVersion":    "DRAFT",
					"inputTags":           "MAYBE",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPut, "/enforcedGuardrailsConfiguration", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_EnforcedGuardrailConfig_DeleteUnknownConfigNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodDelete, "/enforcedGuardrailsConfiguration/no-such-config", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_EnforcedGuardrailConfig_WrongPathRejected locks in that the
// previously-invented "/enforced-guardrail-configuration" (kebab-case) path
// no longer routes -- real AWS uses camelCase "/enforcedGuardrailsConfiguration".
func TestHandler_EnforcedGuardrailConfig_WrongPathRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/enforced-guardrail-configuration", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
