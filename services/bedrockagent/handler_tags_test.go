package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerTagging(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createBody := map[string]any{
		"agentName":            "tagging-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	}

	rec := doRequest(t, h, e, http.MethodPut, "/agents", createBody)

	var createResp map[string]map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	arn := createResp["agent"]["agentArn"].(string)

	t.Run("tag resource", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodPost, "/tags/"+arn, map[string]any{
			"tags": map[string]string{"env": "test"},
		})
		if rec2.Code != http.StatusNoContent {
			t.Errorf("tag: got %d want 204", rec2.Code)
		}
	})

	t.Run("list tags", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodGet, "/tags/"+arn, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("list tags: got %d want 200", rec2.Code)
		}
	})
}
