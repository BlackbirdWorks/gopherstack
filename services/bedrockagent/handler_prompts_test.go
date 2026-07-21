package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerPromptCRUD(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createBody := map[string]any{
		"name":           "test-prompt",
		"defaultVariant": "v1",
		"variants": []any{
			map[string]any{
				"name":         "v1",
				"templateType": "TEXT",
			},
		},
	}

	rec := doRequest(t, h, e, http.MethodPost, "/prompts", createBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create prompt: %d %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	promptID, _ := createResp["id"].(string)

	if promptID == "" {
		t.Fatal("no id in prompt response")
	}

	t.Run("get prompt", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPost, "/prompts", createBody)

		var resp map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["id"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodGet, "/prompts/"+id, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("list prompts", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodGet, "/prompts", nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("create prompt version", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPost, "/prompts", createBody)

		var resp map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["id"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodPost, "/prompts/"+id+"/versions", map[string]any{
			"description": "v1",
		})
		if rec2.Code != http.StatusCreated {
			t.Errorf("got %d want 201: %s", rec2.Code, rec2.Body.String())
		}
	})
}
