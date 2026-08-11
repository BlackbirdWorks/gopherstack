package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerFlowCRUD(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createBody := map[string]any{
		"name":             "test-flow",
		"executionRoleArn": "arn:aws:iam::123456789012:role/FlowRole",
		"definition": map[string]any{
			"nodes":       []any{},
			"connections": []any{},
		},
	}

	rec := doRequest(t, h, e, http.MethodPost, "/flows", createBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create flow: %d %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	flowID, _ := createResp["id"].(string)

	if flowID == "" {
		t.Fatal("no id in flow response")
	}

	t.Run("get flow", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPost, "/flows", createBody)

		var resp map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["id"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodGet, "/flows/"+id, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("list flows", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodGet, "/flows", nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("prepare flow", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPost, "/flows", createBody)

		var resp map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["id"].(string)

		// Real PrepareFlow POSTs to "/flows/{flowIdentifier}/" -- no
		// "/prepare" suffix (botocore bedrock-agent 2023-06-05).
		rec2 := doRequest(t, h2, e2, http.MethodPost, "/flows/"+id, nil)
		if rec2.Code != http.StatusAccepted {
			t.Errorf("got %d want 202", rec2.Code)
		}
	})
}
