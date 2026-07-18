package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerKnowledgeBaseCRUD(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createBody := map[string]any{
		"name":    "test-kb",
		"roleArn": "arn:aws:iam::123456789012:role/KBRole",
		"knowledgeBaseConfiguration": map[string]any{
			"type": "VECTOR",
		},
		"storageConfiguration": map[string]any{
			"type": "OPENSEARCH_SERVERLESS",
		},
	}

	rec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", createBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("create kb: %d %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	kbID := createResp["knowledgeBase"]["knowledgeBaseId"].(string)

	t.Run("get kb", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPut, "/knowledgebases", createBody)

		var resp map[string]map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["knowledgeBase"]["knowledgeBaseId"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodGet, "/knowledgebases/"+id, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("list kbs", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodGet, "/knowledgebases", nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("delete kb", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPut, "/knowledgebases", createBody)

		var resp map[string]map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["knowledgeBase"]["knowledgeBaseId"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodDelete, "/knowledgebases/"+id, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	_ = kbID
}
