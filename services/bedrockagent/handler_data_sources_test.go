package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerDataSourceAndIngestion(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	kbBody := map[string]any{
		"name":                       "ingestion-kb",
		"roleArn":                    "arn:aws:iam::123456789012:role/KBRole",
		"knowledgeBaseConfiguration": map[string]any{"type": "VECTOR"},
		"storageConfiguration":       map[string]any{"type": "OPENSEARCH_SERVERLESS"},
	}

	kbRec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", kbBody)
	if kbRec.Code != http.StatusOK {
		t.Fatalf("create kb: %d", kbRec.Code)
	}

	var kbResp map[string]map[string]any
	_ = json.Unmarshal(kbRec.Body.Bytes(), &kbResp)
	kbID := kbResp["knowledgeBase"]["knowledgeBaseId"].(string)

	dsBody := map[string]any{
		"name":                    "test-ds",
		"dataSourceConfiguration": map[string]any{"type": "S3"},
	}

	dsRec := doRequest(t, h, e, http.MethodPut, "/knowledgebases/"+kbID+"/datasources", dsBody)
	if dsRec.Code != http.StatusOK {
		t.Fatalf("create ds: %d %s", dsRec.Code, dsRec.Body.String())
	}

	var dsResp map[string]map[string]any
	_ = json.Unmarshal(dsRec.Body.Bytes(), &dsResp)
	dsID := dsResp["dataSource"]["dataSourceId"].(string)

	t.Run("start ingestion job", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, e, http.MethodPut,
			"/knowledgebases/"+kbID+"/datasources/"+dsID+"/ingestionjobs", nil)
		if rec.Code != http.StatusAccepted {
			t.Errorf("got %d want 202: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("list ingestion jobs", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, e, http.MethodGet,
			"/knowledgebases/"+kbID+"/datasources/"+dsID+"/ingestionjobs", nil)
		if rec.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec.Code)
		}
	})
}
