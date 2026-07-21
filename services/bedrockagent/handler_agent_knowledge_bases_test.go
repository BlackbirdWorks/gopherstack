package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerAgentKnowledgeBases(t *testing.T) {
	t.Parallel()

	type tc struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}

	h, e := setupHandler(t)

	// Create an agent first
	agentBody := map[string]any{
		"agentName":            "kb-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	}
	rAgent := doRequest(t, h, e, http.MethodPut, "/agents", agentBody)
	var agentResp map[string]map[string]any
	_ = json.Unmarshal(rAgent.Body.Bytes(), &agentResp)
	agentID := agentResp["agent"]["agentId"].(string)

	// Create a knowledge base
	kbBody := map[string]any{
		"name":    "test-kb",
		"roleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		"knowledgeBaseConfiguration": map[string]any{
			"type": "VECTOR",
			"vectorKnowledgeBaseConfiguration": map[string]any{
				"embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1",
			},
		},
		"storageConfiguration": map[string]any{
			"type": "OPENSEARCH_SERVERLESS",
			"opensearchServerlessConfiguration": map[string]any{
				"collectionArn":   "arn:aws:aoss:us-east-1:123456789012:collection/xyz123",
				"vectorIndexName": "bedrock-knowledge-base-default-index",
				"fieldMapping": map[string]any{
					"vectorField":   "bedrock-knowledge-base-default-vector",
					"textField":     "AMAZON_BEDROCK_TEXT_CHUNK",
					"metadataField": "AMAZON_BEDROCK_METADATA",
				},
			},
		},
	}
	rKB := doRequest(t, h, e, http.MethodPut, "/knowledgebases", kbBody)
	var kbsResp map[string]map[string]any
	_ = json.Unmarshal(rKB.Body.Bytes(), &kbsResp)
	kbID := kbsResp["knowledgeBase"]["knowledgeBaseId"].(string)

	basePath := "/agents/" + agentID + "/agentversions/DRAFT/knowledgebases"

	assocBody := map[string]any{
		"knowledgeBaseId":    kbID,
		"description":        "Test KB association",
		"knowledgeBaseState": "ENABLED",
	}

	rAssoc := doRequest(t, h, e, http.MethodPut, basePath, assocBody)
	var assocResp map[string]map[string]any
	_ = json.Unmarshal(rAssoc.Body.Bytes(), &assocResp)

	cases := []tc{
		{
			name:       "ListAgentKBs",
			method:     http.MethodGet,
			path:       basePath,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetAgentKB",
			method:     http.MethodGet,
			path:       basePath + "/" + kbID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetAgentKB_NotFound",
			method:     http.MethodGet,
			path:       basePath + "/notfound",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "UpdateAgentKB",
			method:     http.MethodPut,
			path:       basePath + "/" + kbID,
			body:       assocBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdateAgentKB_NotFound",
			method:     http.MethodPut,
			path:       basePath + "/notfound",
			body:       assocBody,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DisassociateAgentKB",
			method:     http.MethodDelete,
			path:       basePath + "/" + kbID,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "DisassociateAgentKB_NotFound",
			method:     http.MethodDelete,
			path:       basePath + "/notfound",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hLocal, eLocal := setupHandler(t)
			rA := doRequest(t, hLocal, eLocal, http.MethodPut, "/agents", agentBody)
			var aResp map[string]map[string]any
			_ = json.Unmarshal(rA.Body.Bytes(), &aResp)
			aID := aResp["agent"]["agentId"].(string)

			rK := doRequest(t, hLocal, eLocal, http.MethodPut, "/knowledgebases", kbBody)
			var kResp map[string]map[string]any
			_ = json.Unmarshal(rK.Body.Bytes(), &kResp)
			kID := kResp["knowledgeBase"]["knowledgeBaseId"].(string)

			bP := "/agents/" + aID + "/agentversions/DRAFT/knowledgebases"

			assocLocalBody := map[string]any{
				"knowledgeBaseId":    kID,
				"description":        "Test KB association",
				"knowledgeBaseState": "ENABLED",
			}

			doRequest(t, hLocal, eLocal, http.MethodPut, bP, assocLocalBody)

			path := tt.path
			if kbID != "" && kID != "" {
				switch path {
				case basePath:
					path = bP
				case basePath + "/" + kbID:
					path = bP + "/" + kID
				case basePath + "/notfound":
					path = bP + "/notfound"
				}
			}
			// for update we need to pass the real kb id
			reqBody := tt.body
			if reqBody != nil && reqBody["knowledgeBaseId"] == kbID {
				reqBody["knowledgeBaseId"] = kID
			}

			r := doRequest(t, hLocal, eLocal, tt.method, path, reqBody)
			if r.Code != tt.wantStatus {
				t.Errorf("got %d want %d", r.Code, tt.wantStatus)
			}
		})
	}
}
