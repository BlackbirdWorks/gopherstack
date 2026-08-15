package bedrock_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentsHandler_KnowledgeBaseLifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create KB.
	rec := doAgentRequest(t, h, http.MethodPut, "/knowledgebases", map[string]any{
		"name":        "my-kb",
		"description": "test kb",
		"roleArn":     "arn:aws:iam::000000000000:role/test",
	})
	assert.Equal(t, http.StatusAccepted, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	kb := createOut["knowledgeBase"].(map[string]any)
	kbID := kb["knowledgeBaseId"].(string)
	assert.NotEmpty(t, kbID)
	assert.Equal(t, "ACTIVE", kb["status"])

	// List KBs.
	rec2 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.Len(t, listOut["knowledgeBaseSummaries"].([]any), 1)

	// Get KB.
	rec3 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/"+kbID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &getOut))
	assert.Equal(t, "my-kb", getOut["knowledgeBase"].(map[string]any)["name"])

	// Update KB.
	rec4 := doAgentRequest(t, h, http.MethodPut, "/knowledgebases/"+kbID, map[string]any{
		"description": "updated description",
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete KB.
	rec5 := doAgentRequest(t, h, http.MethodDelete, "/knowledgebases/"+kbID, nil)
	assert.Equal(t, http.StatusAccepted, rec5.Code)

	// KB should be gone.
	rec6 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/"+kbID, nil)
	assert.Equal(t, http.StatusNotFound, rec6.Code)
}

func TestAgentsHandler_CreateKnowledgeBase_Duplicate(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	_, err := b.CreateKnowledgeBase("dup-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doAgentRequest(t, h, http.MethodPut, "/knowledgebases", map[string]any{
		"name": "dup-kb",
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestAgentsHandler_KnowledgeBase_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	t.Run("get nonexistent", func(t *testing.T) {
		t.Parallel()

		rec := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/nonexistent", nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("update nonexistent", func(t *testing.T) {
		t.Parallel()

		rec := doAgentRequest(t, h, http.MethodPut, "/knowledgebases/nonexistent", map[string]any{
			"name": "test",
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("delete nonexistent", func(t *testing.T) {
		t.Parallel()

		rec := doAgentRequest(t, h, http.MethodDelete, "/knowledgebases/nonexistent", nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestAgentsHandler_KnowledgeBase_InvalidJSON(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("json-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		path   string
		method string
	}{
		{"/knowledgebases", http.MethodPut},
		{"/knowledgebases/" + kb.KnowledgeBaseID, http.MethodPut},
	} {
		e := echo.New()
		req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader([]byte("bad json")))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h.Handler()(c))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}
}

func TestAccuracy_KnowledgeBase_VectorStoreConfigPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kbConfig    map[string]any
		storageConf map[string]any
		name        string
	}{
		{
			name: "VECTOR type with openSearch",
			kbConfig: map[string]any{
				"type": "VECTOR",
				"vectorKnowledgeBaseConfiguration": map[string]any{
					"embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1",
				},
			},
			storageConf: map[string]any{
				"type": "OPENSEARCH_SERVERLESS",
				"opensearchServerlessConfiguration": map[string]any{
					"collectionArn":   "arn:aws:aoss:us-east-1:000000000000:collection/kb-coll",
					"vectorIndexName": "kb-index",
					"fieldMapping": map[string]any{
						"vectorField":   "embedding",
						"textField":     "text",
						"metadataField": "metadata",
					},
				},
			},
		},
		{
			name: "VECTOR type with pinecone",
			kbConfig: map[string]any{
				"type": "VECTOR",
				"vectorKnowledgeBaseConfiguration": map[string]any{
					"embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0",
				},
			},
			storageConf: map[string]any{
				"type": "PINECONE",
				"pineconeConfiguration": map[string]any{
					"connectionString":     "https://kb.svc.pinecone.io",
					"credentialsSecretArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret/pinecone",
					"namespace":            "kb-ns",
					"fieldMapping": map[string]any{
						"textField":     "chunk",
						"metadataField": "meta",
					},
				},
			},
		},
		{
			name: "KENDRA type",
			kbConfig: map[string]any{
				"type": "KENDRA",
				"kendraKnowledgeBaseConfiguration": map[string]any{
					"kendraIndexArn": "arn:aws:kendra:us-east-1:000000000000:index/12345",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestAgentsHandler(t)

			body := map[string]any{
				"name":                       tt.name,
				"roleArn":                    "arn:aws:iam::000000000000:role/kb-role",
				"knowledgeBaseConfiguration": tt.kbConfig,
			}

			if tt.storageConf != nil {
				body["storageConfiguration"] = tt.storageConf
			}

			rec := doAgentRequest(t, h, http.MethodPut, "/knowledgebases", body)
			require.Equal(t, http.StatusAccepted, rec.Code)

			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			kbID := created["knowledgeBase"].(map[string]any)["knowledgeBaseId"].(string)

			getRec := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/"+kbID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &got))
			kb := got["knowledgeBase"].(map[string]any)

			kbCfg := kb["knowledgeBaseConfiguration"].(map[string]any)
			assert.Equal(t, tt.kbConfig["type"], kbCfg["type"])
		})
	}
}
