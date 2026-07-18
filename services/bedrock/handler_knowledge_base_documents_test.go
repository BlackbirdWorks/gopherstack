package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKBDocumentsCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	kbID, dsID := createKBAndDS(t, h)
	docPath := fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kbID, dsID)

	// Ingest documents
	rec := doAgentRequest(t, h, http.MethodPost, docPath, map[string]any{
		"documentIds": []string{"doc-1", "doc-2"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var ib map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ib))
	assert.Len(t, ib["documents"], 2)

	// List documents
	rec = doAgentRequest(t, h, http.MethodGet, docPath, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["documentDetails"], 2)

	// Update documents
	rec = doAgentRequest(t, h, http.MethodPut, docPath, map[string]any{
		"documentIds": []string{"doc-1", "doc-3"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete documents
	rec = doAgentRequest(t, h, http.MethodDelete, docPath, map[string]any{
		"documentIds": []string{"doc-1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAccuracy_KBDocuments_IngestWithBDAParsingStrategy(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("bda-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	// Create data source with BDA parsing
	dsRec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources", kb.KnowledgeBaseID),
		map[string]any{
			"name": "bda-source",
			"vectorIngestionConfiguration": map[string]any{
				"parsingConfiguration": map[string]any{
					"parsingStrategy": "BEDROCK_DATA_AUTOMATION",
					"bedrockDataAutomationConfiguration": map[string]any{
						"parsingModality": "MULTIMODAL",
					},
				},
			},
		},
	)
	require.Equal(t, http.StatusOK, dsRec.Code)

	var dsBody map[string]any
	require.NoError(t, json.Unmarshal(dsRec.Body.Bytes(), &dsBody))
	dsID := dsBody["dataSource"].(map[string]any)["dataSourceId"].(string)

	// Ingest documents
	ingestRec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kb.KnowledgeBaseID, dsID),
		map[string]any{"documentIds": []string{"doc-1", "doc-2", "doc-3"}},
	)
	require.Equal(t, http.StatusOK, ingestRec.Code)

	var ingestBody map[string]any
	require.NoError(t, json.Unmarshal(ingestRec.Body.Bytes(), &ingestBody))
	docs := ingestBody["documents"].([]any)
	assert.Len(t, docs, 3)

	// Verify documents are active
	for _, doc := range docs {
		d := doc.(map[string]any)
		assert.Equal(t, "ACTIVE", d["status"])
	}
}

func TestAccuracy_KBDocuments_GetSpecificDocuments(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	// Ingest 5 documents
	ingestRec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kbID, dsID),
		map[string]any{"documentIds": []string{"d1", "d2", "d3", "d4", "d5"}},
	)
	require.Equal(t, http.StatusOK, ingestRec.Code)

	// Get specific 2
	getDocRec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents/getDocuments", kbID, dsID),
		map[string]any{"documentIds": []string{"d2", "d4"}},
	)
	require.Equal(t, http.StatusOK, getDocRec.Code)

	var getBody map[string]any
	require.NoError(t, json.Unmarshal(getDocRec.Body.Bytes(), &getBody))
	docs := getBody["documentDetails"].([]any)
	assert.Len(t, docs, 2)
}
