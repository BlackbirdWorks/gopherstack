package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKBDocumentsCRUD drives IngestKnowledgeBaseDocuments,
// ListKnowledgeBaseDocuments, and DeleteKnowledgeBaseDocuments via their real
// wire shapes: Ingest is PUT to the base .../documents path, List is POST to
// that same base path, and Delete is POST to the .../documents/deleteDocuments
// sub-path. See dispatchDocumentOps (handler_knowledge_base_documents.go) and
// dispatchDataSourceIDRoutes (handler_data_sources.go).
func TestKBDocumentsCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	kbID, dsID := createKBAndDS(t, h)
	docPath := fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kbID, dsID)

	// Ingest documents: real IngestKnowledgeBaseDocuments is PUT.
	rec := doAgentRequest(t, h, http.MethodPut, docPath, map[string]any{
		"documentIds": []string{"doc-1", "doc-2"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var ib map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ib))
	assert.Len(t, ib["documentDetails"], 2)

	// List documents: real ListKnowledgeBaseDocuments is POST to the same
	// base path (not GET).
	rec = doAgentRequest(t, h, http.MethodPost, docPath, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["documentDetails"], 2)

	// Delete documents: real DeleteKnowledgeBaseDocuments is POST to the
	// /deleteDocuments sub-path (not DELETE on the base path).
	rec = doAgentRequest(t, h, http.MethodPost, docPath+"/deleteDocuments", map[string]any{
		"documentIds": []string{"doc-1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doAgentRequest(t, h, http.MethodPost, docPath, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["documentDetails"], 1, "doc-1 should have been deleted, leaving only doc-2")
}

// TestKBDocumentsRealWireRouting locks in the real bedrock-agent wire shapes
// for the two operations sharing the .../documents collection path:
// IngestKnowledgeBaseDocuments is PUT, ListKnowledgeBaseDocuments is POST.
// Verified against the vendored SDK's request snapshots
// (aws-sdk-go-v2/service/bedrockagent's IngestKnowledgeBaseDocuments.request.snap
// and ListKnowledgeBaseDocuments.request.snap). A real SDK client issuing
// either request must reach its own handler, not the other operation's.
func TestKBDocumentsRealWireRouting(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)
	docPath := fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kbID, dsID)

	// A real IngestKnowledgeBaseDocuments call is PUT to the base
	// .../documents path and returns the ingested document's details.
	putRec := doAgentRequest(t, h, http.MethodPut, docPath, map[string]any{
		"documentIds": []string{"wire-doc-1"},
	})
	require.Equal(t, http.StatusOK, putRec.Code,
		"PUT .../documents (real Ingest wire shape) should reach IngestKnowledgeBaseDocuments: %s",
		putRec.Body.String())

	var ingestResp map[string]any

	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &ingestResp))
	require.Contains(t, ingestResp, "documentDetails", "PUT response should carry Ingest's response shape")

	// A real ListKnowledgeBaseDocuments call is POST to the same base
	// .../documents path and must return the previously-ingested document --
	// not be silently treated as an empty Ingest of a payload with no
	// documentIds field.
	postRec := doAgentRequest(t, h, http.MethodPost, docPath, nil)
	require.Equal(t, http.StatusOK, postRec.Code,
		"POST .../documents (real List wire shape) should reach ListKnowledgeBaseDocuments: %s",
		postRec.Body.String())

	var listResp struct {
		DocumentDetails []map[string]any `json:"documentDetails"`
	}

	require.NoError(t, json.Unmarshal(postRec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.DocumentDetails, 1,
		"POST .../documents (List) should return the doc ingested via PUT above; "+
			"a length of 0 means the request was silently treated as an empty Ingest instead of a List")
}

func TestAccuracy_KBDocuments_IngestWithBDAParsingStrategy(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("bda-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	// Create data source with BDA parsing
	dsRec := doAgentRequest(
		t, h, http.MethodPut,
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

	// Ingest documents: real IngestKnowledgeBaseDocuments is PUT.
	ingestRec := doAgentRequest(
		t, h, http.MethodPut,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kb.KnowledgeBaseID, dsID),
		map[string]any{"documentIds": []string{"doc-1", "doc-2", "doc-3"}},
	)
	require.Equal(t, http.StatusOK, ingestRec.Code)

	var ingestBody map[string]any
	require.NoError(t, json.Unmarshal(ingestRec.Body.Bytes(), &ingestBody))
	docs := ingestBody["documentDetails"].([]any)
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

	// Ingest 5 documents: real IngestKnowledgeBaseDocuments is PUT.
	ingestRec := doAgentRequest(
		t, h, http.MethodPut,
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
