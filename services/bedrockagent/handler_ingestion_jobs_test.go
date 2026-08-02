package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// ingestionFixture is a freshly created knowledge base + data source, ready
// for StartIngestionJob calls. Each caller gets its own handler/backend so
// parallel subtests never share state.
type ingestionFixture struct {
	h    *bedrockagent.Handler
	e    *echo.Echo
	kbID string
	dsID string
}

func newIngestionFixture(t *testing.T) ingestionFixture {
	t.Helper()

	h, e := setupHandler(t)

	kbBody := map[string]any{
		"name":                       "ingestion-stats-kb",
		"roleArn":                    "arn:aws:iam::123456789012:role/KBRole",
		"knowledgeBaseConfiguration": map[string]any{"type": "VECTOR"},
		"storageConfiguration":       map[string]any{"type": "OPENSEARCH_SERVERLESS"},
	}

	kbRec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", kbBody)
	if kbRec.Code != http.StatusOK {
		t.Fatalf("create kb: %d %s", kbRec.Code, kbRec.Body.String())
	}

	var kbResp map[string]map[string]any

	_ = json.Unmarshal(kbRec.Body.Bytes(), &kbResp)
	kbID, _ := kbResp["knowledgeBase"]["knowledgeBaseId"].(string)

	dsBody := map[string]any{
		"name":                    "ingestion-stats-ds",
		"dataSourceConfiguration": map[string]any{"type": "CUSTOM"},
	}

	dsRec := doRequest(t, h, e, http.MethodPut, "/knowledgebases/"+kbID+"/datasources", dsBody)
	if dsRec.Code != http.StatusOK {
		t.Fatalf("create ds: %d %s", dsRec.Code, dsRec.Body.String())
	}

	var dsResp map[string]map[string]any

	_ = json.Unmarshal(dsRec.Body.Bytes(), &dsResp)
	dsID, _ := dsResp["dataSource"]["dataSourceId"].(string)

	return ingestionFixture{h: h, e: e, kbID: kbID, dsID: dsID}
}

func (f ingestionFixture) ingestDocs(t *testing.T, docIDs ...string) {
	t.Helper()

	docs := make([]map[string]any, 0, len(docIDs))
	for _, id := range docIDs {
		docs = append(docs, map[string]any{
			"documentId": id,
			"content":    map[string]any{"type": "TEXT"},
		})
	}

	rec := doRequest(t, f.h, f.e, http.MethodPut,
		"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/documents",
		map[string]any{"documents": docs})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("ingest docs: %d %s", rec.Code, rec.Body.String())
	}
}

func (f ingestionFixture) startJob(t *testing.T) map[string]any {
	t.Helper()

	rec := doRequest(t, f.h, f.e, http.MethodPut,
		"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/ingestionjobs", nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("start ingestion job: %d %s", rec.Code, rec.Body.String())
	}

	var resp map[string]map[string]any

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	return resp["ingestionJob"]
}

// TestIngestionJobStatistics locks in the real-state-derived statistics
// behavior: StartIngestionJob must report the actual count of documents
// pushed via IngestKnowledgeBaseDocuments for the target data source,
// rather than an always-empty/omitted statistics object.
func TestIngestionJobStatistics(t *testing.T) {
	t.Parallel()

	t.Run("no documents yields zeroed non-nil statistics", func(t *testing.T) {
		t.Parallel()
		checkIngestionStatsZeroedWithNoDocuments(t)
	})

	t.Run("statistics reflect real pushed document count", func(t *testing.T) {
		t.Parallel()
		checkIngestionStatsReflectPushedCount(t)
	})

	t.Run("GetIngestionJob returns the same persisted statistics", func(t *testing.T) {
		t.Parallel()
		checkGetIngestionJobReturnsPersistedStats(t)
	})

	t.Run("ListIngestionJobs summaries include statistics", func(t *testing.T) {
		t.Parallel()
		checkListIngestionJobsSummariesIncludeStats(t)
	})
}

// checkIngestionStatsZeroedWithNoDocuments verifies StartIngestionJob reports
// a zeroed (not omitted) statistics object when no documents were pushed.
func checkIngestionStatsZeroedWithNoDocuments(t *testing.T) {
	t.Helper()

	f := newIngestionFixture(t)
	job := f.startJob(t)

	stats, ok := job["statistics"].(map[string]any)
	if !ok {
		t.Fatalf("expected statistics object in response, got %#v", job["statistics"])
	}

	if got := stats["numberOfDocumentsScanned"]; got != float64(0) {
		t.Errorf("numberOfDocumentsScanned = %v, want 0", got)
	}

	if got := stats["numberOfNewDocumentsIndexed"]; got != float64(0) {
		t.Errorf("numberOfNewDocumentsIndexed = %v, want 0", got)
	}
}

// checkIngestionStatsReflectPushedCount verifies StartIngestionJob reports
// the actual count of documents pushed via IngestKnowledgeBaseDocuments.
func checkIngestionStatsReflectPushedCount(t *testing.T) {
	t.Helper()

	f := newIngestionFixture(t)
	f.ingestDocs(t, "doc-1", "doc-2", "doc-3")

	job := f.startJob(t)

	stats, ok := job["statistics"].(map[string]any)
	if !ok {
		t.Fatalf("expected statistics object in response, got %#v", job["statistics"])
	}

	if got := stats["numberOfDocumentsScanned"]; got != float64(3) {
		t.Errorf("numberOfDocumentsScanned = %v, want 3", got)
	}

	if got := stats["numberOfNewDocumentsIndexed"]; got != float64(3) {
		t.Errorf("numberOfNewDocumentsIndexed = %v, want 3", got)
	}

	if got := stats["numberOfDocumentsDeleted"]; got != float64(0) {
		t.Errorf("numberOfDocumentsDeleted = %v, want 0", got)
	}
}

// checkGetIngestionJobReturnsPersistedStats verifies GetIngestionJob echoes
// back the same statistics that were computed and persisted by StartIngestionJob.
func checkGetIngestionJobReturnsPersistedStats(t *testing.T) {
	t.Helper()

	f := newIngestionFixture(t)
	f.ingestDocs(t, "doc-a")

	job := f.startJob(t)
	jobID, _ := job["ingestionJobId"].(string)

	rec := doRequest(t, f.h, f.e, http.MethodGet,
		"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/ingestionjobs/"+jobID, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get ingestion job: %d %s", rec.Code, rec.Body.String())
	}

	var resp map[string]map[string]any

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	stats, ok := resp["ingestionJob"]["statistics"].(map[string]any)
	if !ok {
		t.Fatalf("expected statistics object, got %#v", resp["ingestionJob"]["statistics"])
	}

	if got := stats["numberOfDocumentsScanned"]; got != float64(1) {
		t.Errorf("numberOfDocumentsScanned = %v, want 1", got)
	}
}

// checkListIngestionJobsSummariesIncludeStats verifies ListIngestionJobs
// summaries carry the same statistics as the underlying job.
func checkListIngestionJobsSummariesIncludeStats(t *testing.T) {
	t.Helper()

	f := newIngestionFixture(t)
	f.ingestDocs(t, "doc-x", "doc-y")
	f.startJob(t)

	rec := doRequest(t, f.h, f.e, http.MethodGet,
		"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/ingestionjobs", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list ingestion jobs: %d %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		IngestionJobSummaries []map[string]any `json:"ingestionJobSummaries"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(resp.IngestionJobSummaries) != 1 {
		t.Fatalf("expected 1 summary, got %d", len(resp.IngestionJobSummaries))
	}

	stats, ok := resp.IngestionJobSummaries[0]["statistics"].(map[string]any)
	if !ok {
		t.Fatalf("expected statistics object, got %#v", resp.IngestionJobSummaries[0]["statistics"])
	}

	if got := stats["numberOfDocumentsScanned"]; got != float64(2) {
		t.Errorf("numberOfDocumentsScanned = %v, want 2", got)
	}
}

// TestKBDocumentsRealWireRouting locks in the real bedrock-agent wire shapes
// for the two operations sharing the .../documents collection path:
// IngestKnowledgeBaseDocuments is PUT, ListKnowledgeBaseDocuments is POST.
// Verified against the vendored SDK's request snapshots
// (aws-sdk-go-v2/service/bedrockagent's IngestKnowledgeBaseDocuments.request.snap
// and ListKnowledgeBaseDocuments.request.snap): Ingest sends PUT with a
// "documents" body; List sends POST with only maxResults/nextToken. A real SDK
// client issuing either request must reach its own handler, not the other
// operation's.
func TestKBDocumentsRealWireRouting(t *testing.T) {
	t.Parallel()

	f := newIngestionFixture(t)

	// A real IngestKnowledgeBaseDocuments call is PUT to the base
	// .../documents path and returns 202 with the ingested document's details.
	putRec := doRequest(t, f.h, f.e, http.MethodPut,
		"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/documents",
		map[string]any{"documents": []map[string]any{
			{"documentId": "wire-doc-1", "content": map[string]any{"type": "TEXT"}},
		}})
	if putRec.Code != http.StatusAccepted {
		t.Fatalf("PUT .../documents (real Ingest wire shape): got %d %s, want %d (IngestKnowledgeBaseDocuments)",
			putRec.Code, putRec.Body.String(), http.StatusAccepted)
	}

	var ingestResp map[string]any

	if err := json.Unmarshal(putRec.Body.Bytes(), &ingestResp); err != nil {
		t.Fatalf("unmarshal PUT response: %v", err)
	}

	if _, ok := ingestResp["documentDetails"]; !ok {
		t.Fatalf("PUT .../documents response missing documentDetails: %#v", ingestResp)
	}

	// A real ListKnowledgeBaseDocuments call is POST to the same base
	// .../documents path and returns 200 with the previously-ingested
	// document's details -- it must NOT be treated as an ingest of an empty
	// payload (which would also 20x but with a zero-length result and the
	// wrong status code).
	postRec := doRequest(t, f.h, f.e, http.MethodPost,
		"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/documents", nil)
	if postRec.Code != http.StatusOK {
		t.Fatalf("POST .../documents (real List wire shape): got %d %s, want %d (ListKnowledgeBaseDocuments)",
			postRec.Code, postRec.Body.String(), http.StatusOK)
	}

	var listResp struct {
		DocumentDetails []map[string]any `json:"documentDetails"`
	}

	if err := json.Unmarshal(postRec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal POST response: %v", err)
	}

	if len(listResp.DocumentDetails) != 1 {
		t.Fatalf("POST .../documents (List): got %d document(s), want 1 (the doc ingested via PUT above); "+
			"a length of 0 means the request was silently treated as an empty Ingest instead of a List",
			len(listResp.DocumentDetails))
	}
}

// TestStopIngestionJob locks in the real StopIngestionJob status transition.
func TestStopIngestionJob(t *testing.T) {
	t.Parallel()

	f := newIngestionFixture(t)
	job := f.startJob(t)
	jobID, _ := job["ingestionJobId"].(string)

	rec := doRequest(t, f.h, f.e, http.MethodPut,
		"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/ingestionjobs/"+jobID+"/stop", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("stop ingestion job: %d %s", rec.Code, rec.Body.String())
	}

	var resp map[string]map[string]any

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got := resp["ingestionJob"]["status"]; got != "STOPPED" {
		t.Errorf("status = %v, want STOPPED", got)
	}
}
