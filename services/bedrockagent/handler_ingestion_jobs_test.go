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

	rec := doRequest(t, f.h, f.e, http.MethodPost,
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
	})

	t.Run("statistics reflect real pushed document count", func(t *testing.T) {
		t.Parallel()

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
	})

	t.Run("GetIngestionJob returns the same persisted statistics", func(t *testing.T) {
		t.Parallel()

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
	})

	t.Run("ListIngestionJobs summaries include statistics", func(t *testing.T) {
		t.Parallel()

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
	})
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
