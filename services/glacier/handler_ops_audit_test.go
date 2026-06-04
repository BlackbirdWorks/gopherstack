package glacier_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -------------------------------------------------------------------------
// Issue 30: ListJobs returns JobList:[] not absent when no jobs
// -------------------------------------------------------------------------

func TestOpsAudit_ListJobs_JobListAlwaysPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		seedJobs bool
	}{
		{
			name:     "empty_vault_returns_empty_array",
			seedJobs: false,
		},
		{
			name:     "populated_vault_returns_array",
			seedJobs: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "listjobs-audit-vault")

			if tt.seedJobs {
				body := `{"Type":"inventory-retrieval"}`
				rec := doRequest(t, h, http.MethodPost,
					"/"+testAccountID+"/vaults/listjobs-audit-vault/jobs", body)
				require.Equal(t, http.StatusAccepted, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/listjobs-audit-vault/jobs", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			_, present := resp["JobList"]
			assert.True(t, present, "JobList key must always be present in ListJobs response")

			if !tt.seedJobs {
				assert.Equal(t, "[]", string(resp["JobList"]),
					"JobList must be [] (not null or absent) when no jobs exist")
			}
		})
	}
}

// -------------------------------------------------------------------------
// Issue 31: ListParts returns Parts:[] not null when no parts uploaded
// -------------------------------------------------------------------------

func TestOpsAudit_ListParts_PartsAlwaysArray(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedParts bool
	}{
		{
			name:      "no_parts_returns_empty_array",
			seedParts: false,
		},
		{
			name:      "with_parts_returns_array",
			seedParts: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "listparts-audit-vault")

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost,
				"/"+testAccountID+"/vaults/listparts-audit-vault/multipart-uploads", http.NoBody)
			req.Header.Set("X-Amz-Part-Size", "1048576")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			require.Equal(t, http.StatusCreated, rec.Code)

			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			uploadID := initResp["uploadId"]
			require.NotEmpty(t, uploadID)

			if tt.seedParts {
				req2 := httptest.NewRequest(http.MethodPut,
					"/"+testAccountID+"/vaults/listparts-audit-vault/multipart-uploads/"+uploadID,
					strings.NewReader(strings.Repeat("a", 1<<20)))
				req2.Header.Set("Content-Range", "bytes 0-1048575/*")
				rec2 := httptest.NewRecorder()
				c2 := e.NewContext(req2, rec2)
				require.NoError(t, h.Handler()(c2))
				require.Equal(t, http.StatusNoContent, rec2.Code)
			}

			listRec := doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/listparts-audit-vault/multipart-uploads/"+uploadID, "")
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			partsRaw, present := resp["Parts"]
			assert.True(t, present, "Parts key must be present in ListParts response")
			assert.NotEqual(t, "null", string(partsRaw),
				"Parts must never be null (use [] for empty)")

			if !tt.seedParts {
				assert.Equal(t, "[]", string(partsRaw),
					"Parts must be [] when no parts have been uploaded")
			}
		})
	}
}

// -------------------------------------------------------------------------
// Issue 32: DescribeJob returns SNSTopic when job was initiated with one
// -------------------------------------------------------------------------

func TestOpsAudit_DescribeJob_SNSTopicPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		snsTopic     string
		wantSNSTopic string
		wantPresent  bool
	}{
		{
			name:         "job_with_sns_topic_returns_it",
			snsTopic:     "arn:aws:sns:us-east-1:000000000000:my-topic",
			wantPresent:  true,
			wantSNSTopic: "arn:aws:sns:us-east-1:000000000000:my-topic",
		},
		{
			name:        "job_without_sns_topic_omits_field",
			snsTopic:    "",
			wantPresent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "snstopic-audit-vault")

			var bodyStr string
			if tt.snsTopic != "" {
				bodyStr = `{"Type":"inventory-retrieval","SNSTopic":"` + tt.snsTopic + `"}`
			} else {
				bodyStr = `{"Type":"inventory-retrieval"}`
			}

			initRec := doRequest(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/snstopic-audit-vault/jobs", bodyStr)
			require.Equal(t, http.StatusAccepted, initRec.Code)

			var initResp map[string]string
			require.NoError(t, json.Unmarshal(initRec.Body.Bytes(), &initResp))
			jobID := initResp["jobId"]
			require.NotEmpty(t, jobID)

			descRec := doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/snstopic-audit-vault/jobs/"+jobID, "")
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

			snsRaw, present := resp["SNSTopic"]
			if tt.wantPresent {
				assert.True(t, present, "SNSTopic must be present when job was initiated with SNSTopic")
				var got string
				require.NoError(t, json.Unmarshal(snsRaw, &got))
				assert.Equal(t, tt.wantSNSTopic, got)
			} else if present {
				var got string
				require.NoError(t, json.Unmarshal(snsRaw, &got))
				assert.Empty(t, got, "SNSTopic should be empty or absent when not set")
			}
		})
	}
}

// -------------------------------------------------------------------------
// Issue 33: DescribeJob returns RetrievalByteRange for archive-retrieval jobs
// -------------------------------------------------------------------------

func TestOpsAudit_DescribeJob_RetrievalByteRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		byteRange  string
		wantInResp bool
	}{
		{
			name:       "byte_range_set_on_job_returned_in_describe",
			byteRange:  "0-1048575",
			wantInResp: true,
		},
		{
			name:       "no_byte_range_field_absent",
			byteRange:  "",
			wantInResp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "byterange-audit-vault")

			e := echo.New()
			archiveBody := []byte("archive data")
			req := httptest.NewRequest(http.MethodPost,
				"/"+testAccountID+"/vaults/byterange-audit-vault/archives",
				strings.NewReader(string(archiveBody)))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			require.Equal(t, http.StatusCreated, rec.Code)

			var archResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &archResp))
			archiveID := archResp["archiveId"]
			require.NotEmpty(t, archiveID)

			var bodyStr string
			if tt.byteRange != "" {
				bodyStr = `{"Type":"archive-retrieval","ArchiveId":"` + archiveID +
					`","RetrievalByteRange":"` + tt.byteRange + `"}`
			} else {
				bodyStr = `{"Type":"archive-retrieval","ArchiveId":"` + archiveID + `"}`
			}

			initRec := doRequest(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/byterange-audit-vault/jobs", bodyStr)
			require.Equal(t, http.StatusAccepted, initRec.Code)

			var initResp map[string]string
			require.NoError(t, json.Unmarshal(initRec.Body.Bytes(), &initResp))
			jobID := initResp["jobId"]
			require.NotEmpty(t, jobID)

			descRec := doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/byterange-audit-vault/jobs/"+jobID, "")
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

			rangeRaw, present := resp["RetrievalByteRange"]
			if tt.wantInResp {
				assert.True(t, present, "RetrievalByteRange must be present when set on the job")
				var got string
				require.NoError(t, json.Unmarshal(rangeRaw, &got))
				assert.Equal(t, tt.byteRange, got)
			} else if present {
				var got string
				require.NoError(t, json.Unmarshal(rangeRaw, &got))
				assert.Empty(t, got, "RetrievalByteRange should be empty or absent when not set")
			}
		})
	}
}
