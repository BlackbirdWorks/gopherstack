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

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func newTestHandler() *glacier.Handler {
	bk := glacier.NewInMemoryBackend()
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doRequest(t *testing.T, h *glacier.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	var req *http.Request

	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_CreateDescribeDeleteVault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		wantStatus int
	}{
		{
			name:       "create_describe_delete",
			vaultName:  "my-test-vault",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Describe
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var describeResp map[string]any
			err := json.Unmarshal(rec.Body.Bytes(), &describeResp)
			require.NoError(t, err)
			assert.Equal(t, tt.vaultName, describeResp["VaultName"])

			// List
			rec = doRequest(t, h, http.MethodGet, "/-/vaults", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			// Delete
			rec = doRequest(t, h, http.MethodDelete, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Describe after delete returns 404
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_UploadDeleteArchive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		bodyData  string
	}{
		{
			name:      "upload_and_delete",
			vaultName: "archive-vault",
			bodyData:  "archive content",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault first
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Upload archive
			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPost,
				"/-/vaults/"+tt.vaultName+"/archives",
				strings.NewReader(tt.bodyData),
			)
			req.Header.Set("X-Amz-Archive-Description", "test archive")
			rec2 := httptest.NewRecorder()
			c := e.NewContext(req, rec2)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusCreated, rec2.Code)

			var uploadResp map[string]any
			err = json.Unmarshal(rec2.Body.Bytes(), &uploadResp)
			require.NoError(t, err)

			archiveID := uploadResp["archiveId"].(string)
			assert.NotEmpty(t, archiveID)

			// Delete archive
			rec = doRequest(t, h, http.MethodDelete, "/-/vaults/"+tt.vaultName+"/archives/"+archiveID, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}

func TestHandler_InitiateListDescribeJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		jobType   string
	}{
		{
			name:      "inventory_job",
			vaultName: "job-vault",
			jobType:   "InventoryRetrieval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Initiate job
			jobReq := `{"Type":"` + tt.jobType + `"}`
			rec = doRequest(t, h, http.MethodPost, "/-/vaults/"+tt.vaultName+"/jobs", jobReq)
			assert.Equal(t, http.StatusAccepted, rec.Code)

			var jobResp map[string]any
			err := json.Unmarshal(rec.Body.Bytes(), &jobResp)
			require.NoError(t, err)

			jobID := jobResp["jobId"].(string)
			assert.NotEmpty(t, jobID)

			// List jobs
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/jobs", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			// Describe job
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/jobs/"+jobID, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			err = json.Unmarshal(rec.Body.Bytes(), &descResp)
			require.NoError(t, err)
			assert.Equal(t, jobID, descResp["JobId"])

			// Get job output
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/jobs/"+jobID+"/output", "")
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		addTags   string
	}{
		{
			name:      "add_list_remove_tags",
			vaultName: "tag-vault",
			addTags:   `{"Tags":{"env":"test","team":"infra"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Add tags
			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPost,
				"/-/vaults/"+tt.vaultName+"/tags?operation=add",
				strings.NewReader(tt.addTags),
			)
			rec2 := httptest.NewRecorder()
			c := e.NewContext(req, rec2)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNoContent, rec2.Code)

			// List tags
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/tags", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var tagsResp map[string]any
			err = json.Unmarshal(rec.Body.Bytes(), &tagsResp)
			require.NoError(t, err)

			tags := tagsResp["Tags"].(map[string]any)
			assert.Equal(t, "test", tags["env"])
			assert.Equal(t, "infra", tags["team"])

			// Remove tags
			e2 := echo.New()
			req2 := httptest.NewRequest(http.MethodPost, "/-/vaults/"+tt.vaultName+"/tags?operation=remove",
				strings.NewReader(`{"TagKeys":["team"]}`))
			rec3 := httptest.NewRecorder()
			c2 := e2.NewContext(req2, rec3)
			err = h.Handler()(c2)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNoContent, rec3.Code)
		})
	}
}

func TestHandler_Notifications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		notifBody string
	}{
		{
			name:      "set_get_delete",
			vaultName: "notif-vault",
			notifBody: `{"SNSTopic":"arn:aws:sns:us-east-1:000000000000:topic","Events":["ArchiveRetrievalCompleted"]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Set notifications
			rec = doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName+"/notification-configuration", tt.notifBody)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get notifications
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/notification-configuration", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			// Delete notifications
			rec = doRequest(t, h, http.MethodDelete, "/-/vaults/"+tt.vaultName+"/notification-configuration", "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete = 404
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/notification-configuration", "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_AccessPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		policyBody string
	}{
		{
			name:       "set_get_delete",
			vaultName:  "policy-vault",
			policyBody: `{"Policy":"{\"Version\":\"2012-10-17\",\"Statement\":[]}"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Set access policy
			rec = doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName+"/access-policy", tt.policyBody)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get access policy
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/access-policy", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			// Delete access policy
			rec = doRequest(t, h, http.MethodDelete, "/-/vaults/"+tt.vaultName+"/access-policy", "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete = 404
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/access-policy", "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		path  string
		match bool
	}{
		{
			name:  "vaults_root",
			path:  "/-/vaults",
			match: true,
		},
		{
			name:  "vault_name",
			path:  "/-/vaults/my-vault",
			match: true,
		},
		{
			name:  "archives",
			path:  "/-/vaults/my-vault/archives",
			match: true,
		},
		{
			name:  "jobs",
			path:  "/-/vaults/my-vault/jobs/jobId",
			match: true,
		},
		{
			name:  "policies",
			path:  "/-/policies/data-retrieval",
			match: true,
		},
		{
			name:  "s3_bucket",
			path:  "/my-bucket",
			match: false,
		},
		{
			name:  "fis_path",
			path:  "/experimentTemplates",
			match: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.match, matcher(c))
		})
	}
}
