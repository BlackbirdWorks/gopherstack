package organizations_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// TestResourcePolicy_Lifecycle tests the full resource policy lifecycle.
func TestResourcePolicy_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		replace string
		wantID  bool
		wantARN bool
	}{
		{
			name:    "put_describes_deletes",
			content: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"*","Resource":"*"}]}`,
			wantID:  true,
			wantARN: true,
		},
		{
			name:    "put_replace_describes",
			content: `{"Version":"2012-10-17","Statement":[]}`,
			replace: `{"Version":"2012-10-17","Statement":[{"Effect":"Deny"}]}`,
			wantID:  true,
			wantARN: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			rp, err := b.PutResourcePolicy(tt.content)
			require.NoError(t, err)
			assert.Equal(t, tt.content, rp.Content)

			if tt.wantID {
				assert.NotEmpty(t, rp.ID)
			}

			if tt.wantARN {
				assert.NotEmpty(t, rp.ARN)
			}

			if tt.replace != "" {
				rp2, replaceErr := b.PutResourcePolicy(tt.replace)
				require.NoError(t, replaceErr)
				assert.Equal(t, tt.replace, rp2.Content)
				assert.Equal(t, rp.ID, rp2.ID, "ID should be stable after replacement")
			}

			described, err := b.DescribeResourcePolicy()
			require.NoError(t, err)

			expectedContent := tt.content
			if tt.replace != "" {
				expectedContent = tt.replace
			}

			assert.Equal(t, expectedContent, described.Content)

			require.NoError(t, b.DeleteResourcePolicy())

			_, err = b.DescribeResourcePolicy()
			require.Error(t, err, "describe after delete must fail")
		})
	}
}

// TestResourcePolicy_ErrorCases tests error conditions.
func TestResourcePolicy_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string // "describe", "delete", "put"
		hasRP   bool
		wantErr bool
	}{
		{name: "describe_missing", op: "describe", hasRP: false, wantErr: true},
		{name: "delete_missing", op: "delete", hasRP: false, wantErr: true},
		{name: "describe_present", op: "describe", hasRP: true},
		{name: "delete_present", op: "delete", hasRP: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			if tt.hasRP {
				_, err := b.PutResourcePolicy(`{"Version":"2012-10-17"}`)
				require.NoError(t, err)
			}

			var err error

			switch tt.op {
			case "describe":
				_, err = b.DescribeResourcePolicy()
			case "delete":
				err = b.DeleteResourcePolicy()
			}

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestResourcePolicy_Handler tests the HTTP handler for resource policy.
func TestResourcePolicy_Handler(t *testing.T) {
	t.Parallel()

	const content = `{"Version":"2012-10-17","Statement":[]}`

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
		needRP     bool
	}{
		{
			name:       "put_resource_policy",
			op:         "PutResourcePolicy",
			body:       map[string]any{"Content": content},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe_resource_policy",
			op:         "DescribeResourcePolicy",
			body:       nil,
			wantStatus: http.StatusOK,
			needRP:     true,
		},
		{
			name:       "delete_resource_policy",
			op:         "DeleteResourcePolicy",
			body:       nil,
			wantStatus: http.StatusOK,
			needRP:     true,
		},
		{
			name:       "put_empty_content_fails",
			op:         "PutResourcePolicy",
			body:       map[string]any{"Content": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe_missing_fails",
			op:         "DescribeResourcePolicy",
			body:       nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			if tt.needRP {
				rec := doRequest(t, h, "PutResourcePolicy", map[string]any{"Content": content})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Handler – additional operation error paths
// ---------------------------------------------------------------------------

// TestHandler_ResourcePolicy tests DeleteResourcePolicy and DescribeResourcePolicy.
func TestHandler_ResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		seedPolicy bool
		wantStatus int
	}{
		{
			name:       "describe_no_policy",
			op:         "DescribeResourcePolicy",
			seedPolicy: false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_no_policy",
			op:         "DeleteResourcePolicy",
			seedPolicy: false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe_existing_policy",
			op:         "DescribeResourcePolicy",
			seedPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_existing_policy",
			op:         "DeleteResourcePolicy",
			seedPolicy: true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := organizations.NewHandler(b)

			// Create an organization first (required for resource policy operations).
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			if tt.seedPolicy {
				// Seed via PutResourcePolicy backend method.
				_, err := b.PutResourcePolicy(`{"Version":"2012-10-17","Statement":[]}`)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, tt.op, map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.op == "DescribeResourcePolicy" && tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				rp, ok := resp["ResourcePolicy"].(map[string]any)
				require.True(t, ok, "response must have ResourcePolicy")
				assert.NotEmpty(t, rp["Content"])
				summary, ok := rp["ResourcePolicySummary"].(map[string]any)
				require.True(t, ok, "response must have ResourcePolicySummary")
				assert.NotEmpty(t, summary["Id"])
				assert.NotEmpty(t, summary["Arn"])
			}
		})
	}
}

// TestBackend_ResourcePolicyOperations tests resource policy CRUD methods.
func TestBackend_ResourcePolicyOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		content    string
		seedPolicy bool
		wantErr    bool
	}{
		{
			name:    "describe_no_policy",
			op:      "DescribeResourcePolicy",
			wantErr: true,
		},
		{
			name:    "delete_no_policy",
			op:      "DeleteResourcePolicy",
			wantErr: true,
		},
		{
			name:       "describe_existing_policy",
			op:         "DescribeResourcePolicy",
			seedPolicy: true,
			content:    `{"Version":"2012-10-17","Statement":[]}`,
		},
		{
			name:       "delete_existing_policy",
			op:         "DeleteResourcePolicy",
			seedPolicy: true,
			content:    `{"Version":"2012-10-17","Statement":[]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			if tt.seedPolicy {
				_, err := b.PutResourcePolicy(tt.content)
				require.NoError(t, err)
			}

			switch tt.op {
			case "DescribeResourcePolicy":
				rp, err := b.DescribeResourcePolicy()

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.NotNil(t, rp)
				assert.Equal(t, tt.content, rp.Content)
				assert.NotEmpty(t, rp.ID)
				assert.NotEmpty(t, rp.ARN)

			case "DeleteResourcePolicy":
				err := b.DeleteResourcePolicy()

				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)

				// After deletion, describe should fail.
				_, descErr := b.DescribeResourcePolicy()
				require.Error(t, descErr)
			}
		})
	}
}

// TestPutResourcePolicy_Happy verifies PutResourcePolicy via the HTTP handler.
func TestPutResourcePolicy_Happy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		wantOK  bool
	}{
		{name: "creates_policy", content: `{"Version":"2012-10-17","Statement":[]}`, wantOK: true},
		{name: "replaces_policy", content: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`, wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			createOrgOn(t, b)

			h := organizations.NewHandler(b)

			rec := doRequest(t, h, "PutResourcePolicy", map[string]string{"Content": tt.content})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			assert.NotNil(t, resp["ResourcePolicy"])
		})
	}
}

// TestPutResourcePolicy_ContentSizeLimit verifies PutResourcePolicy enforces
// the ResourcePolicyContent shape's 40,000-character max.
func TestPutResourcePolicy_ContentSizeLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		length  int
		wantErr bool
	}{
		{name: "over_limit_rejected", length: 40001, wantErr: true},
		{name: "at_limit_accepted", length: 40000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			createOrgOn(t, b)

			content := `{"k":"` + strings.Repeat("a", tt.length-8) + `"}`
			require.Len(t, content, tt.length)

			_, err := b.PutResourcePolicy(content)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ConstraintViolationException")

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestPutResourcePolicy_NoOrg verifies PutResourcePolicy returns 400 without org.
func TestPutResourcePolicy_NoOrg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "no_org_returns_error", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "PutResourcePolicy", map[string]string{"Content": "{}"})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestPutResourcePolicy_BadJSON verifies PutResourcePolicy handles malformed JSON.
func TestPutResourcePolicy_BadJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name:       "bad_json",
			body:       []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_content",
			body:       []byte(`{"Content":""}`),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			createOrgOn(t, b)
			h := organizations.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "AWSOrganizationsV20161128.PutResourcePolicy")

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
