package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

func TestHandlerKnowledgeBaseCRUD(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createBody := map[string]any{
		"name":    "test-kb",
		"roleArn": "arn:aws:iam::123456789012:role/KBRole",
		"knowledgeBaseConfiguration": map[string]any{
			"type": "VECTOR",
		},
		"storageConfiguration": map[string]any{
			"type": "OPENSEARCH_SERVERLESS",
		},
	}

	rec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", createBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("create kb: %d %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	kbID := createResp["knowledgeBase"]["knowledgeBaseId"].(string)

	t.Run("get kb", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPut, "/knowledgebases", createBody)

		var resp map[string]map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["knowledgeBase"]["knowledgeBaseId"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodGet, "/knowledgebases/"+id, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("list kbs", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodGet, "/knowledgebases", nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("delete kb", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPut, "/knowledgebases", createBody)

		var resp map[string]map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["knowledgeBase"]["knowledgeBaseId"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodDelete, "/knowledgebases/"+id, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	_ = kbID
}

// createKBForResourcePolicyTest creates a knowledge base against h/e and
// returns its real ARN (not just its ID), since bedrock-agent's
// PutResourcePolicy/GetResourcePolicy/DeleteResourcePolicy address a
// knowledge base by ARN, not by ID.
func createKBForResourcePolicyTest(t *testing.T, h *bedrockagent.Handler, e *echo.Echo, name string) string {
	t.Helper()

	rec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", map[string]any{
		"name":    name,
		"roleArn": "arn:aws:iam::123456789012:role/KBRole",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("create kb: %d %s", rec.Code, rec.Body.String())
	}

	var resp map[string]map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal create kb response: %v", err)
	}

	arn, _ := resp["knowledgeBase"]["knowledgeBaseArn"].(string)
	if arn == "" {
		t.Fatalf("create kb response missing knowledgeBaseArn: %s", rec.Body.String())
	}

	return arn
}

// putKBResourcePolicy PUTs a resource policy onto resourceArn and returns the
// revisionId from the response, failing the test on any non-200.
func putKBResourcePolicy(
	t *testing.T, h *bedrockagent.Handler, e *echo.Echo, resourceArn string, body map[string]any,
) string {
	t.Helper()

	rec := doRequest(t, h, e, http.MethodPut, "/resourcepolicy/"+resourceArn, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("put resource policy: %d %s", rec.Code, rec.Body.String())
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal put resource policy response: %v", err)
	}

	return resp["revisionId"]
}

// TestHandlerResourcePolicyLifecycle exercises bedrock-agent's
// PutResourcePolicy/GetResourcePolicy/DeleteResourcePolicy through the real
// HTTP router (not whitebox backend calls), locking in: the real wire shapes
// (PUT/GET/DELETE /resourcepolicy/{resourceArn}, "policy"/"resourceArn"/
// "revisionId" field names), that a policy cannot attach to a knowledge base
// this backend doesn't model, and the expectedRevisionId optimistic-
// concurrency contract (absent -> unconditional write, stale -> real
// ConflictException, matching -> succeeds).
func TestHandlerResourcePolicyLifecycle(t *testing.T) {
	t.Parallel()

	const policyDoc = `{"Version":"2012-10-17","Statement":[]}`

	cases := []struct {
		setup       func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string
		check       func(t *testing.T, rec *httptest.ResponseRecorder)
		body        map[string]any
		name        string
		method      string
		wantErrType string
		wantStatus  int
	}{
		{
			name:   "put creates policy on existing kb",
			method: http.MethodPut,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				return createKBForResourcePolicyTest(t, h, e, "rp-put-kb")
			},
			body:       map[string]any{"policy": policyDoc},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}

				if resp["revisionId"] == "" {
					t.Errorf("revisionId missing from response: %s", rec.Body.String())
				}

				if resp["resourceArn"] == "" {
					t.Errorf("resourceArn missing from response: %s", rec.Body.String())
				}

				if _, hasPolicy := resp["policy"]; hasPolicy {
					t.Errorf("PutResourcePolicyOutput must not echo 'policy' back: %s", rec.Body.String())
				}
			},
		},
		{
			name:   "put on nonexistent kb returns not found",
			method: http.MethodPut,
			setup: func(t *testing.T, _ *bedrockagent.Handler, _ *echo.Echo) string {
				t.Helper()

				return "arn:aws:bedrock:us-east-1:123456789012:knowledge-base/kb-99999999"
			},
			body:        map[string]any{"policy": policyDoc},
			wantStatus:  http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:   "put with malformed arn returns validation error",
			method: http.MethodPut,
			setup: func(t *testing.T, _ *bedrockagent.Handler, _ *echo.Echo) string {
				t.Helper()

				return "not-a-knowledge-base-arn"
			},
			body:        map[string]any{"policy": policyDoc},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ValidationException",
		},
		{
			name:   "put against an agent arn is rejected (not scoped to agents)",
			method: http.MethodPut,
			setup: func(t *testing.T, _ *bedrockagent.Handler, _ *echo.Echo) string {
				t.Helper()

				return "arn:aws:bedrock:us-east-1:123456789012:agent/abcdefghij"
			},
			body:        map[string]any{"policy": policyDoc},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ValidationException",
		},
		{
			name:   "put with empty policy returns validation error",
			method: http.MethodPut,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				return createKBForResourcePolicyTest(t, h, e, "rp-empty-policy-kb")
			},
			body:        map[string]any{"policy": ""},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ValidationException",
		},
		{
			name:   "put with matching expected revision succeeds",
			method: http.MethodPut,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				arn := createKBForResourcePolicyTest(t, h, e, "rp-match-rev-kb")
				rev := putKBResourcePolicy(t, h, e, arn, map[string]any{"policy": policyDoc})

				// This fresh per-subtest backend's very first PutResourcePolicy
				// call always mints revisionId "1" (see resource_policy.go's
				// strconv.Itoa(b.resourcePolicyCounter)).
				if rev != "1" {
					t.Fatalf("precondition: expected first revisionId to be %q, got %q", "1", rev)
				}

				return arn
			},
			body:       map[string]any{"policy": policyDoc, "expectedRevisionId": "1"},
			wantStatus: http.StatusOK,
		},
		{
			name:   "put with stale expected revision returns conflict",
			method: http.MethodPut,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				arn := createKBForResourcePolicyTest(t, h, e, "rp-stale-rev-kb")
				putKBResourcePolicy(t, h, e, arn, map[string]any{"policy": policyDoc})

				return arn
			},
			body:        map[string]any{"policy": policyDoc, "expectedRevisionId": "does-not-exist"},
			wantStatus:  http.StatusConflict,
			wantErrType: "ConflictException",
		},
		{
			name:   "put with expected revision against a policy-less resource returns conflict",
			method: http.MethodPut,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				return createKBForResourcePolicyTest(t, h, e, "rp-no-policy-yet-kb")
			},
			body:        map[string]any{"policy": policyDoc, "expectedRevisionId": "1"},
			wantStatus:  http.StatusConflict,
			wantErrType: "ConflictException",
		},
		{
			name:   "get existing policy returns real wire shape",
			method: http.MethodGet,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				arn := createKBForResourcePolicyTest(t, h, e, "rp-get-kb")
				putKBResourcePolicy(t, h, e, arn, map[string]any{"policy": policyDoc})

				return arn
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}

				if resp["policy"] != policyDoc {
					t.Errorf("policy = %q, want %q", resp["policy"], policyDoc)
				}

				if resp["revisionId"] == "" {
					t.Errorf("revisionId missing from response: %s", rec.Body.String())
				}
			},
		},
		{
			name:   "get nonexistent policy returns not found",
			method: http.MethodGet,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				return createKBForResourcePolicyTest(t, h, e, "rp-get-missing-kb")
			},
			wantStatus:  http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:   "delete existing policy succeeds and returns its revision",
			method: http.MethodDelete,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				arn := createKBForResourcePolicyTest(t, h, e, "rp-delete-kb")
				putKBResourcePolicy(t, h, e, arn, map[string]any{"policy": policyDoc})

				return arn
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}

				if resp["revisionId"] != "1" {
					t.Errorf("revisionId = %q, want %q", resp["revisionId"], "1")
				}

				if _, hasPolicy := resp["policy"]; hasPolicy {
					t.Errorf("DeleteResourcePolicyOutput must not echo 'policy' back: %s", rec.Body.String())
				}
			},
		},
		{
			name:   "delete with stale expected revision returns conflict",
			method: http.MethodDelete,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				arn := createKBForResourcePolicyTest(t, h, e, "rp-delete-stale-kb")
				putKBResourcePolicy(t, h, e, arn, map[string]any{"policy": policyDoc})

				return arn + "?expectedRevisionId=does-not-exist"
			},
			wantStatus:  http.StatusConflict,
			wantErrType: "ConflictException",
		},
		{
			name:   "delete nonexistent policy returns not found",
			method: http.MethodDelete,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				return createKBForResourcePolicyTest(t, h, e, "rp-delete-missing-kb")
			},
			wantStatus:  http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, e := setupHandler(t)
			resourceArn := tc.setup(t, h, e)

			var body any
			if tc.body != nil {
				body = tc.body
			}

			rec := doRequest(t, h, e, tc.method, "/resourcepolicy/"+resourceArn, body)
			if rec.Code != tc.wantStatus {
				t.Fatalf("got %d want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}

			if tc.wantErrType != "" {
				if got := rec.Header().Get("X-Amzn-Errortype"); got != tc.wantErrType {
					t.Errorf("X-Amzn-Errortype = %q, want %q", got, tc.wantErrType)
				}
			}

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}
}
