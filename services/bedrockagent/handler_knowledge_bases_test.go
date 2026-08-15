package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// resourcePolicyPolicyDoc is the policy document body reused by every
// resource-policy subtest below.
const resourcePolicyPolicyDoc = `{"Version":"2012-10-17","Statement":[]}`

// resourcePolicyCase is one HTTP round trip through
// PUT/GET/DELETE /resourcepolicy/{resourceArn}, shared by the
// TestHandlerResourcePolicy* functions below. Each of those functions
// exercises bedrock-agent's PutResourcePolicy/GetResourcePolicy/
// DeleteResourcePolicy through the real HTTP router (not whitebox backend
// calls), together locking in: the real wire shapes (PUT/GET/DELETE
// /resourcepolicy/{resourceArn}, "policy"/"resourceArn"/"revisionId" field
// names), that a policy cannot attach to a knowledge base this backend
// doesn't model, and the expectedRevisionId optimistic-concurrency contract
// (absent -> unconditional write, stale -> real ConflictException,
// matching -> succeeds).
type resourcePolicyCase struct {
	setup       func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string
	check       func(t *testing.T, rec *httptest.ResponseRecorder)
	body        map[string]any
	name        string
	method      string
	wantErrType string
	wantStatus  int
}

// runResourcePolicyCases fires each case's request through the real router
// and checks the shared status/error-type/body contract. Each case builds
// its own handler and knowledge base inside setup, so cases never depend on
// each other's execution order.
func runResourcePolicyCases(t *testing.T, cases []resourcePolicyCase) {
	t.Helper()

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

// TestHandlerResourcePolicyPut covers PutResourcePolicy's wire shape
// (the "policy"/"resourceArn"/"revisionId" field names, and that the
// response never echoes "policy" back) and that a policy cannot attach to a
// knowledge base this backend doesn't model (missing kb, malformed arn,
// an arn scoped to a different resource type, and an invalid payload).
func TestHandlerResourcePolicyPut(t *testing.T) {
	t.Parallel()

	runResourcePolicyCases(t, []resourcePolicyCase{
		{
			name:   "put creates policy on existing kb",
			method: http.MethodPut,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				return createKBForResourcePolicyTest(t, h, e, "rp-put-kb")
			},
			body:       map[string]any{"policy": resourcePolicyPolicyDoc},
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
			body:        map[string]any{"policy": resourcePolicyPolicyDoc},
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
			body:        map[string]any{"policy": resourcePolicyPolicyDoc},
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
			body:        map[string]any{"policy": resourcePolicyPolicyDoc},
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
	})
}

// TestHandlerResourcePolicyPutRevisionConcurrency covers PutResourcePolicy's
// expectedRevisionId optimistic-concurrency contract: a matching revision
// succeeds, a stale revision returns a real ConflictException, and
// supplying expectedRevisionId against a resource with no policy yet also
// conflicts (there is no revision to match).
func TestHandlerResourcePolicyPutRevisionConcurrency(t *testing.T) {
	t.Parallel()

	runResourcePolicyCases(t, []resourcePolicyCase{
		{
			name:   "put with matching expected revision succeeds",
			method: http.MethodPut,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				arn := createKBForResourcePolicyTest(t, h, e, "rp-match-rev-kb")
				rev := putKBResourcePolicy(t, h, e, arn, map[string]any{"policy": resourcePolicyPolicyDoc})

				// This fresh per-subtest backend's very first PutResourcePolicy
				// call always mints revisionId "1" (see resource_policy.go's
				// strconv.Itoa(b.resourcePolicyCounter)).
				if rev != "1" {
					t.Fatalf("precondition: expected first revisionId to be %q, got %q", "1", rev)
				}

				return arn
			},
			body:       map[string]any{"policy": resourcePolicyPolicyDoc, "expectedRevisionId": "1"},
			wantStatus: http.StatusOK,
		},
		{
			name:   "put with stale expected revision returns conflict",
			method: http.MethodPut,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				arn := createKBForResourcePolicyTest(t, h, e, "rp-stale-rev-kb")
				putKBResourcePolicy(t, h, e, arn, map[string]any{"policy": resourcePolicyPolicyDoc})

				return arn
			},
			body: map[string]any{
				"policy":             resourcePolicyPolicyDoc,
				"expectedRevisionId": "does-not-exist",
			},
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
			body:        map[string]any{"policy": resourcePolicyPolicyDoc, "expectedRevisionId": "1"},
			wantStatus:  http.StatusConflict,
			wantErrType: "ConflictException",
		},
	})
}

// TestHandlerResourcePolicyGet covers GetResourcePolicy's wire shape (the
// "policy"/"revisionId" field names round-tripping the value that was put)
// and the not-found case for a resource with no policy attached.
func TestHandlerResourcePolicyGet(t *testing.T) {
	t.Parallel()

	runResourcePolicyCases(t, []resourcePolicyCase{
		{
			name:   "get existing policy returns real wire shape",
			method: http.MethodGet,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				arn := createKBForResourcePolicyTest(t, h, e, "rp-get-kb")
				putKBResourcePolicy(t, h, e, arn, map[string]any{"policy": resourcePolicyPolicyDoc})

				return arn
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}

				if resp["policy"] != resourcePolicyPolicyDoc {
					t.Errorf("policy = %q, want %q", resp["policy"], resourcePolicyPolicyDoc)
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
	})
}

// TestHandlerResourcePolicyDelete covers DeleteResourcePolicy's wire shape
// (it returns the deleted revisionId and never echoes "policy" back), its
// own expectedRevisionId conflict check, and the not-found case for a
// resource with no policy attached.
func TestHandlerResourcePolicyDelete(t *testing.T) {
	t.Parallel()

	runResourcePolicyCases(t, []resourcePolicyCase{
		{
			name:   "delete existing policy succeeds and returns its revision",
			method: http.MethodDelete,
			setup: func(t *testing.T, h *bedrockagent.Handler, e *echo.Echo) string {
				t.Helper()

				arn := createKBForResourcePolicyTest(t, h, e, "rp-delete-kb")
				putKBResourcePolicy(t, h, e, arn, map[string]any{"policy": resourcePolicyPolicyDoc})

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
				putKBResourcePolicy(t, h, e, arn, map[string]any{"policy": resourcePolicyPolicyDoc})

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
	})
}

// kbDocumentIdentifierCase pairs a real IngestKnowledgeBaseDocuments
// document.content body with the real GetKnowledgeBaseDocuments/
// DeleteKnowledgeBaseDocuments documentIdentifiers entry that must resolve
// to the document it ingests, for each DocumentIdentifier.DataSourceType
// variant (gopherstack-wzwn: these ops used to decode a nonexistent
// "documentIds" string-array key instead of the real "documentIdentifiers"
// object-array key, so they always operated on an empty list).
type kbDocumentIdentifierCase struct {
	content    map[string]any
	identifier map[string]any
	name       string
}

//nolint:gochecknoglobals // shared table-driven cases reused by two test functions below
var kbDocumentIdentifierCases = []kbDocumentIdentifierCase{
	{
		name: "custom",
		content: map[string]any{
			"dataSourceType": "CUSTOM",
			"custom": map[string]any{
				"customDocumentIdentifier": map[string]any{"id": "doc-custom-1"},
				"sourceType":               "IN_LINE",
			},
		},
		identifier: map[string]any{
			"dataSourceType": "CUSTOM",
			"custom":         map[string]any{"id": "doc-custom-1"},
		},
	},
	{
		name: "s3",
		content: map[string]any{
			"dataSourceType": "S3",
			"s3": map[string]any{
				"s3Location": map[string]any{"uri": "s3://kb-bucket/doc-1.txt"},
			},
		},
		identifier: map[string]any{
			"dataSourceType": "S3",
			"s3":             map[string]any{"uri": "s3://kb-bucket/doc-1.txt"},
		},
	},
}

// ingestKBDocForIdentifierTest ingests one document with the given real
// content body and requires the call to succeed.
func ingestKBDocForIdentifierTest(t *testing.T, f ingestionFixture, content map[string]any) {
	t.Helper()

	rec := doRequest(t, f.h, f.e, http.MethodPut,
		"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/documents",
		map[string]any{"documents": []map[string]any{{"content": content}}})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
}

// listKBDocsForIdentifierTest returns the current documentDetails for the
// fixture's data source.
func listKBDocsForIdentifierTest(t *testing.T, f ingestionFixture) []map[string]any {
	t.Helper()

	rec := doRequest(t, f.h, f.e, http.MethodPost,
		"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/documents", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		DocumentDetails []map[string]any `json:"documentDetails"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp.DocumentDetails
}

// TestHandlerGetKBDocuments_RealWireIdentifier proves GetKnowledgeBaseDocuments
// decodes the real "documentIdentifiers" object-array body and finds the
// document ingested under that identifier, for both DataSourceType variants.
func TestHandlerGetKBDocuments_RealWireIdentifier(t *testing.T) {
	t.Parallel()

	for _, tc := range kbDocumentIdentifierCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newIngestionFixture(t)
			ingestKBDocForIdentifierTest(t, f, tc.content)

			getRec := doRequest(t, f.h, f.e, http.MethodPost,
				"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/documents/getDocuments",
				map[string]any{"documentIdentifiers": []map[string]any{tc.identifier}})
			require.Equal(t, http.StatusOK, getRec.Code, getRec.Body.String())

			var resp struct {
				DocumentDetails []map[string]any `json:"documentDetails"`
			}
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
			require.Len(t, resp.DocumentDetails, 1)

			assert.Equal(t, "INDEXED", resp.DocumentDetails[0]["status"])
			assert.Equal(t, tc.identifier, resp.DocumentDetails[0]["identifier"])
		})
	}
}

// TestHandlerDeleteKBDocuments_RealWireIdentifier proves
// DeleteKnowledgeBaseDocuments decodes the real "documentIdentifiers" body
// and actually deletes the targeted document -- not merely reports success
// while the document (and every document, since the field never parsed)
// remains untouched.
func TestHandlerDeleteKBDocuments_RealWireIdentifier(t *testing.T) {
	t.Parallel()

	for _, tc := range kbDocumentIdentifierCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newIngestionFixture(t)
			ingestKBDocForIdentifierTest(t, f, tc.content)

			require.Len(t, listKBDocsForIdentifierTest(t, f), 1, "precondition: document must exist before delete")

			deleteRec := doRequest(t, f.h, f.e, http.MethodPost,
				"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/documents/deleteDocuments",
				map[string]any{"documentIdentifiers": []map[string]any{tc.identifier}})
			require.Equal(t, http.StatusAccepted, deleteRec.Code, deleteRec.Body.String())

			var deleteResp struct {
				DocumentDetails []map[string]any `json:"documentDetails"`
			}
			require.NoError(t, json.Unmarshal(deleteRec.Body.Bytes(), &deleteResp))
			require.Len(t, deleteResp.DocumentDetails, 1)
			assert.Equal(t, "DELETING", deleteResp.DocumentDetails[0]["status"])

			assert.Empty(t, listKBDocsForIdentifierTest(t, f),
				"document must actually be gone after delete, not merely reported deleted")
		})
	}
}

// TestHandlerKBDocuments_MissingIdentifierIsValidationException proves an
// identifier that doesn't carry the sub-object its own dataSourceType
// requires (real AWS has no other way to name a document) is rejected as
// ValidationException -- part of DocumentIdentifier's declared error set for
// GetKnowledgeBaseDocuments/DeleteKnowledgeBaseDocuments -- rather than
// silently matching nothing.
func TestHandlerKBDocuments_MissingIdentifierIsValidationException(t *testing.T) {
	t.Parallel()

	paths := []string{"getDocuments", "deleteDocuments"}

	for _, p := range paths {
		t.Run(p, func(t *testing.T) {
			t.Parallel()

			f := newIngestionFixture(t)

			rec := doRequest(t, f.h, f.e, http.MethodPost,
				"/knowledgebases/"+f.kbID+"/datasources/"+f.dsID+"/documents/"+p,
				map[string]any{"documentIdentifiers": []map[string]any{
					{"dataSourceType": "CUSTOM"},
				}})

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Equal(t, "ValidationException", rec.Header().Get("X-Amzn-Errortype"))
		})
	}
}
