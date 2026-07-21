package cleanrooms_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

func newTestServer(t *testing.T) *echo.Echo {
	t.Helper()
	backend := cleanrooms.NewInMemoryBackend("123456789012", "us-east-1")
	h := cleanrooms.NewHandler(backend)
	e := echo.New()
	e.Any("/*", h.Handler())

	return e
}

func doRequest(
	t *testing.T,
	e *echo.Echo,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()
	var reqBody []byte
	if body != nil {
		var err error
		reqBody, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(reqBody))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

func TestCollaborationCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"CollaborationCRUD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)

			createBody := map[string]any{
				"name":                   "test-collab",
				"description":            "desc",
				"creatorDisplayName":     "Alice",
				"creatorMemberAbilities": []string{"CAN_QUERY"},
				"members":                []any{},
				"queryLogStatus":         "ENABLED",
			}

			// Create collaboration
			rec := doRequest(t, e, http.MethodPost, "/collaborations", createBody)
			if rec.Code != http.StatusOK {
				t.Fatalf("create: status %d want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			var createResp map[string]any
			if err := json.NewDecoder(rec.Body).Decode(&createResp); err != nil {
				t.Fatalf("decode create: %v", err)
			}
			if _, ok := createResp["collaboration"]; !ok {
				t.Fatalf("missing key %q in response: %v", "collaboration", createResp)
			}
			collabID := createResp["collaboration"].(map[string]any)["collaborationIdentifier"].(string)

			// List collaborations
			rec = doRequest(t, e, http.MethodGet, "/collaborations", nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("list: status %d want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			var listResp map[string]any
			if err := json.NewDecoder(rec.Body).Decode(&listResp); err != nil {
				t.Fatalf("decode list: %v", err)
			}
			if _, ok := listResp["collaborationList"]; !ok {
				t.Fatalf("missing key %q in response: %v", "collaborationList", listResp)
			}

			// Get collaboration
			rec = doRequest(t, e, http.MethodGet, "/collaborations/"+collabID, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("get: status %d: %s", rec.Code, rec.Body.String())
			}

			// Delete collaboration
			rec = doRequest(t, e, http.MethodDelete, "/collaborations/"+collabID, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("delete: status %d: %s", rec.Code, rec.Body.String())
			}

			// Get deleted collaboration returns 404
			rec = doRequest(t, e, http.MethodGet, "/collaborations/"+collabID, nil)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("get deleted: status %d want 404: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestConfiguredTableCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"ConfiguredTableCRUD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)

			createBody := map[string]any{
				"name":        "my-table",
				"description": "desc",
				"tableReference": map[string]any{
					"glue": map[string]any{"databaseName": "db", "tableName": "tbl"},
				},
				"allowedColumns": []string{"col1"},
				"analysisMethod": "DIRECT_QUERY",
			}

			// Create configured table
			rec := doRequest(t, e, http.MethodPost, "/configuredTables", createBody)
			if rec.Code != http.StatusOK {
				t.Fatalf("create: status %d want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			var createResp map[string]any
			if err := json.NewDecoder(rec.Body).Decode(&createResp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			ctID := createResp["configuredTable"].(map[string]any)["configuredTableIdentifier"].(string)

			// List configured tables
			rec = doRequest(t, e, http.MethodGet, "/configuredTables", nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("list: status %d want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
			}

			// Update configured table
			rec = doRequest(
				t,
				e,
				http.MethodPatch,
				"/configuredTables/"+ctID,
				map[string]any{"name": "new-name"},
			)
			if rec.Code != http.StatusOK {
				t.Fatalf("update: status %d: %s", rec.Code, rec.Body.String())
			}

			// Delete configured table
			rec = doRequest(t, e, http.MethodDelete, "/configuredTables/"+ctID, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("delete: status %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestMembershipCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"MembershipCRUD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)

			colRec := doRequest(t, e, http.MethodPost, "/collaborations", map[string]any{
				"name":                   "c1",
				"description":            "d",
				"creatorDisplayName":     "Bob",
				"creatorMemberAbilities": []string{},
				"members":                []any{},
				"queryLogStatus":         "DISABLED",
			})
			if colRec.Code != http.StatusOK {
				t.Fatalf("create collab: %s", colRec.Body.String())
			}
			var colResp map[string]any
			_ = json.NewDecoder(colRec.Body).Decode(&colResp)
			colID := colResp["collaboration"].(map[string]any)["collaborationIdentifier"].(string)

			createBody := map[string]any{
				"collaborationIdentifier": colID,
				"queryLogStatus":          "DISABLED",
			}

			// Create membership
			rec := doRequest(t, e, http.MethodPost, "/memberships", createBody)
			if rec.Code != http.StatusOK {
				t.Fatalf("create: status %d want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			var createResp map[string]any
			_ = json.NewDecoder(rec.Body).Decode(&createResp)
			mID := createResp["membership"].(map[string]any)["membershipIdentifier"].(string)

			// List memberships
			rec = doRequest(t, e, http.MethodGet, "/memberships", nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("list: status %d want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
			}

			// Get membership
			rec = doRequest(t, e, http.MethodGet, "/memberships/"+mID, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("get: status %d: %s", rec.Code, rec.Body.String())
			}

			// Delete membership
			rec = doRequest(t, e, http.MethodDelete, "/memberships/"+mID, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("delete: status %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestTagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"TagOperations"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)

			// TagResource/UntagResource/ListTagsForResource all document
			// ResourceNotFoundException for an ARN that isn't a real resource, so tag
			// a collaboration actually created through the API rather than a
			// fabricated ARN.
			colRec := doRequest(t, e, http.MethodPost, "/collaborations", map[string]any{
				"name": "tag-collab", "creatorDisplayName": "Dana",
				"creatorMemberAbilities": []string{}, "members": []any{}, "queryLogStatus": "DISABLED",
			})
			var colResp map[string]any
			_ = json.NewDecoder(colRec.Body).Decode(&colResp)
			testARN := colResp["collaboration"].(map[string]any)["arn"].(string)

			// Tag resource
			rec := doRequest(
				t,
				e,
				http.MethodPost,
				"/tags/"+testARN,
				map[string]any{"tags": map[string]string{"env": "test"}},
			)
			if rec.Code != http.StatusOK {
				t.Fatalf("tag: status %d want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
			}

			// List tags
			rec = doRequest(t, e, http.MethodGet, "/tags/"+testARN, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("list tags: status %d want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
			}

			// Untag resource
			rec = doRequest(t, e, http.MethodDelete, "/tags/"+testARN+"?tagKeys=env", nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("untag: status %d want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
			}
		})
	}
}

// TestTagOperations_UnknownResourceARN verifies TagResource, UntagResource,
// and ListTagsForResource all reject an ARN that doesn't correspond to any
// real resource with ResourceNotFoundException, matching the AWS API model
// (previously these silently no-op'd/succeeded on a fabricated ARN).
func TestTagOperations_UnknownResourceARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"TagOperations_UnknownResourceARN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)

			const fakeARN = "arn:aws:cleanrooms:us-east-1:123456789012:collaboration/does-not-exist"

			cases := []struct {
				body   map[string]any
				method string
				path   string
			}{
				{map[string]any{"tags": map[string]string{"env": "test"}}, http.MethodPost, "/tags/" + fakeARN},
				{nil, http.MethodGet, "/tags/" + fakeARN},
				{nil, http.MethodDelete, "/tags/" + fakeARN + "?tagKeys=env"},
			}
			for _, tc := range cases {
				rec := doRequest(t, e, tc.method, tc.path, tc.body)
				if rec.Code != http.StatusNotFound {
					t.Fatalf(
						"%s %s: status %d want %d: %s",
						tc.method, tc.path, rec.Code, http.StatusNotFound, rec.Body.String(),
					)
				}
			}
		})
	}
}

func TestProtectedQueryLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"ProtectedQueryLifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)

			// Create collaboration
			colRec := doRequest(t, e, http.MethodPost, "/collaborations", map[string]any{
				"name": "c2", "description": "d", "creatorDisplayName": "Carol",
				"creatorMemberAbilities": []string{}, "members": []any{}, "queryLogStatus": "DISABLED",
			})
			var colResp map[string]any
			_ = json.NewDecoder(colRec.Body).Decode(&colResp)
			colID := colResp["collaboration"].(map[string]any)["collaborationIdentifier"].(string)

			// Create membership
			memRec := doRequest(t, e, http.MethodPost, "/memberships",
				map[string]any{"collaborationIdentifier": colID, "queryLogStatus": "DISABLED"})
			var memResp map[string]any
			_ = json.NewDecoder(memRec.Body).Decode(&memResp)
			mID := memResp["membership"].(map[string]any)["membershipIdentifier"].(string)

			// Start protected query
			rec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/protectedQueries",
				map[string]any{
					"sqlParameters":       map[string]any{"queryString": "SELECT 1"},
					"resultConfiguration": map[string]any{},
				})
			if rec.Code != http.StatusOK {
				t.Fatalf("start query: status %d: %s", rec.Code, rec.Body.String())
			}
			var resp map[string]any
			_ = json.NewDecoder(rec.Body).Decode(&resp)
			if _, ok := resp["protectedQuery"]; !ok {
				t.Fatal("missing protectedQuery in response")
			}

			// List protected queries
			rec = doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/protectedQueries", nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("list queries: status %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}
