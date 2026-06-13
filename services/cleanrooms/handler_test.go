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

func newTestServer(t *testing.T) (*cleanrooms.Handler, *echo.Echo) {
	t.Helper()
	backend := cleanrooms.NewInMemoryBackend("123456789012", "us-east-1")
	h := cleanrooms.NewHandler(backend)
	e := echo.New()
	e.Any("/*", h.Handler())
	return h, e
}

func doRequest(t *testing.T, e *echo.Echo, method, path string, body any) *httptest.ResponseRecorder {
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

	type tc struct {
		name       string
		method     string
		path       string
		body       any
		wantStatus int
		wantKey    string
	}

	_, e := newTestServer(t)

	createBody := map[string]any{
		"name":                   "test-collab",
		"description":            "desc",
		"creatorDisplayName":     "Alice",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{},
		"queryLogStatus":         "ENABLED",
	}

	tests := []tc{
		{
			name:       "create collaboration",
			method:     http.MethodPost,
			path:       "/collaborations",
			body:       createBody,
			wantStatus: http.StatusOK,
			wantKey:    "collaboration",
		},
		{
			name:       "list collaborations",
			method:     http.MethodGet,
			path:       "/collaborations",
			wantStatus: http.StatusOK,
			wantKey:    "collaborationList",
		},
	}

	var collabID string
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, e, tt.method, tt.path, tt.body)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status %d want %d: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			var resp map[string]any
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if _, ok := resp[tt.wantKey]; !ok {
				t.Fatalf("missing key %q in response: %v", tt.wantKey, resp)
			}
			if tt.wantKey == "collaboration" {
				c := resp["collaboration"].(map[string]any)
				collabID = c["collaborationIdentifier"].(string)
			}
		})
	}

	t.Run("get collaboration", func(t *testing.T) {
		rec := doRequest(t, e, http.MethodGet, "/collaborations/"+collabID, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("delete collaboration", func(t *testing.T) {
		rec := doRequest(t, e, http.MethodDelete, "/collaborations/"+collabID, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("get deleted collaboration returns 404", func(t *testing.T) {
		rec := doRequest(t, e, http.MethodGet, "/collaborations/"+collabID, nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status %d want 404: %s", rec.Code, rec.Body.String())
		}
	})
}

func TestConfiguredTableCRUD(t *testing.T) {
	t.Parallel()

	_, e := newTestServer(t)

	type tc struct {
		name       string
		method     string
		path       string
		body       any
		wantStatus int
	}

	createBody := map[string]any{
		"name":           "my-table",
		"description":    "desc",
		"tableReference": map[string]any{"glue": map[string]any{"databaseName": "db", "tableName": "tbl"}},
		"allowedColumns": []string{"col1"},
		"analysisMethod": "DIRECT_QUERY",
	}

	tests := []tc{
		{
			name:       "create",
			method:     http.MethodPost,
			path:       "/configuredTables",
			body:       createBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "list",
			method:     http.MethodGet,
			path:       "/configuredTables",
			wantStatus: http.StatusOK,
		},
	}

	var ctID string
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, e, tt.method, tt.path, tt.body)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status %d want %d: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.method == http.MethodPost {
				var resp map[string]any
				if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
					t.Fatalf("decode: %v", err)
				}
				ct := resp["configuredTable"].(map[string]any)
				ctID = ct["configuredTableIdentifier"].(string)
			}
		})
	}

	t.Run("update", func(t *testing.T) {
		rec := doRequest(t, e, http.MethodPatch, "/configuredTables/"+ctID, map[string]any{"name": "new-name"})
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("delete", func(t *testing.T) {
		rec := doRequest(t, e, http.MethodDelete, "/configuredTables/"+ctID, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	})
}

func TestMembershipCRUD(t *testing.T) {
	t.Parallel()

	_, e := newTestServer(t)

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

	type tc struct {
		name       string
		method     string
		path       string
		body       any
		wantStatus int
	}

	tests := []tc{
		{name: "create", method: http.MethodPost, path: "/memberships", body: createBody, wantStatus: http.StatusOK},
		{name: "list", method: http.MethodGet, path: "/memberships", wantStatus: http.StatusOK},
	}

	var mID string
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, e, tt.method, tt.path, tt.body)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status %d want %d: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.method == http.MethodPost {
				var resp map[string]any
				_ = json.NewDecoder(rec.Body).Decode(&resp)
				mID = resp["membership"].(map[string]any)["membershipIdentifier"].(string)

			}
		})
	}

	t.Run("get", func(t *testing.T) {
		rec := doRequest(t, e, http.MethodGet, "/memberships/"+mID, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("delete", func(t *testing.T) {
		rec := doRequest(t, e, http.MethodDelete, "/memberships/"+mID, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	})
}

func TestTagOperations(t *testing.T) {
	t.Parallel()

	_, e := newTestServer(t)

	const testARN = "arn:aws:cleanrooms:us-east-1:123456789012:collaboration/abc123"

	type tc struct {
		name       string
		method     string
		path       string
		body       any
		wantStatus int
	}

	tests := []tc{
		{
			name:       "tag resource",
			method:     http.MethodPost,
			path:       "/tags/" + testARN,
			body:       map[string]any{"tags": map[string]string{"env": "test"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "list tags",
			method:     http.MethodGet,
			path:       "/tags/" + testARN,
			wantStatus: http.StatusOK,
		},
		{
			name:       "untag resource",
			method:     http.MethodDelete,
			path:       "/tags/" + testARN + "?tagKeys=env",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, e, tt.method, tt.path, tt.body)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status %d want %d: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestProtectedQueryLifecycle(t *testing.T) {
	t.Parallel()

	_, e := newTestServer(t)

	// Create membership
	colRec := doRequest(t, e, http.MethodPost, "/collaborations", map[string]any{
		"name": "c2", "description": "d", "creatorDisplayName": "Carol",
		"creatorMemberAbilities": []string{}, "members": []any{}, "queryLogStatus": "DISABLED",
	})
	var colResp map[string]any
	_ = json.NewDecoder(colRec.Body).Decode(&colResp)
	colID := colResp["collaboration"].(map[string]any)["collaborationIdentifier"].(string)

	memRec := doRequest(t, e, http.MethodPost, "/memberships",
		map[string]any{"collaborationIdentifier": colID, "queryLogStatus": "DISABLED"})
	var memResp map[string]any
	_ = json.NewDecoder(memRec.Body).Decode(&memResp)
	mID := memResp["membership"].(map[string]any)["membershipIdentifier"].(string)

	// Start protected query
	t.Run("start protected query", func(t *testing.T) {
		rec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/protectedQueries",
			map[string]any{
				"sqlParameters":       map[string]any{"queryString": "SELECT 1"},
				"resultConfiguration": map[string]any{},
			})
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		var resp map[string]any
		_ = json.NewDecoder(rec.Body).Decode(&resp)
		if _, ok := resp["protectedQuery"]; !ok {
			t.Fatal("missing protectedQuery in response")
		}
	})

	// List protected queries
	t.Run("list protected queries", func(t *testing.T) {
		rec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/protectedQueries", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	})
}
