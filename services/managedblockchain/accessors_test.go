package managedblockchain_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

func TestHandler_CreateAccessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantKey    string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"AccessorType":       "BILLING_TOKEN",
				"NetworkType":        "ETHEREUM_MAINNET",
				"ClientRequestToken": "tok-accessor",
			},
			wantStatus: http.StatusOK,
			wantKey:    "AccessorId",
		},
		{
			// Real AWS's client-side validator marks ClientRequestToken required
			// (validators.go, v1.34.4); a raw HTTP caller omitting it is rejected.
			name:       "empty body is rejected for missing ClientRequestToken",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.body == nil {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/accessors", bytes.NewReader([]byte("{bad json")))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				c := e.NewContext(req, w)
				err := h.Handler()(c)
				require.NoError(t, err)
				rec = w
			} else {
				rec = doRequest(t, h, http.MethodPost, "/accessors", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantKey)
			}
		})
	}
}

func TestHandler_GetAccessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accessorID string
		wantStatus int
	}{
		{
			name:       "get existing accessor",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			accessorID: "nonexistent-accessor-id",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/accessors", map[string]any{
				"AccessorType":       "BILLING_TOKEN",
				"ClientRequestToken": "tok-accessor-crud",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var out struct {
				AccessorID string `json:"AccessorId"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&out))

			id := tt.accessorID
			if id == "" {
				id = out.AccessorID
			}

			rec := doRequest(t, h, http.MethodGet, "/accessors/"+id, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Accessor")
			}
		})
	}
}

func TestHandler_DeleteAccessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "success", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/accessors", map[string]any{
				"AccessorType":       "BILLING_TOKEN",
				"ClientRequestToken": "tok-accessor-crud",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var out struct {
				AccessorID string `json:"AccessorId"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&out))

			rec := doRequest(t, h, http.MethodDelete, "/accessors/"+out.AccessorID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Verify deleted.
			getRec := doRequest(t, h, http.MethodGet, "/accessors/"+out.AccessorID, nil)
			assert.Equal(t, http.StatusNotFound, getRec.Code)
		})
	}
}

func TestHandler_ListAccessors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createCount int
		wantLen     int
		wantStatus  int
	}{
		{name: "empty list", createCount: 0, wantLen: 0, wantStatus: http.StatusOK},
		{name: "two accessors", createCount: 2, wantLen: 2, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.createCount {
				rec := doRequest(t, h, http.MethodPost, "/accessors", map[string]any{
					"AccessorType":       "BILLING_TOKEN",
					"ClientRequestToken": fmt.Sprintf("tok-listaccessor-%d", i),
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/accessors", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			accessors, ok := resp["Accessors"].([]any)
			require.True(t, ok)
			assert.Len(t, accessors, tt.wantLen)
		})
	}
}

func TestHandler_AccessorRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create get list delete accessor"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create.
			createRec := doRequest(t, h, http.MethodPost, "/accessors", map[string]any{
				"AccessorType":       "BILLING_TOKEN",
				"NetworkType":        "ETHEREUM_MAINNET",
				"ClientRequestToken": "tok-accessor-tags",
				"Tags":               map[string]string{"env": "test"},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				AccessorID   string `json:"AccessorId"`
				BillingToken string `json:"BillingToken"`
				NetworkType  string `json:"NetworkType"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
			assert.NotEmpty(t, createOut.AccessorID)
			assert.NotEmpty(t, createOut.BillingToken)
			assert.Equal(t, "ETHEREUM_MAINNET", createOut.NetworkType)

			// Get.
			getRec := doRequest(t, h, http.MethodGet, "/accessors/"+createOut.AccessorID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut struct {
				Accessor map[string]any `json:"Accessor"`
			}
			require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
			assert.Equal(t, "AVAILABLE", getOut.Accessor["Status"])

			// List.
			listRec := doRequest(t, h, http.MethodGet, "/accessors", nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut struct {
				Accessors []map[string]any `json:"Accessors"`
			}
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
			assert.Len(t, listOut.Accessors, 1)

			// Delete.
			delRec := doRequest(t, h, http.MethodDelete, "/accessors/"+createOut.AccessorID, nil)
			assert.Equal(t, http.StatusNoContent, delRec.Code)

			// Verify gone.
			listRec2 := doRequest(t, h, http.MethodGet, "/accessors", nil)
			require.Equal(t, http.StatusOK, listRec2.Code)

			var listOut2 struct {
				Accessors []map[string]any `json:"Accessors"`
			}
			require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&listOut2))
			assert.Empty(t, listOut2.Accessors)
		})
	}
}

func TestHandler_AccessorDeleteNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "delete nonexistent accessor", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodDelete, "/accessors/nonexistent", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListAccessorsFilters verifies networkType filter for ListAccessors.
func TestHandler_ListAccessorsFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query     map[string]string
		name      string
		wantCount int
	}{
		{
			name:      "no filter returns all",
			query:     map[string]string{},
			wantCount: 2,
		},
		{
			name:      "filter by ETHEREUM_MAINNET",
			query:     map[string]string{"networkType": "ETHEREUM_MAINNET"},
			wantCount: 1,
		},
		{
			name:      "filter by ETHEREUM_GOERLI",
			query:     map[string]string{"networkType": "ETHEREUM_GOERLI"},
			wantCount: 1,
		},
		{
			name:      "filter by unknown type returns none",
			query:     map[string]string{"networkType": "UNKNOWN_NET"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")
			b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_GOERLI")

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doRequestWithQuery(t, h, "/accessors", tt.query)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			accessors := resp["Accessors"].([]any)
			assert.Len(t, accessors, tt.wantCount)
		})
	}
}

// TestInMemoryBackend_ListAccessorsFilter verifies backend-level filtering for ListAccessors.
func TestInMemoryBackend_ListAccessorsFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    managedblockchain.ListAccessorsFilter
		wantCount int
	}{
		{
			name:      "empty filter returns all",
			filter:    managedblockchain.ListAccessorsFilter{},
			wantCount: 2,
		},
		{
			name:      "filter by ETHEREUM_MAINNET",
			filter:    managedblockchain.ListAccessorsFilter{NetworkType: "ETHEREUM_MAINNET"},
			wantCount: 1,
		},
		{
			name:      "filter by ETHEREUM_GOERLI",
			filter:    managedblockchain.ListAccessorsFilter{NetworkType: "ETHEREUM_GOERLI"},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")
			b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_GOERLI")

			accessors, err := b.ListAccessors(tt.filter)
			require.NoError(t, err)
			assert.Len(t, accessors, tt.wantCount)
		})
	}
}

// TestInMemoryBackend_TagAccessor verifies accessors can be tagged.
func TestInMemoryBackend_TagAccessor(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	a := b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")

	err := b.TagResource(a.Arn, map[string]string{"project": "defi"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(a.Arn)
	require.NoError(t, err)
	assert.Equal(t, "defi", tags["project"])

	err = b.UntagResource(a.Arn, []string{"project"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(a.Arn)
	require.NoError(t, err)
	assert.Empty(t, tags)
}

// TestInMemoryBackend_DeleteAccessorARNCleanup verifies accessor ARN is removed on delete.
func TestInMemoryBackend_DeleteAccessorARNCleanup(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	a := b.AddAccessorInternal(testRegion, testAccountID, "BILLING_TOKEN", "ETHEREUM_MAINNET")

	require.Equal(t, 1, managedblockchain.ARNIndexSize(b))

	err := b.DeleteAccessor(a.ID)
	require.NoError(t, err)

	assert.Equal(t, 0, managedblockchain.ARNIndexSize(b))

	// Tagging deleted accessor should fail
	err = b.TagResource(a.Arn, map[string]string{"k": "v"})
	require.Error(t, err)
}

// TestHandler_AccessorLifecycleViaHTTP exercises accessor CRUD over HTTP.
func TestHandler_AccessorLifecycleViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "accessor round trip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// CreateAccessor
			rec := doRequest(t, h, http.MethodPost, "/accessors", map[string]any{
				"AccessorType":       "BILLING_TOKEN",
				"NetworkType":        "ETHEREUM_MAINNET",
				"ClientRequestToken": "tok-accessor-roundtrip",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			accessorID, _ := createResp["AccessorId"].(string)
			require.NotEmpty(t, accessorID)
			assert.NotEmpty(t, createResp["BillingToken"])

			// GetAccessor
			rec2 := doRequest(t, h, http.MethodGet, "/accessors/"+accessorID, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

			accessor := getResp["Accessor"].(map[string]any)
			assert.Equal(t, accessorID, accessor["Id"])
			assert.Equal(t, "BILLING_TOKEN", accessor["Type"])

			// ListAccessors
			rec3 := doRequest(t, h, http.MethodGet, "/accessors", nil)
			require.Equal(t, http.StatusOK, rec3.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listResp))
			accessors := listResp["Accessors"].([]any)
			assert.Len(t, accessors, 1)

			// DeleteAccessor
			rec4 := doRequest(t, h, http.MethodDelete, "/accessors/"+accessorID, nil)
			require.Equal(t, http.StatusNoContent, rec4.Code)

			// Verify deleted
			rec5 := doRequest(t, h, http.MethodGet, "/accessors/"+accessorID, nil)
			assert.Equal(t, http.StatusNotFound, rec5.Code)
		})
	}
}
