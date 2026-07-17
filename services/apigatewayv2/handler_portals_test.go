package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreatePortal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantPortal bool
	}{
		{
			name:       "success",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
			wantPortal: true,
		},
		{
			name:       "with_logo",
			body:       map[string]any{"logoUri": "https://example.com/logo.png"},
			wantStatus: http.StatusCreated,
			wantPortal: true,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "method_not_allowed",
			body:       nil,
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var rr *httptest.ResponseRecorder

			if tt.name == "method_not_allowed" {
				rr = doRequest(t, h, http.MethodDelete, "/v2/portals", nil)
			} else if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, "/v2/portals", s)
			} else {
				rr = doRequest(t, h, http.MethodPost, "/v2/portals", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantPortal {
				var portal apigatewayv2.Portal
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &portal))
				assert.NotEmpty(t, portal.PortalID)
				assert.Equal(t, "ACTIVE", portal.Status)
			}
		})
	}
}

func TestHandler_CreatePortalProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"displayName": "My Product"},
			wantName:   "My Product",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "with_description",
			body:       map[string]any{"displayName": "Product 2", "description": "A product"},
			wantName:   "Product 2",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, "/v2/portalproducts", s)
			} else {
				rr = doRequest(t, h, http.MethodPost, "/v2/portalproducts", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantName != "" {
				var product apigatewayv2.PortalProduct
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &product))
				assert.Equal(t, tt.wantName, product.DisplayName)
				assert.NotEmpty(t, product.PortalProductID)
			}
		})
	}
}

func TestHandler_CreateProductPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            any
		name            string
		portalProductID string
		wantStatus      int
		wantPage        bool
	}{
		{
			name:       "success",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
			wantPage:   true,
		},
		{
			name:            "product_not_found",
			portalProductID: "nonexistent",
			body:            map[string]any{},
			wantStatus:      http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			portalProductID := tt.portalProductID
			if portalProductID == "" {
				rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts",
					map[string]any{"displayName": "Test Product"})
				require.Equal(t, http.StatusCreated, rr.Code)

				var product apigatewayv2.PortalProduct
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &product))
				portalProductID = product.PortalProductID
			}

			path := fmt.Sprintf("/v2/portalproducts/%s/productpages", portalProductID)

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, path, s)
			} else {
				rr = doRequest(t, h, http.MethodPost, path, tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantPage {
				var page apigatewayv2.ProductPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &page))
				assert.NotEmpty(t, page.ProductPageID)
			}
		})
	}
}

func TestHandler_CreateProductRestEndpointPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            any
		name            string
		portalProductID string
		wantStatus      int
		wantPage        bool
	}{
		{
			name:       "success",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
			wantPage:   true,
		},
		{
			name:            "product_not_found",
			portalProductID: "nonexistent",
			body:            map[string]any{},
			wantStatus:      http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			portalProductID := tt.portalProductID
			if portalProductID == "" {
				rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts",
					map[string]any{"displayName": "Test Product"})
				require.Equal(t, http.StatusCreated, rr.Code)

				var product apigatewayv2.PortalProduct
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &product))
				portalProductID = product.PortalProductID
			}

			path := fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", portalProductID)

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, path, s)
			} else {
				rr = doRequest(t, h, http.MethodPost, path, tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantPage {
				var page apigatewayv2.ProductRestEndpointPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &page))
				assert.NotEmpty(t, page.ProductRestEndpointPageID)
			}
		})
	}
}

func TestHandler_ListPortals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		portalCnt  int
		wantStatus int
	}{
		{
			name:       "empty",
			portalCnt:  0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple",
			portalCnt:  2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for range tt.portalCnt {
				rr := doRequest(t, h, http.MethodPost, "/v2/portals", map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, "/v2/portals", nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			var out struct {
				Items []apigatewayv2.Portal `json:"items"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
			assert.Len(t, out.Items, tt.portalCnt)
		})
	}
}

func TestHandler_GetPortal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "found",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rr := doRequest(t, h, http.MethodPost, "/v2/portals", map[string]any{})
			require.Equal(t, http.StatusCreated, rr.Code)

			var portal apigatewayv2.Portal
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &portal))

			portalID := portal.PortalID
			if tt.useWrongID {
				portalID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodGet, "/v2/portals/"+portalID, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_ListPortalProducts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		productCnt int
		wantStatus int
	}{
		{
			name:       "empty",
			productCnt: 0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple",
			productCnt: 2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for i := range tt.productCnt {
				rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{
					"displayName": fmt.Sprintf("Product %d", i),
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, "/v2/portalproducts", nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			var out struct {
				Items []apigatewayv2.PortalProduct `json:"items"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
			assert.Len(t, out.Items, tt.productCnt)
		})
	}
}

func TestHandler_GetPortalProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "found",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{
				"displayName": "My Product",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var pp apigatewayv2.PortalProduct
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &pp))

			ppID := pp.PortalProductID
			if tt.useWrongID {
				ppID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodGet, "/v2/portalproducts/"+ppID, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_ListProductPages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ppExists   bool
		pageCnt    int
		wantStatus int
	}{
		{
			name:       "empty",
			ppExists:   true,
			pageCnt:    0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "one_page",
			ppExists:   true,
			pageCnt:    1,
			wantStatus: http.StatusOK,
		},
		{
			name:       "portal_product_not_found",
			ppExists:   false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			ppID := "nonexistent"
			if tt.ppExists {
				rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{
					"displayName": "My Product",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var pp apigatewayv2.PortalProduct
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &pp))
				ppID = pp.PortalProductID
			}

			for range tt.pageCnt {
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/portalproducts/%s/productpages", ppID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.ProductPage `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.pageCnt)
			}
		})
	}
}

func TestHandler_ListProductRestEndpointPages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ppExists   bool
		pageCnt    int
		wantStatus int
	}{
		{
			name:       "empty",
			ppExists:   true,
			pageCnt:    0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "one_page",
			ppExists:   true,
			pageCnt:    1,
			wantStatus: http.StatusOK,
		},
		{
			name:       "portal_product_not_found",
			ppExists:   false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			ppID := "nonexistent"
			if tt.ppExists {
				rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{
					"displayName": "My Product",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var pp apigatewayv2.PortalProduct
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &pp))
				ppID = pp.PortalProductID
			}

			for range tt.pageCnt {
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", ppID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.ProductRestEndpointPage `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.pageCnt)
			}
		})
	}
}

func createPortal(t *testing.T, h *apigatewayv2.Handler) string {
	t.Helper()
	rr := doRequest(t, h, http.MethodPost, "/v2/portals", map[string]any{})
	require.Equal(t, http.StatusCreated, rr.Code)
	var p apigatewayv2.Portal
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &p))

	return p.PortalID
}

func createPortalProduct(t *testing.T, h *apigatewayv2.Handler) string {
	t.Helper()
	rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{
		"displayName": "test product",
	})
	require.Equal(t, http.StatusCreated, rr.Code)
	var pp apigatewayv2.PortalProduct
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &pp))

	return pp.PortalProductID
}

func TestHandler_UpdatePortal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) string {
				return createPortal(t, h)
			},
			body:       map[string]any{"logoUri": "https://example.com/logo.png"},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent"
			},
			body:       map[string]any{"logoUri": "https://example.com/logo.png"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			portalID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch, "/v2/portals/"+portalID, tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeletePortal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) string {
				return createPortal(t, h)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			portalID := tt.setup(h)

			rr := doRequest(t, h, http.MethodDelete, "/v2/portals/"+portalID, nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdatePortalProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) string {
				return createPortalProduct(t, h)
			},
			body:       map[string]any{"displayName": "Updated Product"},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent"
			},
			body:       map[string]any{"displayName": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch, "/v2/portalproducts/"+ppID, tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeletePortalProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) string {
				return createPortalProduct(t, h)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID := tt.setup(h)

			rr := doRequest(t, h, http.MethodDelete, "/v2/portalproducts/"+ppID, nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetProductPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (ppID, pageID string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				ppID := createPortalProduct(t, h)
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
				var p apigatewayv2.ProductPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &p))

				return ppID, p.ProductPageID
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "product_not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string) {
				return "nonexistent", "page123"
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "page_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createPortalProduct(t, h), "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID, pageID := tt.setup(h)

			rr := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/portalproducts/%s/productpages/%s", ppID, pageID), nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteProductPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (ppID, pageID string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				ppID := createPortalProduct(t, h)
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
				var p apigatewayv2.ProductPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &p))

				return ppID, p.ProductPageID
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createPortalProduct(t, h), "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID, pageID := tt.setup(h)

			rr := doRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/v2/portalproducts/%s/productpages/%s", ppID, pageID), nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetProductRestEndpointPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (ppID, pageID string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				ppID := createPortalProduct(t, h)
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
				var p apigatewayv2.ProductRestEndpointPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &p))

				return ppID, p.ProductRestEndpointPageID
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "page_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createPortalProduct(t, h), "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID, pageID := tt.setup(h)

			rr := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages/%s", ppID, pageID), nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteProductRestEndpointPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (ppID, pageID string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				ppID := createPortalProduct(t, h)
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
				var p apigatewayv2.ProductRestEndpointPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &p))

				return ppID, p.ProductRestEndpointPageID
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createPortalProduct(t, h), "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID, pageID := tt.setup(h)

			rr := doRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages/%s", ppID, pageID), nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}
