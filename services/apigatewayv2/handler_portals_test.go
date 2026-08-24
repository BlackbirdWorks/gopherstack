package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// validCustomColorsBody returns a body fragment satisfying CustomColors'
// six required color fields (validateCustomColors, validators.go).
func validCustomColorsBody() map[string]any {
	return map[string]any{
		"accentColor":          "#000000",
		"backgroundColor":      "#ffffff",
		"errorValidationColor": "#ff0000",
		"headerColor":          "#111111",
		"navigationColor":      "#222222",
		"textColor":            "#333333",
	}
}

// validCreatePortalBody returns a CreatePortal request body satisfying every
// required member: Authorization, EndpointConfiguration, and PortalContent
// (with its required Theme.CustomColors) -- see validateOpCreatePortalInput.
func validCreatePortalBody() map[string]any {
	return map[string]any{
		"authorization":         map[string]any{"none": map[string]any{}},
		"endpointConfiguration": map[string]any{"none": map[string]any{}},
		"portalContent": map[string]any{
			"displayName": "My Portal",
			"theme":       map[string]any{"customColors": validCustomColorsBody()},
		},
	}
}

func TestHandler_CreatePortal(t *testing.T) {
	t.Parallel()

	withLogo := validCreatePortalBody()
	withLogo["logoUri"] = "https://example.com/logo.png"

	missingAuth := validCreatePortalBody()
	delete(missingAuth, "authorization")

	missingPortalContent := validCreatePortalBody()
	delete(missingPortalContent, "portalContent")

	missingCustomColors := validCreatePortalBody()
	missingCustomColors["portalContent"] = map[string]any{
		"displayName": "My Portal",
		"theme":       map[string]any{},
	}

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantPortal bool
	}{
		{
			name:       "success",
			body:       validCreatePortalBody(),
			wantStatus: http.StatusCreated,
			wantPortal: true,
		},
		{
			name:       "with_logo",
			body:       withLogo,
			wantStatus: http.StatusCreated,
			wantPortal: true,
		},
		{
			name:       "missing_authorization",
			body:       missingAuth,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_portal_content",
			body:       missingPortalContent,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_custom_colors",
			body:       missingCustomColors,
			wantStatus: http.StatusBadRequest,
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
				assert.Empty(t, portal.PublishStatus)
				require.NotNil(t, portal.LastModified)
				require.NotNil(t, portal.Authorization)
				require.NotNil(t, portal.EndpointConfiguration)
				assert.NotEmpty(t, portal.EndpointConfiguration.PortalDefaultDomainName)
				assert.NotEmpty(t, portal.EndpointConfiguration.PortalDomainHostedZoneID)
				require.NotNil(t, portal.PortalContent)
				assert.Equal(t, "My Portal", portal.PortalContent.DisplayName)
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

// validRestEndpointIdentifierBody returns a CreateProductRestEndpointPage
// request body satisfying its required RestEndpointIdentifier member --
// IdentifierParts itself is optional, but its four fields are required
// whenever it's present (validateOpCreateProductRestEndpointPageInput,
// validators.go).
func validRestEndpointIdentifierBody() map[string]any {
	return map[string]any{
		"restEndpointIdentifier": map[string]any{
			"identifierParts": map[string]any{
				"method":    "GET",
				"path":      "/widgets",
				"restApiId": "abc123",
				"stage":     "prod",
			},
		},
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
			body:       validRestEndpointIdentifierBody(),
			wantStatus: http.StatusCreated,
			wantPage:   true,
		},
		{
			name:            "product_not_found",
			portalProductID: "nonexistent",
			body:            validRestEndpointIdentifierBody(),
			wantStatus:      http.StatusNotFound,
		},
		{
			name:       "missing_rest_endpoint_identifier",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "incomplete_identifier_parts",
			body: map[string]any{
				"restEndpointIdentifier": map[string]any{
					"identifierParts": map[string]any{"method": "GET"},
				},
			},
			wantStatus: http.StatusBadRequest,
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
				require.NotNil(t, page.RestEndpointIdentifier)
				require.NotNil(t, page.RestEndpointIdentifier.IdentifierParts)
				assert.Equal(t, "GET", page.RestEndpointIdentifier.IdentifierParts.Method)
				assert.Equal(t, "/widgets", page.RestEndpointIdentifier.IdentifierParts.Path)
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
				rr := doRequest(t, h, http.MethodPost, "/v2/portals", validCreatePortalBody())
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

			rr := doRequest(t, h, http.MethodPost, "/v2/portals", validCreatePortalBody())
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
				rr := doRequest(
					t,
					h,
					http.MethodPost,
					fmt.Sprintf(
						"/v2/portalproducts/%s/productrestendpointpages",
						ppID,
					),
					validRestEndpointIdentifierBody(),
				)
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
	rr := doRequest(t, h, http.MethodPost, "/v2/portals", validCreatePortalBody())
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
			// DeletePortal is idempotent: apigatewayv2@v1.37.4's own
			// deserializeOpErrorDeletePortal has no NotFoundException case,
			// so a real client can never type a 404 here (unlike GetPortal/
			// UpdatePortal/DisablePortal/DeletePortalProduct, which all model
			// it). Matches that asymmetry with a 204 regardless.
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent"
			},
			wantStatus: http.StatusNoContent,
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
				rr := doRequest(
					t,
					h,
					http.MethodPost,
					fmt.Sprintf(
						"/v2/portalproducts/%s/productrestendpointpages",
						ppID,
					),
					validRestEndpointIdentifierBody(),
				)
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
				rr := doRequest(
					t,
					h,
					http.MethodPost,
					fmt.Sprintf(
						"/v2/portalproducts/%s/productrestendpointpages",
						ppID,
					),
					validRestEndpointIdentifierBody(),
				)
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
