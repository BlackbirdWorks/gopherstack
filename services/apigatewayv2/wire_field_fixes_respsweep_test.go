package apigatewayv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestPortalFamily_RequiredSummaryFieldsSurviveOmission drives Create/List at
// the raw-HTTP level for the Portal/PortalProduct/ProductPage family and
// asserts that required List(Summary)-shape members
// (PortalSummary.IncludedPortalProductArns, PortalProductSummary.Description,
// ProductPageSummaryNoBody.ProductPageArn/PageTitle -- apigatewayv2@v1.37.4
// types.go:985/941/1080/1075) stay PRESENT in the JSON body even when the
// client omits the corresponding optional Create input
// (IncludedPortalProductArns, Description) or the field has no
// client-supplied source at all (ProductPageArn, PageTitle). Before the fix
// these keys were silently dropped by omitempty on a reachable-empty value,
// or never populated at all. A typed SDK-client assertion can't distinguish
// "key absent" from "key present with the Go zero value" after unmarshaling
// a []string/string into a struct field, so this test inspects the raw JSON
// via map[string]any instead (gopherstack-mven).
func TestPortalFamily_RequiredSummaryFieldsSurviveOmission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *apigatewayv2.Handler)
		name string
	}{
		{
			name: "portal includedPortalProductArns",
			run:  testPortalIncludedPortalProductArnsPresent,
		},
		{
			name: "portalProduct description",
			run:  testPortalProductDescriptionPresent,
		},
		{
			name: "productPage arn and title",
			run:  testProductPageArnAndTitlePresent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newTestHandler())
		})
	}
}

func testPortalIncludedPortalProductArnsPresent(t *testing.T, h *apigatewayv2.Handler) {
	t.Helper()

	rr := doRequest(t, h, http.MethodPost, "/v2/portals", map[string]any{
		"authorization":         map[string]any{"none": map[string]any{}},
		"endpointConfiguration": map[string]any{"none": map[string]any{}},
		"portalContent": map[string]any{
			"displayName": "p1",
			"theme": map[string]any{
				"customColors": map[string]any{
					"accentColor": "#000", "backgroundColor": "#000",
					"errorValidationColor": "#000", "headerColor": "#000",
					"navigationColor": "#000", "textColor": "#000",
				},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &created))
	arns, ok := created["includedPortalProductArns"]
	assert.True(t, ok, "includedPortalProductArns key must be present on Create")
	assert.Equal(t, []any{}, arns)

	rr = doRequest(t, h, http.MethodGet, "/v2/portals", nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var listed struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &listed))
	require.Len(t, listed.Items, 1)

	arns, ok = listed.Items[0]["includedPortalProductArns"]
	assert.True(t, ok, "includedPortalProductArns key must be present on ListPortals")
	assert.Equal(t, []any{}, arns)
}

func testPortalProductDescriptionPresent(t *testing.T, h *apigatewayv2.Handler) {
	t.Helper()

	rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{"displayName": "pp1"})
	require.Equal(t, http.StatusCreated, rr.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &created))
	desc, ok := created["description"]
	assert.True(t, ok, "description key must be present on Create")
	assert.Empty(t, desc)

	rr = doRequest(t, h, http.MethodGet, "/v2/portalproducts", nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var listed struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &listed))
	require.Len(t, listed.Items, 1)

	desc, ok = listed.Items[0]["description"]
	assert.True(t, ok, "description key must be present on ListPortalProducts")
	assert.Empty(t, desc)
}

func testProductPageArnAndTitlePresent(t *testing.T, h *apigatewayv2.Handler) {
	t.Helper()

	rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{"displayName": "pp2"})
	require.Equal(t, http.StatusCreated, rr.Code)

	var product map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &product))
	portalProductID, _ := product["portalProductId"].(string)
	require.NotEmpty(t, portalProductID)

	path := "/v2/portalproducts/" + portalProductID + "/productpages"
	rr = doRequest(t, h, http.MethodPost, path, map[string]any{
		"displayContent": map[string]any{"body": "b", "title": "My Page"},
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &created))
	assert.NotEmpty(t, created["productPageArn"])
	assert.Equal(t, "My Page", created["pageTitle"])

	rr = doRequest(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var listed struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &listed))
	require.Len(t, listed.Items, 1)
	assert.NotEmpty(t, listed.Items[0]["productPageArn"])
	assert.Equal(t, "My Page", listed.Items[0]["pageTitle"])
}
