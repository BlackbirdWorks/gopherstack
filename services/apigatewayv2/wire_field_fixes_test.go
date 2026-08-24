package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestListRoutingRules_WireKey drives ListRoutingRules through the real SDK
// client. Before the fix, gopherstack's listRoutingRulesOutput wrapped items
// under "items" -- every other List/Get collection op in this service's
// wrapper key, but ListRoutingRulesOutput alone uses "routingRules"
// (aws-sdk-go-v2/service/apigatewayv2@v1.37.4's api_op_ListRoutingRules.go:56,
// confirmed in deserializers.go's
// awsRestjson1_deserializeOpDocumentListRoutingRulesOutput case list). A real
// client's typed .RoutingRules field was always empty regardless of backend
// state.
func TestListRoutingRules_WireKey(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	dn, err := client.CreateDomainName(t.Context(), &apigatewayv2sdk.CreateDomainNameInput{
		DomainName: aws.String("rr-wirekey.example.com"),
	})
	require.NoError(t, err)

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:         aws.String("rr-wirekey-api"),
		ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	_, err = client.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
		ApiId:     api.ApiId,
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	created, err := client.CreateRoutingRule(t.Context(), &apigatewayv2sdk.CreateRoutingRuleInput{
		DomainName: dn.DomainName,
		Priority:   aws.Int32(1),
		Actions: []apigatewayv2types.RoutingRuleAction{
			{InvokeApi: &apigatewayv2types.RoutingRuleActionInvokeApi{
				ApiId: api.ApiId,
				Stage: aws.String("prod"),
			}},
		},
		Conditions: []apigatewayv2types.RoutingRuleCondition{
			{MatchBasePaths: &apigatewayv2types.RoutingRuleMatchBasePaths{AnyOf: []string{"/foo"}}},
		},
	})
	require.NoError(t, err)

	out, err := client.ListRoutingRules(t.Context(), &apigatewayv2sdk.ListRoutingRulesInput{
		DomainName: dn.DomainName,
	})
	require.NoError(t, err)
	require.Len(t, out.RoutingRules, 1)
	require.Equal(t, aws.ToString(created.RoutingRuleId), aws.ToString(out.RoutingRules[0].RoutingRuleId))
}

// TestPortal_PublishStatusWireKeyAndLifecycle drives CreatePortal/
// PublishPortal/DisablePortal/GetPortal through the real SDK client. Before
// the fix, gopherstack emitted the portal's publish state under "status"
// (also seeding a freshly-created portal with the invented value "ACTIVE",
// which exists nowhere in the real six-value PublishStatus enum); the real
// GetPortalOutput/PortalSummary member is "publishStatus"
// (api_op_GetPortal.go, deserializers.go's
// awsRestjson1_deserializeOpDocumentGetPortalOutput case list). A real
// client's typed .PublishStatus was always empty regardless of backend
// state, and UpdatePortalInput (which has no real "status" member at all,
// api_op_UpdatePortal.go) accepted one anyway from any caller.
func TestPortal_PublishStatusWireKeyAndLifecycle(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	created, err := client.CreatePortal(t.Context(), &apigatewayv2sdk.CreatePortalInput{
		Authorization: &apigatewayv2types.Authorization{
			None: &apigatewayv2types.None{},
		},
		EndpointConfiguration: &apigatewayv2types.EndpointConfigurationRequest{
			None: &apigatewayv2types.None{},
		},
		PortalContent: testPortalContent(),
	})
	require.NoError(t, err)
	require.Empty(t, created.PublishStatus, "a freshly created portal has no publish status yet")
	require.NotNil(t, created.LastModified)

	_, err = client.PublishPortal(t.Context(), &apigatewayv2sdk.PublishPortalInput{
		PortalId: created.PortalId,
	})
	require.NoError(t, err)

	got, err := client.GetPortal(t.Context(), &apigatewayv2sdk.GetPortalInput{
		PortalId: created.PortalId,
	})
	require.NoError(t, err)
	require.Equal(t, apigatewayv2types.PublishStatusPublished, got.PublishStatus)

	_, err = client.DisablePortal(t.Context(), &apigatewayv2sdk.DisablePortalInput{
		PortalId: created.PortalId,
	})
	require.NoError(t, err)

	got, err = client.GetPortal(t.Context(), &apigatewayv2sdk.GetPortalInput{
		PortalId: created.PortalId,
	})
	require.NoError(t, err)
	require.Equal(t, apigatewayv2types.PublishStatusDisabled, got.PublishStatus)
}

// TestPortal_IncludedPortalProductArnsAndRumAppMonitorName drives
// CreatePortal/UpdatePortal/GetPortal through the real SDK client.
// IncludedPortalProductArns and RumAppMonitorName are real members of
// CreatePortalInput/UpdatePortalInput/GetPortalOutput/PortalSummary
// (aws-sdk-go-v2/service/apigatewayv2@v1.37.4's api_op_CreatePortal.go,
// api_op_UpdatePortal.go, api_op_GetPortal.go, types.PortalSummary --
// IncludedPortalProductArns is even a *required* PortalSummary member)
// gopherstack's Portal model had no field to receive either into, so both
// were silently dropped on Create and Update and a real client's typed
// .IncludedPortalProductArns/.RumAppMonitorName were always empty regardless
// of what was sent.
func TestPortal_IncludedPortalProductArnsAndRumAppMonitorName(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	product, err := client.CreatePortalProduct(t.Context(), &apigatewayv2sdk.CreatePortalProductInput{
		DisplayName: aws.String("included-product"),
	})
	require.NoError(t, err)

	created, err := client.CreatePortal(t.Context(), &apigatewayv2sdk.CreatePortalInput{
		Authorization: &apigatewayv2types.Authorization{
			None: &apigatewayv2types.None{},
		},
		EndpointConfiguration: &apigatewayv2types.EndpointConfigurationRequest{
			None: &apigatewayv2types.None{},
		},
		PortalContent:             testPortalContent(),
		IncludedPortalProductArns: []string{aws.ToString(product.PortalProductArn)},
		RumAppMonitorName:         aws.String("created-monitor"),
	})
	require.NoError(t, err)
	require.Equal(t, []string{aws.ToString(product.PortalProductArn)}, created.IncludedPortalProductArns)
	require.Equal(t, "created-monitor", aws.ToString(created.RumAppMonitorName))

	updated, err := client.UpdatePortal(t.Context(), &apigatewayv2sdk.UpdatePortalInput{
		PortalId:                  created.PortalId,
		IncludedPortalProductArns: []string{},
		RumAppMonitorName:         aws.String("updated-monitor"),
	})
	require.NoError(t, err)
	require.Empty(t, updated.IncludedPortalProductArns)
	require.Equal(t, "updated-monitor", aws.ToString(updated.RumAppMonitorName))

	got, err := client.GetPortal(t.Context(), &apigatewayv2sdk.GetPortalInput{
		PortalId: created.PortalId,
	})
	require.NoError(t, err)
	require.Empty(t, got.IncludedPortalProductArns)
	require.Equal(t, "updated-monitor", aws.ToString(got.RumAppMonitorName))
}

// TestPortal_LastPublishedDescription drives CreatePortal/PublishPortal/
// GetPortal through the real SDK client. PublishPortalInput.Description is
// documented as becoming the portal's LastPublishedDescription
// (aws-sdk-go-v2/service/apigatewayv2@v1.37.4's api_op_PublishPortal.go:
// "When the portal is published, this description becomes the last
// published description.") -- gopherstack decoded Description off the wire
// but never used it, and GetPortalOutput.LastPublished/
// LastPublishedDescription had no backing field at all, so a real client
// polling GetPortal after PublishPortal always saw both unset.
func TestPortal_LastPublishedDescription(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	created, err := client.CreatePortal(t.Context(), &apigatewayv2sdk.CreatePortalInput{
		Authorization: &apigatewayv2types.Authorization{
			None: &apigatewayv2types.None{},
		},
		EndpointConfiguration: &apigatewayv2types.EndpointConfigurationRequest{
			None: &apigatewayv2types.None{},
		},
		PortalContent: testPortalContent(),
	})
	require.NoError(t, err)

	before, err := client.GetPortal(t.Context(), &apigatewayv2sdk.GetPortalInput{
		PortalId: created.PortalId,
	})
	require.NoError(t, err)
	require.Nil(t, before.LastPublished)

	_, err = client.PublishPortal(t.Context(), &apigatewayv2sdk.PublishPortalInput{
		PortalId:    created.PortalId,
		Description: aws.String("v1 release notes"),
	})
	require.NoError(t, err)

	after, err := client.GetPortal(t.Context(), &apigatewayv2sdk.GetPortalInput{
		PortalId: created.PortalId,
	})
	require.NoError(t, err)
	require.NotNil(t, after.LastPublished)
	require.Equal(t, "v1 release notes", aws.ToString(after.LastPublishedDescription))
}

// TestPortalProduct_LastModified drives CreatePortalProduct/
// GetPortalProduct through the real SDK client. PortalProductSummary/
// GetPortalProductOutput's LastModified is a real member
// (aws-sdk-go-v2/service/apigatewayv2@v1.37.4's api_op_GetPortalProduct.go)
// gopherstack's PortalProduct model never tracked at all.
func TestPortalProduct_LastModified(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	created, err := client.CreatePortalProduct(t.Context(), &apigatewayv2sdk.CreatePortalProductInput{
		DisplayName: aws.String("lastmod-product"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.LastModified)

	got, err := client.GetPortalProduct(t.Context(), &apigatewayv2sdk.GetPortalProductInput{
		PortalProductId: created.PortalProductId,
	})
	require.NoError(t, err)
	require.NotNil(t, got.LastModified)
}

// TestCreateProductPage_DisplayContent drives CreateProductPage/
// GetProductPage through the real SDK client. DisplayContent is a real,
// required CreateProductPageInput member
// (aws-sdk-go-v2/service/apigatewayv2@v1.37.4's api_op_CreateProductPage.go)
// this backend previously discarded entirely -- the handler decoded the
// request body into a CreateProductPageInput with no field to receive it,
// so every product page was created empty regardless of what a real client
// sent.
func TestCreateProductPage_DisplayContent(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	product, err := client.CreatePortalProduct(t.Context(), &apigatewayv2sdk.CreatePortalProductInput{
		DisplayName: aws.String("dc-product"),
	})
	require.NoError(t, err)

	created, err := client.CreateProductPage(t.Context(), &apigatewayv2sdk.CreateProductPageInput{
		PortalProductId: product.PortalProductId,
		DisplayContent: &apigatewayv2types.DisplayContent{
			Body:  aws.String("hello world"),
			Title: aws.String("My Page"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.DisplayContent)
	require.Equal(t, "My Page", aws.ToString(created.DisplayContent.Title))
	require.Equal(t, "hello world", aws.ToString(created.DisplayContent.Body))

	got, err := client.GetProductPage(t.Context(), &apigatewayv2sdk.GetProductPageInput{
		PortalProductId: product.PortalProductId,
		ProductPageId:   created.ProductPageId,
	})
	require.NoError(t, err)
	require.NotNil(t, got.DisplayContent)
	require.Equal(t, "My Page", aws.ToString(got.DisplayContent.Title))
}

// TestCreateProductRestEndpointPage_DisplayContent drives
// CreateProductRestEndpointPage/GetProductRestEndpointPage at the raw-HTTP
// level (not the typed SDK client: the real request member is
// *types.EndpointDisplayContent, the real response member is the
// differently-shaped *types.EndpointDisplayContentResponse, and gopherstack
// stores/echoes both as an opaque map[string]any passthrough -- the same
// simplification UpdateProductRestEndpointPage already uses, matched here
// for parity between the two ops rather than fought). Before the fix,
// CreateProductRestEndpointPageInput had no DisplayContent field at all, so
// a real client's DisplayContent was silently dropped on create even though
// Update already accepted and stored it correctly on the same
// ProductRestEndpointPage.DisplayContent field.
func TestCreateProductRestEndpointPage_DisplayContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{"displayName": "dc-rep-product"})
	require.Equal(t, http.StatusCreated, rr.Code)

	var product apigatewayv2.PortalProduct
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &product))

	body := map[string]any{
		"restEndpointIdentifier": map[string]any{
			"identifierParts": map[string]any{
				"method":    "GET",
				"path":      "/widgets",
				"restApiId": "abc123",
				"stage":     "prod",
			},
		},
		"displayContent": map[string]any{"title": "My REST Page"},
	}

	path := fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", product.PortalProductID)
	rr = doRequest(t, h, http.MethodPost, path, body)
	require.Equal(t, http.StatusCreated, rr.Code)

	var created apigatewayv2.ProductRestEndpointPage
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &created))
	require.Equal(t, "My REST Page", created.DisplayContent["title"])

	rr = doRequest(t, h, http.MethodGet,
		fmt.Sprintf("%s/%s", path, created.ProductRestEndpointPageID), nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var got apigatewayv2.ProductRestEndpointPage
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	require.Equal(t, "My REST Page", got.DisplayContent["title"])
}
