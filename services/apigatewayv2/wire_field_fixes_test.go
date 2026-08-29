package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/stretchr/testify/assert"
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

// TestListRoutingRules_MaxResultsAndNextToken drives ListRoutingRules through
// the real SDK client with MaxResults set. Before the fix,
// handleRoutingRulesCollection never read the maxResults/nextToken query
// params at all (unlike every other List/Get collection op in this service,
// which goes through the shared handleGetList/apigwPaginationParams path) --
// MaxResults is a real *int32 member of ListRoutingRulesInput
// (aws-sdk-go-v2/service/apigatewayv2@v1.37.4's api_op_ListRoutingRules.go:40,
// serialized via encoder.SetQuery("maxResults").Integer, serializers.go:6988)
// -- so a real client always got every routing rule back in one page
// regardless of the limit it asked for, and NextToken was always empty.
func TestListRoutingRules_MaxResultsAndNextToken(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	dn, err := client.CreateDomainName(t.Context(), &apigatewayv2sdk.CreateDomainNameInput{
		DomainName: aws.String("rr-maxresults.example.com"),
	})
	require.NoError(t, err)

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:         aws.String("rr-maxresults-api"),
		ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	_, err = client.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
		ApiId:     api.ApiId,
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	const numRules = 3

	for i := range numRules {
		_, err = client.CreateRoutingRule(t.Context(), &apigatewayv2sdk.CreateRoutingRuleInput{
			DomainName: dn.DomainName,
			Priority:   aws.Int32(int32(i + 1)),
			Actions: []apigatewayv2types.RoutingRuleAction{
				{InvokeApi: &apigatewayv2types.RoutingRuleActionInvokeApi{
					ApiId: api.ApiId,
					Stage: aws.String("prod"),
				}},
			},
			Conditions: []apigatewayv2types.RoutingRuleCondition{
				{MatchBasePaths: &apigatewayv2types.RoutingRuleMatchBasePaths{
					AnyOf: []string{fmt.Sprintf("/foo%d", i)},
				}},
			},
		})
		require.NoError(t, err)
	}

	first, err := client.ListRoutingRules(t.Context(), &apigatewayv2sdk.ListRoutingRulesInput{
		DomainName: dn.DomainName,
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, first.RoutingRules, 2, "MaxResults=2 must cap the first page at 2 rules")
	require.NotNil(t, first.NextToken)
	require.NotEmpty(t, aws.ToString(first.NextToken))

	second, err := client.ListRoutingRules(t.Context(), &apigatewayv2sdk.ListRoutingRulesInput{
		DomainName: dn.DomainName,
		MaxResults: aws.Int32(2),
		NextToken:  first.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, second.RoutingRules, 1, "the remaining rule must be on the second page")
	require.Empty(t, aws.ToString(second.NextToken))
}

// TestExportApi_IncludeExtensions drives ExportApi through the real SDK
// client with IncludeExtensions set. Before the fix, handleExportAPI never
// read the includeExtensions query param at all -- IncludeExtensions is a
// real *bool member of ExportApiInput (api_op_ExportApi.go:52), serialized
// via encoder.SetQuery("includeExtensions").Boolean (serializers.go:3975) --
// so AWS API Gateway extensions (x-amazon-apigateway-authtype and friends)
// were always emitted regardless of what a real client asked for.
func TestExportApi_IncludeExtensions(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:         aws.String("export-ext-api"),
		ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	_, err = client.CreateRoute(t.Context(), &apigatewayv2sdk.CreateRouteInput{
		ApiId:             api.ApiId,
		RouteKey:          aws.String("GET /secure"),
		AuthorizationType: apigatewayv2types.AuthorizationTypeAwsIam,
	})
	require.NoError(t, err)

	withExt, err := client.ExportApi(t.Context(), &apigatewayv2sdk.ExportApiInput{
		ApiId:             api.ApiId,
		OutputType:        aws.String("JSON"),
		Specification:     aws.String("OAS30"),
		IncludeExtensions: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Contains(t, string(withExt.Body), "x-amazon-apigateway-authtype",
		"IncludeExtensions=true must include AWS extensions")

	withoutExt, err := client.ExportApi(t.Context(), &apigatewayv2sdk.ExportApiInput{
		ApiId:             api.ApiId,
		OutputType:        aws.String("JSON"),
		Specification:     aws.String("OAS30"),
		IncludeExtensions: aws.Bool(false),
	})
	require.NoError(t, err)
	assert.NotContains(t, string(withoutExt.Body), "x-amazon-apigateway-authtype",
		"IncludeExtensions=false must strip AWS extensions")
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

// TestUpdateAuthorizer_TTLAndSimpleResponsesCanBeCleared drives
// CreateAuthorizer/UpdateAuthorizer/GetAuthorizer through the real SDK
// client. Before the fix, UpdateAuthorizerInput.AuthorizerResultTtlInSeconds
// and .EnableSimpleResponses were plain int32/bool (not *int32/*bool, unlike
// the real SDK's UpdateAuthorizerInput, api_op_UpdateAuthorizer.go), and the
// backend only applied them when non-zero/true -- so a real client's
// documented way to disable caching (TTL=0, "If it equals 0, authorization
// caching is disabled" per AuthorizerResultTtlInSeconds's doc comment) or
// disable simple responses (false) via Update was silently dropped, leaving
// the previous value forever. The Authorizer response shape itself also
// carried `omitempty` on both fields, which would have hidden a real 0/false
// value as an absent key -- also fixed.
func TestUpdateAuthorizer_TTLAndSimpleResponsesCanBeCleared(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:         aws.String("authz-ttl-clear-api"),
		ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	created, err := client.CreateAuthorizer(t.Context(), &apigatewayv2sdk.CreateAuthorizerInput{
		ApiId:          api.ApiId,
		AuthorizerType: apigatewayv2types.AuthorizerTypeRequest,
		Name:           aws.String("authz-ttl-clear"),
		IdentitySource: []string{"$request.header.Authorization"},
		AuthorizerUri: aws.String(
			"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/" +
				"arn:aws:lambda:us-east-1:123456789012:function:authz/invocations",
		),
		AuthorizerPayloadFormatVersion: aws.String("2.0"),
		AuthorizerResultTtlInSeconds:   aws.Int32(300),
		EnableSimpleResponses:          aws.Bool(true),
	})
	require.NoError(t, err)
	require.Equal(t, int32(300), aws.ToInt32(created.AuthorizerResultTtlInSeconds))
	require.True(t, aws.ToBool(created.EnableSimpleResponses))

	updated, err := client.UpdateAuthorizer(t.Context(), &apigatewayv2sdk.UpdateAuthorizerInput{
		ApiId:                        api.ApiId,
		AuthorizerId:                 created.AuthorizerId,
		AuthorizerResultTtlInSeconds: aws.Int32(0),
		EnableSimpleResponses:        aws.Bool(false),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.AuthorizerResultTtlInSeconds,
		"explicit TTL=0 must survive the update, not be dropped as a zero value")
	require.Equal(t, int32(0), aws.ToInt32(updated.AuthorizerResultTtlInSeconds))
	require.NotNil(t, updated.EnableSimpleResponses)
	require.False(t, aws.ToBool(updated.EnableSimpleResponses))

	got, err := client.GetAuthorizer(t.Context(), &apigatewayv2sdk.GetAuthorizerInput{
		ApiId:        api.ApiId,
		AuthorizerId: created.AuthorizerId,
	})
	require.NoError(t, err)
	require.NotNil(t, got.AuthorizerResultTtlInSeconds,
		"a real 0 TTL must round-trip through GetAuthorizer, not be omitted as an unset field")
	require.Equal(t, int32(0), aws.ToInt32(got.AuthorizerResultTtlInSeconds))
	require.NotNil(t, got.EnableSimpleResponses)
	require.False(t, aws.ToBool(got.EnableSimpleResponses))
}

// TestUpdateAuthorizer_URICredentialsAndPayloadVersionCanBeCleared drives
// CreateAuthorizer/UpdateAuthorizer/GetAuthorizer through the real SDK
// client. AuthorizerURI, AuthorizerCredentialsArn and
// AuthorizerPayloadFormatVersion were plain strings guarded by != "" (not
// *string like the real SDK's UpdateAuthorizerInput fields,
// api_op_UpdateAuthorizer.go), so a client explicitly clearing any of them
// (e.g. dropping AuthorizerCredentialsArn to switch a REQUEST authorizer to
// resource-based Lambda permissions -- "To use resource-based permissions on
// the Lambda function, don't specify this parameter") was silently ignored,
// leaving the old value in place. None of the three is required at create
// time (unlike Name, which is), so unlike Name an explicit empty value is a
// legitimate clear, not an invalid state.
func TestUpdateAuthorizer_URICredentialsAndPayloadVersionCanBeCleared(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:         aws.String("authz-clear-fields-api"),
		ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	created, err := client.CreateAuthorizer(t.Context(), &apigatewayv2sdk.CreateAuthorizerInput{
		ApiId:                          api.ApiId,
		AuthorizerType:                 apigatewayv2types.AuthorizerTypeRequest,
		Name:                           aws.String("authz-clear-fields"),
		IdentitySource:                 []string{"$request.header.Authorization"},
		AuthorizerCredentialsArn:       aws.String("arn:aws:iam::123456789012:role/authz-role"),
		AuthorizerPayloadFormatVersion: aws.String("2.0"),
		AuthorizerUri: aws.String(
			"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/" +
				"arn:aws:lambda:us-east-1:123456789012:function:authz/invocations",
		),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.AuthorizerUri))
	require.NotEmpty(t, aws.ToString(created.AuthorizerCredentialsArn))
	require.NotEmpty(t, aws.ToString(created.AuthorizerPayloadFormatVersion))

	updated, err := client.UpdateAuthorizer(t.Context(), &apigatewayv2sdk.UpdateAuthorizerInput{
		ApiId:                          api.ApiId,
		AuthorizerId:                   created.AuthorizerId,
		AuthorizerUri:                  aws.String(""),
		AuthorizerCredentialsArn:       aws.String(""),
		AuthorizerPayloadFormatVersion: aws.String(""),
	})
	require.NoError(t, err)
	require.Empty(t, aws.ToString(updated.AuthorizerUri),
		"explicit empty AuthorizerUri on Update must clear it, not be silently ignored")
	require.Empty(t, aws.ToString(updated.AuthorizerCredentialsArn),
		"explicit empty AuthorizerCredentialsArn on Update must clear it, not be silently ignored")
	require.Empty(t, aws.ToString(updated.AuthorizerPayloadFormatVersion),
		"explicit empty AuthorizerPayloadFormatVersion on Update must clear it, not be silently ignored")

	got, err := client.GetAuthorizer(t.Context(), &apigatewayv2sdk.GetAuthorizerInput{
		ApiId:        api.ApiId,
		AuthorizerId: created.AuthorizerId,
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(got.AuthorizerUri))
	assert.Empty(t, aws.ToString(got.AuthorizerCredentialsArn))
	assert.Empty(t, aws.ToString(got.AuthorizerPayloadFormatVersion))
}

// TestUpdateAuthorizer_EmptyNameRejected verifies that, unlike
// AuthorizerUri/AuthorizerCredentialsArn/AuthorizerPayloadFormatVersion,
// Name is required at create time (CreateAuthorizerInput.Name, "This member
// is required", api_op_CreateAuthorizer.go) and so has no valid cleared
// state: an explicit empty Name on Update is rejected as a validation error
// rather than silently ignored or applied.
func TestUpdateAuthorizer_EmptyNameRejected(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(backend))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:         aws.String("authz-empty-name-api"),
		ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	created, err := client.CreateAuthorizer(t.Context(), &apigatewayv2sdk.CreateAuthorizerInput{
		ApiId:          api.ApiId,
		AuthorizerType: apigatewayv2types.AuthorizerTypeJwt,
		Name:           aws.String("authz-empty-name"),
		IdentitySource: []string{"$request.header.Authorization"},
		JwtConfiguration: &apigatewayv2types.JWTConfiguration{
			Issuer:   aws.String("https://issuer.example.com"),
			Audience: []string{"client-id"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateAuthorizer(t.Context(), &apigatewayv2sdk.UpdateAuthorizerInput{
		ApiId:        api.ApiId,
		AuthorizerId: created.AuthorizerId,
		Name:         aws.String(""),
	})
	require.Error(t, err)

	var badReq *apigatewayv2types.BadRequestException
	require.ErrorAs(t, err, &badReq, "an explicit empty Name must be rejected as a validation error")

	got, err := client.GetAuthorizer(t.Context(), &apigatewayv2sdk.GetAuthorizerInput{
		ApiId:        api.ApiId,
		AuthorizerId: created.AuthorizerId,
	})
	require.NoError(t, err)
	assert.Equal(t, "authz-empty-name", aws.ToString(got.Name), "a rejected Update must not clear Name")
}
