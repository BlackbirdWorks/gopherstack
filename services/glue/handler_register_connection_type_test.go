package glue_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/glue"
)

// newTestGlueClient stands up the real aws-sdk-go-v2 Glue client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestGlueClient(t *testing.T, h *glue.Handler) *gluesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return gluesdk.NewFromConfig(cfg, func(o *gluesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestRegisterConnectionType_RequiredMembers locks in ConnectionProperties,
// ConnectorAuthenticationConfiguration, IntegrationType and RestConfiguration
// being rejected when absent (glue@v1.152.0 api_op_RegisterConnectionType.go:38-70,
// all "This member is required" -- two more, ConnectionProperties and
// IntegrationType, than the sweep that filed this issue caught, which only
// named ConnectorAuthenticationConfiguration/IntegrationType/RestConfiguration).
// The real SDK client validates all four client-side
// (validateOpRegisterConnectionTypeInput), so this drives the raw HTTP
// handler directly to exercise the server-side InvalidInputException path a
// non-SDK caller would still hit.
func TestRegisterConnectionType_RequiredMembers(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"ConnectionType":  "TESTCUSTOMTYPE",
		"IntegrationType": "REST",
		"ConnectionProperties": map[string]any{
			"Url": map[string]any{"Name": "endpoint"},
		},
		"ConnectorAuthenticationConfiguration": map[string]any{
			"AuthenticationTypes": []any{"BASIC"},
		},
		"RestConfiguration": map[string]any{},
	}

	withoutKey := func(key string) map[string]any {
		body := make(map[string]any, len(full))
		for k, v := range full {
			if k != key {
				body[k] = v
			}
		}

		return body
	}

	tests := []struct {
		name   string
		absent string
	}{
		{name: "missing integrationtype rejected", absent: "IntegrationType"},
		{name: "missing connectionproperties rejected", absent: "ConnectionProperties"},
		{name: "missing connectorauthenticationconfiguration rejected", absent: "ConnectorAuthenticationConfiguration"},
		{name: "missing restconfiguration rejected", absent: "RestConfiguration"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "RegisterConnectionType", withoutKey(tt.absent))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidInputException")
		})
	}

	t.Run("missing authenticationtypes within configuration rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		body := make(map[string]any, len(full))
		maps.Copy(body, full)

		body["ConnectorAuthenticationConfiguration"] = map[string]any{}

		rec := doGlueRequest(t, h, "RegisterConnectionType", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid integrationtype rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		body := make(map[string]any, len(full))
		maps.Copy(body, full)

		body["IntegrationType"] = "SOAP"

		rec := doGlueRequest(t, h, "RegisterConnectionType", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestSDKRoundTrip_RegisterConnectionType_EchoesRequiredMembers drives
// RegisterConnectionType through the real aws-sdk-go-v2 client and proves
// RestConfiguration -- the one required member with a matching field on
// DescribeConnectionType's real output (both share types.RestConfiguration
// verbatim; api_op_DescribeConnectionType.go:76-79) -- round-trips there,
// and that RegisterConnectionType itself returns ConnectionTypeArn, the
// real RegisterConnectionTypeOutput's only field.
func TestSDKRoundTrip_RegisterConnectionType_EchoesRequiredMembers(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	created, err := client.RegisterConnectionType(t.Context(), &gluesdk.RegisterConnectionTypeInput{
		ConnectionType:  aws.String("RTCUSTOMTYPE"),
		IntegrationType: types.IntegrationTypeRest,
		ConnectionProperties: &types.ConnectionPropertiesConfiguration{
			Url: &types.ConnectorProperty{
				Name:         aws.String("endpoint"),
				PropertyType: types.PropertyTypeUserInput,
				Required:     aws.Bool(true),
			},
		},
		ConnectorAuthenticationConfiguration: &types.ConnectorAuthenticationConfiguration{
			AuthenticationTypes: []types.AuthenticationType{types.AuthenticationTypeBasic},
		},
		RestConfiguration: &types.RestConfiguration{
			ValidationEndpointConfiguration: &types.SourceConfiguration{
				RequestMethod: types.HTTPMethodGet,
				RequestPath:   aws.String("/health"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.ConnectionTypeArn)
	assert.NotEmpty(t, *created.ConnectionTypeArn)

	// DescribeConnectionType can't be driven through the real client here:
	// its Capabilities field predates this fix and is already fabricated as
	// []string ("READ"/"WRITE") instead of the real *types.Capabilities
	// struct (SupportedAuthenticationTypes/SupportedComputeEnvironments/
	// SupportedDataOperations, all required) -- a real client's
	// deserializer rejects the whole response body on that mismatch before
	// RestConfiguration is ever reached. That's a distinct, pre-existing
	// wrong-shape bug in DescribeConnectionType/ListConnectionTypes, not one
	// of the five required-member drops this pass fixes; tracked in
	// PARITY.md rather than fixed here. So RestConfiguration's echo is
	// verified over raw HTTP instead, which this backend's JSON shape still
	// supports correctly.
	descRec := doGlueRequest(t, glue.NewHandler(backend), "DescribeConnectionType", map[string]any{
		"ConnectionType": "RTCUSTOMTYPE",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		RestConfiguration struct {
			ValidationEndpointConfiguration struct {
				RequestMethod string `json:"RequestMethod"`
				RequestPath   string `json:"RequestPath"`
			} `json:"ValidationEndpointConfiguration"`
		} `json:"RestConfiguration"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "GET", descOut.RestConfiguration.ValidationEndpointConfiguration.RequestMethod)
	assert.Equal(t, "/health", descOut.RestConfiguration.ValidationEndpointConfiguration.RequestPath)
}
