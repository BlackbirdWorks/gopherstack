package apigateway_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestSDKCreateRestAPIDefaultsAndValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in        apigatewaysdk.CreateRestApiInput
		name      string
		wantSrc   apigwtypes.ApiKeySourceType
		wantErr   string
		wantTypes []apigwtypes.EndpointType
	}{
		{
			name:      "defaults",
			in:        apigatewaysdk.CreateRestApiInput{Name: aws.String("a")},
			wantTypes: []apigwtypes.EndpointType{apigwtypes.EndpointTypeEdge},
			wantSrc:   apigwtypes.ApiKeySourceTypeHeader,
		},
		{
			name: "regional",
			in: apigatewaysdk.CreateRestApiInput{
				Name: aws.String("a"),
				EndpointConfiguration: &apigwtypes.EndpointConfiguration{
					Types: []apigwtypes.EndpointType{apigwtypes.EndpointTypeRegional},
				},
				ApiKeySource: apigwtypes.ApiKeySourceTypeAuthorizer,
			},
			wantTypes: []apigwtypes.EndpointType{apigwtypes.EndpointTypeRegional},
			wantSrc:   apigwtypes.ApiKeySourceTypeAuthorizer,
		},
		{
			name: "bad_endpoint_type",
			in: apigatewaysdk.CreateRestApiInput{
				Name: aws.String("a"),
				EndpointConfiguration: &apigwtypes.EndpointConfiguration{
					Types: []apigwtypes.EndpointType{"BOGUS"},
				},
			},
			wantErr: "BadRequestException",
		},
		{
			name:    "bad_key_source",
			in:      apigatewaysdk.CreateRestApiInput{Name: aws.String("a"), ApiKeySource: "NOPE"},
			wantErr: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

			out, err := client.CreateRestApi(t.Context(), &tt.in)
			if tt.wantErr != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantErr, apiErr.ErrorCode())
				assert.False(t, strings.HasPrefix(apiErr.ErrorMessage(), tt.wantErr+":"))

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.EndpointConfiguration)
			assert.Equal(t, tt.wantTypes, out.EndpointConfiguration.Types)
			assert.Equal(t, tt.wantSrc, out.ApiKeySource)
		})
	}
}

func TestSDKStageAndAPIKeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stageName string
		wantCode  string
	}{
		{name: "valid", stageName: "prod_1"},
		{name: "hyphen", stageName: "my-stage", wantCode: "BadRequestException"},
		{name: "space", stageName: "my stage", wantCode: "BadRequestException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))
			api, err := client.CreateRestApi(t.Context(), &apigatewaysdk.CreateRestApiInput{Name: aws.String("a")})
			require.NoError(t, err)

			depl, err := client.CreateDeployment(t.Context(), &apigatewaysdk.CreateDeploymentInput{RestApiId: api.Id})
			require.NoError(t, err)

			_, err = client.CreateStage(t.Context(), &apigatewaysdk.CreateStageInput{
				RestApiId: api.Id, DeploymentId: depl.Id, StageName: aws.String(tt.stageName),
			})
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}

func TestSDKCreateAPIKeyDuplicateValue(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))
	_, err := client.CreateApiKey(t.Context(), &apigatewaysdk.CreateApiKeyInput{
		Name: aws.String("k1"), Value: aws.String("same-value-same-value-1"),
	})
	require.NoError(t, err)

	_, err = client.CreateApiKey(t.Context(), &apigatewaysdk.CreateApiKeyInput{
		Name: aws.String("k2"), Value: aws.String("same-value-same-value-1"),
	})

	var conflict *apigwtypes.ConflictException
	require.ErrorAs(t, err, &conflict)
}

func TestSDKResourceAndModelValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		call     func(c *apigatewaysdk.Client, apiID, rootID *string) error
		wantCode string
	}{
		{
			name: "duplicate_path_part",
			call: func(c *apigatewaysdk.Client, apiID, rootID *string) error {
				in := &apigatewaysdk.CreateResourceInput{RestApiId: apiID, ParentId: rootID, PathPart: aws.String("p")}
				if _, err := c.CreateResource(t.Context(), in); err != nil {
					return err
				}

				_, err := c.CreateResource(t.Context(), in)

				return err
			},
			wantCode: "ConflictException",
		},
		{
			name: "space_in_path_part",
			call: func(c *apigatewaysdk.Client, apiID, rootID *string) error {
				_, err := c.CreateResource(t.Context(), &apigatewaysdk.CreateResourceInput{
					RestApiId: apiID, ParentId: rootID, PathPart: aws.String("x y"),
				})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "greedy_path_part_ok",
			call: func(c *apigatewaysdk.Client, apiID, rootID *string) error {
				_, err := c.CreateResource(t.Context(), &apigatewaysdk.CreateResourceInput{
					RestApiId: apiID, ParentId: rootID, PathPart: aws.String("{proxy+}"),
				})

				return err
			},
		},
		{
			name: "model_name_hyphen",
			call: func(c *apigatewaysdk.Client, apiID, _ *string) error {
				_, err := c.CreateModel(t.Context(), &apigatewaysdk.CreateModelInput{
					RestApiId: apiID, Name: aws.String("bad-name"), ContentType: aws.String("application/json"),
					Schema: aws.String("{}"),
				})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "model_schema_not_json",
			call: func(c *apigatewaysdk.Client, apiID, _ *string) error {
				_, err := c.CreateModel(t.Context(), &apigatewaysdk.CreateModelInput{
					RestApiId: apiID, Name: aws.String("M1"), ContentType: aws.String("application/json"),
					Schema: aws.String("notjson"),
				})

				return err
			},
			wantCode: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))
			api, err := client.CreateRestApi(t.Context(), &apigatewaysdk.CreateRestApiInput{Name: aws.String("a")})
			require.NoError(t, err)

			err = tt.call(client, api.Id, api.RootResourceId)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}
