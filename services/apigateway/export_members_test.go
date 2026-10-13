package apigateway_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestGetExport_Members(t *testing.T) {
	t.Parallel()

	tests := []struct {
		accepts         *string
		params          map[string]string
		name            string
		exportType      string
		stage           string
		wantContentType string
		wantIntegration bool
		wantAuthorizer  bool
		wantErr         bool
	}{
		{name: "default_json", exportType: "oas30", stage: "prod", wantContentType: "application/json"},
		{
			name: "yaml", exportType: "swagger", stage: "prod", accepts: aws.String("application/yaml"),
			wantContentType: "application/yaml",
		},
		{
			name:            "integrations",
			exportType:      "oas30",
			stage:           "prod",
			params:          map[string]string{"extensions": "integrations"},
			wantContentType: "application/json",
			wantIntegration: true,
		},
		{
			name:            "apigateway_alias",
			exportType:      "swagger",
			stage:           "prod",
			params:          map[string]string{"extensions": "apigateway"},
			wantContentType: "application/json",
			wantIntegration: true,
		},
		{
			name:            "authorizers",
			exportType:      "oas30",
			stage:           "prod",
			params:          map[string]string{"extensions": "authorizers"},
			wantContentType: "application/json",
			wantAuthorizer:  true,
		},
		{name: "unknown_stage", exportType: "oas30", stage: "nope", wantErr: true},
		{name: "unknown_type", exportType: "raml", stage: "prod", wantErr: true},
		{name: "bad_accept", exportType: "oas30", stage: "prod", accepts: aws.String("text/plain"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, apiID, rootID := setupSDKMethod(t, nil)

			_, err := client.PutIntegration(t.Context(), &apigwsdk.PutIntegrationInput{
				RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
				Type: apigwtypes.IntegrationTypeMock,
			})
			require.NoError(t, err)

			_, err = client.CreateAuthorizer(t.Context(), &apigwsdk.CreateAuthorizerInput{
				RestApiId: aws.String(apiID),
				Name:      aws.String("tok"),
				Type:      apigwtypes.AuthorizerTypeToken,
				AuthorizerUri: aws.String(
					"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:1:function:f/invocations",
				),
				IdentitySource: aws.String("method.request.header.Auth"),
			})
			require.NoError(t, err)

			_, err = client.CreateDeployment(t.Context(), &apigwsdk.CreateDeploymentInput{
				RestApiId: aws.String(apiID), StageName: aws.String("prod"),
			})
			require.NoError(t, err)

			out, err := client.GetExport(t.Context(), &apigwsdk.GetExportInput{
				RestApiId: aws.String(apiID), StageName: aws.String(tt.stage), ExportType: aws.String(tt.exportType),
				Parameters: tt.params,
			}, withAccept(aws.ToString(tt.accepts)))

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantContentType, aws.ToString(out.ContentType))

			var doc map[string]any
			require.NoError(t, yaml.Unmarshal(out.Body, &doc), "JSON is valid YAML, so one parser checks both")
			assert.Contains(t, doc, map[bool]string{true: "openapi", false: "swagger"}[tt.exportType == "oas30"])
			assert.Equal(t, tt.wantIntegration, bytesContain(out.Body, "x-amazon-apigateway-integration"))
			assert.Equal(t, tt.wantAuthorizer, bytesContain(out.Body, "x-amazon-apigateway-authorizer"))
		})
	}
}

func bytesContain(b []byte, s string) bool { return strings.Contains(string(b), s) }

// withAccept overrides the Accept header the SDK's apigateway customization forces to application/json.
func withAccept(value string) func(*apigwsdk.Options) {
	return func(o *apigwsdk.Options) {
		if value == "" {
			return
		}

		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("setAccept",
				func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
				) (middleware.FinalizeOutput, middleware.Metadata, error) {
					if req, ok := in.Request.(*smithyhttp.Request); ok {
						req.Header.Set("Accept", value)
					}

					return next.HandleFinalize(ctx, in)
				}), middleware.After)
		})
	}
}

func TestGetSdk_RequiredParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params  map[string]string
		name    string
		sdkType string
		wantErr bool
	}{
		{name: "javascript_none", sdkType: "javascript"},
		{name: "ruby_none", sdkType: "ruby"},
		{name: "java_complete", sdkType: "java", params: map[string]string{"serviceName": "s", "javaPackageName": "p"}},
		{name: "java_missing", sdkType: "java", params: map[string]string{"serviceName": "s"}, wantErr: true},
		{name: "swift_prefix", sdkType: "swift", params: map[string]string{"classPrefix": "X"}},
		{name: "objectivec_missing", sdkType: "objectivec", wantErr: true},
		{
			name: "android_complete", sdkType: "android",
			params: map[string]string{"groupId": "g", "artifactId": "a", "artifactVersion": "1", "invokerPackage": "p"},
		},
		{name: "android_partial", sdkType: "android", params: map[string]string{"groupId": "g"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, apiID, _ := setupSDKMethod(t, nil)

			_, err := client.CreateDeployment(t.Context(), &apigwsdk.CreateDeploymentInput{
				RestApiId: aws.String(apiID), StageName: aws.String("prod"),
			})
			require.NoError(t, err)

			_, err = client.GetSdk(t.Context(), &apigwsdk.GetSdkInput{
				RestApiId: aws.String(apiID), StageName: aws.String("prod"),
				SdkType: aws.String(tt.sdkType), Parameters: tt.params,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			assert.NoError(t, err)
		})
	}
}
