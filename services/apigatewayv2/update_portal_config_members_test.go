package apigatewayv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestUpdatePortal_ReplacesConfigMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update func(id *string) *apigatewayv2sdk.UpdatePortalInput
		check  func(t *testing.T, out *apigatewayv2sdk.UpdatePortalOutput)
		name   string
	}{
		{
			name: "authorization",
			update: func(id *string) *apigatewayv2sdk.UpdatePortalInput {
				return &apigatewayv2sdk.UpdatePortalInput{PortalId: id, Authorization: &apigatewayv2types.Authorization{
					CognitoConfig: &apigatewayv2types.CognitoConfig{
						AppClientId:    aws.String("client"),
						UserPoolArn:    aws.String("arn:aws:cognito-idp:us-east-1:000000000000:userpool/p"),
						UserPoolDomain: aws.String("dom"),
					},
				}}
			},
			check: func(t *testing.T, out *apigatewayv2sdk.UpdatePortalOutput) {
				t.Helper()
				require.NotNil(t, out.Authorization.CognitoConfig)
				assert.Equal(t, "client", aws.ToString(out.Authorization.CognitoConfig.AppClientId))
				assert.Equal(t, "test-portal", aws.ToString(out.PortalContent.DisplayName), "content untouched")
			},
		},
		{
			name: "endpoint configuration",
			update: func(id *string) *apigatewayv2sdk.UpdatePortalInput {
				return &apigatewayv2sdk.UpdatePortalInput{
					PortalId: id,
					EndpointConfiguration: &apigatewayv2types.EndpointConfigurationRequest{
						AcmManaged: &apigatewayv2types.ACMManaged{
							CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/c"),
							DomainName:     aws.String("portal.example.com"),
						},
					},
				}
			},
			check: func(t *testing.T, out *apigatewayv2sdk.UpdatePortalOutput) {
				t.Helper()
				assert.Equal(t, "portal.example.com", aws.ToString(out.EndpointConfiguration.DomainName))
				assert.NotEmpty(t, aws.ToString(out.EndpointConfiguration.PortalDefaultDomainName))
			},
		},
		{
			name: "portal content",
			update: func(id *string) *apigatewayv2sdk.UpdatePortalInput {
				pc := testPortalContent()
				pc.DisplayName = aws.String("renamed")

				return &apigatewayv2sdk.UpdatePortalInput{PortalId: id, PortalContent: pc}
			},
			check: func(t *testing.T, out *apigatewayv2sdk.UpdatePortalOutput) {
				t.Helper()
				assert.Equal(t, "renamed", aws.ToString(out.PortalContent.DisplayName))
				assert.NotNil(t, out.Authorization.None, "authorization untouched")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

			created, err := client.CreatePortal(t.Context(), &apigatewayv2sdk.CreatePortalInput{
				Authorization:         &apigatewayv2types.Authorization{None: &apigatewayv2types.None{}},
				EndpointConfiguration: &apigatewayv2types.EndpointConfigurationRequest{None: &apigatewayv2types.None{}},
				PortalContent:         testPortalContent(),
			})
			require.NoError(t, err)

			out, err := client.UpdatePortal(t.Context(), tt.update(created.PortalId))
			require.NoError(t, err)
			tt.check(t, out)

			got, err := client.GetPortal(t.Context(), &apigatewayv2sdk.GetPortalInput{PortalId: created.PortalId})
			require.NoError(t, err)
			assert.Equal(t, out.PortalContent, got.PortalContent)
			assert.Equal(t, out.Authorization, got.Authorization)
		})
	}
}

func TestUpdatePortal_RejectsInvalidConfigMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input apigatewayv2.UpdatePortalInput
	}{
		{name: "cognito missing members", input: apigatewayv2.UpdatePortalInput{
			Authorization: &apigatewayv2.Authorization{CognitoConfig: &apigatewayv2.CognitoConfig{AppClientID: "x"}},
		}},
		{name: "acm missing members", input: apigatewayv2.UpdatePortalInput{
			EndpointConfiguration: &apigatewayv2.EndpointConfigurationRequest{
				AcmManaged: &apigatewayv2.ACMManaged{DomainName: "d"},
			},
		}},
		{name: "content missing display name", input: apigatewayv2.UpdatePortalInput{
			PortalContent: &apigatewayv2.PortalContent{},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := apigatewayv2.NewInMemoryBackend().UpdatePortal("missing", tt.input)
			require.ErrorIs(t, err, apigatewayv2.ErrBadRequest)
		})
	}
}

func TestPortalAndPortalProduct_SurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		get  func(t *testing.T, c *apigatewayv2sdk.Client, portalID, productID *string) string
		name string
		want string
	}{
		{
			name: "portal", want: "test-portal",
			get: func(t *testing.T, c *apigatewayv2sdk.Client, portalID, _ *string) string {
				t.Helper()
				out, err := c.GetPortal(t.Context(), &apigatewayv2sdk.GetPortalInput{PortalId: portalID})
				require.NoError(t, err)

				return aws.ToString(out.PortalContent.DisplayName)
			},
		},
		{
			name: "portal product", want: "prod",
			get: func(t *testing.T, c *apigatewayv2sdk.Client, _, productID *string) string {
				t.Helper()
				out, err := c.GetPortalProduct(
					t.Context(), &apigatewayv2sdk.GetPortalProductInput{PortalProductId: productID},
				)
				require.NoError(t, err)

				return aws.ToString(out.DisplayName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := apigatewayv2.NewInMemoryBackend()
			srcClient := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(src))

			portal, err := srcClient.CreatePortal(t.Context(), &apigatewayv2sdk.CreatePortalInput{
				Authorization:         &apigatewayv2types.Authorization{None: &apigatewayv2types.None{}},
				EndpointConfiguration: &apigatewayv2types.EndpointConfigurationRequest{None: &apigatewayv2types.None{}},
				PortalContent:         testPortalContent(),
			})
			require.NoError(t, err)

			product, err := srcClient.CreatePortalProduct(t.Context(), &apigatewayv2sdk.CreatePortalProductInput{
				DisplayName: aws.String("prod"),
			})
			require.NoError(t, err)

			dst := apigatewayv2.NewInMemoryBackend()
			require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))
			dstClient := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(dst))

			assert.Equal(t, tt.want, tt.get(t, dstClient, portal.PortalId, product.PortalProductId))
		})
	}
}

func TestUpdatePortalProduct_DisplayOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "get echoes display order and omitted member survives"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

			created, err := client.CreatePortalProduct(t.Context(), &apigatewayv2sdk.CreatePortalProductInput{
				DisplayName: aws.String("prod"),
			})
			require.NoError(t, err)

			upd, err := client.UpdatePortalProduct(t.Context(), &apigatewayv2sdk.UpdatePortalProductInput{
				PortalProductId: created.PortalProductId,
				DisplayOrder: &apigatewayv2types.DisplayOrder{
					OverviewPageArn: aws.String("arn:overview"),
					ProductPageArns: []string{"arn:page-1"},
					Contents: []apigatewayv2types.Section{
						{SectionName: aws.String("s1"), ProductRestEndpointPageArns: []string{"arn:ep-1"}},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, upd.DisplayOrder)

			got, err := client.GetPortalProduct(t.Context(), &apigatewayv2sdk.GetPortalProductInput{
				PortalProductId: created.PortalProductId,
			})
			require.NoError(t, err)
			require.NotNil(t, got.DisplayOrder)
			assert.Equal(t, "arn:overview", aws.ToString(got.DisplayOrder.OverviewPageArn))
			assert.Equal(t, []string{"arn:page-1"}, got.DisplayOrder.ProductPageArns)
			require.Len(t, got.DisplayOrder.Contents, 1)
			assert.Equal(t, "s1", aws.ToString(got.DisplayOrder.Contents[0].SectionName))

			again, err := client.UpdatePortalProduct(t.Context(), &apigatewayv2sdk.UpdatePortalProductInput{
				PortalProductId: created.PortalProductId, DisplayName: aws.String("renamed"),
			})
			require.NoError(t, err)
			assert.NotNil(t, again.DisplayOrder, "omitted displayOrder must survive")
		})
	}
}

func TestListProductRestEndpointPages_OperationName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		overrides *apigatewayv2types.DisplayContentOverrides
		name      string
		want      string
	}{
		{
			name:      "from overrides",
			overrides: &apigatewayv2types.DisplayContentOverrides{OperationName: aws.String("GetPets")},
			want:      "GetPets",
		},
		{name: "absent without overrides"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

			product, err := client.CreatePortalProduct(t.Context(), &apigatewayv2sdk.CreatePortalProductInput{
				DisplayName: aws.String("prod"),
			})
			require.NoError(t, err)

			in := &apigatewayv2sdk.CreateProductRestEndpointPageInput{
				PortalProductId: product.PortalProductId,
				RestEndpointIdentifier: &apigatewayv2types.RestEndpointIdentifier{
					IdentifierParts: &apigatewayv2types.IdentifierParts{
						RestApiId: aws.String("api1"), Stage: aws.String("prod"),
						Method: aws.String("GET"), Path: aws.String("/pets"),
					},
				},
			}
			if tt.overrides != nil {
				in.DisplayContent = &apigatewayv2types.EndpointDisplayContent{Overrides: tt.overrides}
			}

			_, err = client.CreateProductRestEndpointPage(t.Context(), in)
			require.NoError(t, err)

			list, err := client.ListProductRestEndpointPages(
				t.Context(),
				&apigatewayv2sdk.ListProductRestEndpointPagesInput{
					PortalProductId: product.PortalProductId,
				},
			)
			require.NoError(t, err)
			require.Len(t, list.Items, 1)
			assert.Equal(t, tt.want, aws.ToString(list.Items[0].OperationName))
		})
	}
}
