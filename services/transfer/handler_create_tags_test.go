package transfer_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	transfertypes "github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

const transferTagsRTRegion = "us-east-1"

// newTestTransferClient stands up the real aws-sdk-go-v2 transfer client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestTransferClient(t *testing.T, h *transfer.Handler) *transfersdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(transferTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return transfersdk.NewFromConfig(cfg, func(o *transfersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every transfer Create op whose real
// Input struct accepts Tags (transfer@v1.75.4: api_op_CreateProfile.go:56,
// api_op_CreateWebApp.go:61, api_op_CreateServer.go:291,
// api_op_CreateAgreement.go:138, api_op_CreateConnector.go:99,
// api_op_CreateUser.go:154, api_op_CreateWorkflow.go:69) through the real
// SDK client and asserts ListTagsForResource sees what was supplied at
// creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *transfersdk.Client) string
		name  string
	}{
		{
			name: "profile",
			setup: func(t *testing.T, client *transfersdk.Client) string {
				t.Helper()

				out, err := client.CreateProfile(t.Context(), &transfersdk.CreateProfileInput{
					As2Id:       aws.String("AS2ID"),
					ProfileType: transfertypes.ProfileTypeLocal,
					Tags: []transfertypes.Tag{
						{Key: aws.String("env"), Value: aws.String("test")},
					},
				})
				require.NoError(t, err)

				desc, err := client.DescribeProfile(
					t.Context(),
					&transfersdk.DescribeProfileInput{ProfileId: out.ProfileId},
				)
				require.NoError(t, err)

				return aws.ToString(desc.Profile.Arn)
			},
		},
		{
			name: "server",
			setup: func(t *testing.T, client *transfersdk.Client) string {
				t.Helper()

				out, err := client.CreateServer(t.Context(), &transfersdk.CreateServerInput{
					Tags: []transfertypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				desc, err := client.DescribeServer(
					t.Context(),
					&transfersdk.DescribeServerInput{ServerId: out.ServerId},
				)
				require.NoError(t, err)

				return aws.ToString(desc.Server.Arn)
			},
		},
		{
			name: "workflow",
			setup: func(t *testing.T, client *transfersdk.Client) string {
				t.Helper()

				out, err := client.CreateWorkflow(t.Context(), &transfersdk.CreateWorkflowInput{
					Steps: []transfertypes.WorkflowStep{},
					Tags:  []transfertypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				desc, err := client.DescribeWorkflow(
					t.Context(),
					&transfersdk.DescribeWorkflowInput{WorkflowId: out.WorkflowId},
				)
				require.NoError(t, err)

				return aws.ToString(desc.Workflow.Arn)
			},
		},
		{
			name: "connector",
			setup: func(t *testing.T, client *transfersdk.Client) string {
				t.Helper()

				out, err := client.CreateConnector(t.Context(), &transfersdk.CreateConnectorInput{
					Url:        aws.String("sftp://example.com"),
					AccessRole: aws.String("arn:aws:iam::000000000000:role/access"),
					SftpConfig: &transfertypes.SftpConnectorConfig{
						UserSecretId: aws.String(
							"arn:aws:secretsmanager:us-east-1:000000000000:secret:s",
						),
						TrustedHostKeys: []string{
							"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC example",
						},
					},
					Tags: []transfertypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				desc, err := client.DescribeConnector(
					t.Context(),
					&transfersdk.DescribeConnectorInput{ConnectorId: out.ConnectorId},
				)
				require.NoError(t, err)

				return aws.ToString(desc.Connector.Arn)
			},
		},
		{
			name: "user",
			setup: func(t *testing.T, client *transfersdk.Client) string {
				t.Helper()

				srv, err := client.CreateServer(t.Context(), &transfersdk.CreateServerInput{})
				require.NoError(t, err)

				out, err := client.CreateUser(t.Context(), &transfersdk.CreateUserInput{
					ServerId: srv.ServerId,
					UserName: aws.String("tagged-user"),
					Role:     aws.String("arn:aws:iam::000000000000:role/access"),
					Tags: []transfertypes.Tag{
						{Key: aws.String("env"), Value: aws.String("test")},
					},
				})
				require.NoError(t, err)

				desc, err := client.DescribeUser(t.Context(), &transfersdk.DescribeUserInput{
					ServerId: out.ServerId,
					UserName: out.UserName,
				})
				require.NoError(t, err)

				return aws.ToString(desc.User.Arn)
			},
		},
		{
			name: "agreement",
			setup: func(t *testing.T, client *transfersdk.Client) string {
				t.Helper()

				srv, err := client.CreateServer(t.Context(), &transfersdk.CreateServerInput{})
				require.NoError(t, err)

				out, err := client.CreateAgreement(t.Context(), &transfersdk.CreateAgreementInput{
					ServerId:         srv.ServerId,
					LocalProfileId:   aws.String("p-local"),
					PartnerProfileId: aws.String("p-partner"),
					AccessRole:       aws.String("arn:aws:iam::000000000000:role/access"),
					Tags: []transfertypes.Tag{
						{Key: aws.String("env"), Value: aws.String("test")},
					},
				})
				require.NoError(t, err)

				desc, err := client.DescribeAgreement(
					t.Context(),
					&transfersdk.DescribeAgreementInput{
						ServerId:    srv.ServerId,
						AgreementId: out.AgreementId,
					},
				)
				require.NoError(t, err)

				return aws.ToString(desc.Agreement.Arn)
			},
		},
		{
			name: "web app",
			setup: func(t *testing.T, client *transfersdk.Client) string {
				t.Helper()

				out, err := client.CreateWebApp(t.Context(), &transfersdk.CreateWebAppInput{
					IdentityProviderDetails: &transfertypes.WebAppIdentityProviderDetailsMemberIdentityCenterConfig{
						Value: transfertypes.IdentityCenterConfig{
							InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890"),
							Role:        aws.String("arn:aws:iam::000000000000:role/access"),
						},
					},
					Tags: []transfertypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				desc, err := client.DescribeWebApp(
					t.Context(),
					&transfersdk.DescribeWebAppInput{WebAppId: out.WebAppId},
				)
				require.NoError(t, err)

				return aws.ToString(desc.WebApp.Arn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := transfer.NewInMemoryBackend(
				context.Background(),
				"000000000000",
				transferTagsRTRegion,
			)
			client := newTestTransferClient(t, transfer.NewHandler(backend))

			resourceARN := tt.setup(t, client)
			require.NotEmpty(t, resourceARN)

			got, err := client.ListTagsForResource(
				t.Context(),
				&transfersdk.ListTagsForResourceInput{Arn: aws.String(resourceARN)},
			)
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
		})
	}
}
