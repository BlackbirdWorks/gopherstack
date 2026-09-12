package detective_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	detectivesdk "github.com/aws/aws-sdk-go-v2/service/detective"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/detective"
	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// newTestDetectiveGuardDutyRegistryServer wires up Detective's and GuardDuty's
// real Handlers into one shared service.Registry/NewServiceRouter, the same
// way cli.go registers every service in production. Both send an exact
// "/invitation" REST path with no shared HTTP method -- detective's
// AcceptInvitation is PUT /invitation (aws-sdk-go-v2/service/detective@v1.41.4
// serializers.go:44) and guardduty's ListInvitations is GET /invitation
// (aws-sdk-go-v2/service/guardduty@v1.85.4 serializers.go:5486) -- so a real
// router proves the fix routes each to its own handler, not just that
// RouteMatcher() returns the right bool in isolation (gopherstack-39710).
func newTestDetectiveGuardDutyRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(
		detective.NewHandler(detective.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		guardduty.NewHandler(guardduty.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func newInvitationRoutingDetectiveClient(t *testing.T, baseURL string) *detectivesdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return detectivesdk.NewFromConfig(cfg, func(o *detectivesdk.Options) {
		o.BaseEndpoint = aws.String(baseURL)
	})
}

func newInvitationRoutingGuardDutyClient(t *testing.T, baseURL string) *guarddutysdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return guarddutysdk.NewFromConfig(cfg, func(o *guarddutysdk.Options) {
		o.BaseEndpoint = aws.String(baseURL)
	})
}

// TestInvitationRouting_CrossServiceIsolation proves detective's exact
// "/invitation" RouteMatcher claim (MatchPriority 85) no longer swallows
// guardduty's own request on the identical path (guardduty registers at -1,
// so before the SigV4 scope guard detective always won the tie regardless of
// HTTP method): a real, SigV4-signed guardduty ListInvitations (GET
// /invitation) must reach guardduty's own handler, and a real detective
// AcceptInvitation (PUT /invitation) must still reach detective's.
func TestInvitationRouting_CrossServiceIsolation(t *testing.T) {
	t.Parallel()

	srv := newTestDetectiveGuardDutyRegistryServer(t)

	tests := []struct {
		run  func(t *testing.T, srv *httptest.Server)
		name string
	}{
		{
			name: "guardduty scoped GET invitation reaches guardduty",
			run: func(t *testing.T, srv *httptest.Server) {
				t.Helper()

				client := newInvitationRoutingGuardDutyClient(t, srv.URL)

				out, err := client.ListInvitations(t.Context(), &guarddutysdk.ListInvitationsInput{})
				require.NoError(
					t, err,
					"guardduty ListInvitations must reach guardduty's handler, not detective's "+
						"unknown-operation 400 fallback for a non-PUT method on /invitation",
				)
				assert.NotNil(t, out.Invitations, "guardduty's own ListInvitationsOutput shape")
			},
		},
		{
			name: "detective scoped PUT invitation still reaches detective",
			run: func(t *testing.T, srv *httptest.Server) {
				t.Helper()

				client := newInvitationRoutingDetectiveClient(t, srv.URL)

				_, err := client.AcceptInvitation(t.Context(), &detectivesdk.AcceptInvitationInput{
					GraphArn: aws.String("arn:aws:detective:us-east-1:000000000000:graph:nonexistent"),
				})
				require.Error(t, err, "accepting an invitation to an unknown graph must fail")

				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(
					t, "ResourceNotFoundException", apiErr.ErrorCode(),
					"detective's own error shape must still be reachable, not guardduty's",
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, srv)
		})
	}
}
