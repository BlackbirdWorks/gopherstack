package iam

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// newDelegationTestClient stands up the real aws-sdk-go-v2 IAM client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newDelegationTestClient(t *testing.T, h *Handler) *iamsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return iamsdk.NewFromConfig(cfg, func(o *iamsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestDelegationRequestOps_RealWireKeys drives the temporary-access-delegation
// ops through the real SDK client (iam@v1.58.1). CreateDelegationRequest reads
// its account field as "TargetAccountId", but the real request sends
// "OwnerAccountId" (api_op_CreateDelegationRequest.go, serializers.go:12772) --
// so a real client's value never reaches the backend. AcceptDelegationRequest
// and AssociateDelegationRequest read "DelegationId", but the real request
// sends "DelegationRequestId" (serializers.go:12590,12653) -- so a real client
// can never accept or associate an existing delegation request at all.
func TestDelegationRequestOps_RealWireKeys(t *testing.T) {
	t.Parallel()

	t.Run("createdelegationrequest_captures_owneraccountid", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newDelegationTestClient(t, h)

		_, err := client.CreateDelegationRequest(t.Context(), &iamsdk.CreateDelegationRequestInput{
			OwnerAccountId:      aws.String("111122223333"),
			Description:         aws.String("test delegation"),
			NotificationChannel: aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
			RequestorWorkflowId: aws.String("workflow-1"),
			SessionDuration:     aws.Int32(3600),
			Permissions: &types.DelegationPermission{
				PolicyTemplateArn: aws.String("arn:aws:iam::aws:policy/ReadOnlyAccess"),
			},
		})
		require.NoError(t, err)

		all := b.delegationRequests.All()
		require.Len(t, all, 1)
		assert.Equal(t, "111122223333", all[0].TargetAccountID)
	})

	t.Run("acceptdelegationrequest_uses_real_id_key", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newDelegationTestClient(t, h)

		seeded, err := b.CreateDelegationRequest("111122223333")
		require.NoError(t, err)

		_, err = client.AcceptDelegationRequest(t.Context(), &iamsdk.AcceptDelegationRequestInput{
			DelegationRequestId: aws.String(seeded.DelegationID),
		})
		require.NoError(t, err)
	})

	t.Run("associatedelegationrequest_uses_real_id_key", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newDelegationTestClient(t, h)

		seeded, err := b.CreateDelegationRequest("111122223333")
		require.NoError(t, err)

		_, err = client.AssociateDelegationRequest(t.Context(), &iamsdk.AssociateDelegationRequestInput{
			DelegationRequestId: aws.String(seeded.DelegationID),
		})
		require.NoError(t, err)
	})
}
