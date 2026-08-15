package iam

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// TestAcceptAssociateDelegationRequest_UnknownID_NoSuchEntity covers
// gopherstack-xh42: AcceptDelegationRequest and AssociateDelegationRequest
// both declare NoSuchEntity (404) for an unknown DelegationRequestId in their
// own deserializeOpError switch (api_op_AcceptDelegationRequest.go,
// api_op_AssociateDelegationRequest.go: ConcurrentModification/NoSuchEntity/
// ServiceFailure -- AssociateDelegationRequest additionally has InvalidInput),
// but previously returned InvalidAction (400) instead, unlike the three
// sibling ops (Reject/Send/UpdateDelegationRequest) already fixed in
// 00f9a47ef. Driven through the real SDK client since the type of
// AssociateDelegationRequestInput below has no PolicyArn field at all --
// the real input carries only DelegationRequestId -- proving that field was
// never reachable by a real caller.
func TestAcceptAssociateDelegationRequest_UnknownID_NoSuchEntity(t *testing.T) {
	t.Parallel()

	t.Run("acceptdelegationrequest unknown id is nosuchentity", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newDelegationTestClient(t, h)

		_, err := client.AcceptDelegationRequest(t.Context(), &iamsdk.AcceptDelegationRequestInput{
			DelegationRequestId: aws.String("does-not-exist"),
		})
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "NoSuchEntity", apiErr.ErrorCode())
	})

	t.Run("associatedelegationrequest unknown id is nosuchentity", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newDelegationTestClient(t, h)

		_, err := client.AssociateDelegationRequest(t.Context(), &iamsdk.AssociateDelegationRequestInput{
			DelegationRequestId: aws.String("does-not-exist"),
		})
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "NoSuchEntity", apiErr.ErrorCode())
	})
}

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

		seeded, err := b.CreateDelegationRequest(CreateDelegationRequestInput{
			OwnerAccountID:      "111122223333",
			Description:         "test delegation",
			NotificationChannel: "arn:aws:sns:us-east-1:000000000000:topic",
			RequestorWorkflowID: "workflow-1",
			SessionDuration:     3600,
			PolicyTemplateArn:   "arn:aws:iam::aws:policy/ReadOnlyAccess",
		})
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

		seeded, err := b.CreateDelegationRequest(CreateDelegationRequestInput{
			OwnerAccountID:      "111122223333",
			Description:         "test delegation",
			NotificationChannel: "arn:aws:sns:us-east-1:000000000000:topic",
			RequestorWorkflowID: "workflow-1",
			SessionDuration:     3600,
			PolicyTemplateArn:   "arn:aws:iam::aws:policy/ReadOnlyAccess",
		})
		require.NoError(t, err)

		_, err = client.AssociateDelegationRequest(t.Context(), &iamsdk.AssociateDelegationRequestInput{
			DelegationRequestId: aws.String(seeded.DelegationID),
		})
		require.NoError(t, err)
	})
}

// seedDelegationRequestForMutationTests creates a real delegation request
// directly against the backend, mirroring the fixture already used above.
func seedDelegationRequestForMutationTests(t *testing.T, b *InMemoryBackend) *DelegationRequest {
	t.Helper()

	req, err := b.CreateDelegationRequest(CreateDelegationRequestInput{
		OwnerAccountID:      "111122223333",
		Description:         "test delegation",
		NotificationChannel: "arn:aws:sns:us-east-1:000000000000:topic",
		RequestorWorkflowID: "workflow-1",
		SessionDuration:     3600,
		PolicyTemplateArn:   "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	require.NoError(t, err)

	return req
}

// TestRejectSendUpdateDelegationRequest_MutateRealState covers
// RejectDelegationRequest, SendDelegationToken and UpdateDelegationRequest
// (handler_account.go), which previously ignored their required
// DelegationRequestId entirely and returned success without reading or
// mutating any stored delegation-request state -- the same undisclosed-drop
// shape CreateDelegationRequest had before gopherstack-oxuf (38d3ee94b).
// Driven through the real aws-sdk-go-v2 client so the wire keys asserted
// against (DelegationRequestId, Notes) are what AWS actually sends, not what
// the handler assumes.
func TestRejectSendUpdateDelegationRequest_MutateRealState(t *testing.T) {
	t.Parallel()

	t.Run("rejectdelegationrequest_sets_rejected_and_stores_notes", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newDelegationTestClient(t, h)
		seeded := seedDelegationRequestForMutationTests(t, b)

		_, err := client.RejectDelegationRequest(t.Context(), &iamsdk.RejectDelegationRequestInput{
			DelegationRequestId: aws.String(seeded.DelegationID),
			Notes:               aws.String("no longer needed"),
		})
		require.NoError(t, err)

		stored, exists := b.delegationRequests.Get(seeded.DelegationID)
		require.True(t, exists)
		assert.Equal(t, "REJECTED", stored.Status)
		assert.Equal(t, "no longer needed", stored.Notes)
	})

	t.Run("senddelegationtoken_sets_finalized", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newDelegationTestClient(t, h)
		seeded := seedDelegationRequestForMutationTests(t, b)

		_, err := client.SendDelegationToken(t.Context(), &iamsdk.SendDelegationTokenInput{
			DelegationRequestId: aws.String(seeded.DelegationID),
		})
		require.NoError(t, err)

		stored, exists := b.delegationRequests.Get(seeded.DelegationID)
		require.True(t, exists)
		assert.Equal(t, "FINALIZED", stored.Status)
	})

	t.Run("updatedelegationrequest_sets_pending_approval_and_stores_notes", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newDelegationTestClient(t, h)
		seeded := seedDelegationRequestForMutationTests(t, b)

		_, err := client.UpdateDelegationRequest(t.Context(), &iamsdk.UpdateDelegationRequestInput{
			DelegationRequestId: aws.String(seeded.DelegationID),
			Notes:               aws.String("adding more context"),
		})
		require.NoError(t, err)

		stored, exists := b.delegationRequests.Get(seeded.DelegationID)
		require.True(t, exists)
		assert.Equal(t, "PENDING_APPROVAL", stored.Status)
		assert.Equal(t, "adding more context", stored.Notes)
	})

	t.Run("unknown delegation request id is NoSuchEntity for all three", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend()
		h := NewHandler(b)
		client := newDelegationTestClient(t, h)

		_, rejectErr := client.RejectDelegationRequest(t.Context(), &iamsdk.RejectDelegationRequestInput{
			DelegationRequestId: aws.String("does-not-exist"),
		})
		_, sendErr := client.SendDelegationToken(t.Context(), &iamsdk.SendDelegationTokenInput{
			DelegationRequestId: aws.String("does-not-exist"),
		})
		_, updateErr := client.UpdateDelegationRequest(t.Context(), &iamsdk.UpdateDelegationRequestInput{
			DelegationRequestId: aws.String("does-not-exist"),
		})

		for _, err := range []error{rejectErr, sendErr, updateErr} {
			require.Error(t, err)

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "NoSuchEntity", apiErr.ErrorCode())
		}
	})
}
