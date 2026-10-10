package iam_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

func newCallerDelegationClient(t *testing.T, h *iam.Handler, callerARN string) *iamsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			m := &awsmeta.Metadata{
				Account:   "111122223333",
				Partition: awsmeta.DefaultPartition,
				Principal: &awsmeta.Principal{Arn: callerARN, AccountID: "111122223333"},
			}
			c.SetRequest(c.Request().WithContext(awsmeta.Set(c.Request().Context(), m)))

			return next(c)
		}
	})
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return iamsdk.NewFromConfig(cfg, func(o *iamsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

func TestDelegationRequest_CallerIdentity(t *testing.T) {
	t.Parallel()

	const (
		ownerARN    = "arn:aws:iam::111122223333:user/owner"
		approverARN = "arn:aws:iam::111122223333:user/approver"
	)

	b := iam.NewInMemoryBackend()
	h := iam.NewHandler(b)
	owner := newCallerDelegationClient(t, h, ownerARN)
	approver := newCallerDelegationClient(t, h, approverARN)

	mk := func(t *testing.T) string {
		t.Helper()

		out, err := owner.CreateDelegationRequest(t.Context(), &iamsdk.CreateDelegationRequestInput{
			Description: aws.String("d"), NotificationChannel: aws.String("arn:aws:sns:us-east-1:111122223333:t"),
			RequestorWorkflowId: aws.String("wf-" + t.Name()), SessionDuration: aws.Int32(3600),
			Permissions: &iamtypes.DelegationPermission{PolicyTemplateArn: aws.String("arn:aws:iam::aws:policy/p")},
		})
		require.NoError(t, err)

		return aws.ToString(out.DelegationRequestId)
	}

	owned, other := mk(t), mk(t)

	_, err := owner.AssociateDelegationRequest(t.Context(), &iamsdk.AssociateDelegationRequestInput{
		DelegationRequestId: aws.String(owned),
	})
	require.NoError(t, err)

	_, err = approver.AcceptDelegationRequest(t.Context(), &iamsdk.AcceptDelegationRequestInput{
		DelegationRequestId: aws.String(owned),
	})
	require.NoError(t, err)

	got, err := owner.GetDelegationRequest(t.Context(), &iamsdk.GetDelegationRequestInput{
		DelegationRequestId: aws.String(owned),
	})
	require.NoError(t, err)
	assert.Equal(t, ownerARN, aws.ToString(got.DelegationRequest.OwnerId))
	assert.Equal(t, approverARN, aws.ToString(got.DelegationRequest.ApproverId))
	assert.Equal(t, "111122223333", aws.ToString(got.DelegationRequest.RequestorId))
	assert.Equal(t, "111122223333", aws.ToString(got.DelegationRequest.OwnerAccountId))

	filtered, err := owner.ListDelegationRequests(t.Context(), &iamsdk.ListDelegationRequestsInput{
		OwnerId: aws.String(ownerARN),
	})
	require.NoError(t, err)
	require.Len(t, filtered.DelegationRequests, 1)
	assert.Equal(t, owned, aws.ToString(filtered.DelegationRequests[0].DelegationRequestId))

	all, err := owner.ListDelegationRequests(t.Context(), &iamsdk.ListDelegationRequestsInput{})
	require.NoError(t, err)
	assert.Len(t, all.DelegationRequests, 2)
	assert.NotEqual(t, owned, other)
}
