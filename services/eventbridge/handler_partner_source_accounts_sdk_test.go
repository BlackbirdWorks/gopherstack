package eventbridge_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func newTestEventBridgeClient(t *testing.T, h *eventbridge.Handler) *eventbridgesdk.Client {
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

	return eventbridgesdk.NewFromConfig(cfg, func(o *eventbridgesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestListPartnerEventSourceAccounts_RoundTrip drives
// CreatePartnerEventSource -> ListPartnerEventSourceAccounts through a real
// SDK client and proves the account genuinely offered a partner source is
// returned, instead of the pre-fix behavior that unconditionally returned an
// empty list regardless of EventSourceName -- discarding real state this
// backend already tracks (partnerSourcesTable's Account field, set at
// CreatePartnerEventSource) behind a comment claiming cross-account state
// has "no meaningful in-process simulation" (gopherstack-h910).
func TestListPartnerEventSourceAccounts_RoundTrip(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	const sourceName = "acme/orders/created"
	const offeredAccount = "111122223333"

	_, err := client.CreatePartnerEventSource(t.Context(), &eventbridgesdk.CreatePartnerEventSourceInput{
		Name:    aws.String(sourceName),
		Account: aws.String(offeredAccount),
	})
	require.NoError(t, err)

	pending, err := client.ListPartnerEventSourceAccounts(
		t.Context(),
		&eventbridgesdk.ListPartnerEventSourceAccountsInput{
			EventSourceName: aws.String(sourceName),
		},
	)
	require.NoError(t, err)
	require.Len(t, pending.PartnerEventSourceAccounts, 1)
	assert.Equal(t, offeredAccount, aws.ToString(pending.PartnerEventSourceAccounts[0].Account))
	assert.Equal(t, "PENDING", string(pending.PartnerEventSourceAccounts[0].State))

	_, err = client.ActivateEventSource(t.Context(), &eventbridgesdk.ActivateEventSourceInput{
		Name: aws.String(sourceName),
	})
	require.NoError(t, err)

	active, err := client.ListPartnerEventSourceAccounts(
		t.Context(),
		&eventbridgesdk.ListPartnerEventSourceAccountsInput{
			EventSourceName: aws.String(sourceName),
		},
	)
	require.NoError(t, err)
	require.Len(t, active.PartnerEventSourceAccounts, 1)
	assert.Equal(t, "ACTIVE", string(active.PartnerEventSourceAccounts[0].State))

	_, err = client.ListPartnerEventSourceAccounts(t.Context(), &eventbridgesdk.ListPartnerEventSourceAccountsInput{
		EventSourceName: aws.String("no/such/source"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

// TestListPartnerEventSourceAccounts_ClientRequiresEventSourceName proves the
// real SDK client itself refuses to send ListPartnerEventSourceAccounts
// without EventSourceName, confirming it is genuinely a required member on
// the pinned SDK, not an assumption (gopherstack-h910).
func TestListPartnerEventSourceAccounts_ClientRequiresEventSourceName(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	_, err := client.ListPartnerEventSourceAccounts(t.Context(), &eventbridgesdk.ListPartnerEventSourceAccountsInput{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EventSourceName")
}
