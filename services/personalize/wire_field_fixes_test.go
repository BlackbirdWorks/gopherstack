package personalize_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// newTestPersonalizeClient stands up the real aws-sdk-go-v2 personalize
// (control-plane, JSON-RPC 1.1) client against an httptest server running
// this package's Handler, wired through the same pkgs/service registry/
// router used in production.
func newTestPersonalizeClient(t *testing.T, h *personalize.Handler) *personalizesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return personalizesdk.NewFromConfig(cfg, func(o *personalizesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestListFilters_RealSDKClient locks ListFiltersOutput's real wrapper key.
// The real deserializer's case is "Filters" (PascalCase) -- the sole
// PascalCase top-level wrapper key in this JSON-RPC 1.1 service, unlike
// every sibling List op's lowerCamelCase. gopherstack emitted "filters"
// (lowercase); a raw-body test that only checks for a "filters" key can't
// catch this since both sides agreed on the wrong name -- only a real typed
// client, whose case-sensitive deserializer silently drops unrecognised
// keys, proves it.
func TestListFilters_RealSDKClient(t *testing.T) {
	t.Parallel()

	b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
	h := personalize.NewHandler(b)
	client := newTestPersonalizeClient(t, h)

	dgArn := personalizeCreateDatasetGroup(t, h, "list-filters-real-dg")
	rec := personalizeDo(t, h, "CreateFilter", map[string]any{
		"name":             "list-filters-real",
		"datasetGroupArn":  dgArn,
		"filterExpression": "INCLUDE ItemID WHERE Items.CATEGORY IN ($CATEGORIES)",
	})
	require.Equal(t, 200, rec.Code)

	out, err := client.ListFilters(t.Context(), &personalizesdk.ListFiltersInput{
		DatasetGroupArn: aws.String(dgArn),
	})
	require.NoError(t, err)
	require.Len(t, out.Filters, 1)
	assert.Equal(t, "list-filters-real", aws.ToString(out.Filters[0].Name))
}

// TestDescribeEventTracker_AccountID locks that DescribeEventTracker emits
// accountId -- a real, always-populated EventTracker member ("The Amazon Web
// Services account that owns the event tracker") this backend already knows
// (the same accountID used to build every ARN) but never wired onto this
// one response before this fix.
func TestDescribeEventTracker_AccountID(t *testing.T) {
	t.Parallel()

	b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
	h := personalize.NewHandler(b)
	client := newTestPersonalizeClient(t, h)

	dgArn := personalizeCreateDatasetGroup(t, h, "event-tracker-account-dg")
	rec := personalizeDo(t, h, "CreateEventTracker", map[string]any{
		"name":            "event-tracker-account",
		"datasetGroupArn": dgArn,
	})
	require.Equal(t, 200, rec.Code)
	etArn, _ := personalizeUnmarshal(t, rec)["eventTrackerArn"].(string)
	require.NotEmpty(t, etArn)

	out, err := client.DescribeEventTracker(t.Context(), &personalizesdk.DescribeEventTrackerInput{
		EventTrackerArn: aws.String(etArn),
	})
	require.NoError(t, err)
	assert.Equal(t, "000000000000", aws.ToString(out.EventTracker.AccountId))
}
