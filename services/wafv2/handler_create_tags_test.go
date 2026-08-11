package wafv2_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// newTestWAFV2Client stands up the real aws-sdk-go-v2 WAFv2 client against
// an httptest server running this package's Handler, wired through the
// same pkgs/service registry/router used in production.
func newTestWAFV2Client(t *testing.T, h *wafv2.Handler) *wafv2sdk.Client {
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

	return wafv2sdk.NewFromConfig(cfg, func(o *wafv2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives every WAFv2 Create op that accepts
// Tags in the real SDK (wafv2@v1.77.3: CreateIPSet, CreateRegexPatternSet,
// CreateRuleGroup, CreateWebACL) through a real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}
	vc := &types.VisibilityConfig{
		CloudWatchMetricsEnabled: true,
		MetricName:               aws.String("metric"),
		SampledRequestsEnabled:   true,
	}

	requireTags := func(t *testing.T, client *wafv2sdk.Client, resourceARN *string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &wafv2sdk.ListTagsForResourceInput{
			ResourceARN: resourceARN,
		})
		require.NoError(t, err)
		require.NotNil(t, out.TagInfoForResource)
		require.Len(t, out.TagInfoForResource.TagList, 1)
		assert.Equal(t, "env", aws.ToString(out.TagInfoForResource.TagList[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.TagInfoForResource.TagList[0].Value))
	}

	newClient := func(t *testing.T) *wafv2sdk.Client {
		t.Helper()

		h := wafv2.NewHandler(wafv2.NewInMemoryBackend("123456789012", "us-east-1"))

		return newTestWAFV2Client(t, h)
	}

	t.Run("createipset", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateIPSet(t.Context(), &wafv2sdk.CreateIPSetInput{
			Name:             aws.String("tagged-ipset"),
			Scope:            types.ScopeRegional,
			IPAddressVersion: types.IPAddressVersionIpv4,
			Addresses:        []string{"10.0.0.0/8"},
			Tags:             tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.Summary.ARN)
	})

	t.Run("createregexpatternset", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateRegexPatternSet(t.Context(), &wafv2sdk.CreateRegexPatternSetInput{
			Name:                  aws.String("tagged-regexset"),
			Scope:                 types.ScopeRegional,
			RegularExpressionList: []types.Regex{{RegexString: aws.String("^abc$")}},
			Tags:                  tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.Summary.ARN)
	})

	t.Run("createrulegroup", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateRuleGroup(t.Context(), &wafv2sdk.CreateRuleGroupInput{
			Name:             aws.String("tagged-rulegroup"),
			Scope:            types.ScopeRegional,
			Capacity:         aws.Int64(10),
			VisibilityConfig: vc,
			Tags:             tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.Summary.ARN)
	})

	t.Run("createwebacl", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateWebACL(t.Context(), &wafv2sdk.CreateWebACLInput{
			Name:             aws.String("tagged-webacl"),
			Scope:            types.ScopeRegional,
			DefaultAction:    &types.DefaultAction{Allow: &types.AllowAction{}},
			VisibilityConfig: vc,
			Tags:             tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.Summary.ARN)
	})
}
