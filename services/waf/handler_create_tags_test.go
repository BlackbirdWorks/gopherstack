package waf_test

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	wafsdk "github.com/aws/aws-sdk-go-v2/service/waf"
	"github.com/aws/aws-sdk-go-v2/service/waf/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/waf"
)

// newTestWAFClient stands up the real aws-sdk-go-v2 WAF (Classic) client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestWAFClient(t *testing.T, h *waf.Handler) *wafsdk.Client {
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

	return wafsdk.NewFromConfig(cfg, func(o *wafsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives every WAF Classic Create op that
// accepts Tags in the real SDK (waf@v1.33.4: CreateRateBasedRule, CreateRule,
// CreateRuleGroup, CreateWebACL) through a real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
// Also covers a wire-shape fix found along the way: ListTagsForResourceOutput
// nests NextMarker as a sibling of TagInfoForResource, not inside it
// (verified: waf@v1.33.4 api_op_ListTagsForResource.go:62-68) -- gopherstack
// nested it under TagInfoForResource, which would have hidden pagination
// tokens from a real client.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	requireTags := func(t *testing.T, client *wafsdk.Client, resourceARN *string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &wafsdk.ListTagsForResourceInput{
			ResourceARN: resourceARN,
		})
		require.NoError(t, err)
		require.NotNil(t, out.TagInfoForResource)
		require.Len(t, out.TagInfoForResource.TagList, 1)
		assert.Equal(t, "env", aws.ToString(out.TagInfoForResource.TagList[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.TagInfoForResource.TagList[0].Value))
	}

	newClient := func(t *testing.T) *wafsdk.Client {
		t.Helper()

		h := waf.NewHandler(waf.NewInMemoryBackend("123456789012", "us-east-1"))

		return newTestWAFClient(t, h)
	}

	changeToken := func(t *testing.T, client *wafsdk.Client) *string {
		t.Helper()

		out, err := client.GetChangeToken(t.Context(), &wafsdk.GetChangeTokenInput{})
		require.NoError(t, err)

		return out.ChangeToken
	}

	t.Run("createratebasedrule", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateRateBasedRule(t.Context(), &wafsdk.CreateRateBasedRuleInput{
			Name:        aws.String("tagged-rbr"),
			MetricName:  aws.String("taggedrbr"),
			RateKey:     types.RateKeyIp,
			RateLimit:   aws.Int64(2000),
			ChangeToken: changeToken(t, client),
			Tags:        tags,
		})
		require.NoError(t, err)
		requireTags(t, client, resourceARN(t, "ratebasedrule", out.Rule.RuleId))
	})

	t.Run("createrule", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateRule(t.Context(), &wafsdk.CreateRuleInput{
			Name:        aws.String("tagged-rule"),
			MetricName:  aws.String("taggedrule"),
			ChangeToken: changeToken(t, client),
			Tags:        tags,
		})
		require.NoError(t, err)
		requireTags(t, client, resourceARN(t, "rule", out.Rule.RuleId))
	})

	t.Run("createrulegroup", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateRuleGroup(t.Context(), &wafsdk.CreateRuleGroupInput{
			Name:        aws.String("tagged-rulegroup"),
			MetricName:  aws.String("taggedrulegroup"),
			ChangeToken: changeToken(t, client),
			Tags:        tags,
		})
		require.NoError(t, err)
		requireTags(t, client, resourceARN(t, "rulegroup", out.RuleGroup.RuleGroupId))
	})

	t.Run("createwebacl", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateWebACL(t.Context(), &wafsdk.CreateWebACLInput{
			Name:       aws.String("tagged-webacl"),
			MetricName: aws.String("taggedwebacl"),
			DefaultAction: &types.WafAction{
				Type: types.WafActionTypeAllow,
			},
			ChangeToken: changeToken(t, client),
			Tags:        tags,
		})
		require.NoError(t, err)
		requireTags(t, client, resourceARN(t, "webacl", out.WebACL.WebACLId))
	})

	t.Run("listtagsforresource_nextmarker_is_top_level", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateWebACL(t.Context(), &wafsdk.CreateWebACLInput{
			Name:       aws.String("paginated-webacl"),
			MetricName: aws.String("paginatedwebacl"),
			DefaultAction: &types.WafAction{
				Type: types.WafActionTypeAllow,
			},
			ChangeToken: changeToken(t, client),
			Tags: []types.Tag{
				{Key: aws.String("a"), Value: aws.String("1")},
				{Key: aws.String("b"), Value: aws.String("2")},
			},
		})
		require.NoError(t, err)

		tagsOut, err := client.ListTagsForResource(t.Context(), &wafsdk.ListTagsForResourceInput{
			ResourceARN: resourceARN(t, "webacl", out.WebACL.WebACLId),
			Limit:       1,
		})
		require.NoError(t, err)
		require.NotNil(
			t, tagsOut.NextMarker,
			"NextMarker must be readable at the top level, not nested inside TagInfoForResource",
		)
		require.Len(t, tagsOut.TagInfoForResource.TagList, 1)
	})
}

// resourceARN builds a WAF Classic ARN the way a real client must: none of
// Rule/RuleGroup/RateBasedRule/WebACL's Create responses include an ARN, so
// callers construct it from the documented global format
// "arn:aws:waf::{account}:{resourcetype}/{id}" -- matching gopherstack's own
// webACLARN/ruleARN/ruleGroupARN/rateBasedRuleARN builders.
func resourceARN(t *testing.T, resourceType string, id *string) *string {
	t.Helper()

	return aws.String(fmt.Sprintf("arn:aws:waf::123456789012:%s/%s", resourceType, aws.ToString(id)))
}
