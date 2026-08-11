package iot_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

// newTestIoTClient stands up the real aws-sdk-go-v2 IoT client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestIoTClient(t *testing.T, h *iot.Handler) *iotsdk.Client {
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

	return iotsdk.NewFromConfig(cfg, func(o *iotsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip re-verifies iot's tag-creation wiring
// (gopherstack-2mwl) since the prior fix (commit 9e811a1a7) predates the
// decode-drop and header-vs-body failure modes later sweep passes found
// elsewhere. It drives every Create op through a real SDK client and
// asserts ListTagsForResource sees what was supplied at creation.
//
// This re-verification found the largest bug in the whole sweep: RouteMatcher
// (matchIoTPath) never listed "/tags" at all, so TagResource/UntagResource/
// ListTagsForResource -- the read path this entire issue is about -- 404'd
// for a real client regardless of whether creation-time tag storage was
// correct. The same gate was also missing the Create path for roughly half
// of iot's 24 tag-accepting resource families (authorizers, billing groups,
// commands, custom metrics, dimensions, domain configurations, dynamic thing
// groups, fleet metrics, OTA updates, packages, provisioning templates, role
// aliases, streams), making those Create ops unreachable outright. Both
// gaps were invisible to source review and to unit tests that call the
// backend/handler directly, bypassing service.Router entirely -- only a real
// client round-tripped through the router catches them. Fixed via
// handler_routing.go's new matchTaggableResourcePath.
//
// createotaupdate and createtopicrule are additionally genuine decode-drop
// bugs on top of the routing gap: CreateOTAUpdate's backend signature had no
// tags parameter at all; CreateTopicRule never read tags at all --
// CreateTopicRuleInput.Tags travels as a query-string-encoded
// "key=value&..." string in the X-Amz-Tagging HTTP header, not the JSON body
// (verified: iot@v1.77.4 api_op_CreateTopicRule.go:55, serializers.go:5083
// awsRestjson1_serializeOpHttpBindingsCreateTopicRuleInput).
//
// The remaining subtests are spot checks: createthinggroup/createbillinggroup
// confirm the already-wired putResourceTagsLocked call sites still work now
// that they're reachable; createauthorizer/createstream/createrolealias/
// createdomainconfiguration/createcustommetric spot-check resource families
// that were unreachable before the routing fix.
//
// Not itself a routing-gate gap: 25 unrelated iot paths remain unmatched by
// matchIoTPath (certificate management, logging config, destinations, etc.)
// -- none accept tags at creation, so they are out of this issue's scope and
// left for separate follow-up (tracked for the orchestrator's report).
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	requireTags := func(t *testing.T, client *iotsdk.Client, resourceARN *string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &iotsdk.ListTagsForResourceInput{
			ResourceArn: resourceARN,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	}

	newClient := func(t *testing.T) *iotsdk.Client {
		t.Helper()

		h := iot.NewHandler(iot.NewInMemoryBackendWithConfig("123456789012", "us-east-1"), nil)

		return newTestIoTClient(t, h)
	}

	t.Run("createotaupdate", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateOTAUpdate(t.Context(), &iotsdk.CreateOTAUpdateInput{
			OtaUpdateId: aws.String("tagged-ota-update"),
			RoleArn:     aws.String("arn:aws:iam::123456789012:role/ota-role"),
			Targets:     []string{"arn:aws:iot:us-east-1:123456789012:thing/some-thing"},
			Files: []types.OTAUpdateFile{
				{FileName: aws.String("firmware.bin")},
			},
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.OtaUpdateArn)
	})

	t.Run("createtopicrule", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		_, err := client.CreateTopicRule(t.Context(), &iotsdk.CreateTopicRuleInput{
			RuleName: aws.String("taggedTopicRule"),
			TopicRulePayload: &types.TopicRulePayload{
				Sql:     aws.String("SELECT * FROM 'topic/#'"),
				Actions: []types.Action{},
			},
			Tags: aws.String("env=prod"),
		})
		require.NoError(t, err)

		ruleARN := "arn:aws:iot:us-east-1:123456789012:rule/taggedTopicRule"
		requireTags(t, client, aws.String(ruleARN))
	})

	t.Run("createthinggroup", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{
			ThingGroupName: aws.String("tagged-thing-group"),
			Tags:           tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.ThingGroupArn)
	})

	t.Run("createbillinggroup", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateBillingGroup(t.Context(), &iotsdk.CreateBillingGroupInput{
			BillingGroupName: aws.String("tagged-billing-group"),
			Tags:             tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.BillingGroupArn)
	})

	// The remaining subtests below exercise resource families whose Create
	// path was entirely unreachable through service.Router before this
	// pass -- matchIoTPath's route-matcher gate never listed "/tags" or
	// most of these Create paths at all, so a real client 404'd before
	// resolveOperation/the handler ever ran, regardless of whether the tag
	// wiring inside the handler was correct. Fixed in handler_routing.go's
	// new matchTaggableResourcePath.

	t.Run("createauthorizer", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateAuthorizer(t.Context(), &iotsdk.CreateAuthorizerInput{
			AuthorizerName:        aws.String("tagged-authorizer"),
			AuthorizerFunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:auth"),
			Tags:                  tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.AuthorizerArn)
	})

	t.Run("createstream", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateStream(t.Context(), &iotsdk.CreateStreamInput{
			StreamId: aws.String("tagged-stream"),
			RoleArn:  aws.String("arn:aws:iam::123456789012:role/stream-role"),
			Files: []types.StreamFile{
				{FileId: aws.Int32(1)},
			},
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.StreamArn)
	})

	t.Run("createrolealias", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateRoleAlias(t.Context(), &iotsdk.CreateRoleAliasInput{
			RoleAlias: aws.String("tagged-role-alias"),
			RoleArn:   aws.String("arn:aws:iam::123456789012:role/aliased-role"),
			Tags:      tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.RoleAliasArn)
	})

	t.Run("createdomainconfiguration", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateDomainConfiguration(t.Context(), &iotsdk.CreateDomainConfigurationInput{
			DomainConfigurationName: aws.String("tagged-domain-config"),
			Tags:                    tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.DomainConfigurationArn)
	})

	t.Run("createcustommetric", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateCustomMetric(t.Context(), &iotsdk.CreateCustomMetricInput{
			MetricName:         aws.String("taggedCustomMetric"),
			MetricType:         types.CustomMetricTypeNumberList,
			ClientRequestToken: aws.String("token-1"),
			Tags:               tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.MetricArn)
	})
}
