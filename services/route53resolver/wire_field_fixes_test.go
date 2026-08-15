package route53resolver_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	route53resolversdk "github.com/aws/aws-sdk-go-v2/service/route53resolver"
	"github.com/aws/aws-sdk-go-v2/service/route53resolver/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// newTestRoute53ResolverClient stands up the real aws-sdk-go-v2 Resolver
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestRoute53ResolverClient(
	t *testing.T,
	h *route53resolver.Handler,
) *route53resolversdk.Client {
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

	return route53resolversdk.NewFromConfig(cfg, func(o *route53resolversdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestListResolverQueryLogConfigs_TotalCounts covers gopherstack-6flj:
// ListResolverQueryLogConfigsOutput.TotalCount/TotalFilteredCount are real,
// always-populated members (deserializers.go's
// awsAwsjson11_deserializeOpDocumentListResolverQueryLogConfigsOutput has
// both cases) that were never wired at all, leaving a real SDK client's
// typed fields at 0 regardless of how many configs existed.
func TestListResolverQueryLogConfigs_TotalCounts(t *testing.T) {
	t.Parallel()

	backend := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	h := route53resolver.NewHandler(backend)
	client := newTestRoute53ResolverClient(t, h)
	ctx := t.Context()

	for i := range 3 {
		name := []string{"cfg-a", "cfg-b", "cfg-c"}[i]
		_, err := client.CreateResolverQueryLogConfig(
			ctx,
			&route53resolversdk.CreateResolverQueryLogConfigInput{
				Name:           aws.String(name),
				DestinationArn: aws.String("arn:aws:s3:::bucket-" + name),
			},
		)
		require.NoError(t, err)
	}

	all, err := client.ListResolverQueryLogConfigs(
		ctx,
		&route53resolversdk.ListResolverQueryLogConfigsInput{},
	)
	require.NoError(t, err)
	require.Equal(t, int32(3), all.TotalCount)
	require.Equal(t, int32(3), all.TotalFilteredCount)

	filtered, err := client.ListResolverQueryLogConfigs(
		ctx,
		&route53resolversdk.ListResolverQueryLogConfigsInput{
			Filters: []types.Filter{{Name: aws.String("Name"), Values: []string{"cfg-a"}}},
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(3), filtered.TotalCount, "TotalCount is the unfiltered account total")
	require.Equal(
		t,
		int32(1),
		filtered.TotalFilteredCount,
		"TotalFilteredCount reflects the Filters applied",
	)
}

// TestListResolverQueryLogConfigAssociations_TotalCounts is the association
// sibling of TestListResolverQueryLogConfigs_TotalCounts -- same gap, same
// two real output members, same fix (gopherstack-6flj).
func TestListResolverQueryLogConfigAssociations_TotalCounts(t *testing.T) {
	t.Parallel()

	backend := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	h := route53resolver.NewHandler(backend)
	client := newTestRoute53ResolverClient(t, h)
	ctx := t.Context()

	cfg, err := client.CreateResolverQueryLogConfig(
		ctx,
		&route53resolversdk.CreateResolverQueryLogConfigInput{
			Name:           aws.String("cfg"),
			DestinationArn: aws.String("arn:aws:s3:::bucket"),
		},
	)
	require.NoError(t, err)

	for i := range 2 {
		resourceID := []string{"vpc-1", "vpc-2"}[i]
		_, assocErr := client.AssociateResolverQueryLogConfig(
			ctx,
			&route53resolversdk.AssociateResolverQueryLogConfigInput{
				ResolverQueryLogConfigId: cfg.ResolverQueryLogConfig.Id,
				ResourceId:               aws.String(resourceID),
			},
		)
		require.NoError(t, assocErr)
	}

	out, err := client.ListResolverQueryLogConfigAssociations(
		ctx, &route53resolversdk.ListResolverQueryLogConfigAssociationsInput{},
	)
	require.NoError(t, err)
	require.Equal(t, int32(2), out.TotalCount)
	require.Equal(t, int32(2), out.TotalFilteredCount)
}

// No test for resolverRuleAssociationOutput.StatusMessage (gopherstack-6flj,
// see handler_rule_associations.go's doc comment): the field is real
// (types.ResolverRuleAssociation, deserializers.go) but this backend never
// has a non-empty value to put in it, and it is tagged omitempty to match
// the real API's own "absent when there's nothing to report" behavior. That
// makes "key present" and "key absent" indistinguishable on the wire in
// both the fixed and unfixed code -- any round-trip assertion here would
// pass identically either way, the exact "assertion too weak to fail" trap
// this campaign warns about. A first attempt at this test was written,
// confirmed to pass against the pre-fix code too, and deliberately dropped
// rather than kept as false assurance. The shape fix stands undemonstrated
// by a test; flagged here rather than silently omitted.
