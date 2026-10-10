package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/aws/aws-sdk-go-v2/service/wafv2"
	wafv2types "github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppSyncWafWebACLArnWiring(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	as := appsync.NewFromConfig(fx.cfg)
	waf := wafv2.NewFromConfig(fx.cfg)

	api, err := as.CreateGraphqlApi(t.Context(), &appsync.CreateGraphqlApiInput{
		Name: aws.String("g"), AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	before, err := as.GetGraphqlApi(t.Context(), &appsync.GetGraphqlApiInput{ApiId: api.GraphqlApi.ApiId})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(before.GraphqlApi.WafWebAclArn))

	acl, err := waf.CreateWebACL(t.Context(), &wafv2.CreateWebACLInput{
		Name: aws.String("acl"), Scope: wafv2types.ScopeRegional,
		DefaultAction: &wafv2types.DefaultAction{Allow: &wafv2types.AllowAction{}},
		VisibilityConfig: &wafv2types.VisibilityConfig{
			CloudWatchMetricsEnabled: true, MetricName: aws.String("m"), SampledRequestsEnabled: true,
		},
	})
	require.NoError(t, err)

	_, err = waf.AssociateWebACL(t.Context(), &wafv2.AssociateWebACLInput{
		WebACLArn: acl.Summary.ARN, ResourceArn: api.GraphqlApi.Arn,
	})
	require.NoError(t, err)

	after, err := as.GetGraphqlApi(t.Context(), &appsync.GetGraphqlApiInput{ApiId: api.GraphqlApi.ApiId})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(acl.Summary.ARN), aws.ToString(after.GraphqlApi.WafWebAclArn))
}
