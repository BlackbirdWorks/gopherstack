package app

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	configsdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	configtypes "github.com/aws/aws-sdk-go-v2/service/configservice/types"
	tagsdk "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const taggingOtherAcct = "111111111111"

func taggingAccountServer(t *testing.T) string {
	t.Helper()

	byName := crossRegionServices(t)
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(byName["AWSConfig"]))
	require.NoError(t, registry.Register(byName["ResourceGroupsTaggingAPI"]))

	e := echo.New()
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			r := c.Request()
			c.SetRequest(r.WithContext(awsmeta.Set(r.Context(), awsmeta.FromRequest(r, crossHome))))

			return next(c)
		}
	})
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv.URL
}

func taggingSDKConfig(t *testing.T, account, region string) aws.Config {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(account, "secret", "")),
	)
	require.NoError(t, err)

	return cfg
}

func TestTaggingBridge_AWSConfigIsAccountScoped(t *testing.T) {
	t.Parallel()

	url := taggingAccountServer(t)
	ctx := t.Context()

	cfgClient := func(account, region string) *configsdk.Client {
		return configsdk.NewFromConfig(taggingSDKConfig(t, account, region), func(o *configsdk.Options) {
			o.BaseEndpoint = aws.String(url)
		})
	}
	tagClient := func(account, region string) *tagsdk.Client {
		return tagsdk.NewFromConfig(taggingSDKConfig(t, account, region), func(o *tagsdk.Options) {
			o.BaseEndpoint = aws.String(url)
		})
	}

	type scope struct{ account, region string }

	scopes := []scope{
		{crossAcct, crossHome}, {crossAcct, euRegion},
		{taggingOtherAcct, crossHome}, {taggingOtherAcct, euRegion},
	}

	for _, s := range scopes {
		_, err := cfgClient(s.account, s.region).PutConfigRule(ctx, &configsdk.PutConfigRuleInput{
			ConfigRule: &configtypes.ConfigRule{
				ConfigRuleName: aws.String("rule-" + s.account),
				Source: &configtypes.Source{
					Owner: configtypes.OwnerAws, SourceIdentifier: aws.String("S3_BUCKET_VERSIONING_ENABLED"),
				},
			},
			Tags: []configtypes.Tag{{Key: aws.String("Scope"), Value: aws.String(s.account + s.region)}},
		})
		require.NoError(t, err)
	}

	listARNs := func(s scope) []string {
		out, err := tagClient(s.account, s.region).GetResources(ctx, &tagsdk.GetResourcesInput{})
		require.NoError(t, err)

		arns := make([]string, 0, len(out.ResourceTagMappingList))
		for _, m := range out.ResourceTagMappingList {
			arns = append(arns, aws.ToString(m.ResourceARN))
		}

		return arns
	}

	for _, s := range scopes {
		arns := listARNs(s)
		require.Len(t, arns, 1, "scope %v", s)
		assert.Contains(t, arns[0], ":"+s.region+":"+s.account+":", "scope %v", s)
	}

	other := scope{taggingOtherAcct, crossHome}
	otherARN := listARNs(other)[0]
	tc := tagClient(other.account, other.region)

	res, err := tc.TagResources(ctx, &tagsdk.TagResourcesInput{
		ResourceARNList: []string{otherARN}, Tags: map[string]string{"Team": "x"},
	})
	require.NoError(t, err)
	assert.Empty(t, res.FailedResourcesMap)

	keys, err := tc.GetTagKeys(ctx, &tagsdk.GetTagKeysInput{})
	require.NoError(t, err)
	assert.Contains(t, keys.TagKeys, "Team")

	homeKeys, err := tagClient(crossAcct, crossHome).GetTagKeys(ctx, &tagsdk.GetTagKeysInput{})
	require.NoError(t, err)
	assert.NotContains(t, homeKeys.TagKeys, "Team")

	vals, err := tc.GetTagValues(ctx, &tagsdk.GetTagValuesInput{Key: aws.String("Team")})
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, vals.TagValues)

	_, err = tc.UntagResources(ctx, &tagsdk.UntagResourcesInput{
		ResourceARNList: []string{otherARN}, TagKeys: []string{"Team"},
	})
	require.NoError(t, err)

	keys, err = tc.GetTagKeys(ctx, &tagsdk.GetTagKeysInput{})
	require.NoError(t, err)
	assert.NotContains(t, keys.TagKeys, "Team")
}
