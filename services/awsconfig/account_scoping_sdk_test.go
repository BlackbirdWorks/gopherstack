package awsconfig_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

const (
	acctHome  = "000000000000"
	acctOther = "111111111111"
)

type scopedServer struct {
	url string
}

func newScopedServer(t *testing.T, h *awsconfig.Handler) scopedServer {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			r := c.Request()
			c.SetRequest(r.WithContext(awsmeta.Set(r.Context(), awsmeta.FromRequest(r, "us-east-1"))))

			return next(c)
		}
	})
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return scopedServer{url: srv.URL}
}

func (s scopedServer) client(t *testing.T, account, region string) *configservicesdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(account, "secret", "")),
	)
	require.NoError(t, err)

	return configservicesdk.NewFromConfig(cfg, func(o *configservicesdk.Options) {
		o.BaseEndpoint = aws.String(s.url)
	})
}

func newScopedHandler() *awsconfig.Handler {
	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackendWithMeta(acctHome, "us-east-1"))
	h.EnableRegions()

	return h
}

func putRecorder(t *testing.T, c *configservicesdk.Client, name string) error {
	t.Helper()

	_, err := c.PutConfigurationRecorder(t.Context(), &configservicesdk.PutConfigurationRecorderInput{
		ConfigurationRecorder: &types.ConfigurationRecorder{
			Name:    aws.String(name),
			RoleARN: aws.String("arn:aws:iam::000000000000:role/config"),
		},
	})

	return err
}

func recorderNames(t *testing.T, c *configservicesdk.Client) []string {
	t.Helper()

	out, err := c.DescribeConfigurationRecorders(t.Context(), &configservicesdk.DescribeConfigurationRecordersInput{})
	require.NoError(t, err)

	names := make([]string, 0, len(out.ConfigurationRecorders))
	for _, r := range out.ConfigurationRecorders {
		names = append(names, aws.ToString(r.Name))
	}

	return names
}

func TestAccountScoping_Recorders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		acctA      string
		regionA    string
		acctB      string
		regionB    string
		nameA      string
		nameB      string
		wantLimitB bool
	}{
		{name: "different_accounts", acctA: acctHome, regionA: "us-east-1", acctB: acctOther, regionB: "us-east-1",
			nameA: "rec-a", nameB: "rec-b"},
		{name: "different_regions", acctA: acctHome, regionA: "us-east-1", acctB: acctHome, regionB: "eu-west-1",
			nameA: "rec-a", nameB: "rec-b"},
		{name: "different_both", acctA: acctHome, regionA: "us-east-1", acctB: acctOther, regionB: "eu-west-1",
			nameA: "rec-a", nameB: "rec-b"},
		{name: "same_scope_hits_limit", acctA: acctOther, regionA: "eu-west-1", acctB: acctOther, regionB: "eu-west-1",
			nameA: "rec-a", nameB: "rec-b", wantLimitB: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newScopedServer(t, newScopedHandler())
			a := srv.client(t, tc.acctA, tc.regionA)
			b := srv.client(t, tc.acctB, tc.regionB)

			require.NoError(t, putRecorder(t, a, tc.nameA))

			errB := putRecorder(t, b, tc.nameB)
			if tc.wantLimitB {
				var limit *types.MaxNumberOfConfigurationRecordersExceededException
				require.ErrorAs(t, errB, &limit)
				assert.Equal(t, []string{tc.nameA}, recorderNames(t, b))

				return
			}

			require.NoError(t, errB)
			assert.Equal(t, []string{tc.nameA}, recorderNames(t, a))
			assert.Equal(t, []string{tc.nameB}, recorderNames(t, b))

			_, err := b.StartConfigurationRecorder(t.Context(), &configservicesdk.StartConfigurationRecorderInput{
				ConfigurationRecorderName: aws.String(tc.nameA),
			})
			var nosuch *types.NoSuchConfigurationRecorderException
			require.ErrorAs(t, err, &nosuch)

			_, err = b.DeleteConfigurationRecorder(t.Context(), &configservicesdk.DeleteConfigurationRecorderInput{
				ConfigurationRecorderName: aws.String(tc.nameA),
			})
			require.ErrorAs(t, err, &nosuch)
			assert.Equal(t, []string{tc.nameA}, recorderNames(t, a))
		})
	}
}

func TestAccountScoping_OtherResources(t *testing.T) {
	t.Parallel()

	srv := newScopedServer(t, newScopedHandler())
	a := srv.client(t, acctHome, "us-east-1")
	b := srv.client(t, acctOther, "us-east-1")
	c := srv.client(t, acctHome, "eu-west-1")
	ctx := t.Context()

	_, err := a.PutDeliveryChannel(ctx, &configservicesdk.PutDeliveryChannelInput{
		DeliveryChannel: &types.DeliveryChannel{Name: aws.String("chan"), S3BucketName: aws.String("bkt")},
	})
	require.NoError(t, err)

	_, err = a.PutConfigRule(ctx, &configservicesdk.PutConfigRuleInput{ConfigRule: &types.ConfigRule{
		ConfigRuleName: aws.String("rule"),
		Source: &types.Source{
			Owner: types.OwnerAws, SourceIdentifier: aws.String("S3_BUCKET_VERSIONING_ENABLED"),
		},
	}})
	require.NoError(t, err)

	_, err = a.PutEvaluations(ctx, &configservicesdk.PutEvaluationsInput{
		ResultToken: aws.String("rule"),
		Evaluations: []types.Evaluation{{
			ComplianceResourceType: aws.String("AWS::S3::Bucket"),
			ComplianceResourceId:   aws.String("b1"),
			ComplianceType:         types.ComplianceTypeCompliant,
			OrderingTimestamp:      aws.Time(time.Now()),
		}},
	})
	require.NoError(t, err)

	_, err = a.PutConfigurationAggregator(ctx, &configservicesdk.PutConfigurationAggregatorInput{
		ConfigurationAggregatorName: aws.String("agg"),
		AccountAggregationSources: []types.AccountAggregationSource{
			{AccountIds: []string{acctHome}, AllAwsRegions: true},
		},
	})
	require.NoError(t, err)

	_, err = a.PutRetentionConfiguration(ctx, &configservicesdk.PutRetentionConfigurationInput{
		RetentionPeriodInDays: aws.Int32(30),
	})
	require.NoError(t, err)

	for _, other := range []*configservicesdk.Client{b, c} {
		ch, chErr := other.DescribeDeliveryChannels(ctx, &configservicesdk.DescribeDeliveryChannelsInput{})
		require.NoError(t, chErr)
		assert.Empty(t, ch.DeliveryChannels)

		rules, rulesErr := other.DescribeConfigRules(ctx, &configservicesdk.DescribeConfigRulesInput{})
		require.NoError(t, rulesErr)
		assert.Empty(t, rules.ConfigRules)

		cmp, cmpErr := other.DescribeComplianceByConfigRule(
			ctx, &configservicesdk.DescribeComplianceByConfigRuleInput{},
		)
		require.NoError(t, cmpErr)
		assert.Empty(t, cmp.ComplianceByConfigRules)

		det, detErr := other.GetComplianceDetailsByConfigRule(
			ctx, &configservicesdk.GetComplianceDetailsByConfigRuleInput{ConfigRuleName: aws.String("rule")},
		)
		if detErr == nil {
			assert.Empty(t, det.EvaluationResults)
		}

		aggs, aggErr := other.DescribeConfigurationAggregators(
			ctx, &configservicesdk.DescribeConfigurationAggregatorsInput{},
		)
		require.NoError(t, aggErr)
		assert.Empty(t, aggs.ConfigurationAggregators)

		ret, retErr := other.DescribeRetentionConfigurations(
			ctx,
			&configservicesdk.DescribeRetentionConfigurationsInput{},
		)
		require.NoError(t, retErr)
		assert.Empty(t, ret.RetentionConfigurations)
	}

	comp, err := a.DescribeComplianceByConfigRule(ctx, &configservicesdk.DescribeComplianceByConfigRuleInput{
		ConfigRuleNames: []string{"rule"},
	})
	require.NoError(t, err)
	assert.Len(t, comp.ComplianceByConfigRules, 1)
}

func TestAccountScoping_Persistence(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h := newScopedHandler()
		srv := newScopedServer(t, h)
		require.NoError(t, putRecorder(t, srv.client(t, acctHome, "us-east-1"), "home-rec"))
		require.NoError(t, putRecorder(t, srv.client(t, acctOther, "us-east-1"), "other-rec"))
		require.NoError(t, putRecorder(t, srv.client(t, acctOther, "eu-west-1"), "other-eu-rec"))

		snap := h.Snapshot(t.Context())
		require.NotNil(t, snap)

		h2 := newScopedHandler()
		require.NoError(t, h2.Restore(t.Context(), snap))
		srv2 := newScopedServer(t, h2)

		assert.Equal(t, []string{"home-rec"}, recorderNames(t, srv2.client(t, acctHome, "us-east-1")))
		assert.Equal(t, []string{"other-rec"}, recorderNames(t, srv2.client(t, acctOther, "us-east-1")))
		assert.Equal(t, []string{"other-eu-rec"}, recorderNames(t, srv2.client(t, acctOther, "eu-west-1")))
	})

	t.Run("old_format_loads_into_default_account", func(t *testing.T) {
		t.Parallel()

		old := awsconfig.NewInMemoryBackendWithMeta(acctHome, "us-east-1")
		require.NoError(t, old.PutConfigurationRecorder("legacy", "arn:aws:iam::000000000000:role/r", nil))
		snap := old.Snapshot(t.Context())
		require.NotNil(t, snap)

		h := newScopedHandler()
		srv := newScopedServer(t, h)
		require.NoError(t, putRecorder(t, srv.client(t, acctOther, "us-east-1"), "stale"))
		require.NoError(t, h.Restore(t.Context(), snap))

		assert.Equal(t, []string{"legacy"}, recorderNames(t, srv.client(t, acctHome, "us-east-1")))
		assert.Empty(t, recorderNames(t, srv.client(t, acctOther, "us-east-1")))
	})
}
