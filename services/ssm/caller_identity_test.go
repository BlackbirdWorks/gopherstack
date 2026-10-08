package ssm_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

const callerIdentityARN = "arn:aws:iam::123456789012:user/alice"

func newCallerIdentityClient(t *testing.T, callerARN string) *ssmsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(ssm.NewHandler(ssm.NewInMemoryBackend())))

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if callerARN != "" {
				meta := awsmeta.Get(c.Request().Context())
				m := *meta
				m.Principal = &awsmeta.Principal{Arn: callerARN}
				c.SetRequest(c.Request().WithContext(awsmeta.Set(c.Request().Context(), &m)))
			}

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

	return ssmsdk.NewFromConfig(cfg, func(o *ssmsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

func TestCallerIdentity_Attribution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		caller string
	}{
		{name: "resolved", caller: callerIdentityARN},
		{name: "unresolved", caller: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			c := newCallerIdentityClient(t, tt.caller)

			_, err := c.UpdateServiceSetting(ctx, &ssmsdk.UpdateServiceSettingInput{
				SettingId: aws.String("/ssm/parameter-store/high-throughput-enabled"), SettingValue: aws.String("true"),
			})
			require.NoError(t, err)

			ss, err := c.GetServiceSetting(ctx, &ssmsdk.GetServiceSettingInput{
				SettingId: aws.String("/ssm/parameter-store/high-throughput-enabled"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.caller, aws.ToString(ss.ServiceSetting.LastModifiedUser))

			_, err = c.PutParameter(ctx, &ssmsdk.PutParameterInput{
				Name: aws.String("/p"), Value: aws.String("v"), Type: ssmtypes.ParameterTypeString,
			})
			require.NoError(t, err)

			dp, err := c.DescribeParameters(ctx, &ssmsdk.DescribeParametersInput{})
			require.NoError(t, err)
			require.Len(t, dp.Parameters, 1)
			assert.Equal(t, tt.caller, aws.ToString(dp.Parameters[0].LastModifiedUser))

			hist, err := c.GetParameterHistory(ctx, &ssmsdk.GetParameterHistoryInput{Name: aws.String("/p")})
			require.NoError(t, err)
			require.Len(t, hist.Parameters, 1)
			assert.Equal(t, tt.caller, aws.ToString(hist.Parameters[0].LastModifiedUser))

			ops, err := c.CreateOpsItem(ctx, &ssmsdk.CreateOpsItemInput{
				Title: aws.String("t"), Source: aws.String("s"), Description: aws.String("d"),
			})
			require.NoError(t, err)

			got, err := c.GetOpsItem(ctx, &ssmsdk.GetOpsItemInput{OpsItemId: ops.OpsItemId})
			require.NoError(t, err)
			assert.Equal(t, tt.caller, aws.ToString(got.OpsItem.CreatedBy))
			assert.Equal(t, tt.caller, aws.ToString(got.OpsItem.LastModifiedBy))

			sums, err := c.DescribeOpsItems(ctx, &ssmsdk.DescribeOpsItemsInput{})
			require.NoError(t, err)
			require.Len(t, sums.OpsItemSummaries, 1)
			assert.Equal(t, tt.caller, aws.ToString(sums.OpsItemSummaries[0].CreatedBy))

			_, err = c.AssociateOpsItemRelatedItem(ctx, &ssmsdk.AssociateOpsItemRelatedItemInput{
				OpsItemId: ops.OpsItemId, AssociationType: aws.String("IsParentOf"),
				ResourceType: aws.String("AWS::SSMIncidents::IncidentRecord"),
				ResourceUri:  aws.String("arn:aws:ssm-incidents::123456789012:incident-record/x/y"),
			})
			require.NoError(t, err)

			rel, err := c.ListOpsItemRelatedItems(ctx, &ssmsdk.ListOpsItemRelatedItemsInput{OpsItemId: ops.OpsItemId})
			require.NoError(t, err)
			require.Len(t, rel.Summaries, 1)
			assertIdentity(t, tt.caller, rel.Summaries[0].CreatedBy)

			ev, err := c.ListOpsItemEvents(ctx, &ssmsdk.ListOpsItemEventsInput{})
			require.NoError(t, err)
			require.NotEmpty(t, ev.Summaries)
			assertIdentity(t, tt.caller, ev.Summaries[0].CreatedBy)

			_, err = c.CreateOpsMetadata(ctx, &ssmsdk.CreateOpsMetadataInput{ResourceId: aws.String("r1")})
			require.NoError(t, err)

			lm, err := c.ListOpsMetadata(ctx, &ssmsdk.ListOpsMetadataInput{})
			require.NoError(t, err)
			require.Len(t, lm.OpsMetadataList, 1)
			assert.Equal(t, tt.caller, aws.ToString(lm.OpsMetadataList[0].LastModifiedUser))
		})
	}
}

func assertIdentity(t *testing.T, want string, got *ssmtypes.OpsItemIdentity) {
	t.Helper()

	if want == "" {
		assert.Nil(t, got)

		return
	}

	require.NotNil(t, got)
	assert.Equal(t, want, aws.ToString(got.Arn))
}
