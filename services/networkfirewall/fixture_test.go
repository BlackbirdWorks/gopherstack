package networkfirewall_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	nf "github.com/blackbirdworks/gopherstack/services/networkfirewall"
)

const (
	testAccount = "123456789012"
	testRegion  = "us-east-1"
	testVpc     = "vpc-0123456789abcdef0"
	transition  = 40 * time.Millisecond
)

type fixture struct {
	handler *nf.Handler
	client  *sdk.Client
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	backend := nf.NewInMemoryBackend(testAccount, testRegion)
	backend.SetTransitionDelay(transition)

	handler := nf.NewHandler(backend)
	handler.AccountID = testAccount
	handler.DefaultRegion = testRegion
	handler.EnableRegions()

	return &fixture{handler: handler, client: newClient(t, handler, testRegion)}
}

func newClient(t *testing.T, h *nf.Handler, region string) *sdk.Client {
	t.Helper()

	e := echo.New()
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			r := c.Request()
			c.SetRequest(r.WithContext(awsmeta.Set(r.Context(), awsmeta.FromRequest(r, testRegion))))

			return next(c)
		}
	})

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	cfg := aws.Config{
		Region:       region,
		BaseEndpoint: aws.String(sdktest.InMemoryEndpoint),
		HTTPClient:   sdktest.HandlerClient(e),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	}

	return sdk.NewFromConfig(cfg)
}

func errCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr, "want smithy.APIError, got %v", err)

	return apiErr.ErrorCode()
}

func (f *fixture) policy(t *testing.T, name string) *sdk.CreateFirewallPolicyOutput {
	t.Helper()

	out, err := f.client.CreateFirewallPolicy(t.Context(), &sdk.CreateFirewallPolicyInput{
		FirewallPolicyName: aws.String(name),
		FirewallPolicy: &types.FirewallPolicy{
			StatelessDefaultActions:         []string{"aws:forward_to_sfe"},
			StatelessFragmentDefaultActions: []string{"aws:forward_to_sfe"},
		},
	})
	require.NoError(t, err)

	return out
}

func (f *fixture) firewall(t *testing.T, name, policyARN string) *sdk.CreateFirewallOutput {
	t.Helper()

	out, err := f.client.CreateFirewall(t.Context(), &sdk.CreateFirewallInput{
		FirewallName:      aws.String(name),
		FirewallPolicyArn: aws.String(policyARN),
		VpcId:             aws.String(testVpc),
		SubnetMappings:    []types.SubnetMapping{{SubnetId: aws.String("subnet-aaaa")}},
	})
	require.NoError(t, err)

	return out
}

func (f *fixture) statefulGroup(t *testing.T, name string) *sdk.CreateRuleGroupOutput {
	t.Helper()

	out, err := f.client.CreateRuleGroup(t.Context(), &sdk.CreateRuleGroupInput{
		RuleGroupName: aws.String(name),
		Type:          types.RuleGroupTypeStateful,
		Capacity:      aws.Int32(10),
		Rules: aws.String(
			"pass tcp any any -> any 80 (sid:1;)\n# comment\ndrop tcp any any -> any 22 (sid:2;)\n",
		),
	})
	require.NoError(t, err)

	return out
}
