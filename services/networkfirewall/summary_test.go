package networkfirewall_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	nf "github.com/blackbirdworks/gopherstack/services/networkfirewall"
)

func TestDescribeRuleGroupSummary(t *testing.T) {
	t.Parallel()

	rules := "# header\n" +
		`alert tcp any any -> any 80 (msg:"web; traffic"; sid:101; metadata:a b, c d; rev:1;)` + "\n" +
		`drop tcp any any -> any 22 (msg:"ssh"; sid:102;)` + "\n"

	tests := []struct {
		name    string
		summary *types.SummaryConfiguration
		want    []types.RuleSummary
	}{
		{
			name: "sid and msg",
			summary: &types.SummaryConfiguration{
				RuleOptions: []types.SummaryRuleOption{types.SummaryRuleOptionSid, types.SummaryRuleOptionMsg},
			},
			want: []types.RuleSummary{
				{SID: aws.String("101"), Msg: aws.String("web; traffic")},
				{SID: aws.String("102"), Msg: aws.String("ssh")},
			},
		},
		{
			name: "metadata only",
			summary: &types.SummaryConfiguration{
				RuleOptions: []types.SummaryRuleOption{types.SummaryRuleOptionMetadata},
			},
			want: []types.RuleSummary{{Metadata: aws.String("a b, c d")}, {}},
		},
		{name: "no options selected", want: []types.RuleSummary{{}, {}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			_, err := f.client.CreateRuleGroup(t.Context(), &sdk.CreateRuleGroupInput{
				RuleGroupName: aws.String("sf"), Type: types.RuleGroupTypeStateful, Capacity: aws.Int32(10),
				Rules: aws.String(rules), SummaryConfiguration: tt.summary, Description: aws.String("d"),
			})
			require.NoError(t, err)

			out, err := f.client.DescribeRuleGroupSummary(t.Context(), &sdk.DescribeRuleGroupSummaryInput{
				RuleGroupName: aws.String("sf"), Type: types.RuleGroupTypeStateful,
			})
			require.NoError(t, err)
			assert.Equal(t, "sf", aws.ToString(out.RuleGroupName))
			assert.Equal(t, "d", aws.ToString(out.Description))
			require.Len(t, out.Summary.RuleSummaries, len(tt.want))

			for i, w := range tt.want {
				assert.Equal(t, aws.ToString(w.SID), aws.ToString(out.Summary.RuleSummaries[i].SID))
				assert.Equal(t, aws.ToString(w.Msg), aws.ToString(out.Summary.RuleSummaries[i].Msg))
				assert.Equal(t, aws.ToString(w.Metadata), aws.ToString(out.Summary.RuleSummaries[i].Metadata))
			}
		})
	}
}

func TestDescribeRuleGroupSummaryErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	_, err := f.client.DescribeRuleGroupSummary(t.Context(), &sdk.DescribeRuleGroupSummaryInput{
		RuleGroupName: aws.String("ghost"), Type: types.RuleGroupTypeStateful,
	})
	assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

	_, err = f.client.CreateRuleGroup(t.Context(), &sdk.CreateRuleGroupInput{
		RuleGroupName: aws.String("sl"), Type: types.RuleGroupTypeStateless, Capacity: aws.Int32(5),
		RuleGroup: &types.RuleGroup{RulesSource: &types.RulesSource{
			StatelessRulesAndCustomActions: &types.StatelessRulesAndCustomActions{
				StatelessRules: []types.StatelessRule{},
			},
		}},
	})
	require.NoError(t, err)

	_, err = f.client.DescribeRuleGroupSummary(t.Context(), &sdk.DescribeRuleGroupSummaryInput{
		RuleGroupName: aws.String("sl"), Type: types.RuleGroupTypeStateless,
	})
	assert.Equal(t, "InvalidRequestException", errCode(t, err))
}

func TestSetLifecycleDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		delay    time.Duration
		settles  bool
		stillRun bool
	}{
		{name: "unset keeps built-in window", delay: 0, stillRun: true},
		{name: "negative keeps built-in window", delay: -time.Second, stillRun: true},
		{name: "short window settles", delay: 20 * time.Millisecond, settles: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := nf.NewInMemoryBackend(testAccount, testRegion)
				b.SetLifecycleDelay(tt.delay)

				h := nf.NewHandler(b)
				h.AccountID, h.DefaultRegion = testAccount, testRegion
				f := &fixture{handler: h, client: newClient(t, h, testRegion)}

				pol := f.policy(t, "pol")
				created := f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))
				assert.Equal(t, types.FirewallStatusValueProvisioning, created.FirewallStatus.Status)

				status := func() types.FirewallStatusValue {
					d, err := f.client.DescribeFirewall(
						t.Context(),
						&sdk.DescribeFirewallInput{FirewallName: aws.String("fw")},
					)
					require.NoError(t, err)

					return d.FirewallStatus.Status
				}

				if tt.settles {
					require.Eventually(
						t,
						func() bool { return status() == types.FirewallStatusValueReady },
						5*time.Second,
						5*time.Millisecond,
					)
				}

				if tt.stillRun {
					assert.Equal(t, types.FirewallStatusValueProvisioning, status())
				}
			})
		})
	}
}
