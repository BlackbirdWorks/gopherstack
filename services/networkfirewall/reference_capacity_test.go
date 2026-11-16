package networkfirewall_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testResourceGroup = "arn:aws:resource-groups:us-east-1:123456789012:group/web"
	testCluster       = "arn:aws:ecs:us-east-1:123456789012:cluster/c1"
)

type fakeRefs struct {
	fakeNet
}

func (fakeRefs) ResourceGroupCIDRCount(arn string) (int, bool) { return 4, arn == testResourceGroup }

func (fakeRefs) ECSClusterCIDRCount(arn string, filters map[string]string) (int, bool) {
	if arn != testCluster {
		return 0, false
	}

	if filters["ecs.instance-type"] == "c5.xlarge" {
		return 2, true
	}

	return 3, true
}

func TestFirewallCapacityResourceGroupAndContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filters []types.ContainerAttribute
		useCA   bool
		want    int32
	}{
		{name: "resource group", want: 4},
		{name: "container no filter", useCA: true, want: 3},
		{name: "container filtered", useCA: true, want: 2, filters: []types.ContainerAttribute{
			{Key: aws.String("ecs.instance-type"), Value: aws.String("c5.xlarge")},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.handler.SetNetworkResolver(fakeRefs{})

			ref := testResourceGroup

			if tc.useCA {
				ca, err := f.client.CreateContainerAssociation(t.Context(), &sdk.CreateContainerAssociationInput{
					ContainerAssociationName: aws.String("ca"),
					Type:                     types.ContainerMonitoringTypeEcs,
					ContainerMonitoringConfigurations: []types.ContainerMonitoringConfiguration{
						{ClusterArn: aws.String(testCluster), AttributeFilters: tc.filters},
					},
				})
				require.NoError(t, err)

				ref = aws.ToString(ca.ContainerAssociationArn)

				desc, err := f.client.DescribeContainerAssociation(t.Context(), &sdk.DescribeContainerAssociationInput{
					ContainerAssociationArn: ca.ContainerAssociationArn,
				})
				require.NoError(t, err)
				assert.Equal(t, tc.want, aws.ToInt32(desc.ResolvedCidrCount))
			}

			group, err := f.client.CreateRuleGroup(t.Context(), &sdk.CreateRuleGroupInput{
				RuleGroupName: aws.String("refs"), Type: types.RuleGroupTypeStateful, Capacity: aws.Int32(10),
				RuleGroup: &types.RuleGroup{
					ReferenceSets: &types.ReferenceSets{IPSetReferences: map[string]types.IPSetReference{
						"REF": {ReferenceArn: aws.String(ref)},
					}},
					RulesSource: &types.RulesSource{RulesString: aws.String("pass tcp $REF any -> any 80 (sid:1;)\n")},
				},
			})
			require.NoError(t, err)

			pol, err := f.client.CreateFirewallPolicy(t.Context(), &sdk.CreateFirewallPolicyInput{
				FirewallPolicyName: aws.String("p"),
				FirewallPolicy: &types.FirewallPolicy{
					StatelessDefaultActions:         []string{"aws:forward_to_sfe"},
					StatelessFragmentDefaultActions: []string{"aws:forward_to_sfe"},
					StatefulRuleGroupReferences: []types.StatefulRuleGroupReference{
						{ResourceArn: group.RuleGroupResponse.RuleGroupArn},
					},
				},
			})
			require.NoError(t, err)

			fw := f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))
			cidrs := fw.FirewallStatus.CapacityUsageSummary.CIDRs
			assert.Equal(t, tc.want, aws.ToInt32(cidrs.UtilizedCIDRCount))
		})
	}
}
