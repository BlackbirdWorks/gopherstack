package networkfirewall_test

import (
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFirewall_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "provisioning to ready", run: func(t *testing.T, f *fixture) {
			t.Helper()

			pol := f.policy(t, "pol")
			created := f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))

			assert.Equal(t, types.FirewallStatusValueProvisioning, created.FirewallStatus.Status)
			assert.Equal(t, types.ConfigurationSyncStatePending, created.FirewallStatus.ConfigurationSyncStateSummary)
			assert.Contains(t, aws.ToString(created.Firewall.FirewallArn), ":firewall/fw")
			assert.Equal(t, types.EncryptionTypeAwsOwnedKmsKey, created.Firewall.EncryptionConfiguration.Type)

			require.Eventually(t, func() bool {
				d, err := f.client.DescribeFirewall(
					t.Context(),
					&sdk.DescribeFirewallInput{FirewallName: aws.String("fw")},
				)

				return err == nil && d.FirewallStatus.Status == types.FirewallStatusValueReady &&
					d.FirewallStatus.ConfigurationSyncStateSummary == types.ConfigurationSyncStateInSync
			}, 5e9, 10e6)

			d, err := f.client.DescribeFirewall(t.Context(), &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)
			require.Len(t, d.FirewallStatus.SyncStates, 1)

			for az, st := range d.FirewallStatus.SyncStates {
				assert.Equal(t, testRegion+"a", az)
				assert.Equal(t, types.AttachmentStatusReady, st.Attachment.Status)
				assert.Equal(t, "subnet-aaaa", aws.ToString(st.Attachment.SubnetId))
				assert.NotEmpty(t, aws.ToString(st.Attachment.EndpointId))
				assert.Contains(t, st.Config, aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))
			}

			meta, err := f.client.DescribeFirewallMetadata(t.Context(),
				&sdk.DescribeFirewallMetadataInput{FirewallArn: created.Firewall.FirewallArn})
			require.NoError(t, err)
			assert.Equal(t, types.FirewallStatusValueReady, meta.Status)
		}},
		{name: "delete protection and removal", run: func(t *testing.T, f *fixture) {
			t.Helper()

			pol := f.policy(t, "pol")
			fw := f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))

			prot, err := f.client.UpdateFirewallDeleteProtection(t.Context(), &sdk.UpdateFirewallDeleteProtectionInput{
				FirewallName: aws.String("fw"), DeleteProtection: true,
			})
			require.NoError(t, err)
			assert.True(t, prot.DeleteProtection)

			_, err = f.client.DeleteFirewall(
				t.Context(),
				&sdk.DeleteFirewallInput{FirewallArn: fw.Firewall.FirewallArn},
			)
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			_, err = f.client.DeleteFirewallPolicy(t.Context(), &sdk.DeleteFirewallPolicyInput{
				FirewallPolicyArn: pol.FirewallPolicyResponse.FirewallPolicyArn,
			})
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			_, err = f.client.UpdateFirewallDeleteProtection(t.Context(), &sdk.UpdateFirewallDeleteProtectionInput{
				FirewallName: aws.String("fw"), DeleteProtection: false,
			})
			require.NoError(t, err)

			del, err := f.client.DeleteFirewall(
				t.Context(),
				&sdk.DeleteFirewallInput{FirewallArn: fw.Firewall.FirewallArn},
			)
			require.NoError(t, err)
			assert.Equal(t, types.FirewallStatusValueDeleting, del.FirewallStatus.Status)

			require.Eventually(t, func() bool {
				_, derr := f.client.DescribeFirewall(
					t.Context(),
					&sdk.DescribeFirewallInput{FirewallName: aws.String("fw")},
				)

				return derr != nil && errCode(t, derr) == "ResourceNotFoundException"
			}, 5e9, 10e6)

			_, err = f.client.DeleteFirewallPolicy(t.Context(), &sdk.DeleteFirewallPolicyInput{
				FirewallPolicyArn: pol.FirewallPolicyResponse.FirewallPolicyArn,
			})
			require.NoError(t, err)
		}},
		{name: "update token semantics", run: func(t *testing.T, f *fixture) {
			t.Helper()

			pol := f.policy(t, "pol")
			f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))

			d, err := f.client.DescribeFirewall(t.Context(), &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)

			up, err := f.client.UpdateFirewallDescription(t.Context(), &sdk.UpdateFirewallDescriptionInput{
				FirewallName: aws.String("fw"), Description: aws.String("hello"), UpdateToken: d.UpdateToken,
			})
			require.NoError(t, err)
			assert.NotEqual(t, aws.ToString(d.UpdateToken), aws.ToString(up.UpdateToken))
			assert.Equal(t, "hello", aws.ToString(up.Description))

			_, err = f.client.UpdateFirewallDescription(t.Context(), &sdk.UpdateFirewallDescriptionInput{
				FirewallName: aws.String("fw"), Description: aws.String("stale"), UpdateToken: d.UpdateToken,
			})
			assert.Equal(t, "InvalidTokenException", errCode(t, err))

			d2, err := f.client.DescribeFirewall(
				t.Context(),
				&sdk.DescribeFirewallInput{FirewallName: aws.String("fw")},
			)
			require.NoError(t, err)
			assert.Equal(t, "hello", aws.ToString(d2.Firewall.Description))
			assert.Equal(t, aws.ToString(up.UpdateToken), aws.ToString(d2.UpdateToken))
		}},
		{name: "subnets and policy association", run: func(t *testing.T, f *fixture) {
			t.Helper()

			pol := f.policy(t, "pol")
			pol2 := f.policy(t, "pol2")
			f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))

			added, err := f.client.AssociateSubnets(t.Context(), &sdk.AssociateSubnetsInput{
				FirewallName: aws.String("fw"),
				SubnetMappings: []types.SubnetMapping{
					{SubnetId: aws.String("subnet-bbbb"), IPAddressType: types.IPAddressTypeDualstack},
				},
			})
			require.NoError(t, err)
			assert.Len(t, added.SubnetMappings, 2)

			_, err = f.client.AssociateSubnets(t.Context(), &sdk.AssociateSubnetsInput{
				FirewallName:   aws.String("fw"),
				SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-bbbb")}},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.UpdateSubnetChangeProtection(t.Context(), &sdk.UpdateSubnetChangeProtectionInput{
				FirewallName: aws.String("fw"), SubnetChangeProtection: true,
			})
			require.NoError(t, err)

			_, err = f.client.DisassociateSubnets(t.Context(), &sdk.DisassociateSubnetsInput{
				FirewallName: aws.String("fw"), SubnetIds: []string{"subnet-bbbb"},
			})
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			_, err = f.client.UpdateSubnetChangeProtection(t.Context(), &sdk.UpdateSubnetChangeProtectionInput{
				FirewallName: aws.String("fw"), SubnetChangeProtection: false,
			})
			require.NoError(t, err)

			removed, err := f.client.DisassociateSubnets(t.Context(), &sdk.DisassociateSubnetsInput{
				FirewallName: aws.String("fw"), SubnetIds: []string{"subnet-bbbb"},
			})
			require.NoError(t, err)
			assert.Len(t, removed.SubnetMappings, 1)

			_, err = f.client.DisassociateSubnets(t.Context(), &sdk.DisassociateSubnetsInput{
				FirewallName: aws.String("fw"), SubnetIds: []string{"subnet-aaaa"},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.UpdateFirewallPolicyChangeProtection(
				t.Context(),
				&sdk.UpdateFirewallPolicyChangeProtectionInput{
					FirewallName: aws.String("fw"), FirewallPolicyChangeProtection: true,
				},
			)
			require.NoError(t, err)

			_, err = f.client.AssociateFirewallPolicy(t.Context(), &sdk.AssociateFirewallPolicyInput{
				FirewallName: aws.String("fw"), FirewallPolicyArn: pol2.FirewallPolicyResponse.FirewallPolicyArn,
			})
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			_, err = f.client.UpdateFirewallPolicyChangeProtection(
				t.Context(),
				&sdk.UpdateFirewallPolicyChangeProtectionInput{
					FirewallName: aws.String("fw"), FirewallPolicyChangeProtection: false,
				},
			)
			require.NoError(t, err)

			assoc, err := f.client.AssociateFirewallPolicy(t.Context(), &sdk.AssociateFirewallPolicyInput{
				FirewallName: aws.String("fw"), FirewallPolicyArn: pol2.FirewallPolicyResponse.FirewallPolicyArn,
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				aws.ToString(pol2.FirewallPolicyResponse.FirewallPolicyArn),
				aws.ToString(assoc.FirewallPolicyArn),
			)

			dp, err := f.client.DescribeFirewallPolicy(
				t.Context(),
				&sdk.DescribeFirewallPolicyInput{FirewallPolicyName: aws.String("pol2")},
			)
			require.NoError(t, err)
			assert.EqualValues(t, 1, aws.ToInt32(dp.FirewallPolicyResponse.NumberOfAssociations))

			enc, err := f.client.UpdateFirewallEncryptionConfiguration(
				t.Context(),
				&sdk.UpdateFirewallEncryptionConfigurationInput{
					FirewallName: aws.String("fw"),
					EncryptionConfiguration: &types.EncryptionConfiguration{
						Type:  types.EncryptionTypeCustomerKms,
						KeyId: aws.String("key-1"),
					},
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "key-1", aws.ToString(enc.EncryptionConfiguration.KeyId))
		}},
		{name: "validation and not found", run: func(t *testing.T, f *fixture) {
			t.Helper()

			pol := f.policy(t, "pol")
			polARN := aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn)
			f.firewall(t, "fw", polARN)

			_, err := f.client.CreateFirewall(t.Context(), &sdk.CreateFirewallInput{
				FirewallName: aws.String("fw"), FirewallPolicyArn: aws.String(polARN), VpcId: aws.String(testVpc),
				SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-x")}},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.CreateFirewall(t.Context(), &sdk.CreateFirewallInput{
				FirewallName: aws.String("other"), FirewallPolicyArn: aws.String(polARN),
				NatGatewayMappings: []types.NatGatewayMapping{{NatGatewayId: aws.String("nat-1")}},
			})
			assert.Equal(t, "UnsupportedOperationException", errCode(t, err))

			_, err = f.client.CreateFirewall(t.Context(), &sdk.CreateFirewallInput{
				FirewallName: aws.String("other"), FirewallPolicyArn: aws.String(polARN + "-missing"),
				VpcId: aws.String(testVpc), SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-x")}},
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			_, err = f.client.DescribeFirewall(
				t.Context(),
				&sdk.DescribeFirewallInput{FirewallName: aws.String("nope")},
			)
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			_, err = f.client.DescribeFirewall(t.Context(), &sdk.DescribeFirewallInput{})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.UpdateFirewallDescription(t.Context(), &sdk.UpdateFirewallDescriptionInput{
				FirewallName: aws.String("nope"), Description: aws.String("x"),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		}},
		{name: "list with vpc filter and paging", run: func(t *testing.T, f *fixture) {
			t.Helper()

			pol := f.policy(t, "pol")
			polARN := aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn)

			for _, n := range []string{"fw-a", "fw-b", "fw-c"} {
				f.firewall(t, n, polARN)
			}

			var names []string

			p := sdk.NewListFirewallsPaginator(f.client, &sdk.ListFirewallsInput{MaxResults: aws.Int32(2)})
			for p.HasMorePages() {
				page, err := p.NextPage(t.Context())
				require.NoError(t, err)

				for _, m := range page.Firewalls {
					names = append(names, aws.ToString(m.FirewallName))
				}
			}

			assert.Equal(t, []string{"fw-a", "fw-b", "fw-c"}, names)

			other, err := f.client.ListFirewalls(t.Context(), &sdk.ListFirewallsInput{VpcIds: []string{"vpc-other"}})
			require.NoError(t, err)
			assert.Empty(t, other.Firewalls)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				tt.run(t, newFixture(t))
			})
		})
	}
}
