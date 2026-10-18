package networkfirewall_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func s3Dest(logType types.LogType, bucket string) types.LogDestinationConfig {
	return types.LogDestinationConfig{
		LogType:            logType,
		LogDestinationType: types.LogDestinationTypeS3,
		LogDestination:     map[string]string{"bucketName": bucket},
	}
}

func TestLoggingTagsTLSAndResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "logging one destination at a time", run: func(t *testing.T, f *fixture) {
			t.Helper()

			pol := f.policy(t, "pol")
			fw := f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))

			empty, err := f.client.DescribeLoggingConfiguration(t.Context(),
				&sdk.DescribeLoggingConfigurationInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)
			assert.Empty(t, empty.LoggingConfiguration.LogDestinationConfigs)

			one, err := f.client.UpdateLoggingConfiguration(t.Context(), &sdk.UpdateLoggingConfigurationInput{
				FirewallArn:               fw.Firewall.FirewallArn,
				EnableMonitoringDashboard: aws.Bool(true),
				LoggingConfiguration: &types.LoggingConfiguration{
					LogDestinationConfigs: []types.LogDestinationConfig{s3Dest(types.LogTypeAlert, "b1")},
				},
			})
			require.NoError(t, err)
			assert.Len(t, one.LoggingConfiguration.LogDestinationConfigs, 1)
			assert.True(t, aws.ToBool(one.EnableMonitoringDashboard))

			_, err = f.client.UpdateLoggingConfiguration(t.Context(), &sdk.UpdateLoggingConfigurationInput{
				FirewallName: aws.String("fw"),
				LoggingConfiguration: &types.LoggingConfiguration{
					LogDestinationConfigs: []types.LogDestinationConfig{
						s3Dest(
							types.LogTypeAlert,
							"b1",
						),
						s3Dest(types.LogTypeFlow, "b2"),
						s3Dest(types.LogTypeTls, "b3"),
					},
				},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.UpdateLoggingConfiguration(t.Context(), &sdk.UpdateLoggingConfigurationInput{
				FirewallName: aws.String("fw"),
				LoggingConfiguration: &types.LoggingConfiguration{
					LogDestinationConfigs: []types.LogDestinationConfig{{
						LogType: types.LogTypeAlert, LogDestinationType: types.LogDestinationTypeS3,
						LogDestination: map[string]string{"wrong": "x"},
					}},
				},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			two, err := f.client.UpdateLoggingConfiguration(t.Context(), &sdk.UpdateLoggingConfigurationInput{
				FirewallName: aws.String("fw"),
				LoggingConfiguration: &types.LoggingConfiguration{
					LogDestinationConfigs: []types.LogDestinationConfig{
						s3Dest(types.LogTypeAlert, "b1"),
						s3Dest(types.LogTypeFlow, "b2"),
					},
				},
			})
			require.NoError(t, err)
			assert.Len(t, two.LoggingConfiguration.LogDestinationConfigs, 2)

			got, err := f.client.DescribeLoggingConfiguration(t.Context(),
				&sdk.DescribeLoggingConfigurationInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)
			assert.Equal(t, "b2", got.LoggingConfiguration.LogDestinationConfigs[1].LogDestination["bucketName"])
			assert.True(t, aws.ToBool(got.EnableMonitoringDashboard))
		}},
		{name: "tags across resource kinds", run: func(t *testing.T, f *fixture) {
			t.Helper()

			pol := f.policy(t, "pol")
			fw := f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))
			rg := f.statefulGroup(t, "sf")

			for _, arn := range []*string{
				fw.Firewall.FirewallArn, pol.FirewallPolicyResponse.FirewallPolicyArn, rg.RuleGroupResponse.RuleGroupArn,
			} {
				_, err := f.client.TagResource(t.Context(), &sdk.TagResourceInput{
					ResourceArn: arn,
					Tags: []types.Tag{
						{Key: aws.String("env"), Value: aws.String("dev")},
						{Key: aws.String("a"), Value: aws.String("1")},
					},
				})
				require.NoError(t, err)

				_, err = f.client.UntagResource(
					t.Context(),
					&sdk.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"a"}},
				)
				require.NoError(t, err)

				got, err := f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: arn})
				require.NoError(t, err)
				require.Len(t, got.Tags, 1)
				assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			}

			d, err := f.client.DescribeFirewall(t.Context(), &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)
			require.Len(t, d.Firewall.Tags, 1)

			_, err = f.client.TagResource(t.Context(), &sdk.TagResourceInput{
				ResourceArn: aws.String("arn:aws:network-firewall:us-east-1:123456789012:firewall/ghost"),
				Tags:        []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			many := make([]types.Tag, 51)
			for i := range many {
				many[i] = types.Tag{Key: aws.String("k" + string(rune('A'+i))), Value: aws.String("v")}
			}

			_, err = f.client.TagResource(
				t.Context(),
				&sdk.TagResourceInput{ResourceArn: fw.Firewall.FirewallArn, Tags: many},
			)
			assert.Equal(t, "InvalidRequestException", errCode(t, err))
		}},
		{name: "tls inspection configuration", run: func(t *testing.T, f *fixture) {
			t.Helper()

			cfg := &types.TLSInspectionConfiguration{
				ServerCertificateConfigurations: []types.ServerCertificateConfiguration{
					{
						CertificateAuthorityArn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/ca"),
						ServerCertificates: []types.ServerCertificate{
							{ResourceArn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/c1")},
						},
					},
				},
			}

			created, err := f.client.CreateTLSInspectionConfiguration(
				t.Context(),
				&sdk.CreateTLSInspectionConfigurationInput{
					TLSInspectionConfigurationName: aws.String("tls1"), TLSInspectionConfiguration: cfg,
				},
			)
			require.NoError(t, err)

			resp := created.TLSInspectionConfigurationResponse
			assert.Contains(t, aws.ToString(resp.TLSInspectionConfigurationArn), ":tls-configuration/tls1")
			require.Len(t, resp.Certificates, 1)
			assert.NotNil(t, resp.CertificateAuthority)

			_, err = f.client.CreateTLSInspectionConfiguration(t.Context(), &sdk.CreateTLSInspectionConfigurationInput{
				TLSInspectionConfigurationName: aws.String("tls1"), TLSInspectionConfiguration: cfg,
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			pol, err := f.client.CreateFirewallPolicy(t.Context(), &sdk.CreateFirewallPolicyInput{
				FirewallPolicyName: aws.String("pol"),
				FirewallPolicy: &types.FirewallPolicy{
					StatelessDefaultActions:         []string{"aws:pass"},
					StatelessFragmentDefaultActions: []string{"aws:pass"},
					TLSInspectionConfigurationArn:   resp.TLSInspectionConfigurationArn,
				},
			})
			require.NoError(t, err)

			_, err = f.client.DeleteTLSInspectionConfiguration(t.Context(), &sdk.DeleteTLSInspectionConfigurationInput{
				TLSInspectionConfigurationArn: resp.TLSInspectionConfigurationArn,
			})
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			d, err := f.client.DescribeTLSInspectionConfiguration(
				t.Context(),
				&sdk.DescribeTLSInspectionConfigurationInput{
					TLSInspectionConfigurationName: aws.String("tls1"),
				},
			)
			require.NoError(t, err)
			assert.EqualValues(t, 1, aws.ToInt32(d.TLSInspectionConfigurationResponse.NumberOfAssociations))

			_, err = f.client.UpdateTLSInspectionConfiguration(t.Context(), &sdk.UpdateTLSInspectionConfigurationInput{
				TLSInspectionConfigurationArn: resp.TLSInspectionConfigurationArn, TLSInspectionConfiguration: cfg,
				UpdateToken: aws.String("stale"),
			})
			assert.Equal(t, "InvalidTokenException", errCode(t, err))

			up, err := f.client.UpdateTLSInspectionConfiguration(
				t.Context(),
				&sdk.UpdateTLSInspectionConfigurationInput{
					TLSInspectionConfigurationArn: resp.TLSInspectionConfigurationArn, TLSInspectionConfiguration: cfg,
					UpdateToken: d.UpdateToken, Description: aws.String("d"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "d", aws.ToString(up.TLSInspectionConfigurationResponse.Description))

			_, err = f.client.DeleteFirewallPolicy(t.Context(), &sdk.DeleteFirewallPolicyInput{
				FirewallPolicyArn: pol.FirewallPolicyResponse.FirewallPolicyArn,
			})
			require.NoError(t, err)

			_, err = f.client.DeleteTLSInspectionConfiguration(t.Context(), &sdk.DeleteTLSInspectionConfigurationInput{
				TLSInspectionConfigurationArn: resp.TLSInspectionConfigurationArn,
			})
			require.NoError(t, err)

			list, err := f.client.ListTLSInspectionConfigurations(
				t.Context(),
				&sdk.ListTLSInspectionConfigurationsInput{},
			)
			require.NoError(t, err)
			assert.Empty(t, list.TLSInspectionConfigurations)
		}},
		{name: "resource policy", run: func(t *testing.T, f *fixture) {
			t.Helper()

			rg := f.statefulGroup(t, "sf")
			arn := rg.RuleGroupResponse.RuleGroupArn
			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":"arn:aws:iam::111122223333:root"},` +
				`"Action":"network-firewall:ListRuleGroups","Resource":"*"}]}`

			_, err := f.client.DescribeResourcePolicy(t.Context(), &sdk.DescribeResourcePolicyInput{ResourceArn: arn})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			_, err = f.client.PutResourcePolicy(
				t.Context(),
				&sdk.PutResourcePolicyInput{ResourceArn: arn, Policy: aws.String("not json")},
			)
			assert.Equal(t, "InvalidResourcePolicyException", errCode(t, err))

			_, err = f.client.PutResourcePolicy(t.Context(), &sdk.PutResourcePolicyInput{
				ResourceArn: aws.String(
					"arn:aws:network-firewall:us-east-1:123456789012:stateful-rulegroup/none",
				),
				Policy: aws.String(doc),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			_, err = f.client.PutResourcePolicy(
				t.Context(),
				&sdk.PutResourcePolicyInput{ResourceArn: arn, Policy: aws.String(doc)},
			)
			require.NoError(t, err)

			got, err := f.client.DescribeResourcePolicy(t.Context(), &sdk.DescribeResourcePolicyInput{ResourceArn: arn})
			require.NoError(t, err)
			assert.JSONEq(t, doc, aws.ToString(got.Policy))

			_, err = f.client.DeleteResourcePolicy(t.Context(), &sdk.DeleteResourcePolicyInput{ResourceArn: arn})
			require.NoError(t, err)

			_, err = f.client.DeleteResourcePolicy(t.Context(), &sdk.DeleteResourcePolicyInput{ResourceArn: arn})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}
