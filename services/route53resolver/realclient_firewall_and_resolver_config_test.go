package route53resolver_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53resolversdk "github.com/aws/aws-sdk-go-v2/service/route53resolver"
	"github.com/aws/aws-sdk-go-v2/service/route53resolver/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// policyDocument builds a minimal, valid resource-sharing policy document for
// the Put*Policy ops' request bodies.
func policyDocument(action, resourceARN string) string {
	return fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow",`+
			`"Principal":{"AWS":"arn:aws:iam::111111111111:root"},`+
			`"Action":"%s","Resource":"%s"}]}`,
		action, resourceARN,
	)
}

// newRealClient stands up a fresh backend/handler/client
// triple for gopherstack-n3zi typed slice 16.
func newRealClient(t *testing.T) *route53resolversdk.Client {
	t.Helper()

	h := route53resolver.NewHandler(route53resolver.NewInMemoryBackend("000000000000", "us-east-1"))

	return newTestRoute53ResolverClient(t, h)
}

// TestRealClient_FirewallAndResolverConfig drives every op the census
// still listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage).
func TestRealClient_FirewallAndResolverConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "firewall_domain_list_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				created, err := client.CreateFirewallDomainList(
					t.Context(),
					&route53resolversdk.CreateFirewallDomainListInput{
						CreatorRequestId: aws.String("cr-fdl-1"),
						Name:             aws.String("fdl-1"),
					},
				)
				require.NoError(t, err)
				listID := aws.ToString(created.FirewallDomainList.Id)
				require.NotEmpty(t, listID)

				_, err = client.UpdateFirewallDomains(t.Context(), &route53resolversdk.UpdateFirewallDomainsInput{
					FirewallDomainListId: aws.String(listID),
					Operation:            types.FirewallDomainUpdateOperationAdd,
					Domains:              []string{"example.com"},
				})
				require.NoError(t, err)

				got, err := client.GetFirewallDomainList(t.Context(), &route53resolversdk.GetFirewallDomainListInput{
					FirewallDomainListId: aws.String(listID),
				})
				require.NoError(t, err)
				assert.Equal(t, "fdl-1", aws.ToString(got.FirewallDomainList.Name))

				listedDomains, err := client.ListFirewallDomains(
					t.Context(),
					&route53resolversdk.ListFirewallDomainsInput{
						FirewallDomainListId: aws.String(listID),
					},
				)
				require.NoError(t, err)
				assert.Contains(t, listedDomains.Domains, "example.com")

				listedLists, err := client.ListFirewallDomainLists(
					t.Context(),
					&route53resolversdk.ListFirewallDomainListsInput{},
				)
				require.NoError(t, err)
				require.NotEmpty(t, listedLists.FirewallDomainLists)

				_, err = client.ImportFirewallDomains(t.Context(), &route53resolversdk.ImportFirewallDomainsInput{
					FirewallDomainListId: aws.String(listID),
					Operation:            types.FirewallDomainImportOperationReplace,
					DomainFileUrl:        aws.String("s3://bucket/domains.txt"),
				})
				require.NoError(t, err)

				_, err = client.DeleteFirewallDomainList(t.Context(), &route53resolversdk.DeleteFirewallDomainListInput{
					FirewallDomainListId: aws.String(listID),
				})
				require.NoError(t, err)

				_, err = client.GetFirewallDomainList(t.Context(), &route53resolversdk.GetFirewallDomainListInput{
					FirewallDomainListId: aws.String(listID),
				})
				require.Error(t, err)
			},
		},
		{
			name: "firewall_rule_group_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				createdGroup, err := client.CreateFirewallRuleGroup(
					t.Context(),
					&route53resolversdk.CreateFirewallRuleGroupInput{
						CreatorRequestId: aws.String("cr-frg-1"),
						Name:             aws.String("frg-1"),
					},
				)
				require.NoError(t, err)
				groupID := aws.ToString(createdGroup.FirewallRuleGroup.Id)
				require.NotEmpty(t, groupID)

				listedGroups, err := client.ListFirewallRuleGroups(
					t.Context(),
					&route53resolversdk.ListFirewallRuleGroupsInput{},
				)
				require.NoError(t, err)
				require.NotEmpty(t, listedGroups.FirewallRuleGroups)

				groupARN := aws.ToString(createdGroup.FirewallRuleGroup.Arn)
				require.NotEmpty(t, groupARN)

				_, err = client.PutFirewallRuleGroupPolicy(
					t.Context(),
					&route53resolversdk.PutFirewallRuleGroupPolicyInput{
						Arn: aws.String(groupARN),
						FirewallRuleGroupPolicy: aws.String(
							policyDocument("route53resolver:GetFirewallRuleGroup", groupARN),
						),
					},
				)
				require.NoError(t, err)

				gotPolicy, err := client.GetFirewallRuleGroupPolicy(
					t.Context(),
					&route53resolversdk.GetFirewallRuleGroupPolicyInput{
						Arn: aws.String(groupARN),
					},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(gotPolicy.FirewallRuleGroupPolicy))

				associated, err := client.AssociateFirewallRuleGroup(
					t.Context(),
					&route53resolversdk.AssociateFirewallRuleGroupInput{
						CreatorRequestId:    aws.String("cr-frga-1"),
						FirewallRuleGroupId: aws.String(groupID),
						Name:                aws.String("frga-1"),
						Priority:            aws.Int32(101),
						VpcId:               aws.String("vpc-11111111"),
					},
				)
				require.NoError(t, err)
				assocID := aws.ToString(associated.FirewallRuleGroupAssociation.Id)
				require.NotEmpty(t, assocID)

				gotAssoc, err := client.GetFirewallRuleGroupAssociation(
					t.Context(),
					&route53resolversdk.GetFirewallRuleGroupAssociationInput{
						FirewallRuleGroupAssociationId: aws.String(assocID),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, groupID, aws.ToString(gotAssoc.FirewallRuleGroupAssociation.FirewallRuleGroupId))

				listedAssocs, err := client.ListFirewallRuleGroupAssociations(
					t.Context(),
					&route53resolversdk.ListFirewallRuleGroupAssociationsInput{},
				)
				require.NoError(t, err)
				require.NotEmpty(t, listedAssocs.FirewallRuleGroupAssociations)

				updatedAssoc, err := client.UpdateFirewallRuleGroupAssociation(
					t.Context(),
					&route53resolversdk.UpdateFirewallRuleGroupAssociationInput{
						FirewallRuleGroupAssociationId: aws.String(assocID),
						Priority:                       aws.Int32(202),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, updatedAssoc.FirewallRuleGroupAssociation)
				require.NotNil(t, updatedAssoc.FirewallRuleGroupAssociation.Priority)
				assert.Equal(t, int32(202), *updatedAssoc.FirewallRuleGroupAssociation.Priority)

				_, err = client.DisassociateFirewallRuleGroup(
					t.Context(),
					&route53resolversdk.DisassociateFirewallRuleGroupInput{
						FirewallRuleGroupAssociationId: aws.String(assocID),
					},
				)
				require.NoError(t, err)

				_, err = client.DeleteFirewallRuleGroup(t.Context(), &route53resolversdk.DeleteFirewallRuleGroupInput{
					FirewallRuleGroupId: aws.String(groupID),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "firewall_rule_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				createdGroup, err := client.CreateFirewallRuleGroup(
					t.Context(),
					&route53resolversdk.CreateFirewallRuleGroupInput{
						CreatorRequestId: aws.String("cr-frg-2"),
						Name:             aws.String("frg-2"),
					},
				)
				require.NoError(t, err)
				groupID := aws.ToString(createdGroup.FirewallRuleGroup.Id)

				createdList, err := client.CreateFirewallDomainList(
					t.Context(),
					&route53resolversdk.CreateFirewallDomainListInput{
						CreatorRequestId: aws.String("cr-fdl-2"),
						Name:             aws.String("fdl-2"),
					},
				)
				require.NoError(t, err)
				listID := aws.ToString(createdList.FirewallDomainList.Id)

				createdRule, err := client.CreateFirewallRule(t.Context(), &route53resolversdk.CreateFirewallRuleInput{
					Action:               types.ActionBlock,
					CreatorRequestId:     aws.String("cr-fr-1"),
					FirewallRuleGroupId:  aws.String(groupID),
					FirewallDomainListId: aws.String(listID),
					Name:                 aws.String("fr-1"),
					Priority:             aws.Int32(1),
					BlockResponse:        types.BlockResponseNxdomain,
				})
				require.NoError(t, err)
				require.NotNil(t, createdRule.FirewallRule)

				listed, err := client.ListFirewallRules(t.Context(), &route53resolversdk.ListFirewallRulesInput{
					FirewallRuleGroupId: aws.String(groupID),
				})
				require.NoError(t, err)
				require.NotEmpty(t, listed.FirewallRules)

				_, err = client.UpdateFirewallRule(t.Context(), &route53resolversdk.UpdateFirewallRuleInput{
					FirewallRuleGroupId:  aws.String(groupID),
					FirewallDomainListId: aws.String(listID),
					Name:                 aws.String("fr-1-renamed"),
					Priority:             aws.Int32(1),
				})
				require.NoError(t, err)

				listedTypes, err := client.ListFirewallRuleTypes(
					t.Context(),
					&route53resolversdk.ListFirewallRuleTypesInput{},
				)
				require.NoError(t, err)
				require.NotEmpty(t, listedTypes.FirewallRuleTypes)

				_, err = client.DeleteFirewallRule(t.Context(), &route53resolversdk.DeleteFirewallRuleInput{
					FirewallRuleGroupId:  aws.String(groupID),
					FirewallDomainListId: aws.String(listID),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "firewall_rule_batch_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				createdGroup, err := client.CreateFirewallRuleGroup(
					t.Context(),
					&route53resolversdk.CreateFirewallRuleGroupInput{
						CreatorRequestId: aws.String("cr-frg-3"),
						Name:             aws.String("frg-3"),
					},
				)
				require.NoError(t, err)
				groupID := aws.ToString(createdGroup.FirewallRuleGroup.Id)

				createdList, err := client.CreateFirewallDomainList(
					t.Context(),
					&route53resolversdk.CreateFirewallDomainListInput{
						CreatorRequestId: aws.String("cr-fdl-3"),
						Name:             aws.String("fdl-3"),
					},
				)
				require.NoError(t, err)
				listID := aws.ToString(createdList.FirewallDomainList.Id)

				batchCreated, err := client.BatchCreateFirewallRule(
					t.Context(),
					&route53resolversdk.BatchCreateFirewallRuleInput{
						CreateFirewallRuleEntries: []types.CreateFirewallRuleEntry{
							{
								Action:               types.ActionAllow,
								CreatorRequestId:     aws.String("cr-bfr-1"),
								FirewallRuleGroupId:  aws.String(groupID),
								FirewallDomainListId: aws.String(listID),
								Name:                 aws.String("bfr-1"),
								Priority:             aws.Int32(10),
							},
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, batchCreated.CreatedFirewallRules, 1)
				assert.Empty(t, batchCreated.CreateErrors)

				batchUpdated, err := client.BatchUpdateFirewallRule(
					t.Context(),
					&route53resolversdk.BatchUpdateFirewallRuleInput{
						UpdateFirewallRuleEntries: []types.UpdateFirewallRuleEntry{
							{
								FirewallRuleGroupId:  aws.String(groupID),
								FirewallDomainListId: aws.String(listID),
								Name:                 aws.String("bfr-1-renamed"),
							},
						},
					},
				)
				require.NoError(t, err)
				assert.Empty(t, batchUpdated.UpdateErrors)

				batchDeleted, err := client.BatchDeleteFirewallRule(
					t.Context(),
					&route53resolversdk.BatchDeleteFirewallRuleInput{
						DeleteFirewallRuleEntries: []types.DeleteFirewallRuleEntry{
							{
								FirewallRuleGroupId:  aws.String(groupID),
								FirewallDomainListId: aws.String(listID),
							},
						},
					},
				)
				require.NoError(t, err)
				assert.Empty(t, batchDeleted.DeleteErrors)
			},
		},
		{
			name: "resolver_config_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				const vpcID = "vpc-config-1"

				got, err := client.GetResolverConfig(t.Context(), &route53resolversdk.GetResolverConfigInput{
					ResourceId: aws.String(vpcID),
				})
				require.NoError(t, err)
				require.NotNil(t, got.ResolverConfig)

				updated, err := client.UpdateResolverConfig(t.Context(), &route53resolversdk.UpdateResolverConfigInput{
					ResourceId:             aws.String(vpcID),
					AutodefinedReverseFlag: types.AutodefinedReverseFlagDisable,
				})
				require.NoError(t, err)
				require.NotNil(t, updated.ResolverConfig)
				assert.Equal(
					t,
					types.ResolverAutodefinedReverseStatusDisabled,
					updated.ResolverConfig.AutodefinedReverse,
				)

				listed, err := client.ListResolverConfigs(t.Context(), &route53resolversdk.ListResolverConfigsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, listed.ResolverConfigs)
			},
		},
		{
			name: "dnssec_config_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				const vpcID = "vpc-dnssec-1"

				got, err := client.GetResolverDnssecConfig(
					t.Context(),
					&route53resolversdk.GetResolverDnssecConfigInput{
						ResourceId: aws.String(vpcID),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, got.ResolverDNSSECConfig)

				updated, err := client.UpdateResolverDnssecConfig(
					t.Context(),
					&route53resolversdk.UpdateResolverDnssecConfigInput{
						ResourceId: aws.String(vpcID),
						Validation: types.ValidationEnable,
					},
				)
				require.NoError(t, err)
				require.NotNil(t, updated.ResolverDNSSECConfig)

				listed, err := client.ListResolverDnssecConfigs(
					t.Context(),
					&route53resolversdk.ListResolverDnssecConfigsInput{},
				)
				require.NoError(t, err)
				require.NotEmpty(t, listed.ResolverDnssecConfigs)
			},
		},
		{
			name: "firewall_config_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				_, err := client.UpdateFirewallConfig(t.Context(), &route53resolversdk.UpdateFirewallConfigInput{
					ResourceId:       aws.String("vpc-fwconfig-1"),
					FirewallFailOpen: types.FirewallFailOpenStatusEnabled,
				})
				require.NoError(t, err)

				listed, err := client.ListFirewallConfigs(t.Context(), &route53resolversdk.ListFirewallConfigsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, listed.FirewallConfigs)
			},
		},
		{
			name: "resolver_endpoint_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				created, err := client.CreateResolverEndpoint(
					t.Context(),
					&route53resolversdk.CreateResolverEndpointInput{
						CreatorRequestId: aws.String("cr-ep-1"),
						Direction:        types.ResolverEndpointDirectionInbound,
						Name:             aws.String("ep-1"),
						IpAddresses: []types.IpAddressRequest{
							{SubnetId: aws.String("subnet-11111111")},
						},
						SecurityGroupIds: []string{"sg-11111111"},
					},
				)
				require.NoError(t, err)
				endpointID := aws.ToString(created.ResolverEndpoint.Id)
				require.NotEmpty(t, endpointID)

				associatedIP, err := client.AssociateResolverEndpointIpAddress(
					t.Context(),
					&route53resolversdk.AssociateResolverEndpointIpAddressInput{
						ResolverEndpointId: aws.String(endpointID),
						IpAddress: &types.IpAddressUpdate{
							SubnetId: aws.String("subnet-22222222"),
						},
					},
				)
				require.NoError(t, err)
				require.NotNil(t, associatedIP.ResolverEndpoint)

				listedIPs, err := client.ListResolverEndpointIpAddresses(
					t.Context(),
					&route53resolversdk.ListResolverEndpointIpAddressesInput{
						ResolverEndpointId: aws.String(endpointID),
					},
				)
				require.NoError(t, err)
				require.Len(t, listedIPs.IpAddresses, 2)

				ipID := aws.ToString(listedIPs.IpAddresses[1].IpId)
				require.NotEmpty(t, ipID)

				_, err = client.DisassociateResolverEndpointIpAddress(
					t.Context(),
					&route53resolversdk.DisassociateResolverEndpointIpAddressInput{
						ResolverEndpointId: aws.String(endpointID),
						IpAddress:          &types.IpAddressUpdate{IpId: aws.String(ipID)},
					},
				)
				require.NoError(t, err)

				updated, err := client.UpdateResolverEndpoint(
					t.Context(),
					&route53resolversdk.UpdateResolverEndpointInput{
						ResolverEndpointId: aws.String(endpointID),
						Name:               aws.String("ep-1-renamed"),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, updated.ResolverEndpoint)
				assert.Equal(t, "ep-1-renamed", aws.ToString(updated.ResolverEndpoint.Name))

				_, err = client.DeleteResolverEndpoint(t.Context(), &route53resolversdk.DeleteResolverEndpointInput{
					ResolverEndpointId: aws.String(endpointID),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "outpost_resolver_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				created, err := client.CreateOutpostResolver(
					t.Context(),
					&route53resolversdk.CreateOutpostResolverInput{
						CreatorRequestId: aws.String("cr-outp-1"),
						Name:             aws.String("outp-1"),
						OutpostArn: aws.String(
							"arn:aws:outposts:us-east-1:000000000000:outpost/op-11111111",
						),
						InstanceCount:         aws.Int32(2),
						PreferredInstanceType: aws.String("r5.large"),
					},
				)
				require.NoError(t, err)
				outpostResolverID := aws.ToString(created.OutpostResolver.Id)
				require.NotEmpty(t, outpostResolverID)

				listed, err := client.ListOutpostResolvers(t.Context(), &route53resolversdk.ListOutpostResolversInput{})
				require.NoError(t, err)
				require.NotEmpty(t, listed.OutpostResolvers)

				_, err = client.DeleteOutpostResolver(t.Context(), &route53resolversdk.DeleteOutpostResolverInput{
					Id: aws.String(outpostResolverID),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "query_log_config_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				created, err := client.CreateResolverQueryLogConfig(
					t.Context(),
					&route53resolversdk.CreateResolverQueryLogConfigInput{
						CreatorRequestId: aws.String("cr-qlc-1"),
						DestinationArn:   aws.String("arn:aws:s3:::qlc-bucket"),
						Name:             aws.String("qlc-1"),
					},
				)
				require.NoError(t, err)
				qlcID := aws.ToString(created.ResolverQueryLogConfig.Id)
				require.NotEmpty(t, qlcID)
				qlcARN := aws.ToString(created.ResolverQueryLogConfig.Arn)
				require.NotEmpty(t, qlcARN)

				got, err := client.GetResolverQueryLogConfig(
					t.Context(),
					&route53resolversdk.GetResolverQueryLogConfigInput{
						ResolverQueryLogConfigId: aws.String(qlcID),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "qlc-1", aws.ToString(got.ResolverQueryLogConfig.Name))

				_, err = client.PutResolverQueryLogConfigPolicy(
					t.Context(),
					&route53resolversdk.PutResolverQueryLogConfigPolicyInput{
						Arn: aws.String(qlcARN),
						ResolverQueryLogConfigPolicy: aws.String(
							policyDocument("route53resolver:AssociateResolverQueryLogConfig", qlcARN),
						),
					},
				)
				require.NoError(t, err)

				gotPolicy, err := client.GetResolverQueryLogConfigPolicy(
					t.Context(),
					&route53resolversdk.GetResolverQueryLogConfigPolicyInput{Arn: aws.String(qlcARN)},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(gotPolicy.ResolverQueryLogConfigPolicy))

				_, err = client.DeleteResolverQueryLogConfig(
					t.Context(),
					&route53resolversdk.DeleteResolverQueryLogConfigInput{
						ResolverQueryLogConfigId: aws.String(qlcID),
					},
				)
				require.NoError(t, err)

				_, err = client.GetResolverQueryLogConfig(
					t.Context(),
					&route53resolversdk.GetResolverQueryLogConfigInput{
						ResolverQueryLogConfigId: aws.String(qlcID),
					},
				)
				require.Error(t, err)
			},
		},
		{
			name: "query_log_association_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				created, err := client.CreateResolverQueryLogConfig(
					t.Context(),
					&route53resolversdk.CreateResolverQueryLogConfigInput{
						CreatorRequestId: aws.String("cr-qlc-2"),
						DestinationArn:   aws.String("arn:aws:s3:::qlc-bucket-2"),
						Name:             aws.String("qlc-2"),
					},
				)
				require.NoError(t, err)
				qlcID := aws.ToString(created.ResolverQueryLogConfig.Id)

				associated, err := client.AssociateResolverQueryLogConfig(
					t.Context(),
					&route53resolversdk.AssociateResolverQueryLogConfigInput{
						ResolverQueryLogConfigId: aws.String(qlcID),
						ResourceId:               aws.String("vpc-qlca-1"),
					},
				)
				require.NoError(t, err)
				assocID := aws.ToString(associated.ResolverQueryLogConfigAssociation.Id)
				require.NotEmpty(t, assocID)

				gotAssoc, err := client.GetResolverQueryLogConfigAssociation(
					t.Context(),
					&route53resolversdk.GetResolverQueryLogConfigAssociationInput{
						ResolverQueryLogConfigAssociationId: aws.String(assocID),
					},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					qlcID,
					aws.ToString(gotAssoc.ResolverQueryLogConfigAssociation.ResolverQueryLogConfigId),
				)

				_, err = client.DisassociateResolverQueryLogConfig(
					t.Context(),
					&route53resolversdk.DisassociateResolverQueryLogConfigInput{
						ResolverQueryLogConfigId: aws.String(qlcID),
						ResourceId:               aws.String("vpc-qlca-1"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "resolver_rule_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				createdRule, err := client.CreateResolverRule(t.Context(), &route53resolversdk.CreateResolverRuleInput{
					CreatorRequestId: aws.String("cr-rr-1"),
					RuleType:         types.RuleTypeOptionSystem,
					DomainName:       aws.String("apex.example.com"),
					Name:             aws.String("rr-1"),
				})
				require.NoError(t, err)
				ruleID := aws.ToString(createdRule.ResolverRule.Id)
				require.NotEmpty(t, ruleID)
				ruleARN := aws.ToString(createdRule.ResolverRule.Arn)
				require.NotEmpty(t, ruleARN)

				_, err = client.PutResolverRulePolicy(t.Context(), &route53resolversdk.PutResolverRulePolicyInput{
					Arn: aws.String(ruleARN),
					ResolverRulePolicy: aws.String(
						policyDocument("route53resolver:GetResolverRule", ruleARN),
					),
				})
				require.NoError(t, err)

				_, err = client.UpdateResolverRule(t.Context(), &route53resolversdk.UpdateResolverRuleInput{
					ResolverRuleId: aws.String(ruleID),
					Config:         &types.ResolverRuleConfig{Name: aws.String("rr-1-renamed")},
				})
				require.NoError(t, err)

				associated, err := client.AssociateResolverRule(
					t.Context(),
					&route53resolversdk.AssociateResolverRuleInput{
						ResolverRuleId: aws.String(ruleID),
						VPCId:          aws.String("vpc-rr-1"),
						Name:           aws.String("rra-1"),
					},
				)
				require.NoError(t, err)
				assocID := aws.ToString(associated.ResolverRuleAssociation.Id)
				require.NotEmpty(t, assocID)

				gotAssoc, err := client.GetResolverRuleAssociation(
					t.Context(),
					&route53resolversdk.GetResolverRuleAssociationInput{
						ResolverRuleAssociationId: aws.String(assocID),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, ruleID, aws.ToString(gotAssoc.ResolverRuleAssociation.ResolverRuleId))

				listedAssocs, err := client.ListResolverRuleAssociations(
					t.Context(),
					&route53resolversdk.ListResolverRuleAssociationsInput{},
				)
				require.NoError(t, err)
				require.NotEmpty(t, listedAssocs.ResolverRuleAssociations)

				_, err = client.DisassociateResolverRule(t.Context(), &route53resolversdk.DisassociateResolverRuleInput{
					ResolverRuleId: aws.String(ruleID),
					VPCId:          aws.String("vpc-rr-1"),
				})
				require.NoError(t, err)

				_, err = client.DeleteResolverRule(t.Context(), &route53resolversdk.DeleteResolverRuleInput{
					ResolverRuleId: aws.String(ruleID),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "tags_family",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				createdGroup, err := client.CreateFirewallRuleGroup(
					t.Context(),
					&route53resolversdk.CreateFirewallRuleGroupInput{
						CreatorRequestId: aws.String("cr-frg-tags"),
						Name:             aws.String("frg-tags"),
					},
				)
				require.NoError(t, err)
				groupARN := aws.ToString(createdGroup.FirewallRuleGroup.Arn)
				require.NotEmpty(t, groupARN)

				_, err = client.TagResource(t.Context(), &route53resolversdk.TagResourceInput{
					ResourceArn: aws.String(groupARN),
					Tags: []types.Tag{
						{Key: aws.String("env"), Value: aws.String("prod")},
					},
				})
				require.NoError(t, err)

				listed, err := client.ListTagsForResource(t.Context(), &route53resolversdk.ListTagsForResourceInput{
					ResourceArn: aws.String(groupARN),
				})
				require.NoError(t, err)
				require.Len(t, listed.Tags, 1)
				assert.Equal(t, "env", aws.ToString(listed.Tags[0].Key))

				_, err = client.UntagResource(t.Context(), &route53resolversdk.UntagResourceInput{
					ResourceArn: aws.String(groupARN),
					TagKeys:     []string{"env"},
				})
				require.NoError(t, err)

				listed, err = client.ListTagsForResource(t.Context(), &route53resolversdk.ListTagsForResourceInput{
					ResourceArn: aws.String(groupARN),
				})
				require.NoError(t, err)
				assert.Empty(t, listed.Tags)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
