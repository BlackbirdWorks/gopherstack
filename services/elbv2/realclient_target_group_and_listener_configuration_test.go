package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestRealClient_TargetGroupAndListenerConfiguration drives elbv2's typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_TargetGroupAndListenerConfiguration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "account limits and ssl policies", run: func(t *testing.T) {
			t.Helper()

			client := newTestELBv2Client(t, elbv2.NewHandler(elbv2.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			limitsOut, err := client.DescribeAccountLimits(ctx, &elbv2sdk.DescribeAccountLimitsInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, limitsOut.Limits)

			policiesOut, err := client.DescribeSSLPolicies(ctx, &elbv2sdk.DescribeSSLPoliciesInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, policiesOut.SslPolicies)
		}},
		{name: "target group attributes and modify", run: func(t *testing.T) {
			t.Helper()

			client := newTestELBv2Client(t, elbv2.NewHandler(elbv2.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			tgOut, err := client.CreateTargetGroup(ctx, &elbv2sdk.CreateTargetGroupInput{
				Name:     aws.String("s11-tg"),
				Protocol: types.ProtocolEnumHttp,
				Port:     aws.Int32(80),
				VpcId:    aws.String("vpc-11111111"),
			})
			require.NoError(t, err)
			tgArn := tgOut.TargetGroups[0].TargetGroupArn

			modOut, err := client.ModifyTargetGroup(ctx, &elbv2sdk.ModifyTargetGroupInput{
				TargetGroupArn:  tgArn,
				HealthCheckPath: aws.String("/s11-health"),
			})
			require.NoError(t, err)
			assert.Equal(t, "/s11-health", aws.ToString(modOut.TargetGroups[0].HealthCheckPath))

			_, err = client.ModifyTargetGroupAttributes(ctx, &elbv2sdk.ModifyTargetGroupAttributesInput{
				TargetGroupArn: tgArn,
				Attributes: []types.TargetGroupAttribute{
					{Key: aws.String("deregistration_delay.timeout_seconds"), Value: aws.String("45")},
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeTargetGroupAttributes(ctx, &elbv2sdk.DescribeTargetGroupAttributesInput{
				TargetGroupArn: tgArn,
			})
			require.NoError(t, err)
			var found bool
			for _, a := range descOut.Attributes {
				if aws.ToString(a.Key) == "deregistration_delay.timeout_seconds" {
					found = true
					assert.Equal(t, "45", aws.ToString(a.Value))
				}
			}
			assert.True(t, found)
		}},
		{name: "listener attributes", run: func(t *testing.T) {
			t.Helper()

			client := newTestELBv2Client(t, elbv2.NewHandler(elbv2.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			lbOut, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
				Name:    aws.String("s11-listener-lb"),
				Subnets: []string{"subnet-11111111", "subnet-22222222"},
			})
			require.NoError(t, err)
			lbArn := lbOut.LoadBalancers[0].LoadBalancerArn

			tgOut, err := client.CreateTargetGroup(ctx, &elbv2sdk.CreateTargetGroupInput{
				Name:     aws.String("s11-listener-tg"),
				Protocol: types.ProtocolEnumHttp,
				Port:     aws.Int32(80),
				VpcId:    aws.String("vpc-11111111"),
			})
			require.NoError(t, err)
			tgArn := tgOut.TargetGroups[0].TargetGroupArn

			lOut, err := client.CreateListener(ctx, &elbv2sdk.CreateListenerInput{
				LoadBalancerArn: lbArn,
				Protocol:        types.ProtocolEnumHttp,
				Port:            aws.Int32(80),
				DefaultActions: []types.Action{
					{Type: types.ActionTypeEnumForward, TargetGroupArn: tgArn},
				},
			})
			require.NoError(t, err)
			listenerArn := lOut.Listeners[0].ListenerArn

			_, err = client.ModifyListenerAttributes(ctx, &elbv2sdk.ModifyListenerAttributesInput{
				ListenerArn: listenerArn,
				Attributes: []types.ListenerAttribute{
					{Key: aws.String("tcp.idle_timeout.seconds"), Value: aws.String("120")},
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeListenerAttributes(ctx, &elbv2sdk.DescribeListenerAttributesInput{
				ListenerArn: listenerArn,
			})
			require.NoError(t, err)
			var found bool
			for _, a := range descOut.Attributes {
				if aws.ToString(a.Key) == "tcp.idle_timeout.seconds" {
					found = true
					assert.Equal(t, "120", aws.ToString(a.Value))
				}
			}
			assert.True(t, found)
		}},
		{name: "modify rule and set rule priorities", run: func(t *testing.T) {
			t.Helper()

			client := newTestELBv2Client(t, elbv2.NewHandler(elbv2.NewInMemoryBackend("123456789012", "us-east-1")))
			ctx := t.Context()

			lbOut, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
				Name:    aws.String("s11-rule-lb"),
				Subnets: []string{"subnet-11111111", "subnet-22222222"},
			})
			require.NoError(t, err)
			lbArn := lbOut.LoadBalancers[0].LoadBalancerArn

			tgOut, err := client.CreateTargetGroup(ctx, &elbv2sdk.CreateTargetGroupInput{
				Name:     aws.String("s11-rule-tg"),
				Protocol: types.ProtocolEnumHttp,
				Port:     aws.Int32(80),
				VpcId:    aws.String("vpc-11111111"),
			})
			require.NoError(t, err)
			tgArn := tgOut.TargetGroups[0].TargetGroupArn

			lOut, err := client.CreateListener(ctx, &elbv2sdk.CreateListenerInput{
				LoadBalancerArn: lbArn,
				Protocol:        types.ProtocolEnumHttp,
				Port:            aws.Int32(80),
				DefaultActions: []types.Action{
					{Type: types.ActionTypeEnumForward, TargetGroupArn: tgArn},
				},
			})
			require.NoError(t, err)
			listenerArn := lOut.Listeners[0].ListenerArn

			rule1Out, err := client.CreateRule(ctx, &elbv2sdk.CreateRuleInput{
				ListenerArn: listenerArn,
				Priority:    aws.Int32(10),
				Conditions: []types.RuleCondition{
					{Field: aws.String("path-pattern"), Values: []string{"/s11-a"}},
				},
				Actions: []types.Action{
					{Type: types.ActionTypeEnumForward, TargetGroupArn: tgArn},
				},
			})
			require.NoError(t, err)
			rule1Arn := rule1Out.Rules[0].RuleArn

			rule2Out, err := client.CreateRule(ctx, &elbv2sdk.CreateRuleInput{
				ListenerArn: listenerArn,
				Priority:    aws.Int32(20),
				Conditions: []types.RuleCondition{
					{Field: aws.String("path-pattern"), Values: []string{"/s11-b"}},
				},
				Actions: []types.Action{
					{Type: types.ActionTypeEnumForward, TargetGroupArn: tgArn},
				},
			})
			require.NoError(t, err)
			rule2Arn := rule2Out.Rules[0].RuleArn

			modOut, err := client.ModifyRule(ctx, &elbv2sdk.ModifyRuleInput{
				RuleArn: rule1Arn,
				Conditions: []types.RuleCondition{
					{Field: aws.String("path-pattern"), Values: []string{"/s11-a-modified"}},
				},
			})
			require.NoError(t, err)
			require.Len(t, modOut.Rules[0].Conditions, 1)

			require.NotNil(t, modOut.Rules[0].Conditions[0].PathPatternConfig)
			require.Len(t, modOut.Rules[0].Conditions[0].PathPatternConfig.Values, 1)
			assert.Equal(t, "/s11-a-modified", modOut.Rules[0].Conditions[0].PathPatternConfig.Values[0])

			setOut, err := client.SetRulePriorities(ctx, &elbv2sdk.SetRulePrioritiesInput{
				RulePriorities: []types.RulePriorityPair{
					{RuleArn: rule1Arn, Priority: aws.Int32(20)},
					{RuleArn: rule2Arn, Priority: aws.Int32(10)},
				},
			})
			require.NoError(t, err)

			priorities := map[string]string{}
			for _, r := range setOut.Rules {
				priorities[aws.ToString(r.RuleArn)] = aws.ToString(r.Priority)
			}
			assert.Equal(t, "20", priorities[aws.ToString(rule1Arn)])
			assert.Equal(t, "10", priorities[aws.ToString(rule2Arn)])
		}},
		{name: "trust store lifecycle", run: func(t *testing.T) {
			t.Helper()

			backend := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestELBv2Client(t, elbv2.NewHandler(backend))
			ctx := t.Context()

			tsOut, err := client.CreateTrustStore(ctx, &elbv2sdk.CreateTrustStoreInput{
				Name:                         aws.String("s11-trust-store"),
				CaCertificatesBundleS3Bucket: aws.String("s11-bucket"),
				CaCertificatesBundleS3Key:    aws.String("s11-key.pem"),
			})
			require.NoError(t, err)
			tsArn := tsOut.TrustStores[0].TrustStoreArn

			modOut, err := client.ModifyTrustStore(ctx, &elbv2sdk.ModifyTrustStoreInput{
				TrustStoreArn:                aws.String(aws.ToString(tsArn)),
				CaCertificatesBundleS3Bucket: aws.String("s11-bucket-2"),
				CaCertificatesBundleS3Key:    aws.String("s11-key-2.pem"),
			})
			require.NoError(t, err)
			assert.NotNil(t, modOut.TrustStores[0].TrustStoreArn)

			lbOut, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
				Name:    aws.String("s11-ts-lb"),
				Subnets: []string{"subnet-11111111", "subnet-22222222"},
			})
			require.NoError(t, err)
			lbArn := lbOut.LoadBalancers[0].LoadBalancerArn

			tgOut, err := client.CreateTargetGroup(ctx, &elbv2sdk.CreateTargetGroupInput{
				Name:     aws.String("s11-ts-tg"),
				Protocol: types.ProtocolEnumHttps,
				Port:     aws.Int32(443),
				VpcId:    aws.String("vpc-11111111"),
			})
			require.NoError(t, err)
			tgArn := tgOut.TargetGroups[0].TargetGroupArn

			lOut, err := client.CreateListener(ctx, &elbv2sdk.CreateListenerInput{
				LoadBalancerArn: lbArn,
				Protocol:        types.ProtocolEnumHttps,
				Port:            aws.Int32(443),
				Certificates: []types.Certificate{
					{CertificateArn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/s11-cert")},
				},
				MutualAuthentication: &types.MutualAuthenticationAttributes{
					Mode:          aws.String("verify"),
					TrustStoreArn: tsArn,
				},
				DefaultActions: []types.Action{
					{Type: types.ActionTypeEnumForward, TargetGroupArn: tgArn},
				},
			})
			require.NoError(t, err)
			listenerArn := lOut.Listeners[0].ListenerArn

			assocOut, err := client.DescribeTrustStoreAssociations(ctx, &elbv2sdk.DescribeTrustStoreAssociationsInput{
				TrustStoreArn: tsArn,
			})
			require.NoError(t, err)
			require.Len(t, assocOut.TrustStoreAssociations, 1)
			assert.Equal(t, aws.ToString(listenerArn), aws.ToString(assocOut.TrustStoreAssociations[0].ResourceArn))

			_, err = client.DeleteSharedTrustStoreAssociation(ctx, &elbv2sdk.DeleteSharedTrustStoreAssociationInput{
				TrustStoreArn: tsArn,
				ResourceArn:   listenerArn,
			})
			require.NoError(t, err)

			afterAssoc, err := client.DescribeTrustStoreAssociations(ctx, &elbv2sdk.DescribeTrustStoreAssociationsInput{
				TrustStoreArn: tsArn,
			})
			require.NoError(t, err)
			assert.Empty(t, afterAssoc.TrustStoreAssociations)

			revOut, err := client.AddTrustStoreRevocations(ctx, &elbv2sdk.AddTrustStoreRevocationsInput{
				TrustStoreArn: tsArn,
				RevocationContents: []types.RevocationContent{
					{RevocationType: types.RevocationTypeCrl},
				},
			})
			require.NoError(t, err)
			require.Len(t, revOut.TrustStoreRevocations, 1)
			revocationID := revOut.TrustStoreRevocations[0].RevocationId

			_, err = client.GetTrustStoreCaCertificatesBundle(ctx, &elbv2sdk.GetTrustStoreCaCertificatesBundleInput{
				TrustStoreArn: tsArn,
			})
			require.NoError(t, err)

			_, err = client.GetTrustStoreRevocationContent(ctx, &elbv2sdk.GetTrustStoreRevocationContentInput{
				TrustStoreArn: tsArn,
				RevocationId:  revocationID,
			})
			require.NoError(t, err)

			_, err = client.DeleteTrustStore(ctx, &elbv2sdk.DeleteTrustStoreInput{
				TrustStoreArn: tsArn,
			})
			require.NoError(t, err)

			_, err = client.DescribeTrustStores(ctx, &elbv2sdk.DescribeTrustStoresInput{
				TrustStoreArns: []string{aws.ToString(tsArn)},
			})
			assert.Error(t, err, "DescribeTrustStores on a deleted trust store must fail")
		}},
		{name: "get resource policy", run: func(t *testing.T) {
			t.Helper()

			backend := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestELBv2Client(t, elbv2.NewHandler(backend))
			ctx := t.Context()

			lbOut, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
				Name:    aws.String("s11-policy-lb"),
				Subnets: []string{"subnet-11111111", "subnet-22222222"},
			})
			require.NoError(t, err)
			lbArn := aws.ToString(lbOut.LoadBalancers[0].LoadBalancerArn)

			policy := `{"Version":"2012-10-17","Statement":[]}`
			require.NoError(t, backend.PutResourcePolicy(lbArn, policy))

			out, err := client.GetResourcePolicy(ctx, &elbv2sdk.GetResourcePolicyInput{
				ResourceArn: aws.String(lbArn),
			})
			require.NoError(t, err)
			assert.JSONEq(t, policy, aws.ToString(out.Policy))
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
