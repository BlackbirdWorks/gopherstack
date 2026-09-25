package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsvc "github.com/aws/aws-sdk-go-v2/service/ecr"
	elbsvc "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbv2svc "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_ElbAlbAndEcr provisions the 13 ELBv2/classic-ELB resource
// types (ALB and its listener/listener-certificate/listener-rule/target-
// group/target-group-attachment "alb"/"lb" aliases, trust store and trust
// store revocation, and the two classic-ELB "lb"-named policies: cookie
// stickiness and SSL negotiation) plus 6 of the 8 ECR resource types that
// had no Terraform fixture coverage (account setting, lifecycle policy,
// pull-through cache rule, registry policy, repository creation template,
// repository policy; registry scanning configuration and replication
// configuration are dropped -- see services/ecr/PARITY.md, gopherstack-101r),
// and verifies each via its own SDK client's Describe/Get path.
func TestTerraform_ElbAlbAndEcr(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "elb-alb-and-ecr",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				cfg := megaConfig(t)

				elbv2Client := elbv2svc.NewFromConfig(cfg, func(o *elbv2svc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				elbClient := elbsvc.NewFromConfig(cfg, func(o *elbsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				ecrClient := ecrsvc.NewFromConfig(cfg, func(o *ecrsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				verifyElbAlbAndEcrELBv2(ctx, t, elbv2Client)
				verifyElbAlbAndEcrELBClassic(ctx, t, elbClient)
				verifyElbAlbAndEcrECR(ctx, t, ecrClient)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyElbAlbAndEcrELBv2(ctx context.Context, t *testing.T, client *elbv2svc.Client) {
	t.Helper()

	lbOut, err := client.DescribeLoadBalancers(ctx, &elbv2svc.DescribeLoadBalancersInput{
		Names: []string{"elae-alb"},
	})
	require.NoError(t, err, "DescribeLoadBalancers should succeed")
	require.Len(t, lbOut.LoadBalancers, 1)
	lbArn := aws.ToString(lbOut.LoadBalancers[0].LoadBalancerArn)

	tgOut, err := client.DescribeTargetGroups(ctx, &elbv2svc.DescribeTargetGroupsInput{
		Names: []string{"elae-tg"},
	})
	require.NoError(t, err, "DescribeTargetGroups should succeed")
	require.Len(t, tgOut.TargetGroups, 1)
	tgArn := aws.ToString(tgOut.TargetGroups[0].TargetGroupArn)

	listenersOut, err := client.DescribeListeners(ctx, &elbv2svc.DescribeListenersInput{
		LoadBalancerArn: aws.String(lbArn),
	})
	require.NoError(t, err, "DescribeListeners should succeed")
	require.Len(t, listenersOut.Listeners, 1)
	listenerArn := aws.ToString(listenersOut.Listeners[0].ListenerArn)
	assert.EqualValues(t, 443, aws.ToInt32(listenersOut.Listeners[0].Port))

	certsOut, err := client.DescribeListenerCertificates(ctx, &elbv2svc.DescribeListenerCertificatesInput{
		ListenerArn: aws.String(listenerArn),
	})
	require.NoError(t, err, "DescribeListenerCertificates should succeed")
	assert.GreaterOrEqual(
		t,
		len(certsOut.Certificates),
		2,
		"both alb_listener_certificate and lb_listener_certificate should attach",
	)

	rulesOut, err := client.DescribeRules(ctx, &elbv2svc.DescribeRulesInput{
		ListenerArn: aws.String(listenerArn),
	})
	require.NoError(t, err, "DescribeRules should succeed")

	var foundALBRule, foundLBRule bool

	for _, r := range rulesOut.Rules {
		for _, c := range r.Conditions {
			if c.PathPatternConfig == nil {
				continue
			}

			for _, v := range c.PathPatternConfig.Values {
				if v == "/alb/*" {
					foundALBRule = true
				}

				if v == "/lb/*" {
					foundLBRule = true
				}
			}
		}
	}

	assert.True(t, foundALBRule, "aws_alb_listener_rule should be listed")
	assert.True(t, foundLBRule, "aws_lb_listener_rule should be listed")

	healthOut, err := client.DescribeTargetHealth(ctx, &elbv2svc.DescribeTargetHealthInput{
		TargetGroupArn: aws.String(tgArn),
	})
	require.NoError(t, err, "DescribeTargetHealth should succeed")

	var foundALBAttachment, foundLBAttachment bool

	for _, th := range healthOut.TargetHealthDescriptions {
		switch aws.ToString(th.Target.Id) {
		case "10.115.1.5":
			foundALBAttachment = true
		case "10.115.1.6":
			foundLBAttachment = true
		}
	}

	assert.True(t, foundALBAttachment, "aws_alb_target_group_attachment target should be registered")
	assert.True(t, foundLBAttachment, "aws_lb_target_group_attachment target should be registered")

	tsOut, err := client.DescribeTrustStores(ctx, &elbv2svc.DescribeTrustStoresInput{
		Names: []string{"elae-ts"},
	})
	require.NoError(t, err, "DescribeTrustStores should succeed")
	require.Len(t, tsOut.TrustStores, 1)
	trustStoreArn := aws.ToString(tsOut.TrustStores[0].TrustStoreArn)

	revOut, err := client.DescribeTrustStoreRevocations(ctx, &elbv2svc.DescribeTrustStoreRevocationsInput{
		TrustStoreArn: aws.String(trustStoreArn),
	})
	require.NoError(t, err, "DescribeTrustStoreRevocations should succeed")
	assert.NotEmpty(t, revOut.TrustStoreRevocations, "trust store revocation should be listed")
}

func verifyElbAlbAndEcrELBClassic(ctx context.Context, t *testing.T, client *elbsvc.Client) {
	t.Helper()

	policiesOut, err := client.DescribeLoadBalancerPolicies(ctx, &elbsvc.DescribeLoadBalancerPoliciesInput{
		LoadBalancerName: aws.String("elae-elb"),
	})
	require.NoError(t, err, "DescribeLoadBalancerPolicies should succeed")

	var foundCookiePolicy, foundSSLPolicy bool

	for _, p := range policiesOut.PolicyDescriptions {
		switch aws.ToString(p.PolicyName) {
		case "elae-cookie-policy":
			foundCookiePolicy = true
		case "elae-ssl-policy":
			foundSSLPolicy = true
		}
	}

	assert.True(t, foundCookiePolicy, "aws_lb_cookie_stickiness_policy should be listed")
	assert.True(t, foundSSLPolicy, "aws_lb_ssl_negotiation_policy should be listed")
}

func verifyElbAlbAndEcrECR(ctx context.Context, t *testing.T, client *ecrsvc.Client) {
	t.Helper()

	policyOut, err := client.GetRepositoryPolicy(ctx, &ecrsvc.GetRepositoryPolicyInput{
		RepositoryName: aws.String("elae-repo"),
	})
	require.NoError(t, err, "GetRepositoryPolicy should succeed")
	assert.Contains(t, aws.ToString(policyOut.PolicyText), "AllowPull")

	lifecycleOut, err := client.GetLifecyclePolicy(ctx, &ecrsvc.GetLifecyclePolicyInput{
		RepositoryName: aws.String("elae-repo"),
	})
	require.NoError(t, err, "GetLifecyclePolicy should succeed")
	assert.Contains(t, aws.ToString(lifecycleOut.LifecyclePolicyText), "sinceImagePushed")

	registryPolicyOut, err := client.GetRegistryPolicy(ctx, &ecrsvc.GetRegistryPolicyInput{})
	require.NoError(t, err, "GetRegistryPolicy should succeed")
	assert.Contains(t, aws.ToString(registryPolicyOut.PolicyText), "AllowReplication")

	// aws_ecr_registry_scanning_configuration and aws_ecr_replication_configuration
	// are intentionally NOT in this fixture: both are dropped per
	// services/ecr/PARITY.md (gopherstack-101r) -- the real
	// terraform-provider-aws v5.100.0 binary fails their apply with
	// "Provider produced inconsistent result after apply ... root object was
	// present, but now absent" even though this emulator's wire responses were
	// verified byte-correct (direct HTTP probe + TF_LOG=trace showed
	// diagnostic_error_count=0 from the provider itself); the failure appears
	// to originate in terraform-plugin-sdk/Terraform Core's own legacy-SDK
	// state-consistency check, not in this emulator.

	ptcOut, err := client.DescribePullThroughCacheRules(ctx, &ecrsvc.DescribePullThroughCacheRulesInput{
		EcrRepositoryPrefixes: []string{"elae-ptc"},
	})
	require.NoError(t, err, "DescribePullThroughCacheRules should succeed")
	require.Len(t, ptcOut.PullThroughCacheRules, 1)
	assert.Equal(t, "public.ecr.aws", aws.ToString(ptcOut.PullThroughCacheRules[0].UpstreamRegistryUrl))

	tmplOut, err := client.DescribeRepositoryCreationTemplates(ctx, &ecrsvc.DescribeRepositoryCreationTemplatesInput{
		Prefixes: []string{"elae-tmpl"},
	})
	require.NoError(t, err, "DescribeRepositoryCreationTemplates should succeed")
	require.Len(t, tmplOut.RepositoryCreationTemplates, 1)

	var foundAppliedFor bool

	for _, af := range tmplOut.RepositoryCreationTemplates[0].AppliedFor {
		if string(af) == "PULL_THROUGH_CACHE" {
			foundAppliedFor = true
		}
	}

	assert.True(t, foundAppliedFor, "repository creation template should apply for PULL_THROUGH_CACHE")

	settingOut, err := client.GetAccountSetting(ctx, &ecrsvc.GetAccountSettingInput{
		Name: aws.String("BASIC_SCAN_TYPE_VERSION"),
	})
	require.NoError(t, err, "GetAccountSetting should succeed")
	assert.Equal(t, "AWS_NATIVE", aws.ToString(settingOut.Value))
}
