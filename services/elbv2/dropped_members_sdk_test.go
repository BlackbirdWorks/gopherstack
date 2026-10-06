package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

func TestCreateLoadBalancerStoresPoolMembers(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestELBv2Backend(t)

	created, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
		Name:                  aws.String("pool-lb"),
		Subnets:               []string{"subnet-11111111", "subnet-22222222"},
		CustomerOwnedIpv4Pool: aws.String("ipv4pool-coip-0123"),
		IpamPools:             &elbv2types.IpamPools{Ipv4IpamPoolId: aws.String("ipam-pool-0abc")},
	})
	require.NoError(t, err)
	require.Len(t, created.LoadBalancers, 1)
	assert.Equal(t, "ipv4pool-coip-0123", aws.ToString(created.LoadBalancers[0].CustomerOwnedIpv4Pool))
	require.NotNil(t, created.LoadBalancers[0].IpamPools)
	assert.Equal(t, "ipam-pool-0abc", aws.ToString(created.LoadBalancers[0].IpamPools.Ipv4IpamPoolId))

	desc, err := client.DescribeLoadBalancers(ctx, &elbv2sdk.DescribeLoadBalancersInput{
		LoadBalancerArns: []string{aws.ToString(created.LoadBalancers[0].LoadBalancerArn)},
	})
	require.NoError(t, err)
	require.Len(t, desc.LoadBalancers, 1)
	assert.Equal(t, "ipv4pool-coip-0123", aws.ToString(desc.LoadBalancers[0].CustomerOwnedIpv4Pool))
	assert.Equal(t, "ipam-pool-0abc", aws.ToString(desc.LoadBalancers[0].IpamPools.Ipv4IpamPoolId))
}

func TestSetSubnetsAppliesIpAddressType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ipType  elbv2types.IpAddressType
		wantErr bool
	}{
		{name: "dualstack", ipType: elbv2types.IpAddressTypeDualstack},
		{name: "invalid", ipType: elbv2types.IpAddressType("bogus"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestELBv2Backend(t)

			created, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
				Name:    aws.String("ip-lb"),
				Subnets: []string{"subnet-11111111", "subnet-22222222"},
			})
			require.NoError(t, err)
			arn := created.LoadBalancers[0].LoadBalancerArn

			out, err := client.SetSubnets(ctx, &elbv2sdk.SetSubnetsInput{
				LoadBalancerArn: arn,
				Subnets:         []string{"subnet-11111111", "subnet-22222222"},
				IpAddressType:   tt.ipType,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.ipType, out.IpAddressType)

			desc, err := client.DescribeLoadBalancers(
				ctx,
				&elbv2sdk.DescribeLoadBalancersInput{LoadBalancerArns: []string{aws.ToString(arn)}},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.ipType, desc.LoadBalancers[0].IpAddressType)
		})
	}
}

func TestCreateTargetGroupStoresTargetControlPort(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestELBv2Backend(t)

	created, err := client.CreateTargetGroup(ctx, &elbv2sdk.CreateTargetGroupInput{
		Name:              aws.String("tcp-tg"),
		Protocol:          elbv2types.ProtocolEnumHttp,
		Port:              aws.Int32(80),
		VpcId:             aws.String("vpc-00000000"),
		TargetControlPort: aws.Int32(8443),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(8443), aws.ToInt32(created.TargetGroups[0].TargetControlPort))

	desc, err := client.DescribeTargetGroups(ctx, &elbv2sdk.DescribeTargetGroupsInput{Names: []string{"tcp-tg"}})
	require.NoError(t, err)
	assert.Equal(t, int32(8443), aws.ToInt32(desc.TargetGroups[0].TargetControlPort))
}

func TestListenerActionsKeepAuthExtraParamsAndJwtConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(t *testing.T, got elbv2types.Action)
		name    string
		actions []elbv2types.Action
		wantErr bool
	}{
		{
			name: "oidc extra params",
			actions: []elbv2types.Action{{
				Type: elbv2types.ActionTypeEnumAuthenticateOidc,
				AuthenticateOidcConfig: &elbv2types.AuthenticateOidcActionConfig{
					Issuer: aws.String("https://i"), AuthorizationEndpoint: aws.String("https://a"),
					TokenEndpoint: aws.String("https://t"), UserInfoEndpoint: aws.String("https://u"),
					ClientId:                         aws.String("c"),
					AuthenticationRequestExtraParams: map[string]string{"display": "page", "prompt": "login"},
				},
			}},
			check: func(t *testing.T, got elbv2types.Action) {
				t.Helper()
				assert.Equal(t, map[string]string{"display": "page", "prompt": "login"},
					got.AuthenticateOidcConfig.AuthenticationRequestExtraParams)
			},
		},
		{
			name: "cognito extra params",
			actions: []elbv2types.Action{{
				Type: elbv2types.ActionTypeEnumAuthenticateCognito,
				AuthenticateCognitoConfig: &elbv2types.AuthenticateCognitoActionConfig{
					UserPoolArn: aws.String(
						"arn:aws:cognito-idp:us-east-1:123456789012:userpool/p",
					),
					UserPoolClientId:                 aws.String("c"),
					UserPoolDomain:                   aws.String("d"),
					AuthenticationRequestExtraParams: map[string]string{"lang": "en"},
				},
			}},
			check: func(t *testing.T, got elbv2types.Action) {
				t.Helper()
				assert.Equal(
					t,
					map[string]string{"lang": "en"},
					got.AuthenticateCognitoConfig.AuthenticationRequestExtraParams,
				)
			},
		},
		{
			name: "jwt validation",
			actions: []elbv2types.Action{{
				Type: elbv2types.ActionTypeEnumJwtValidation,
				JwtValidationConfig: &elbv2types.JwtValidationActionConfig{
					Issuer: aws.String("https://issuer"), JwksEndpoint: aws.String("https://issuer/jwks"),
					AdditionalClaims: []elbv2types.JwtValidationActionAdditionalClaim{{
						Format: elbv2types.JwtValidationActionAdditionalClaimFormatEnumStringArray,
						Name:   aws.String("groups"),
						Values: []string{"a", "b"},
					}},
				},
			}},
			check: func(t *testing.T, got elbv2types.Action) {
				t.Helper()
				require.NotNil(t, got.JwtValidationConfig)
				assert.Equal(t, "https://issuer/jwks", aws.ToString(got.JwtValidationConfig.JwksEndpoint))
				require.Len(t, got.JwtValidationConfig.AdditionalClaims, 1)
				assert.Equal(t, []string{"a", "b"}, got.JwtValidationConfig.AdditionalClaims[0].Values)
				assert.Equal(t, elbv2types.JwtValidationActionAdditionalClaimFormatEnumStringArray,
					got.JwtValidationConfig.AdditionalClaims[0].Format)
			},
		},
		{
			name: "jwt missing issuer",
			actions: []elbv2types.Action{{
				Type:                elbv2types.ActionTypeEnumJwtValidation,
				JwtValidationConfig: &elbv2types.JwtValidationActionConfig{JwksEndpoint: aws.String("https://j")},
			}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestELBv2Backend(t)

			lb, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
				Name:    aws.String("act-lb"),
				Subnets: []string{"subnet-11111111", "subnet-22222222"},
			})
			require.NoError(t, err)

			listener, err := client.CreateListener(ctx, &elbv2sdk.CreateListenerInput{
				LoadBalancerArn: lb.LoadBalancers[0].LoadBalancerArn,
				Protocol:        elbv2types.ProtocolEnumHttps,
				Port:            aws.Int32(443),
				Certificates: []elbv2types.Certificate{
					{CertificateArn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/c")},
				},
				DefaultActions: tt.actions,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			desc, err := client.DescribeListeners(ctx, &elbv2sdk.DescribeListenersInput{
				ListenerArns: []string{aws.ToString(listener.Listeners[0].ListenerArn)},
			})
			require.NoError(t, err)
			require.Len(t, desc.Listeners, 1)
			require.Len(t, desc.Listeners[0].DefaultActions, 1)
			tt.check(t, desc.Listeners[0].DefaultActions[0])
		})
	}
}

func TestSetNetworkRefsValidatedByResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		useSGs   bool
	}{
		{name: "set security groups", useSGs: true, wantCode: "InvalidSecurityGroup"},
		{name: "set subnets", wantCode: "SubnetNotFound"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newCrossServiceHandler(t)
			backend.SetEC2Resolver(&fakeEC2Resolver{
				securityGroups: map[string]bool{"sg-real": true},
				subnets:        map[string]bool{"subnet-real": true},
			})
			client := newTestELBv2Client(t, elbv2.NewHandler(backend))
			ctx := t.Context()

			lb, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
				Name:           aws.String("net-lb"),
				Subnets:        []string{"subnet-real"},
				SecurityGroups: []string{"sg-real"},
			})
			require.NoError(t, err)
			arn := lb.LoadBalancers[0].LoadBalancerArn

			if tt.useSGs {
				_, err = client.SetSecurityGroups(ctx, &elbv2sdk.SetSecurityGroupsInput{
					LoadBalancerArn: arn, SecurityGroups: []string{"sg-unknown"},
				})
				requireAPIErrorCode(t, err, tt.wantCode)

				_, err = client.SetSecurityGroups(ctx, &elbv2sdk.SetSecurityGroupsInput{
					LoadBalancerArn: arn, SecurityGroups: []string{"sg-real"},
				})
				require.NoError(t, err)

				return
			}

			_, err = client.SetSubnets(ctx, &elbv2sdk.SetSubnetsInput{
				LoadBalancerArn: arn, Subnets: []string{"subnet-unknown"},
			})
			requireAPIErrorCode(t, err, tt.wantCode)

			_, err = client.SetSubnets(ctx, &elbv2sdk.SetSubnetsInput{
				LoadBalancerArn: arn, Subnets: []string{"subnet-real"},
			})
			require.NoError(t, err)
		})
	}
}
