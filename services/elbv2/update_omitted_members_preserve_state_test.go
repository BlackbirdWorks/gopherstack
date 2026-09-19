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

// TestModifyListener_PreservesOmittedMembers proves the zeroguard fix
// (cmd/zeroguard): ModifyListenerInput.SslPolicy and .Port are plain string/
// int32 in the real SDK's request type. ELBv2 is awsquery/form-encoded, so
// "omitted" means the form key is absent, not that vals.Get returns "" --
// before the fix SslPolicy read via vals.Get alone, so an omitted field and
// one explicitly sent empty were indistinguishable, and Port compared
// against the zero value so an (unrealistic but wire-valid) explicit 0
// could never be told apart from omission either. ListenerArn is left a
// plain string: it is a required lookup identifier, never written back.
func TestModifyListener_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	b := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(b.Close)
	client := newTestELBv2Client(t, elbv2.NewHandler(b))
	ctx := t.Context()

	lbOut, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
		Name:    aws.String("uds-lb"),
		Subnets: []string{"subnet-11111111", "subnet-22222222"},
	})
	require.NoError(t, err)
	lbArn := aws.ToString(lbOut.LoadBalancers[0].LoadBalancerArn)

	fixedResponse := []elbv2types.Action{
		{
			Type: elbv2types.ActionTypeEnumFixedResponse,
			FixedResponseConfig: &elbv2types.FixedResponseActionConfig{
				StatusCode: aws.String("200"),
			},
		},
	}

	createOut, err := client.CreateListener(ctx, &elbv2sdk.CreateListenerInput{
		LoadBalancerArn: aws.String(lbArn),
		Protocol:        elbv2types.ProtocolEnumHttp,
		Port:            aws.Int32(80),
		DefaultActions:  fixedResponse,
	})
	require.NoError(t, err)
	listenerArn := aws.ToString(createOut.Listeners[0].ListenerArn)

	_, err = client.ModifyListener(ctx, &elbv2sdk.ModifyListenerInput{
		ListenerArn: aws.String(listenerArn),
		SslPolicy:   aws.String("ELBSecurityPolicy-TLS-1-2-2017-01"),
		Port:        aws.Int32(8080),
	})
	require.NoError(t, err)

	desc, err := client.DescribeListeners(ctx, &elbv2sdk.DescribeListenersInput{ListenerArns: []string{listenerArn}})
	require.NoError(t, err)
	require.Len(t, desc.Listeners, 1)
	assert.Equal(t, "ELBSecurityPolicy-TLS-1-2-2017-01", aws.ToString(desc.Listeners[0].SslPolicy))
	assert.Equal(t, int32(8080), aws.ToInt32(desc.Listeners[0].Port))

	// Omitted SslPolicy/Port must preserve the prior values.
	_, err = client.ModifyListener(ctx, &elbv2sdk.ModifyListenerInput{
		ListenerArn: aws.String(listenerArn),
	})
	require.NoError(t, err)

	desc, err = client.DescribeListeners(ctx, &elbv2sdk.DescribeListenersInput{ListenerArns: []string{listenerArn}})
	require.NoError(t, err)
	assert.Equal(t, "ELBSecurityPolicy-TLS-1-2-2017-01", aws.ToString(desc.Listeners[0].SslPolicy),
		"omitted SslPolicy must survive")
	assert.Equal(t, int32(8080), aws.ToInt32(desc.Listeners[0].Port), "omitted Port must survive")
}

// TestModifyTargetGroup_PreservesOmittedMembers proves the zeroguard fix:
// ModifyTargetGroupInput.HealthCheckPort/.HealthCheckPath (plain strings)
// and .HealthCheckIntervalSeconds/.HealthCheckTimeoutSeconds/
// .HealthyThresholdCount/.UnhealthyThresholdCount (plain int32s) in the real
// SDK's request type were read via vals.Get/parseOptionalInt32 (0-for-
// missing), making omitted and explicit-zero indistinguishable.
// TargetGroupArn is left a plain string: required lookup identifier.
func TestModifyTargetGroup_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	b := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(b.Close)
	client := newTestELBv2Client(t, elbv2.NewHandler(b))
	ctx := t.Context()

	tgOut, err := client.CreateTargetGroup(ctx, &elbv2sdk.CreateTargetGroupInput{
		Name:     aws.String("uds-tg"),
		Protocol: elbv2types.ProtocolEnumHttp,
		Port:     aws.Int32(80),
		VpcId:    aws.String("vpc-11111111"),
	})
	require.NoError(t, err)
	tgArn := aws.ToString(tgOut.TargetGroups[0].TargetGroupArn)

	_, err = client.ModifyTargetGroup(ctx, &elbv2sdk.ModifyTargetGroupInput{
		TargetGroupArn:             aws.String(tgArn),
		HealthCheckPort:            aws.String("8080"),
		HealthCheckPath:            aws.String("/healthz"),
		HealthCheckIntervalSeconds: aws.Int32(15),
		HealthCheckTimeoutSeconds:  aws.Int32(10),
		HealthyThresholdCount:      aws.Int32(3),
		UnhealthyThresholdCount:    aws.Int32(4),
	})
	require.NoError(t, err)

	desc, err := client.DescribeTargetGroups(ctx, &elbv2sdk.DescribeTargetGroupsInput{TargetGroupArns: []string{tgArn}})
	require.NoError(t, err)
	require.Len(t, desc.TargetGroups, 1)
	tg := desc.TargetGroups[0]
	assert.Equal(t, "8080", aws.ToString(tg.HealthCheckPort))
	assert.Equal(t, "/healthz", aws.ToString(tg.HealthCheckPath))
	assert.Equal(t, int32(15), aws.ToInt32(tg.HealthCheckIntervalSeconds))
	assert.Equal(t, int32(10), aws.ToInt32(tg.HealthCheckTimeoutSeconds))
	assert.Equal(t, int32(3), aws.ToInt32(tg.HealthyThresholdCount))
	assert.Equal(t, int32(4), aws.ToInt32(tg.UnhealthyThresholdCount))

	// Omitted fields must preserve every prior value.
	_, err = client.ModifyTargetGroup(ctx, &elbv2sdk.ModifyTargetGroupInput{
		TargetGroupArn: aws.String(tgArn),
	})
	require.NoError(t, err)

	desc, err = client.DescribeTargetGroups(ctx, &elbv2sdk.DescribeTargetGroupsInput{TargetGroupArns: []string{tgArn}})
	require.NoError(t, err)
	tg = desc.TargetGroups[0]
	assert.Equal(t, "8080", aws.ToString(tg.HealthCheckPort), "omitted HealthCheckPort must survive")
	assert.Equal(t, "/healthz", aws.ToString(tg.HealthCheckPath), "omitted HealthCheckPath must survive")
	assert.Equal(t, int32(15), aws.ToInt32(tg.HealthCheckIntervalSeconds), "omitted interval must survive")
	assert.Equal(t, int32(10), aws.ToInt32(tg.HealthCheckTimeoutSeconds), "omitted timeout must survive")
	assert.Equal(t, int32(3), aws.ToInt32(tg.HealthyThresholdCount), "omitted healthy threshold must survive")
	assert.Equal(t, int32(4), aws.ToInt32(tg.UnhealthyThresholdCount), "omitted unhealthy threshold must survive")
}
