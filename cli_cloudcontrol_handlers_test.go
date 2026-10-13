package main

import (
	"encoding/json"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ccDelegationCase struct {
	patchedVal     any
	verify         func(t *testing.T, fx *sfnFixture, id string, present bool)
	verifyPatched  func(t *testing.T, fx *sfnFixture, id string)
	name           string
	typeName       string
	desired        string
	wantID         string
	patch          string
	patchedKey     string
	immutablePatch string
	wantKeys       []string
	absentKeys     []string
}

func TestCloudControlDelegatesToServiceBackends(t *testing.T) {
	t.Parallel()

	tests := slices.Concat(
		ccStorageCases(), ccMessagingCases(), ccSecurityCases(),
		ccNetworkCases(), ccComputeCases(), ccAppCases(),
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runCCDelegationCase(t, tt)
		})
	}
}

func runCCDelegationCase(t *testing.T, tt ccDelegationCase) {
	t.Helper()

	fx := newSFNFixture(t)
	cc := cloudcontrol.NewFromConfig(fx.cfg)
	desired, patch, vpc2 := ccExpandTemplates(t, fx, tt.desired, tt.patch)

	id := ccCreate(t, cc, tt, desired)
	tt.verify(t, fx, id, true)

	ccAssertGet(t, cc, tt, id)
	ccAssertImmutable(t, cc, tt, id)
	ccAssertListed(t, cc, tt, id)
	ccUpdateAndVerify(t, cc, fx, tt, id, patch, vpc2)
	ccDeleteAndVerify(t, cc, fx, tt, id)
}

func ccExpandTemplates(t *testing.T, fx *sfnFixture, desired, patch string) (string, string, string) {
	t.Helper()

	vpc2 := ""

	if strings.Contains(desired, "{{vpc}}") {
		desired = strings.ReplaceAll(desired, "{{vpc}}", ccCreateVPC(t, fx, "10.30.0.0/16"))
	}

	if strings.Contains(patch, "{{vpc2}}") {
		vpc2 = ccCreateVPC(t, fx, "10.31.0.0/16")
		patch = strings.ReplaceAll(patch, "{{vpc2}}", vpc2)
	}

	desired = strings.ReplaceAll(desired, "{{region}}", fx.cfg.Region)
	patch = strings.ReplaceAll(patch, "{{region}}", fx.cfg.Region)

	return desired, patch, vpc2
}

func ccCreateVPC(t *testing.T, fx *sfnFixture, cidr string) string {
	t.Helper()

	vpc, err := ec2.NewFromConfig(fx.cfg).CreateVpc(t.Context(), &ec2.CreateVpcInput{CidrBlock: aws.String(cidr)})
	require.NoError(t, err)

	return aws.ToString(vpc.Vpc.VpcId)
}

func ccCreate(t *testing.T, cc *cloudcontrol.Client, tt ccDelegationCase, desired string) string {
	t.Helper()

	created, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
		TypeName: aws.String(tt.typeName), DesiredState: aws.String(desired),
	})
	require.NoError(t, err)

	id := aws.ToString(created.ProgressEvent.Identifier)
	require.NotEmpty(t, id)

	if tt.wantID != "" {
		assert.Equal(t, tt.wantID, id)
	}

	return id
}

func ccAssertGet(t *testing.T, cc *cloudcontrol.Client, tt ccDelegationCase, id string) {
	t.Helper()

	got, err := cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
		TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
	})
	require.NoError(t, err)
	assert.Equal(t, id, aws.ToString(got.ResourceDescription.Identifier))

	var props map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(got.ResourceDescription.Properties)), &props))

	for _, k := range tt.wantKeys {
		assert.Contains(t, props, k)
		assert.NotEmpty(t, props[k], k)
	}

	for _, k := range tt.absentKeys {
		assert.NotContains(t, props, k)
	}
}

func ccAssertImmutable(t *testing.T, cc *cloudcontrol.Client, tt ccDelegationCase, id string) {
	t.Helper()

	if tt.immutablePatch == "" {
		return
	}

	_, err := cc.UpdateResource(t.Context(), &cloudcontrol.UpdateResourceInput{
		TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
		PatchDocument: aws.String(tt.immutablePatch),
	})
	require.Error(t, err)
}

func ccAssertListed(t *testing.T, cc *cloudcontrol.Client, tt ccDelegationCase, id string) {
	t.Helper()

	listed, err := cc.ListResources(t.Context(), &cloudcontrol.ListResourcesInput{
		TypeName: aws.String(tt.typeName),
	})
	require.NoError(t, err)

	listedIDs := make([]string, 0, len(listed.ResourceDescriptions))
	for _, d := range listed.ResourceDescriptions {
		listedIDs = append(listedIDs, aws.ToString(d.Identifier))
	}

	assert.Contains(t, listedIDs, id)
}

func ccUpdateAndVerify(
	t *testing.T, cc *cloudcontrol.Client, fx *sfnFixture, tt ccDelegationCase, id, patch, vpc2 string,
) {
	t.Helper()

	_, err := cc.UpdateResource(t.Context(), &cloudcontrol.UpdateResourceInput{
		TypeName: aws.String(tt.typeName), Identifier: aws.String(id), PatchDocument: aws.String(patch),
	})
	require.NoError(t, err)

	if tt.patchedKey != "" {
		after, getErr := cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
			TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
		})
		require.NoError(t, getErr)

		repl := strings.NewReplacer("{{vpc2}}", vpc2, "{{region}}", fx.cfg.Region)
		want := repl.Replace(jsonScalar(tt.patchedVal))
		assert.JSONEq(t, `{"`+tt.patchedKey+`":`+want+`}`,
			subsetJSON(t, aws.ToString(after.ResourceDescription.Properties), tt.patchedKey))
	}

	if tt.verifyPatched != nil {
		tt.verifyPatched(t, fx, id)
	}
}

func ccDeleteAndVerify(t *testing.T, cc *cloudcontrol.Client, fx *sfnFixture, tt ccDelegationCase, id string) {
	t.Helper()

	_, err := cc.DeleteResource(t.Context(), &cloudcontrol.DeleteResourceInput{
		TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
	})
	require.NoError(t, err)

	tt.verify(t, fx, id, false)

	_, err = cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
		TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
	})
	require.Error(t, err)
}

func jsonScalar(v any) string {
	b, _ := json.Marshal(v)

	return string(b)
}

func subsetJSON(t *testing.T, doc, key string) string {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(doc), &m))

	b, err := json.Marshal(map[string]any{key: m[key]})
	require.NoError(t, err)

	return string(b)
}

func TestCloudControlRejectsUnsupportedProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		typeName string
		desired  string
	}{
		{
			name: "alarm_evaluation_criteria", typeName: "AWS::CloudWatch::Alarm",
			desired: `{"AlarmName":"cc-rej-1","EvaluationCriteria":{"PromQLCriteria":{"Query":"up"}}}`,
		},
		{
			name: "alarm_evaluation_interval", typeName: "AWS::CloudWatch::Alarm",
			desired: `{"AlarmName":"cc-rej-2","ComparisonOperator":"GreaterThanThreshold","EvaluationPeriods":1,` +
				`"MetricName":"CPUUtilization","Namespace":"AWS/EC2","Period":60,"Statistic":"Average",` +
				`"Threshold":1,"EvaluationInterval":30}`,
		},
		{
			name: "alarm_window_both_members", typeName: "AWS::CloudWatch::Alarm",
			desired: `{"AlarmName":"cc-rej-3","ComparisonOperator":"GreaterThanThreshold","EvaluationPeriods":1,` +
				`"MetricName":"CPUUtilization","Namespace":"AWS/EC2","Period":60,"Statistic":"Average",` +
				`"Threshold":1,"EvaluationWindow":{"SlidingWindow":{},"WallClockWindow":{}}}`,
		},
		{
			name: "user_pool_sms_mfa_without_sms_configuration", typeName: "AWS::Cognito::UserPool",
			desired: `{"UserPoolName":"cc-rej-pool-1","EnabledMfas":["SMS_MFA"]}`,
		},
		{
			name: "user_pool_unknown_mfa", typeName: "AWS::Cognito::UserPool",
			desired: `{"UserPoolName":"cc-rej-pool-2","EnabledMfas":["CARRIER_PIGEON"]}`,
		},
		{
			name: "user_pool_email_otp_without_developer_sender", typeName: "AWS::Cognito::UserPool",
			desired: `{"UserPoolName":"cc-rej-pool-3","EnabledMfas":["EMAIL_OTP"]}`,
		},
		{
			name: "file_system_two_replication_destinations", typeName: "AWS::EFS::FileSystem",
			desired: `{"ReplicationConfiguration":{"Destinations":[{"Region":"eu-west-1"},{"Region":"us-west-2"}]}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)

			_, err := cloudcontrol.NewFromConfig(fx.cfg).CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
				TypeName: aws.String(tt.typeName), DesiredState: aws.String(tt.desired),
			})
			require.Error(t, err)
		})
	}
}

func TestCloudControlSubnetIPv6(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		desired string
	}{
		{name: "dual_stack", desired: `{"VpcId":"{{vpc}}","CidrBlock":"10.40.1.0/24","Ipv6CidrBlock":"{{v6}}",` +
			`"AvailabilityZoneId":"{{azid}}","EnableDns64":true,"AssignIpv6AddressOnCreation":true}`},
		{name: "native", desired: `{"VpcId":"{{vpc}}","Ipv6Native":true,"Ipv6CidrBlock":"{{v6}}",` +
			`"AvailabilityZoneId":"{{azid}}","AssignIpv6AddressOnCreation":true}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			c := ec2.NewFromConfig(fx.cfg)

			vpc, err := c.CreateVpc(t.Context(), &ec2.CreateVpcInput{CidrBlock: aws.String("10.40.0.0/16")})
			require.NoError(t, err)

			vpcID := aws.ToString(vpc.Vpc.VpcId)
			assoc, err := c.AssociateVpcCidrBlock(t.Context(), &ec2.AssociateVpcCidrBlockInput{
				VpcId: aws.String(vpcID), AmazonProvidedIpv6CidrBlock: aws.Bool(true),
			})
			require.NoError(t, err)

			pfx := netip.MustParsePrefix(aws.ToString(assoc.Ipv6CidrBlockAssociation.Ipv6CidrBlock))
			raw := pfx.Addr().As16()
			raw[7]++
			v6 := netip.PrefixFrom(netip.AddrFrom16(raw), 64).String()

			azs, err := c.DescribeAvailabilityZones(t.Context(), &ec2.DescribeAvailabilityZonesInput{})
			require.NoError(t, err)
			require.NotEmpty(t, azs.AvailabilityZones)

			azID := aws.ToString(azs.AvailabilityZones[0].ZoneId)
			desired := strings.NewReplacer("{{vpc}}", vpcID, "{{v6}}", v6, "{{azid}}", azID).Replace(tt.desired)

			cc := cloudcontrol.NewFromConfig(fx.cfg)
			created, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
				TypeName: aws.String("AWS::EC2::Subnet"), DesiredState: aws.String(desired),
			})
			require.NoError(t, err)

			id := aws.ToString(created.ProgressEvent.Identifier)

			got, err := cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
				TypeName: aws.String("AWS::EC2::Subnet"), Identifier: aws.String(id),
			})
			require.NoError(t, err)

			var props map[string]any
			require.NoError(t, json.Unmarshal([]byte(aws.ToString(got.ResourceDescription.Properties)), &props))
			assert.Equal(t, []any{v6}, props["Ipv6CidrBlocks"])
			assert.Equal(t, azID, props["AvailabilityZoneId"])
			assert.Equal(t, true, props["AssignIpv6AddressOnCreation"])

			desc, err := c.DescribeSubnets(t.Context(), &ec2.DescribeSubnetsInput{SubnetIds: []string{id}})
			require.NoError(t, err)
			require.Len(t, desc.Subnets, 1)
			assert.True(t, aws.ToBool(desc.Subnets[0].AssignIpv6AddressOnCreation))
			assert.Equal(t, strings.Contains(tt.name, "native"), aws.ToBool(desc.Subnets[0].Ipv6Native))

			_, err = cc.UpdateResource(t.Context(), &cloudcontrol.UpdateResourceInput{
				TypeName: aws.String("AWS::EC2::Subnet"), Identifier: aws.String(id),
				PatchDocument: aws.String(`[{"op":"add","path":"/EnableDns64","value":true}]`),
			})
			require.NoError(t, err)

			desc, err = c.DescribeSubnets(t.Context(), &ec2.DescribeSubnetsInput{SubnetIds: []string{id}})
			require.NoError(t, err)
			assert.True(t, aws.ToBool(desc.Subnets[0].EnableDns64))
		})
	}
}
