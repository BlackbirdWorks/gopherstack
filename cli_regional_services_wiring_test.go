package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	asgbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

const usWest2 = "us-west-2"

func regionFormCall(t *testing.T, h echo.HandlerFunc, region string, form url.Values) {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: crossAcct}))

	rec := httptest.NewRecorder()
	require.NoError(t, h(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestInitializeServices_AutoScalingLaunchesInGroupRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	asgH, ok := byName["Autoscaling"].(*asgbackend.Handler)
	require.True(t, ok)

	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	require.True(t, ok)

	tests := []struct {
		name   string
		region string
		other  []string
	}{
		{name: "us-west-2", region: usWest2, other: []string{crossHome, euRegion}},
		{name: "eu-west-1", region: euRegion, other: []string{crossHome, usWest2}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			asgBk, isASG := asgH.BackendFor(tc.region).(*asgbackend.InMemoryBackend)
			require.True(t, isASG)

			_, err := asgBk.CreateLaunchConfiguration(asgbackend.CreateLaunchConfigurationInput{
				LaunchConfigurationName: "lc-" + tc.region, ImageID: "ami-0123456789abcdef0", InstanceType: "t3.micro",
			})
			require.NoError(t, err)

			group, err := asgBk.CreateAutoScalingGroup(asgbackend.CreateAutoScalingGroupInput{
				AutoScalingGroupName: "asg-" + tc.region, LaunchConfigurationName: "lc-" + tc.region,
				MaxSize: 3, DesiredCapacity: 2,
			})
			require.NoError(t, err)
			require.Len(t, group.Instances, 2)
			assert.Contains(t, group.AutoScalingGroupARN, ":"+tc.region+":")

			ids := []string{group.Instances[0].InstanceID, group.Instances[1].InstanceID}
			assert.Len(t, ec2H.BackendFor(tc.region).DescribeInstances(ids, ""), 2)

			for _, r := range tc.other {
				assert.Empty(t, ec2H.BackendFor(r).DescribeInstances(ids, ""), r)
			}

			require.NoError(t, asgBk.SetDesiredCapacity("asg-"+tc.region, 0, false))

			for _, inst := range ec2H.BackendFor(tc.region).DescribeInstances(ids, "") {
				assert.Contains(t, []string{"shutting-down", "terminated"}, inst.State.Name)
			}
		})
	}
}

const cfnRegionTemplate = `{"Resources":{
"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cfn-q"}},
"V":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.77.0.0/16"}},
"C":{"Type":"AWS::ECS::Cluster","Properties":{"ClusterName":"cfn-c"}}}}`

func TestInitializeServices_CloudFormationProvisionsInStackRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	cfnH, ok := byName["CloudFormation"].(*cfnbackend.Handler)
	require.True(t, ok)

	sqsH, ok := byName["SQS"].(*sqsbackend.Handler)
	require.True(t, ok)

	sqsBk, ok := sqsH.Backend.(*sqsbackend.InMemoryBackend)
	require.True(t, ok)

	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	require.True(t, ok)

	ecsH, ok := byName["ECS"].(*ecsbackend.Handler)
	require.True(t, ok)

	regionFormCall(t, cfnH.Handler(), euRegion, url.Values{
		"Action": {"CreateStack"}, "Version": {"2010-05-15"}, "StackName": {"eu-stack"},
		"TemplateBody": {cfnRegionTemplate},
	})

	hasVPC := func(region string) bool {
		for _, v := range ec2H.BackendFor(region).DescribeVpcs(nil) {
			if v.CIDRBlock == "10.77.0.0/16" {
				return true
			}
		}

		return false
	}

	clusters := func(region string) []string {
		out, err := ecsH.BackendFor(region).ListClusters()
		require.NoError(t, err)

		names := make([]string, 0, len(out))
		for _, c := range out {
			names = append(names, c.ClusterName)
		}

		return names
	}

	queues := func(region string) []string {
		out, err := sqsBk.ListQueues(&sqsbackend.ListQueuesInput{Region: region})
		require.NoError(t, err)

		return out.QueueURLs
	}

	tests := []struct {
		name   string
		region string
		want   bool
	}{
		{name: "eu-west-1", region: euRegion, want: true},
		{name: "us-east-1", region: crossHome},
		{name: "us-west-2", region: usWest2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, hasVPC(tc.region), "vpc")
			assert.Equal(t, tc.want, assert.ObjectsAreEqual([]string{"cfn-c"}, clusters(tc.region)), "ecs")
			assert.Equal(t, tc.want, len(queues(tc.region)) == 1, "sqs")
		})
	}
}

func TestInitializeServices_ELBv2TargetRegistrationInGroupRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	elbH, ok := byName["ELBv2"].(*elbv2backend.Handler)
	require.True(t, ok)

	asgH, ok := byName["Autoscaling"].(*asgbackend.Handler)
	require.True(t, ok)

	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	require.True(t, ok)

	tests := []struct {
		name   string
		region string
		other  string
	}{
		{name: "eu-west-1", region: euRegion, other: "ap-south-1"},
		{name: "us-west-2", region: usWest2, other: "ap-south-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			elbBk, isELB := elbH.BackendFor(tc.region).(*elbv2backend.InMemoryBackend)
			require.True(t, isELB)

			tg, err := elbBk.CreateTargetGroup(elbv2backend.CreateTargetGroupInput{
				Name: "tg-" + tc.region, Protocol: "HTTP", Port: 80, VpcID: "vpc-1",
			})
			require.NoError(t, err)
			assert.Contains(t, tg.TargetGroupArn, ":"+tc.region+":")

			asgBk, isASG := asgH.BackendFor(tc.region).(*asgbackend.InMemoryBackend)
			require.True(t, isASG)

			_, err = asgBk.CreateLaunchConfiguration(asgbackend.CreateLaunchConfigurationInput{
				LaunchConfigurationName: "lc", ImageID: "ami-0123456789abcdef0", InstanceType: "t3.micro",
			})
			require.NoError(t, err)

			group, err := asgBk.CreateAutoScalingGroup(asgbackend.CreateAutoScalingGroupInput{
				AutoScalingGroupName: "asg", LaunchConfigurationName: "lc", MaxSize: 2, DesiredCapacity: 1,
				TargetGroupARNs: []string{tg.TargetGroupArn},
			})
			require.NoError(t, err)
			require.Len(t, group.Instances, 1)

			id := group.Instances[0].InstanceID
			assert.Len(t, ec2H.BackendFor(tc.region).DescribeInstances([]string{id}, ""), 1)

			health, err := elbBk.DescribeTargetHealth(tg.TargetGroupArn)
			require.NoError(t, err)
			require.Len(t, health, 1)
			assert.Equal(t, id, health[0].Target.ID)

			otherBk, isOther := elbH.BackendFor(tc.other).(*elbv2backend.InMemoryBackend)
			require.True(t, isOther)

			groups, err := otherBk.DescribeTargetGroups(nil, nil, "")
			require.NoError(t, err)
			assert.Empty(t, groups)
		})
	}
}

func TestInitializeServices_AlarmScalingPolicyRunsInPolicyRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	asgH, ok := byName["Autoscaling"].(*asgbackend.Handler)
	require.True(t, ok)

	home, ok := asgH.Backend.(*asgbackend.InMemoryBackend)
	require.True(t, ok)

	adapter := &cwAutoScalingAdapter{handler: asgH, backend: home}

	tests := []struct {
		name   string
		region string
	}{
		{name: "eu-west-1", region: euRegion},
		{name: "us-west-2", region: usWest2},
	}

	groupBackend := func(region string) *asgbackend.InMemoryBackend {
		bk, isASG := asgH.BackendFor(region).(*asgbackend.InMemoryBackend)
		require.True(t, isASG)

		return bk
	}

	desired := func(region string) int32 {
		groups, err := groupBackend(region).DescribeAutoScalingGroups([]string{"alarm-asg"}, nil)
		require.NoError(t, err)
		require.Len(t, groups, 1)

		return groups[0].DesiredCapacity
	}

	for _, tc := range tests {
		bk := groupBackend(tc.region)

		_, err := bk.CreateLaunchConfiguration(asgbackend.CreateLaunchConfigurationInput{
			LaunchConfigurationName: "lc", ImageID: "ami-0123456789abcdef0", InstanceType: "t3.micro",
		})
		require.NoError(t, err)

		_, err = bk.CreateAutoScalingGroup(asgbackend.CreateAutoScalingGroupInput{
			AutoScalingGroupName: "alarm-asg", LaunchConfigurationName: "lc", MaxSize: 5, DesiredCapacity: 1,
		})
		require.NoError(t, err)

		_, err = bk.PutScalingPolicy(asgbackend.ScalingPolicyInput{
			AutoScalingGroupName: "alarm-asg", PolicyName: "up", PolicyType: "SimpleScaling",
			AdjustmentType: "ChangeInCapacity", ScalingAdjustment: 2,
		})
		require.NoError(t, err)
	}

	require.NoError(t, adapter.ExecuteScalingPolicyInRegion(euRegion, "alarm-asg", "up"))

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, map[string]int32{euRegion: 3, usWest2: 1}[tc.region], desired(tc.region))
		})
	}
}
