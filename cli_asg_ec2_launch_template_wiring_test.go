package main

import (
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	astypes "github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// newASGEC2LaunchTemplateWiringClients drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) and returns real
// aws-sdk-go-v2 EC2 and Auto Scaling clients bound to it through one
// httptest server, plus the EC2 backend for direct DescribeInstances
// assertions. Proves gopherstack-zesl end to end: an Auto Scaling group
// backed by a LaunchTemplate or MixedInstancesPolicy launches instances
// resolved from the real EC2 launch template state (wireAutoScalingEC2's
// ec2AutoScalingLauncherAdapter.ResolveLaunchTemplate calling EC2's exported
// GetLaunchTemplate), not fabricated defaults.
func newASGEC2LaunchTemplateWiringClients(t *testing.T) (*ec2sdk.Client, *assdk.Client, *ec2backend.Handler) {
	t.Helper()

	cli := &CLI{AccountID: "000000000000"}
	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	require.True(t, ok, "EC2 handler must be registered")

	asgH, ok := byName["Autoscaling"].(*autoscalingbackend.Handler)
	require.True(t, ok, "Autoscaling handler must be registered")

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(ec2H))
	require.NoError(t, registry.Register(asgH))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	ec2Client := ec2sdk.NewFromConfig(cfg, func(o *ec2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	asgClient := assdk.NewFromConfig(cfg, func(o *assdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	return ec2Client, asgClient, ec2H
}

// TestWireAutoScalingEC2_LaunchTemplateVersionResolution proves an ASG
// referencing a LaunchTemplate by "$Latest", "$Default" (including the
// empty-string alias), or an explicit numeric version launches instances
// carrying that specific version's AMI/instance type -- not whichever
// version was most recently mutated. Must fail pre-fix: the old
// GetLaunchTemplate(idOrName, _) discarded its version argument entirely and
// always returned the template's current (most-recently-written) ImageID/
// InstanceType regardless of which version a group asked for.
func TestWireAutoScalingEC2_LaunchTemplateVersionResolution(t *testing.T) {
	t.Parallel()

	ec2Client, asgClient, ec2H := newASGEC2LaunchTemplateWiringClients(t)
	ctx := t.Context()

	ltOut, err := ec2Client.CreateLaunchTemplate(ctx, &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("wiring-lt-versions"),
		LaunchTemplateData: &ec2types.RequestLaunchTemplateData{
			ImageId:      aws.String("ami-v1"),
			InstanceType: ec2types.InstanceTypeT3Micro,
		},
	})
	require.NoError(t, err)
	ltID := aws.ToString(ltOut.LaunchTemplate.LaunchTemplateId)

	_, err = ec2Client.CreateLaunchTemplateVersion(ctx, &ec2sdk.CreateLaunchTemplateVersionInput{
		LaunchTemplateId: aws.String(ltID),
		LaunchTemplateData: &ec2types.RequestLaunchTemplateData{
			ImageId:      aws.String("ami-v2"),
			InstanceType: ec2types.InstanceTypeT3Large,
		},
	})
	require.NoError(t, err)

	_, err = ec2Client.ModifyLaunchTemplate(ctx, &ec2sdk.ModifyLaunchTemplateInput{
		LaunchTemplateId: aws.String(ltID),
		DefaultVersion:   aws.String("2"),
	})
	require.NoError(t, err)

	tests := []struct {
		version      *string
		name         string
		groupName    string
		wantImage    string
		wantInstance string
	}{
		{
			name: "latest_resolves_v2", version: aws.String("$Latest"),
			groupName: "wiring-asg-latest", wantImage: "ami-v2", wantInstance: "t3.large",
		},
		{
			name: "default_alias_resolves_new_default_v2", version: aws.String("$Default"),
			groupName: "wiring-asg-default", wantImage: "ami-v2", wantInstance: "t3.large",
		},
		{
			name: "empty_version_means_default_v2", version: nil,
			groupName: "wiring-asg-empty", wantImage: "ami-v2", wantInstance: "t3.large",
		},
		{
			name: "explicit_numeric_v1_survives_default_change", version: aws.String("1"),
			groupName: "wiring-asg-v1", wantImage: "ami-v1", wantInstance: "t3.micro",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, createErr := asgClient.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String(tc.groupName),
				LaunchTemplate: &astypes.LaunchTemplateSpecification{
					LaunchTemplateId: aws.String(ltID),
					Version:          tc.version,
				},
				MinSize:           aws.Int32(0),
				MaxSize:           aws.Int32(5),
				DesiredCapacity:   aws.Int32(1),
				AvailabilityZones: []string{"us-east-1a"},
			})
			require.NoError(t, createErr)

			groupsOut, describeErr := asgClient.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: []string{tc.groupName},
			})
			require.NoError(t, describeErr)
			require.Len(t, groupsOut.AutoScalingGroups, 1)
			require.Len(t, groupsOut.AutoScalingGroups[0].Instances, 1)

			instanceID := aws.ToString(groupsOut.AutoScalingGroups[0].Instances[0].InstanceId)

			ec2Instances := ec2H.Backend.DescribeInstances([]string{instanceID}, "")
			require.Len(t, ec2Instances, 1,
				"the ASG-reported instance ID must be a real EC2 record, not a fabricated one")
			assert.Equal(t, tc.wantImage, ec2Instances[0].ImageID)
			assert.Equal(t, tc.wantInstance, ec2Instances[0].InstanceType)
		})
	}
}

// TestWireAutoScalingEC2_MixedInstancesPolicyRoundRobin proves a
// MixedInstancesPolicy with multiple Overrides launches instances
// round-robin across them -- overrides[i%len(overrides)] per instance,
// mirroring EC2 Fleet's own round-robin fulfillment -- instead of pinning
// every instance to the first override's InstanceType. Must fail pre-fix:
// the old extractOverrideInstanceType returned only the first override with
// a non-empty InstanceType, applied to every launched instance.
func TestWireAutoScalingEC2_MixedInstancesPolicyRoundRobin(t *testing.T) {
	t.Parallel()

	ec2Client, asgClient, ec2H := newASGEC2LaunchTemplateWiringClients(t)
	ctx := t.Context()

	ltOut, err := ec2Client.CreateLaunchTemplate(ctx, &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("wiring-lt-mip"),
		LaunchTemplateData: &ec2types.RequestLaunchTemplateData{
			ImageId:      aws.String("ami-mip-base"),
			InstanceType: ec2types.InstanceTypeT3Micro,
		},
	})
	require.NoError(t, err)
	ltID := aws.ToString(ltOut.LaunchTemplate.LaunchTemplateId)

	_, err = asgClient.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("wiring-asg-mip"),
		MixedInstancesPolicy: &astypes.MixedInstancesPolicy{
			LaunchTemplate: &astypes.LaunchTemplate{
				LaunchTemplateSpecification: &astypes.LaunchTemplateSpecification{
					LaunchTemplateId: aws.String(ltID),
				},
				Overrides: []astypes.LaunchTemplateOverrides{
					{InstanceType: aws.String("c5.large")},
					{InstanceType: aws.String("m5.xlarge")},
				},
			},
		},
		MinSize:           aws.Int32(0),
		MaxSize:           aws.Int32(10),
		DesiredCapacity:   aws.Int32(4),
		AvailabilityZones: []string{"us-east-1a"},
	})
	require.NoError(t, err)

	groupsOut, err := asgClient.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"wiring-asg-mip"},
	})
	require.NoError(t, err)
	require.Len(t, groupsOut.AutoScalingGroups, 1)
	require.Len(t, groupsOut.AutoScalingGroups[0].Instances, 4)

	instanceIDs := make([]string, 4)
	for i, inst := range groupsOut.AutoScalingGroups[0].Instances {
		instanceIDs[i] = aws.ToString(inst.InstanceId)
	}

	ec2Instances := ec2H.Backend.DescribeInstances(instanceIDs, "")
	require.Len(t, ec2Instances, 4,
		"every ASG-reported instance ID must be a real EC2 record, not a fabricated one")

	byType := map[string]int{}
	for _, inst := range ec2Instances {
		assert.Equal(t, "ami-mip-base", inst.ImageID, "every override inherits the base template's AMI")
		byType[inst.InstanceType]++
	}

	assert.Equal(t, map[string]int{"c5.large": 2, "m5.xlarge": 2}, byType,
		"4 instances round-robined across 2 overrides must split 2/2, not all land on the first override")
}
