package app

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
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestWireECSManagedScaling_ScalesGroupAndPlacesTasks(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000"}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t,
		&service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()},
	)
	require.NoError(t, err)

	byName := serviceByName(services)
	wireECSAutoScaling(byName)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(byName["EC2"]))
	require.NoError(t, registry.Register(byName["Autoscaling"]))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	ec2Client := ec2sdk.NewFromConfig(cfg, func(o *ec2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	asgClient := assdk.NewFromConfig(cfg, func(o *assdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	ctx := t.Context()

	lt, err := ec2Client.CreateLaunchTemplate(ctx, &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("ecs-ms-lt"),
		LaunchTemplateData: &ec2types.RequestLaunchTemplateData{
			ImageId: aws.String("ami-ecs"), InstanceType: ec2types.InstanceTypeT3Medium,
		},
	})
	require.NoError(t, err)

	_, err = asgClient.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("ecs-ms-asg"),
		LaunchTemplate: &astypes.LaunchTemplateSpecification{
			LaunchTemplateId: lt.LaunchTemplate.LaunchTemplateId,
		},
		MinSize:           aws.Int32(0),
		MaxSize:           aws.Int32(5),
		DesiredCapacity:   aws.Int32(0),
		AvailabilityZones: []string{"us-east-1a"},
	})
	require.NoError(t, err)

	ecsH, ok := byName["ECS"].(*ecsbackend.Handler)
	require.True(t, ok)

	bk, ok := ecsH.Backend.(*ecsbackend.InMemoryBackend)
	require.True(t, ok)

	_, err = bk.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "ms"})
	require.NoError(t, err)

	_, err = bk.CreateCapacityProvider(ecsbackend.CreateCapacityProviderInput{
		Name: "ms-cp",
		AutoScalingGroupProvider: &ecsbackend.AutoScalingGroupProvider{
			AutoScalingGroupArn: "ecs-ms-asg", ManagedScaling: &ecsbackend.ManagedScaling{Status: "ENABLED"},
		},
	})
	require.NoError(t, err)

	_, err = bk.PutClusterCapacityProviders("ms", []string{"ms-cp"}, nil)
	require.NoError(t, err)

	td, err := bk.RegisterTaskDefinition(ecsbackend.RegisterTaskDefinitionInput{
		Family: "ms-td",
		ContainerDefinitions: []ecsbackend.ContainerDefinition{
			{Name: "app", Image: "nginx", CPU: 1024, Memory: 1024},
		},
	})
	require.NoError(t, err)

	tasks, failures, err := bk.RunTask(ecsbackend.RunTaskInput{
		Cluster: "ms", TaskDefinition: td.TaskDefinitionArn, Count: 4,
		CapacityProviderStrategy: []ecsbackend.CapacityProviderStrategyItem{{CapacityProvider: "ms-cp", Weight: 1}},
	})
	require.NoError(t, err)
	require.Empty(t, failures)
	require.Len(t, tasks, 4)

	bk.ReconcileCapacity(ctx)

	out, err := asgClient.DescribeAutoScalingGroups(ctx, &assdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{"ecs-ms-asg"},
	})
	require.NoError(t, err)
	require.Len(t, out.AutoScalingGroups, 1)
	assert.Equal(t, int32(2), aws.ToInt32(out.AutoScalingGroups[0].DesiredCapacity))

	described, _, err := bk.DescribeTasks("ms", nil)
	require.NoError(t, err)

	for _, task := range described {
		assert.Equal(t, "RUNNING", task.LastStatus)
		assert.NotEmpty(t, task.ContainerInstanceArn)
	}
}
