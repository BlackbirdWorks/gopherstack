package main

import (
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	codedeploytypes "github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	codedeploybackend "github.com/blackbirdworks/gopherstack/services/codedeploy"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestInitializeServices_CodeDeployEC2Wiring drives the actual composition
// root (initializeServices, the function cli.go's Run() calls) rather than
// constructing services/codedeploy's InMemoryBackend directly and injecting
// a fake sibling, so that deleting provider.go's
// backend.SetAppConfig(ctx.Config) call -- not just breaking
// cross_service.go's matcher logic -- is what this test is sensitive to.
// This is the shape iot/appsync/sagemaker's undetected routing gaps had:
// correct code nothing reaches (gopherstack-2mwl). Covers gopherstack-cj62's
// Ec2TagFilters resolution: a deployment group's EC2 tag targeting must
// resolve against the real services/ec2 instance registry wired through
// cli.go, and must exclude a terminated instance even if its tags match.
func TestInitializeServices_CodeDeployEC2Wiring(t *testing.T) {
	t.Parallel()

	const (
		wiringTestTagKey   = "env"
		wiringTestTagValue = "prod"
	)

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

	cdH, ok := byName["CodeDeploy"].(*codedeploybackend.Handler)
	require.True(t, ok, "CodeDeploy handler must be registered")

	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	require.True(t, ok, "EC2 handler must be registered")

	matched, err := ec2H.Backend.RunInstances("ami-fake", "t3.micro", "", 1)
	require.NoError(t, err)
	require.NoError(
		t, ec2H.Backend.CreateTags([]string{matched[0].ID}, map[string]string{wiringTestTagKey: wiringTestTagValue}),
	)

	terminated, err := ec2H.Backend.RunInstances("ami-fake", "t3.micro", "", 1)
	require.NoError(t, err)
	require.NoError(
		t,
		ec2H.Backend.CreateTags([]string{terminated[0].ID}, map[string]string{wiringTestTagKey: wiringTestTagValue}),
	)
	_, err = ec2H.Backend.TerminateInstances([]string{terminated[0].ID})
	require.NoError(t, err)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(cdH))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	ctx := t.Context()

	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := codedeploysdk.NewFromConfig(cfg, func(o *codedeploysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.CreateApplication(ctx, &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("wired-app"),
		ComputePlatform: codedeploytypes.ComputePlatformServer,
	})
	require.NoError(t, err)

	_, err = client.CreateDeploymentGroup(ctx, &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("wired-app"),
		DeploymentGroupName: aws.String("wired-dg"),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
		Ec2TagFilters: []codedeploytypes.EC2TagFilter{
			{
				Key:   aws.String(wiringTestTagKey),
				Value: aws.String(wiringTestTagValue),
				Type:  codedeploytypes.EC2TagFilterTypeKeyAndValue,
			},
		},
	})
	require.NoError(t, err)

	createOut, err := client.CreateDeployment(ctx, &codedeploysdk.CreateDeploymentInput{
		ApplicationName:     aws.String("wired-app"),
		DeploymentGroupName: aws.String("wired-dg"),
	})
	require.NoError(t, err)

	targetsOut, err := client.ListDeploymentTargets(ctx, &codedeploysdk.ListDeploymentTargetsInput{
		DeploymentId: createOut.DeploymentId,
	})
	require.NoError(t, err)

	require.Contains(t, targetsOut.TargetIds, matched[0].ID,
		"a real EC2 instance matched by Ec2TagFilters must be resolved through the actual "+
			"cli.go composition root's EC2 wiring")
	require.NotContains(t, targetsOut.TargetIds, terminated[0].ID,
		"a terminated EC2 instance must not be targeted even though its tags match")
}
