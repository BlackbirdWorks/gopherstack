package main

import (
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elbsdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
)

// TestInitializeServices_ELBEC2ACMWiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than
// invoking wireELBCrossService directly, so that deleting the wiring call
// from wireComputeAndObservabilityIntegrations -- not just breaking the
// helper function itself -- is what this test is sensitive to. This is the
// shape iot/appsync/sagemaker's undetected routing gaps had: correct code
// nothing reaches (gopherstack-2mwl). Covers both halves gopherstack-6851
// wires: EC2 (SecurityGroups/Subnets existence) and ACM (SSLCertificateId
// existence), since both are wired by the same cli.go call site.
func TestInitializeServices_ELBEC2ACMWiring(t *testing.T) {
	t.Parallel()

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

	elbH, ok := byName["ELB"].(*elbbackend.Handler)
	require.True(t, ok, "ELB handler must be registered")

	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	require.True(t, ok, "EC2 handler must be registered")

	acmH, ok := byName["ACM"].(*acmbackend.Handler)
	require.True(t, ok, "ACM handler must be registered")

	ctx := t.Context()

	vpc, err := ec2H.Backend.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := ec2H.Backend.CreateSecurityGroup("wiring-test-sg", "wiring test", vpc.ID)
	require.NoError(t, err)

	subnet, err := ec2H.Backend.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
	require.NoError(t, err)

	cert, err := acmH.Backend.RequestCertificate(ctx, "wired.example.com", "AMAZON_ISSUED", "DNS", "", "", "", "", nil)
	require.NoError(t, err)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(elbH))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := elbsdk.NewFromConfig(cfg, func(o *elbsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName: aws.String("wired-elb"),
		Subnets:          []string{subnet.ID},
		Listeners: []elbtypes.Listener{
			{
				Protocol: aws.String("HTTP"), LoadBalancerPort: 80,
				InstanceProtocol: aws.String("HTTP"), InstancePort: aws.Int32(8080),
			},
		},
	})
	require.NoError(t, err)

	_, err = client.ApplySecurityGroupsToLoadBalancer(ctx, &elbsdk.ApplySecurityGroupsToLoadBalancerInput{
		LoadBalancerName: aws.String("wired-elb"),
		SecurityGroups:   []string{"sg-does-not-exist"},
	})
	require.Error(t, err,
		"an unknown security group must be rejected through the actual cli.go composition root's EC2 wiring")

	_, err = client.ApplySecurityGroupsToLoadBalancer(ctx, &elbsdk.ApplySecurityGroupsToLoadBalancerInput{
		LoadBalancerName: aws.String("wired-elb"),
		SecurityGroups:   []string{sg.ID},
	})
	require.NoError(t, err, "a real EC2-backed security group must be accepted")

	_, err = client.AttachLoadBalancerToSubnets(ctx, &elbsdk.AttachLoadBalancerToSubnetsInput{
		LoadBalancerName: aws.String("wired-elb"),
		Subnets:          []string{"subnet-does-not-exist"},
	})
	require.Error(t, err,
		"an unknown subnet must be rejected through the actual cli.go composition root's EC2 wiring")

	_, err = client.CreateLoadBalancerListeners(ctx, &elbsdk.CreateLoadBalancerListenersInput{
		LoadBalancerName: aws.String("wired-elb"),
		Listeners: []elbtypes.Listener{
			{
				Protocol: aws.String("HTTPS"), LoadBalancerPort: 443,
				InstanceProtocol: aws.String("HTTPS"), InstancePort: aws.Int32(8443),
				SSLCertificateId: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/does-not-exist"),
			},
		},
	})
	require.Error(t, err,
		"an unknown certificate ARN must be rejected through the actual cli.go composition root's ACM wiring")

	_, err = client.CreateLoadBalancerListeners(ctx, &elbsdk.CreateLoadBalancerListenersInput{
		LoadBalancerName: aws.String("wired-elb"),
		Listeners: []elbtypes.Listener{
			{
				Protocol: aws.String("HTTPS"), LoadBalancerPort: 443,
				InstanceProtocol: aws.String("HTTPS"), InstancePort: aws.Int32(8443),
				SSLCertificateId: aws.String(cert.ARN),
			},
		},
	})
	require.NoError(t, err, "a real ACM certificate must be accepted")
}
