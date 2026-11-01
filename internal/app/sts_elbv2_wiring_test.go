package app

import (
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

func wiredServices(t *testing.T) map[string]service.Registerable {
	t.Helper()

	cli := &CLI{AccountID: "000000000000"}
	cli.faultStore = chaos.NewFaultStore()
	appCtx := &service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()}

	services, err := initializeServicesT(t, appCtx)
	require.NoError(t, err)

	return serviceByName(services)
}

func wiredSDKConfig(t *testing.T, key, secret string) aws.Config {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
	)
	require.NoError(t, err)

	return cfg
}

func TestInitializeServices_STSCallerIdentityUserID(t *testing.T) {
	t.Parallel()

	byName := wiredServices(t)
	iamH, ok := byName["IAM"].(*iambackend.Handler)
	require.True(t, ok)
	stsH, ok := byName["STS"].(*stsbackend.Handler)
	require.True(t, ok)

	user, err := iamH.Backend.CreateUser("wired-user", "/", "")
	require.NoError(t, err)
	key, err := iamH.Backend.CreateAccessKey("wired-user")
	require.NoError(t, err)
	require.NotEmpty(t, user.UserID)

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(stsH))
	e := echo.New()
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	client := stssdk.NewFromConfig(wiredSDKConfig(t, key.AccessKeyID, key.SecretAccessKey), func(o *stssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	out, err := client.GetCallerIdentity(t.Context(), &stssdk.GetCallerIdentityInput{})
	require.NoError(t, err)
	assert.Equal(t, user.UserID, aws.ToString(out.UserId))
	assert.Contains(t, aws.ToString(out.Arn), "user/wired-user")
}

func TestInitializeServices_ELBv2SubnetPlacement(t *testing.T) {
	t.Parallel()

	byName := wiredServices(t)
	elbv2H, ok := byName["ELBv2"].(*elbv2backend.Handler)
	require.True(t, ok)
	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	require.True(t, ok)

	vpc, err := ec2H.Backend.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)
	subnet, err := ec2H.Backend.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1c")
	require.NoError(t, err)

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(elbv2H))
	e := echo.New()
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	client := elbv2sdk.NewFromConfig(wiredSDKConfig(t, "test", "test"), func(o *elbv2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	out, err := client.CreateLoadBalancer(t.Context(), &elbv2sdk.CreateLoadBalancerInput{
		Name:    aws.String("placed-lb"),
		Type:    elbv2types.LoadBalancerTypeEnumApplication,
		Subnets: []string{subnet.ID},
	})
	require.NoError(t, err)
	require.Len(t, out.LoadBalancers, 1)

	lb := out.LoadBalancers[0]
	assert.Equal(t, vpc.ID, aws.ToString(lb.VpcId))
	require.Len(t, lb.AvailabilityZones, 1)
	assert.Equal(t, "us-east-1c", aws.ToString(lb.AvailabilityZones[0].ZoneName))
	assert.Equal(t, subnet.ID, aws.ToString(lb.AvailabilityZones[0].SubnetId))
}
