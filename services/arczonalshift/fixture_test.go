package arczonalshift_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/arczonalshift"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	zs "github.com/blackbirdworks/gopherstack/services/arczonalshift"
	cw "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	elbv2 "github.com/blackbirdworks/gopherstack/services/elbv2"
)

const (
	testAccount = "123456789012"
	testRegion  = "us-east-1"
)

type siblings struct {
	elb service.Registerable
	cw  service.Registerable
}

func (s *siblings) GetELBv2Handler() service.Registerable      { return s.elb }
func (s *siblings) GetCloudWatchHandler() service.Registerable { return s.cw }

type fixture struct {
	backend *zs.InMemoryBackend
	handler *zs.Handler
	client  *sdk.Client
	elb     *elbv2.InMemoryBackend
	cw      *cw.InMemoryBackend
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	elb := elbv2.NewInMemoryBackend(testAccount, testRegion)
	t.Cleanup(elb.Close)

	alarms := cw.NewInMemoryBackendWithConfig(testAccount, testRegion)

	b := zs.NewInMemoryBackend(testAccount)
	b.SetAppConfig(&siblings{elb: elbv2.NewHandler(elb), cw: cw.NewHandler(alarms)})

	h := zs.NewHandler(b)
	h.DefaultRegion = testRegion
	url := sdktest.Serve(t, h)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return &fixture{
		backend: b, handler: h, elb: elb, cw: alarms,
		client: sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) }),
	}
}

// loadBalancer creates an ALB over zones 1..n and returns its ARN.
func (f *fixture) loadBalancer(t *testing.T, name string, zones int) string {
	t.Helper()

	subnets := make([]string, zones)
	for i := range subnets {
		subnets[i] = "subnet-" + string(rune('a'+i))
	}

	lb, err := f.elb.CreateLoadBalancer(
		elbv2.CreateLoadBalancerInput{Name: name, Type: "application", Subnets: subnets},
	)
	require.NoError(t, err)

	return lb.LoadBalancerArn
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
