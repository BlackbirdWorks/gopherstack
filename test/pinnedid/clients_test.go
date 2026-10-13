package pinnedid_test

import (
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cognitosdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	smsdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/teststack"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
)

type clients struct {
	apigw   *apigwsdk.Client
	apigwv2 *apigwv2sdk.Client
	cf      *cfsdk.Client
	cognito *cognitosdk.Client
	ec2     *ec2sdk.Client
	ecs     *ecssdk.Client
	efs     *efssdk.Client
	elbv2   *elbv2sdk.Client
	kms     *kmssdk.Client
	rds     *rdssdk.Client
	sm      *smsdk.Client
	seq     atomic.Int64
}

func newClients(t *testing.T) *clients {
	t.Helper()

	stack := teststack.New(t)
	srv := httptest.NewServer(stack.Echo)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithHTTPClient(sdktest.PlainBodyHTTPClient()),
	)
	require.NoError(t, err)

	ep := aws.String(srv.URL)

	return &clients{
		apigw:   apigwsdk.NewFromConfig(cfg, func(o *apigwsdk.Options) { o.BaseEndpoint = ep }),
		apigwv2: apigwv2sdk.NewFromConfig(cfg, func(o *apigwv2sdk.Options) { o.BaseEndpoint = ep }),
		cf:      cfsdk.NewFromConfig(cfg, func(o *cfsdk.Options) { o.BaseEndpoint = ep }),
		cognito: cognitosdk.NewFromConfig(cfg, func(o *cognitosdk.Options) { o.BaseEndpoint = ep }),
		ec2:     ec2sdk.NewFromConfig(cfg, func(o *ec2sdk.Options) { o.BaseEndpoint = ep }),
		ecs:     ecssdk.NewFromConfig(cfg, func(o *ecssdk.Options) { o.BaseEndpoint = ep }),
		efs:     efssdk.NewFromConfig(cfg, func(o *efssdk.Options) { o.BaseEndpoint = ep }),
		elbv2:   elbv2sdk.NewFromConfig(cfg, func(o *elbv2sdk.Options) { o.BaseEndpoint = ep }),
		kms:     kmssdk.NewFromConfig(cfg, func(o *kmssdk.Options) { o.BaseEndpoint = ep }),
		rds:     rdssdk.NewFromConfig(cfg, func(o *rdssdk.Options) { o.BaseEndpoint = ep }),
		sm:      smsdk.NewFromConfig(cfg, func(o *smsdk.Options) { o.BaseEndpoint = ep }),
	}
}
