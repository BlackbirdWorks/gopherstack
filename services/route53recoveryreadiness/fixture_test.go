package route53recoveryreadiness_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	r53 "github.com/blackbirdworks/gopherstack/services/route53"
	rr "github.com/blackbirdworks/gopherstack/services/route53recoveryreadiness"
	sqsbk "github.com/blackbirdworks/gopherstack/services/sqs"
)

const (
	testAccount = "123456789012"
	testRegion  = "us-east-1"
)

// siblings is a stand-in for *CLI exposing only the sibling services a test wires.
type siblings struct {
	sqs service.Registerable
	r53 service.Registerable
}

func (s *siblings) GetCloudWatchHandler() service.Registerable { return nil }
func (s *siblings) GetDynamoDBHandler() service.Registerable   { return nil }
func (s *siblings) GetEC2Handler() service.Registerable        { return nil }
func (s *siblings) GetELBv2Handler() service.Registerable      { return nil }
func (s *siblings) GetLambdaHandler() service.Registerable     { return nil }
func (s *siblings) GetRoute53Handler() service.Registerable    { return s.r53 }
func (s *siblings) GetSNSHandler() service.Registerable        { return nil }
func (s *siblings) GetSQSHandler() service.Registerable        { return s.sqs }

type fixture struct {
	backend *rr.InMemoryBackend
	handler *rr.Handler
	client  *sdk.Client
	sqs     *sqsbk.InMemoryBackend
	r53     *r53.InMemoryBackend
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	sq := sqsbk.NewInMemoryBackendWithConfig(testAccount, testRegion)
	t.Cleanup(sq.Close)
	rt := r53.NewInMemoryBackend()

	b := rr.NewInMemoryBackend(testAccount)
	b.SetAppConfig(&siblings{sqs: sqsbk.NewHandler(sq), r53: r53.NewHandler(rt)})

	h := rr.NewHandler(b)
	url := sdktest.Serve(t, h)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-west-2"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return &fixture{
		backend: b, handler: h, sqs: sq, r53: rt,
		client: sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) }),
	}
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func queueARN(name string) string {
	return "arn:aws:sqs:" + testRegion + ":" + testAccount + ":" + name
}
