package kinesisvideo_test

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo/types"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesisvideo"
)

const testRegion = "us-east-1"

// newTestClient stands up the real aws-sdk-go-v2 kinesisvideo client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestClient(t *testing.T, h *kinesisvideo.Handler) *kinesisvideosdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return kinesisvideosdk.NewFromConfig(cfg, func(o *kinesisvideosdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func newTestHandler() *kinesisvideo.Handler {
	backend := kinesisvideo.NewInMemoryBackend()
	h := kinesisvideo.NewHandler(backend)
	h.AccountID = "123456789012"
	h.DefaultRegion = testRegion

	return h
}

const (
	waitTimeout = 5 * time.Second
	waitTick    = 20 * time.Millisecond
)

func waitStreamActive(t *testing.T, client *kinesisvideosdk.Client, arn *string) {
	t.Helper()

	require.Eventually(t, func() bool {
		out, err := client.DescribeStream(t.Context(), &kinesisvideosdk.DescribeStreamInput{StreamARN: arn})

		return err == nil && out.StreamInfo.Status == types.StatusActive
	}, waitTimeout, waitTick)
}

func waitChannelActive(t *testing.T, client *kinesisvideosdk.Client, arn *string) {
	t.Helper()

	require.Eventually(t, func() bool {
		out, err := client.DescribeSignalingChannel(t.Context(), &kinesisvideosdk.DescribeSignalingChannelInput{
			ChannelARN: arn,
		})

		return err == nil && out.ChannelInfo.ChannelStatus == types.StatusActive
	}, waitTimeout, waitTick)
}

func requireNotFoundEventually(t *testing.T, call func() error) {
	t.Helper()

	require.Eventually(t, func() bool {
		var apiErr smithy.APIError

		return errors.As(call(), &apiErr) && apiErr.ErrorCode() == "ResourceNotFoundException"
	}, waitTimeout, waitTick)
}
