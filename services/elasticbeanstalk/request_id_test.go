package elasticbeanstalk_test

import (
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func TestEventRequestID(t *testing.T) {
	t.Parallel()

	h := elasticbeanstalk.NewHandler(elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1"))

	var (
		mu  sync.Mutex
		ids []string
	)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(httputils.RequestIDMiddleware())
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			meta := awsmeta.FromRequest(c.Request(), "us-east-1")
			meta.RequestID = c.Response().Header().Get("X-Amz-Request-Id")

			mu.Lock()
			ids = append(ids, meta.RequestID)
			mu.Unlock()
			c.SetRequest(c.Request().WithContext(awsmeta.Set(c.Request().Context(), meta)))

			return next(c)
		}
	})
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := ebsdk.NewFromConfig(cfg, func(o *ebsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateEnvironment(t.Context(), &ebsdk.CreateEnvironmentInput{
		ApplicationName:   aws.String("rid-app"),
		EnvironmentName:   aws.String("rid-env"),
		SolutionStackName: aws.String(testSolutionStack),
	})
	require.NoError(t, err)

	mu.Lock()
	require.Len(t, ids, 1)
	wantID := ids[0]
	mu.Unlock()
	require.NotEmpty(t, wantID)

	events, err := client.DescribeEvents(t.Context(), &ebsdk.DescribeEventsInput{
		EnvironmentName: aws.String("rid-env"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, events.Events)
	assert.Equal(t, wantID, aws.ToString(events.Events[0].RequestId))
}
