package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

const edgeDistributionConfig = `<DistributionConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
	`<CallerReference>%s</CallerReference><Comment>edge</Comment><Enabled>true</Enabled>` +
	`<Origins><Quantity>1</Quantity><Items><Origin><Id>o</Id><DomainName>127.0.0.1</DomainName>` +
	`<CustomOriginConfig><HTTPPort>9</HTTPPort><HTTPSPort>443</HTTPSPort>` +
	`<OriginProtocolPolicy>http-only</OriginProtocolPolicy></CustomOriginConfig></Origin></Items></Origins>` +
	`<DefaultCacheBehavior><TargetOriginId>o</TargetOriginId><ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy>` +
	`<CachePolicyId>4135ea2d-6df8-44a3-9df3-4b5a84be39ad</CachePolicyId>` +
	`<LambdaFunctionAssociations><Quantity>1</Quantity><Items><LambdaFunctionAssociation>` +
	`<LambdaFunctionARN>%s</LambdaFunctionARN><EventType>viewer-request</EventType>` +
	`</LambdaFunctionAssociation></Items></LambdaFunctionAssociations></DefaultCacheBehavior></DistributionConfig>`

const edgeARNPrefix = "arn:aws:lambda:us-east-1:123456789012:function:"

func newEdgeWiring(t *testing.T, rt container.Runtime) (*cloudfrontbackend.Handler, *lambdabackend.InMemoryBackend) {
	t.Helper()

	cfBackend := cloudfrontbackend.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	t.Cleanup(cfBackend.Close)

	cfHandler := cloudfrontbackend.NewHandler(cfBackend)
	bk := lambdabackend.NewInMemoryBackend(rt, portalloctest.New(t, 10), lambdabackend.DefaultSettings(),
		"123456789012", "us-east-1")
	t.Cleanup(func() { bk.Close(context.Background()) })

	wireCloudFrontDataPlane(map[string]service.Registerable{
		"CloudFront": cfHandler, "Lambda": lambdabackend.NewHandler(bk),
	})

	return cfHandler, bk
}

func createEdgeFunction(t *testing.T, bk *lambdabackend.InMemoryBackend, name, code string) {
	t.Helper()

	require.NoError(t, bk.CreateFunction(&lambdabackend.FunctionConfiguration{
		FunctionName: name, PackageType: lambdabackend.PackageTypeZip, Runtime: "python3.12",
		Handler: "index.handler", ZipData: zipOf(t, "index.py", code), Timeout: 30,
	}))

	_, err := bk.PublishVersion(name, "")
	require.NoError(t, err)
}

func TestWireCloudFrontLambdaEdge_Associations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		wantCode int
	}{
		{name: "published_version", arn: edgeARNPrefix + "edge-fn:1", wantCode: http.StatusCreated},
		{name: "unpublished_version", arn: edgeARNPrefix + "edge-fn:2", wantCode: http.StatusBadRequest},
		{name: "unknown_function", arn: edgeARNPrefix + "missing:1", wantCode: http.StatusBadRequest},
		{name: "latest", arn: edgeARNPrefix + "edge-fn:$LATEST", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfHandler, bk := newEdgeWiring(t, nil)
			createEdgeFunction(t, bk, "edge-fn", "def handler(e, c):\n    return {}\n")

			body := fmt.Sprintf(edgeDistributionConfig, "ref-"+tt.name, tt.arn)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/2020-05-31/distribution",
				strings.NewReader(body))
			rec := httptest.NewRecorder()
			require.NoError(t, cfHandler.Handler()(echo.New().NewContext(req, rec)))

			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidLambdaFunctionAssociation")
			}
		})
	}
}

func TestWireCloudFrontLambdaEdge_UnrunnableFunctionIs503(t *testing.T) {
	t.Parallel()

	cfHandler, bk := newEdgeWiring(t, nil)
	createEdgeFunction(t, bk, "edge-fn", "def handler(e, c):\n    return {}\n")

	raw := fmt.Sprintf(edgeDistributionConfig, "ref-503", edgeARNPrefix+"edge-fn:1")
	dist, err := cfHandler.Backend.CreateDistribution("ref-503", "edge", true, []byte(raw))
	require.NoError(t, err)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil)
	req.Host = dist.DomainName
	rec := httptest.NewRecorder()
	require.NoError(t, cfHandler.Handler()(echo.New().NewContext(req, rec)))

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "LambdaExecutionError from cloudfront", rec.Header().Get("X-Cache"))
}

const edgeDockerHandler = `def handler(event, context):
    req = event["Records"][0]["cf"]["request"]
    return {"status": "200", "statusDescription": "OK",
            "headers": {"x-edge": [{"value": "python"}]}, "body": "hello " + req["uri"]}
`

func TestWireCloudFrontLambdaEdge_ViewerRequestGeneratedResponseDocker(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("docker test")
	}

	if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", efsDockerRuntimeImage).Run(); err != nil {
		t.Skipf("lambda runtime image %s not available locally: %v", efsDockerRuntimeImage, err)
	}

	rt, err := container.NewRuntime(container.Config{Logger: slog.Default(), PoolSize: 2, IdleTimeout: time.Minute})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	cfHandler, bk := newEdgeWiring(t, rt)
	createEdgeFunction(t, bk, "edge-docker", edgeDockerHandler)

	raw := fmt.Sprintf(edgeDistributionConfig, "ref-docker", edgeARNPrefix+"edge-docker:1")
	dist, err := cfHandler.Backend.CreateDistribution("ref-docker", "edge", true, []byte(raw))
	require.NoError(t, err)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/page", nil)
	req.Host = dist.DomainName
	rec := httptest.NewRecorder()
	require.NoError(t, cfHandler.Handler()(echo.New().NewContext(req, rec)))

	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "hello /page", rec.Body.String())
	assert.Equal(t, "python", rec.Header().Get("X-Edge"))
	assert.Equal(t, "LambdaGeneratedResponse from cloudfront", rec.Header().Get("X-Cache"))
}
