package serverlessrepo_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sarsdk "github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

// newTestSARSDKClient stands up the real aws-sdk-go-v2 serverlessapplicationrepository
// client (and returns the underlying httptest server URL) against an httptest server
// running this package's Handler, wired through the same pkgs/service registry/router
// used in production -- so a shape is verified by the real client's own deserializer
// and by the raw wire body, not gopherstack's own JSON tags.
func newTestSARSDKClient(t *testing.T, h *serverlessrepo.Handler) (*sarsdk.Client, string) {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := sarsdk.NewFromConfig(cfg, func(o *sarsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return client, srv.URL
}

// TestCreateApplication_NoTopLevelSourceCodeURL_SDKRoundTrip proves that
// CreateApplicationOutput's wire body carries no top-level "sourceCodeUrl" key. The
// real CreateApplicationOutput/GetApplicationOutput/UpdateApplicationOutput shape
// (api_op_CreateApplication.go, serverlessapplicationrepository@v1.33.4) has no
// SourceCodeUrl member at the response root at all -- SourceCodeUrl only exists
// nested inside the optional Version sub-object. CreateApplicationInput does have a
// request-only SourceCodeUrl (used to seed the initial Version), which must not leak
// onto the response root. The real aws-sdk-go-v2 client silently ignores unknown JSON
// keys, so this is checked against the raw response body, not the decoded Go struct.
func TestCreateApplication_NoTopLevelSourceCodeURL_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1")
	h := serverlessrepo.NewHandler(backend)
	client, baseURL := newTestSARSDKClient(t, h)

	out, err := client.CreateApplication(t.Context(), &sarsdk.CreateApplicationInput{
		Name:            aws.String("wire-test-app"),
		Description:     aws.String("wire test"),
		Author:          aws.String("wire-author"),
		SemanticVersion: aws.String("1.0.0"),
		SourceCodeUrl:   aws.String("https://example.com/src"),
		TemplateUrl:     aws.String("https://example.com/template.yaml"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Version, "expected a Version to be created from the seeded semanticVersion")
	require.NotNil(t, out.Version.SourceCodeUrl, "sourceCodeUrl belongs nested under Version")
	require.Equal(t, "https://example.com/src", aws.ToString(out.Version.SourceCodeUrl))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/applications/wire-test-app", nil)
	require.NoError(t, err)
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/serverlessrepo/aws4_request",
	)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	rawBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rawBody, &raw))

	_, hasTopLevelSourceCodeURL := raw["sourceCodeUrl"]
	require.False(t, hasTopLevelSourceCodeURL,
		"CreateApplication/GetApplication/UpdateApplication responses must not carry a "+
			"top-level sourceCodeUrl -- it exists only nested under version.sourceCodeUrl")

	version, ok := raw["version"].(map[string]any)
	require.True(t, ok, "expected a nested version object")
	require.Equal(t, "https://example.com/src", version["sourceCodeUrl"])
}
