package sagemakerruntime_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sagemakerruntimesdk "github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
	"github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
)

const mrHome = "us-east-1"

func newMRHandler() *sagemakerruntime.Handler {
	h := sagemakerruntime.NewHandler(sagemakerruntime.NewInMemoryBackend("000000000000", mrHome))
	h.EnableRegions()

	return h
}

func mrClient(t *testing.T, h *sagemakerruntime.Handler) func(region string) *sagemakerruntimesdk.Client {
	t.Helper()

	e := echo.New()
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			r := c.Request()
			c.SetRequest(r.WithContext(awsmeta.Set(r.Context(), awsmeta.FromRequest(r, mrHome))))

			return next(c)
		}
	})

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	addr := strings.TrimPrefix(srv.URL, "http://")
	httpClient := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer

			return d.DialContext(ctx, network, addr)
		},
	}}

	return func(region string) *sagemakerruntimesdk.Client {
		cfg, err := awscfg.LoadDefaultConfig(
			t.Context(),
			awscfg.WithRegion(region),
			awscfg.WithHTTPClient(httpClient),
			awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		)
		require.NoError(t, err)

		cfg.BaseEndpoint = aws.String(srv.URL)

		return sagemakerruntimesdk.NewFromConfig(cfg)
	}
}

func mrInvoke(t *testing.T, clients func(string) *sagemakerruntimesdk.Client, region, id string) {
	t.Helper()

	_, err := clients(region).InvokeEndpointAsync(t.Context(), &sagemakerruntimesdk.InvokeEndpointAsyncInput{
		EndpointName: aws.String("ep"), InputLocation: aws.String("s3://bucket/in"), InferenceId: aws.String(id),
	})
	require.NoError(t, err)
}

func mrIDs(h *sagemakerruntime.Handler, region string) []string {
	invs := h.BackendFor(region).ListAsyncInvocations()
	ids := make([]string, 0, len(invs))

	for _, inv := range invs {
		ids = append(ids, inv.InferenceID)
	}

	return ids
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{"us-east-1", "eu-west-1"}},
		{name: "three-regions", regions: []string{"us-east-1", "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newMRHandler()
			clients := mrClient(t, h)

			for _, r := range tc.regions {
				mrInvoke(t, clients, r, "shared")
				mrInvoke(t, clients, r, "only-"+r)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, mrIDs(h, r))
			}
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remote bool
	}{
		{name: "home-only-snapshot-has-no-regions-key"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newMRHandler()
			clients := mrClient(t, src)
			mrInvoke(t, clients, mrHome, "home")

			if tc.remote {
				mrInvoke(t, clients, "eu-west-1", "eu")
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)

			var doc map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(snap, &doc))

			if _, ok := doc["regions"]; ok != tc.remote {
				t.Fatalf("regions key present = %v, want %v", ok, tc.remote)
			}

			dst := newMRHandler()
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, mrIDs(dst, mrHome))

			if tc.remote {
				assert.Equal(t, []string{"eu"}, mrIDs(dst, "eu-west-1"))
			} else {
				assert.Empty(t, mrIDs(dst, "eu-west-1"))
			}

			legacy := sagemakerruntime.NewHandler(sagemakerruntime.NewInMemoryBackend("000000000000", mrHome))
			require.NoError(t, legacy.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, mrIDs(legacy, mrHome))
		})
	}
}

type regionEndpointLookup struct{ region string }

func (l regionEndpointLookup) DescribeEndpoint(ctx context.Context, name string) (*sagemaker.Endpoint, error) {
	if awsmeta.Region(ctx) != l.region {
		return nil, sagemaker.ErrEndpointNotFound
	}

	return &sagemaker.Endpoint{EndpointName: name, EndpointStatus: "InService"}, nil
}

func TestHandler_EndpointLookupFollowsRequestRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		region     string
		wantStatus int
	}{
		{name: "endpoint-region", region: "eu-west-1", wantStatus: http.StatusOK},
		{name: "home-region", region: mrHome, wantStatus: http.StatusBadRequest},
		{name: "other-region", region: "ap-south-1", wantStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := sagemakerruntime.NewHandler(sagemakerruntime.NewInMemoryBackend("000000000000", mrHome))
			h.Backend.SetEndpointLookup(regionEndpointLookup{region: "eu-west-1"})
			h.EnableRegions()

			req := httptest.NewRequest(http.MethodPost, "/endpoints/ep/invocations", strings.NewReader("{}"))
			req = req.WithContext(
				awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: tc.region, Account: "000000000000"}),
			)

			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
			assert.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
		})
	}
}
