package apigatewaymanagementapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	mgmtbackend "github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

const (
	mrHome    = "us-east-1"
	mrAccount = "000000000000"
)

var mrScope = regexp.MustCompile(`Credential=[^/]+/[^/]+/([^/]+)/`)

func newRegionHandler(t *testing.T) *mgmtbackend.Handler {
	t.Helper()

	h := mgmtbackend.NewHandler(mgmtbackend.NewInMemoryBackend())
	h.EnableRegions(mrHome)
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	return h
}

func regionClient(t *testing.T, h *mgmtbackend.Handler, region string) *apigatewaymanagementapi.Client {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqRegion := mrHome
		if m := mrScope.FindStringSubmatch(r.Header.Get("Authorization")); m != nil {
			reqRegion = m[1]
		}

		r = r.WithContext(awsmeta.Set(r.Context(), &awsmeta.Metadata{Region: reqRegion, Account: mrAccount}))
		_ = h.Handler()(echo.New().NewContext(r, w))
	}))
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return apigatewaymanagementapi.NewFromConfig(cfg, func(o *apigatewaymanagementapi.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func hasConnection(t *testing.T, h *mgmtbackend.Handler, region, id string) bool {
	t.Helper()

	_, err := regionClient(t, h, region).GetConnection(t.Context(), &apigatewaymanagementapi.GetConnectionInput{
		ConnectionId: aws.String(id),
	})

	return err == nil
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{mrHome, "eu-west-1"}},
		{name: "three-regions", regions: []string{mrHome, "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)

			for _, r := range tc.regions {
				_, err := h.BackendFor(r).CreateConnection("conn-"+r, "127.0.0.1", "ua", nil)
				require.NoError(t, err)
			}

			for _, r := range tc.regions {
				for _, other := range tc.regions {
					assert.Equal(t, r == other, hasConnection(t, h, r, "conn-"+other), "%s sees conn-%s", r, other)
				}
			}

			assert.Len(t, h.RegionBackends(), len(tc.regions))
		})
	}
}

func TestHandler_MultiRegionPostToConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		region  string
		wantErr bool
	}{
		{name: "owning-region", region: "eu-west-1"},
		{name: "other-region", region: mrHome, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)
			downstream := make(chan []byte, 1)

			_, err := h.BackendFor("eu-west-1").CreateConnection("conn-1", "127.0.0.1", "ua", downstream)
			require.NoError(t, err)

			_, err = regionClient(
				t,
				h,
				tc.region,
			).PostToConnection(t.Context(), &apigatewaymanagementapi.PostToConnectionInput{
				ConnectionId: aws.String("conn-1"), Data: []byte("hello"),
			})

			if tc.wantErr {
				require.Error(t, err)
				assert.Empty(t, downstream)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), <-downstream)
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remote bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler(t)
			_, err := src.BackendFor(mrHome).CreateConnection("conn-home", "127.0.0.1", "ua", nil)
			require.NoError(t, err)

			if tc.remote {
				_, err = src.BackendFor("eu-west-1").CreateConnection("conn-eu", "127.0.0.1", "ua", nil)
				require.NoError(t, err)
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)
			assert.Equal(t, tc.remote, regexp.MustCompile(`"regions"`).Match(snap))

			dst := newRegionHandler(t)
			_, err = dst.BackendFor("ap-south-1").CreateConnection("conn-stale", "127.0.0.1", "ua", nil)
			require.NoError(t, err)
			require.NoError(t, dst.Restore(context.Background(), snap))

			assert.True(t, hasConnection(t, dst, mrHome, "conn-home"))
			assert.False(t, hasConnection(t, dst, "ap-south-1", "conn-stale"))
			assert.Equal(t, tc.remote, hasConnection(t, dst, "eu-west-1", "conn-eu"))
		})
	}
}
