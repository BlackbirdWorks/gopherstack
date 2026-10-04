package appconfigdata_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/appconfigdata"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	appconfigdatabackend "github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

const (
	mrHome    = "us-east-1"
	mrAccount = "000000000000"
)

var mrScope = regexp.MustCompile(`Credential=[^/]+/[^/]+/([^/]+)/`)

func newRegionHandler(t *testing.T) *appconfigdatabackend.Handler {
	t.Helper()

	h := appconfigdatabackend.NewHandler(appconfigdatabackend.NewInMemoryBackend())
	h.EnableRegions(mrHome)
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	return h
}

func regionClient(t *testing.T, h *appconfigdatabackend.Handler, region string) *appconfigdata.Client {
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

	return appconfigdata.NewFromConfig(cfg, func(o *appconfigdata.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

func mrStartSession(ctx context.Context, c *appconfigdata.Client) (string, error) {
	out, err := c.StartConfigurationSession(ctx, &appconfigdata.StartConfigurationSessionInput{
		ApplicationIdentifier:          aws.String("app"),
		EnvironmentIdentifier:          aws.String("env"),
		ConfigurationProfileIdentifier: aws.String("profile"),
	})
	if err != nil {
		return "", err
	}

	return aws.ToString(out.InitialConfigurationToken), nil
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
				require.NoError(
					t,
					h.BackendFor(r).SetConfiguration("app", "env", "profile", "payload-"+r, "text/plain"),
				)
			}

			for _, r := range tc.regions {
				c := regionClient(t, h, r)

				token, err := mrStartSession(t.Context(), c)
				require.NoError(t, err)

				got, err := c.GetLatestConfiguration(t.Context(), &appconfigdata.GetLatestConfigurationInput{
					ConfigurationToken: aws.String(token),
				})
				require.NoError(t, err)
				assert.Equal(t, "payload-"+r, string(got.Configuration))
			}

			assert.Len(t, h.RegionBackends(), len(tc.regions))
		})
	}
}

func TestHandler_MultiRegionMissingConfiguration(t *testing.T) {
	t.Parallel()

	h := newRegionHandler(t)
	require.NoError(t, h.BackendFor(mrHome).SetConfiguration("app", "env", "profile", "home", "text/plain"))

	_, err := mrStartSession(t.Context(), regionClient(t, h, "eu-west-1"))
	require.Error(t, err)

	token, err := mrStartSession(t.Context(), regionClient(t, h, mrHome))
	require.NoError(t, err)

	_, err = regionClient(
		t,
		h,
		"eu-west-1",
	).GetLatestConfiguration(t.Context(), &appconfigdata.GetLatestConfigurationInput{
		ConfigurationToken: aws.String(token),
	})
	require.Error(t, err)
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
			require.NoError(t, src.BackendFor(mrHome).SetConfiguration("app", "env", "profile", "home", "text/plain"))

			if tc.remote {
				require.NoError(
					t,
					src.BackendFor("eu-west-1").SetConfiguration("app", "env", "profile", "eu", "text/plain"),
				)
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)
			assert.Equal(t, tc.remote, regexp.MustCompile(`"regions"`).Match(snap))

			dst := newRegionHandler(t)
			require.NoError(
				t,
				dst.BackendFor("ap-south-1").SetConfiguration("app", "env", "profile", "stale", "text/plain"),
			)
			require.NoError(t, dst.Restore(context.Background(), snap))

			_, err := mrStartSession(t.Context(), regionClient(t, dst, mrHome))
			require.NoError(t, err)

			_, err = mrStartSession(t.Context(), regionClient(t, dst, "ap-south-1"))
			require.Error(t, err)

			_, err = mrStartSession(t.Context(), regionClient(t, dst, "eu-west-1"))
			assert.Equal(t, tc.remote, err == nil)
		})
	}
}

func TestHandler_SiblingJanitorStops(t *testing.T) {
	t.Parallel()

	tests := []struct {
		drop func(h *appconfigdatabackend.Handler)
		name string
	}{
		{name: "shutdown", drop: func(h *appconfigdatabackend.Handler) { h.Shutdown(context.Background()) }},
		{name: "restore", drop: func(h *appconfigdatabackend.Handler) {
			require.NoError(t, h.Restore(context.Background(), []byte(`{}`)))
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			h := appconfigdatabackend.NewHandler(appconfigdatabackend.NewInMemoryBackend())
			h.EnableRegions(mrHome)
			require.NoError(t, h.StartWorker(ctx))
			require.NotNil(t, h.BackendFor("eu-west-1"))

			tc.drop(h)
			assert.Len(t, h.RegionBackends(), 1)
		})
	}
}
