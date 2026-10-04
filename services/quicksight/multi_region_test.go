package quicksight_test

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
	"github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svc "github.com/blackbirdworks/gopherstack/services/quicksight"
)

const mrHome = "us-east-1"

func newMRHandler(t *testing.T) *svc.Handler {
	t.Helper()

	h := svc.NewHandler(svc.NewInMemoryBackend("000000000000", mrHome))

	h.EnableRegions()

	return h
}

func mrConfigs(t *testing.T, h *svc.Handler) func(region string) aws.Config {
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

	return func(region string) aws.Config {
		cfg, err := awscfg.LoadDefaultConfig(
			t.Context(),
			awscfg.WithRegion(region),
			awscfg.WithHTTPClient(httpClient),
			awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		)
		require.NoError(t, err)

		cfg.BaseEndpoint = aws.String(srv.URL)

		return cfg
	}
}

func mrCreate(ctx context.Context, cfg aws.Config, name string) error {
	_, err := quicksight.NewFromConfig(cfg).CreateGroup(ctx, &quicksight.CreateGroupInput{
		AwsAccountId: aws.String("000000000000"), Namespace: aws.String("default"), GroupName: aws.String(name),
	})

	return err
}

func mrList(ctx context.Context, cfg aws.Config) ([]string, error) {
	out, err := quicksight.NewFromConfig(cfg).ListGroups(ctx, &quicksight.ListGroupsInput{
		AwsAccountId: aws.String("000000000000"), Namespace: aws.String("default"),
	})
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(out.GroupList))
	for _, g := range out.GroupList {
		names = append(names, aws.ToString(g.GroupName))
	}

	return names, nil
}

func mrNames(t *testing.T, cfgs func(string) aws.Config, region string) []string {
	t.Helper()

	all, err := mrList(t.Context(), cfgs(region))
	require.NoError(t, err)

	out := make([]string, 0, len(all))

	for _, n := range all {
		if strings.HasPrefix(n, "mr") {
			out = append(out, n)
		}
	}

	return out
}

func mrName(s string) string { return "mr" + strings.ReplaceAll(s, "-", "") }

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

			cfgs := mrConfigs(t, newMRHandler(t))

			for _, r := range tc.regions {
				require.NoError(t, mrCreate(t.Context(), cfgs(r), mrName("shared")))
				require.NoError(t, mrCreate(t.Context(), cfgs(r), mrName("only"+r)))
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{mrName("shared"), mrName("only" + r)}, mrNames(t, cfgs, r))
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

			src := newMRHandler(t)
			srcCfgs := mrConfigs(t, src)
			require.NoError(t, mrCreate(t.Context(), srcCfgs(mrHome), mrName("home")))

			if tc.remote {
				require.NoError(t, mrCreate(t.Context(), srcCfgs("eu-west-1"), mrName("eu")))
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)

			var doc map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(snap, &doc))

			if _, ok := doc["regions"]; ok != tc.remote {
				t.Fatalf("regions key present = %v, want %v", ok, tc.remote)
			}

			dst := newMRHandler(t)
			require.NoError(t, dst.Restore(context.Background(), snap))

			dstCfgs := mrConfigs(t, dst)
			assert.Equal(t, []string{mrName("home")}, mrNames(t, dstCfgs, mrHome))

			if tc.remote {
				assert.Equal(t, []string{mrName("eu")}, mrNames(t, dstCfgs, "eu-west-1"))
			} else {
				assert.Empty(t, mrNames(t, dstCfgs, "eu-west-1"))
			}

			legacy := svc.NewHandler(svc.NewInMemoryBackend("000000000000", mrHome))
			require.NoError(t, legacy.Restore(context.Background(), snap))
			assert.Equal(t, []string{mrName("home")}, mrNames(t, mrConfigs(t, legacy), mrHome))
		})
	}
}

func TestHandler_MultiRegionReset(t *testing.T) {
	t.Parallel()

	h := newMRHandler(t)
	cfgs := mrConfigs(t, h)

	require.NoError(t, mrCreate(t.Context(), cfgs(mrHome), mrName("home")))
	require.NoError(t, mrCreate(t.Context(), cfgs("eu-west-1"), mrName("eu")))

	h.Reset()

	assert.Empty(t, mrNames(t, cfgs, mrHome))
	assert.Empty(t, mrNames(t, cfgs, "eu-west-1"))
}
