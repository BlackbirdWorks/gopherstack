package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	regionA = "us-east-1"
	regionB = "eu-west-1"
)

// regionCase creates one named resource in the region of cfg and lists the names visible there.
type regionCase struct {
	create func(ctx context.Context, cfg aws.Config, name string) error
	list   func(ctx context.Context, cfg aws.Config) ([]string, error)
	name   string
	// global marks services whose resources are not region-scoped by AWS definition.
	global bool
	// knownCollision marks single-region services tracked by gopherstack-7v0p; the test fails once they isolate.
	knownCollision bool
	// uniqueNames marks services whose names are globally unique but whose resources still have a region.
	uniqueNames bool
}

//nolint:gochecknoglobals // one full composition root shared by every region-isolation subtest
var (
	regionSrvOnce sync.Once
	regionSrvURL  string
	errRegionSrv  error
)

func regionIsolationURL(t *testing.T) string {
	t.Helper()

	regionSrvOnce.Do(func() {
		cli := &CLI{AccountID: "000000000000", Region: regionA}
		cli.faultStore = chaos.NewFaultStore()

		services, err := initializeServices(&service.AppContext{
			Logger: slog.Default(), Config: cli, JanitorCtx: context.Background(),
		})
		if err != nil {
			errRegionSrv = err

			return
		}

		registry := service.NewRegistry()

		for _, svc := range services {
			if err = registry.Register(svc); err != nil {
				errRegionSrv = err

				return
			}
		}

		e := echo.New()
		e.Pre(awsMetaMiddleware(regionA, "000000000000"))
		e.Use(service.NewServiceRouter(registry).WithTargetGates(routeTargetGates()).RouteHandler())
		regionSrvURL = httptest.NewServer(e).URL
	})

	require.NoError(t, errRegionSrv)

	return regionSrvURL
}

func regionConfig(t *testing.T, region string) aws.Config {
	t.Helper()

	url := regionIsolationURL(t)
	addr := strings.TrimPrefix(url, "http://")
	httpClient := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer

			return d.DialContext(ctx, network, addr)
		},
	}}

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithHTTPClient(httpClient),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	cfg.BaseEndpoint = aws.String(url)

	return cfg
}

// isolationProblems creates in two regions and reports every way region B is visible from region A or vice versa.
func isolationProblems(t *testing.T, tc regionCase) []string {
	t.Helper()

	ctx := t.Context()
	cfgA, cfgB := regionConfig(t, regionA), regionConfig(t, regionB)
	nameA, nameB, shared := "rg-a-"+tc.name, "rg-b-"+tc.name, "rg-both-"+tc.name

	require.NoError(t, tc.create(ctx, cfgA, nameA))
	require.NoError(t, tc.create(ctx, cfgA, shared))
	require.NoError(t, tc.create(ctx, cfgB, nameB))

	var problems []string

	if !tc.uniqueNames {
		if err := tc.create(ctx, cfgB, shared); err != nil {
			problems = append(problems, "same name rejected in second region: "+err.Error())
		}
	}

	inA, err := tc.list(ctx, cfgA)
	require.NoError(t, err)

	inB, err := tc.list(ctx, cfgB)
	require.NoError(t, err)

	if slices.Contains(inA, nameB) {
		problems = append(problems, "region B resource visible in region A")
	}

	if slices.Contains(inB, nameA) {
		problems = append(problems, "region A resource visible in region B")
	}

	if !slices.Contains(inA, nameA) || !slices.Contains(inB, nameB) {
		problems = append(problems, "resource missing from its own region")
	}

	return problems
}

func TestRegionIsolation(t *testing.T) {
	t.Parallel()

	cases := slices.Concat(
		regionIsolationCases(), platformIsolationCases(),
		servicesAIsolationCases(), servicesBIsolationCases(),
	)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.global {
				require.NoError(t, tc.create(t.Context(), regionConfig(t, regionB), "rg-b-"+tc.name))

				return
			}

			problems := isolationProblems(t, tc)
			if tc.knownCollision {
				assert.NotEmpty(t, problems, "service is now region-isolated: drop knownCollision")

				return
			}

			assert.Empty(t, problems)
		})
	}
}
