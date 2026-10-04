package detective_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/detective"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	detectivebackend "github.com/blackbirdworks/gopherstack/services/detective"
)

const (
	mrHome    = "us-east-1"
	mrAccount = "000000000000"
)

var mrScope = regexp.MustCompile(`Credential=[^/]+/[^/]+/([^/]+)/`)

func newRegionHandler(t *testing.T) *detectivebackend.Handler {
	t.Helper()

	h := detectivebackend.NewHandler(detectivebackend.NewInMemoryBackend(mrAccount, mrHome))
	h.EnableRegions()

	return h
}

func regionClient(t *testing.T, h *detectivebackend.Handler, region string) aws.Config {
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

	cfg.BaseEndpoint = aws.String(srv.URL)

	return cfg
}

func mrCreate(ctx context.Context, cfg aws.Config, name string) error {
	client := detective.NewFromConfig(cfg)

	graph, err := client.CreateGraph(ctx, &detective.CreateGraphInput{})
	if err != nil {
		return err
	}

	_, err = client.TagResource(ctx, &detective.TagResourceInput{
		ResourceArn: graph.GraphArn, Tags: map[string]string{name: "1"},
	})

	return err
}

func mrList(ctx context.Context, cfg aws.Config) ([]string, error) {
	client := detective.NewFromConfig(cfg)

	out, err := client.ListGraphs(ctx, &detective.ListGraphsInput{})
	if err != nil {
		return nil, err
	}

	var names []string

	for _, g := range out.GraphList {
		tags, tagErr := client.ListTagsForResource(ctx, &detective.ListTagsForResourceInput{ResourceArn: g.Arn})
		if tagErr != nil {
			return nil, tagErr
		}

		for k := range tags.Tags {
			names = append(names, k)
		}
	}

	return names, nil
}

func createOne(t *testing.T, h *detectivebackend.Handler, region, name string) {
	t.Helper()

	require.NoError(t, mrCreate(t.Context(), regionClient(t, h, region), name))
}

func names(t *testing.T, h *detectivebackend.Handler, region string) []string {
	t.Helper()

	all, err := mrList(t.Context(), regionClient(t, h, region))
	require.NoError(t, err)

	out := make([]string, 0, len(all))

	for _, n := range all {
		if strings.HasPrefix(n, "mr-") {
			out = append(out, n)
		}
	}

	slices.Sort(out)

	return out
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
				createOne(t, h, r, "mr-shared")
				createOne(t, h, r, "mr-only-"+r)
			}

			for _, r := range tc.regions {
				assert.Equal(t, []string{"mr-only-" + r, "mr-shared"}, names(t, h, r))
			}

			assert.Len(t, h.RegionBackends(), len(tc.regions))
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
			createOne(t, src, mrHome, "mr-home")

			if tc.remote {
				createOne(t, src, "eu-west-1", "mr-eu")
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)
			assert.Equal(t, tc.remote, strings.Contains(string(snap), `"regions"`))

			dst := newRegionHandler(t)
			createOne(t, dst, "eu-west-1", "mr-stale")
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"mr-home"}, names(t, dst, mrHome))

			if tc.remote {
				assert.Equal(t, []string{"mr-eu"}, names(t, dst, "eu-west-1"))
			} else {
				assert.Empty(t, names(t, dst, "eu-west-1"))
			}
		})
	}
}

func TestHandler_MultiRegionReset(t *testing.T) {
	t.Parallel()

	h := newRegionHandler(t)
	createOne(t, h, mrHome, "mr-home")
	createOne(t, h, "eu-west-1", "mr-eu")
	require.Len(t, h.RegionBackends(), 2)

	h.Reset()

	assert.Len(t, h.RegionBackends(), 1)
	assert.Empty(t, names(t, h, mrHome))
	assert.Empty(t, names(t, h, "eu-west-1"))
}
