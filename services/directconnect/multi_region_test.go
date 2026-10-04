package directconnect_test

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
	"github.com/aws/aws-sdk-go-v2/service/directconnect"
	directconnecttypes "github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	directconnectbackend "github.com/blackbirdworks/gopherstack/services/directconnect"
)

const (
	mrHome    = "us-east-1"
	mrAccount = "000000000000"
)

var mrScope = regexp.MustCompile(`Credential=[^/]+/[^/]+/([^/]+)/`)

func newRegionHandler(t *testing.T) *directconnectbackend.Handler {
	t.Helper()

	h := directconnectbackend.NewHandler(directconnectbackend.NewInMemoryBackend(t.Context(), mrAccount, mrHome))
	h.EnableRegions()
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	return h
}

func regionClient(t *testing.T, h *directconnectbackend.Handler, region string) aws.Config {
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
	_, err := directconnect.NewFromConfig(cfg).CreateConnection(ctx, &directconnect.CreateConnectionInput{
		ConnectionName: aws.String(name), Bandwidth: aws.String("1Gbps"), Location: aws.String("EqDC2"),
	})

	return err
}

func mrList(ctx context.Context, cfg aws.Config) ([]string, error) {
	out, err := directconnect.NewFromConfig(cfg).DescribeConnections(ctx, &directconnect.DescribeConnectionsInput{})
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(out.Connections))
	for _, c := range out.Connections {
		names = append(names, aws.ToString(c.ConnectionName))
	}

	return names, nil
}

func createOne(t *testing.T, h *directconnectbackend.Handler, region, name string) {
	t.Helper()

	require.NoError(t, mrCreate(t.Context(), regionClient(t, h, region), name))
}

func names(t *testing.T, h *directconnectbackend.Handler, region string) []string {
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

func TestHandler_GatewaysAreAccountGlobal(t *testing.T) {
	t.Parallel()

	h := newRegionHandler(t)
	west := directconnect.NewFromConfig(regionClient(t, h, "us-west-2"))
	eu := directconnect.NewFromConfig(regionClient(t, h, "eu-west-1"))

	created, err := west.CreateDirectConnectGateway(t.Context(), &directconnect.CreateDirectConnectGatewayInput{
		DirectConnectGatewayName: aws.String("global-gw"),
	})
	require.NoError(t, err)

	id := aws.ToString(created.DirectConnectGateway.DirectConnectGatewayId)

	for _, c := range []*directconnect.Client{west, eu} {
		out, descErr := c.DescribeDirectConnectGateways(
			t.Context(),
			&directconnect.DescribeDirectConnectGatewaysInput{},
		)
		require.NoError(t, descErr)
		require.Len(t, out.DirectConnectGateways, 1)
	}

	_, err = eu.UpdateDirectConnectGateway(t.Context(), &directconnect.UpdateDirectConnectGatewayInput{
		DirectConnectGatewayId: aws.String(id), NewDirectConnectGatewayName: aws.String("renamed"),
	})
	require.NoError(t, err)

	for gw, c := range map[string]*directconnect.Client{"tgw-eu0000000000001": eu, "vgw-west00000000001": west} {
		_, err = c.CreateDirectConnectGatewayAssociation(
			t.Context(),
			&directconnect.CreateDirectConnectGatewayAssociationInput{
				DirectConnectGatewayId: aws.String(id), GatewayId: aws.String(gw),
			},
		)
		require.NoError(t, err, gw)
	}

	assocs, err := west.DescribeDirectConnectGatewayAssociations(
		t.Context(),
		&directconnect.DescribeDirectConnectGatewayAssociationsInput{DirectConnectGatewayId: aws.String(id)},
	)
	require.NoError(t, err)
	assert.Len(t, assocs.DirectConnectGatewayAssociations, 2)

	snap := h.Snapshot(context.Background())
	assert.Equal(t, 1, strings.Count(string(snap), `"global-gw"`)+strings.Count(string(snap), `"renamed"`))

	dst := newRegionHandler(t)
	require.NoError(t, dst.Restore(context.Background(), snap))

	restored, err := directconnect.NewFromConfig(regionClient(t, dst, "eu-west-1")).
		DescribeDirectConnectGateways(t.Context(), &directconnect.DescribeDirectConnectGatewaysInput{})
	require.NoError(t, err)
	require.Len(t, restored.DirectConnectGateways, 1)
	assert.Equal(t, "renamed", aws.ToString(restored.DirectConnectGateways[0].DirectConnectGatewayName))

	for _, a := range assocs.DirectConnectGatewayAssociations {
		_, err = eu.DeleteDirectConnectGatewayAssociation(
			t.Context(),
			&directconnect.DeleteDirectConnectGatewayAssociationInput{
				AssociationId: a.AssociationId,
			},
		)
		require.NoError(t, err)
	}

	_, err = eu.DeleteDirectConnectGateway(t.Context(), &directconnect.DeleteDirectConnectGatewayInput{
		DirectConnectGatewayId: aws.String(id),
	})
	require.NoError(t, err)

	left, err := west.DescribeDirectConnectGateways(t.Context(), &directconnect.DescribeDirectConnectGatewaysInput{})
	require.NoError(t, err)
	require.Len(t, left.DirectConnectGateways, 1)
	assert.Equal(
		t,
		directconnecttypes.DirectConnectGatewayStateDeleting,
		left.DirectConnectGateways[0].DirectConnectGatewayState,
	)
}
