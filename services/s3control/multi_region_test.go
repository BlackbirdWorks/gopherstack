package s3control_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	s3controltypes "github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/s3control"
)

func regionClients(t *testing.T, h *s3control.Handler, regions ...string) map[string]*s3csdk.Client {
	t.Helper()

	e := echo.New()
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			r := c.Request()
			c.SetRequest(r.WithContext(awsmeta.Set(r.Context(), awsmeta.FromRequest(r, createTagsTestRegion))))

			return next(c)
		}
	})

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	httpClient := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer

			return d.DialContext(ctx, network, srv.Listener.Addr().String())
		},
	}}

	out := make(map[string]*s3csdk.Client, len(regions))

	for _, r := range regions {
		cfg, err := awscfg.LoadDefaultConfig(
			t.Context(),
			awscfg.WithRegion(r),
			awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
			awscfg.WithHTTPClient(httpClient),
		)
		require.NoError(t, err)

		out[r] = s3csdk.NewFromConfig(cfg, func(o *s3csdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	}

	return out
}

func accessPointARNs(t *testing.T, c *s3csdk.Client) map[string]string {
	t.Helper()

	out, err := c.ListAccessPoints(t.Context(), &s3csdk.ListAccessPointsInput{
		AccountId: aws.String(createTagsTestAccountID),
	})
	require.NoError(t, err)

	got := map[string]string{}
	for _, ap := range out.AccessPointList {
		got[aws.ToString(ap.Name)] = aws.ToString(ap.AccessPointArn)
	}

	return got
}

func newRegionHandler() *s3control.Handler {
	return s3control.NewHandler(s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion))
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

			clients := regionClients(t, newRegionHandler(), tc.regions...)

			for r, c := range clients {
				for _, n := range []string{"shared", "only-" + r} {
					_, err := c.CreateAccessPoint(t.Context(), &s3csdk.CreateAccessPointInput{
						AccountId: aws.String(createTagsTestAccountID), Name: aws.String(n), Bucket: aws.String("b"),
					})
					require.NoError(t, err)
				}
			}

			for r, c := range clients {
				got := accessPointARNs(t, c)
				assert.Len(t, got, 2)
				assert.Contains(t, got, "only-"+r)
				assert.Contains(t, got["shared"], ":"+r+":")
			}
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		regions  []string
		wantSame bool
	}{
		{name: "home-only-snapshot-unchanged", regions: []string{createTagsTestRegion}, wantSame: true},
		{name: "peer-round-trips", regions: []string{createTagsTestRegion, "eu-west-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler()

			for r, c := range regionClients(t, src, tc.regions...) {
				_, err := c.CreateAccessPoint(t.Context(), &s3csdk.CreateAccessPointInput{
					AccountId: aws.String(createTagsTestAccountID),
					Name:      aws.String("ap-" + r),
					Bucket:    aws.String("b"),
				})
				require.NoError(t, err)
			}

			snap := src.Snapshot(t.Context())
			require.NotNil(t, snap)

			if tc.wantSame {
				legacy := src.Backend.Snapshot(t.Context())
				assert.Equal(t, legacy, snap, "single-region snapshot must keep the legacy bytes")
			}

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(t.Context(), snap))

			for r, c := range regionClients(t, dst, tc.regions...) {
				assert.Contains(t, accessPointARNs(t, c), "ap-"+r)
			}

			legacy := newRegionHandler()
			require.NoError(t, legacy.Backend.Restore(t.Context(), snap), "older builds must ignore the regions key")
			assert.Len(t, legacy.Backend.ListAccessPoints(createTagsTestAccountID), 1)
		})
	}
}

func TestHandler_MultiRegionAccessPointsAreAccountGlobal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createIn   string
		readRegion string
	}{
		{name: "create-west-read-east", createIn: "us-west-2", readRegion: "us-east-1"},
		{name: "create-west-read-west", createIn: "us-west-2", readRegion: "us-west-2"},
		{name: "create-east-read-west", createIn: "us-east-1", readRegion: "us-west-2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			clients := regionClients(t, newRegionHandler(), "us-east-1", "us-west-2")

			created, err := clients[tc.createIn].CreateMultiRegionAccessPoint(t.Context(),
				&s3csdk.CreateMultiRegionAccessPointInput{
					AccountId: aws.String(createTagsTestAccountID),
					Details: &s3controltypes.CreateMultiRegionAccessPointInput{
						Name:    aws.String("mrap"),
						Regions: []s3controltypes.Region{{Bucket: aws.String("b1")}},
					},
				})
			require.NoError(t, err)

			reader := clients[tc.readRegion]

			got, err := reader.GetMultiRegionAccessPoint(t.Context(), &s3csdk.GetMultiRegionAccessPointInput{
				AccountId: aws.String(createTagsTestAccountID), Name: aws.String("mrap"),
			})
			require.NoError(t, err)
			assert.Equal(t, "mrap", aws.ToString(got.AccessPoint.Name))

			list, err := reader.ListMultiRegionAccessPoints(t.Context(), &s3csdk.ListMultiRegionAccessPointsInput{
				AccountId: aws.String(createTagsTestAccountID),
			})
			require.NoError(t, err)
			assert.Len(t, list.AccessPoints, 1)

			op, err := reader.DescribeMultiRegionAccessPointOperation(t.Context(),
				&s3csdk.DescribeMultiRegionAccessPointOperationInput{
					AccountId: aws.String(createTagsTestAccountID), RequestTokenARN: created.RequestTokenARN,
				})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(created.RequestTokenARN), aws.ToString(op.AsyncOperation.RequestTokenARN))

			_, err = reader.DeleteMultiRegionAccessPoint(t.Context(), &s3csdk.DeleteMultiRegionAccessPointInput{
				AccountId: aws.String(createTagsTestAccountID),
				Details:   &s3controltypes.DeleteMultiRegionAccessPointInput{Name: aws.String("mrap")},
			})
			require.NoError(t, err)

			_, err = clients[tc.createIn].GetMultiRegionAccessPoint(t.Context(), &s3csdk.GetMultiRegionAccessPointInput{
				AccountId: aws.String(createTagsTestAccountID), Name: aws.String("mrap"),
			})
			require.Error(t, err)
		})
	}
}
