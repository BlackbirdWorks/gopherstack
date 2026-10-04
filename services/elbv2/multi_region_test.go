package elbv2_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *elbv2.Handler {
	t.Helper()

	h := elbv2.NewHandler(elbv2.NewInMemoryBackend("000000000000", mrHome))
	h.EnableRegions()
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	return h
}

func regionForm(t *testing.T, h *elbv2.Handler, region string, form url.Values) string {
	t.Helper()

	form.Set("Version", "2015-12-01")

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return rec.Body.String()
}

func createTG(t *testing.T, h *elbv2.Handler, region, name string) {
	t.Helper()

	regionForm(t, h, region, url.Values{
		"Action": {"CreateTargetGroup"}, "Name": {name}, "Protocol": {"HTTP"}, "Port": {"80"}, "VpcId": {"vpc-1"},
	})
}

type tgList struct {
	Groups []struct {
		Name string `xml:"TargetGroupName"`
		ARN  string `xml:"TargetGroupArn"`
	} `xml:"DescribeTargetGroupsResult>TargetGroups>member"`
}

func describeTGs(t *testing.T, h *elbv2.Handler, region string) tgList {
	t.Helper()

	var out tgList

	body := regionForm(t, h, region, url.Values{"Action": {"DescribeTargetGroups"}})
	require.NoError(t, xml.Unmarshal([]byte(body), &out), body)

	return out
}

func tgNames(t *testing.T, h *elbv2.Handler, region string) []string {
	t.Helper()

	groups := describeTGs(t, h, region).Groups
	names := make([]string, 0, len(groups))

	for _, g := range groups {
		names = append(names, g.Name)
	}

	return names
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

			h := newRegionHandler(t)

			for _, r := range tc.regions {
				createTG(t, h, r, "shared")
				createTG(t, h, r, "only-"+r)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, tgNames(t, h, r))

				for _, g := range describeTGs(t, h, r).Groups {
					assert.Contains(t, g.ARN, ":"+r+":")
				}
			}
		})
	}
}

func TestHandler_MultiRegionBackendFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		home   bool
	}{
		{name: "home", region: mrHome, home: true},
		{name: "empty-is-home", region: "", home: true},
		{name: "sibling", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)
			assert.Equal(t, tc.home, h.BackendFor(tc.region) == h.Backend)
			assert.Len(t, h.RegionBackends(), map[bool]int{true: 1, false: 2}[tc.home])
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
			createTG(t, src, mrHome, "home")

			if tc.remote {
				createTG(t, src, "eu-west-1", "eu")
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)

			if !tc.remote {
				home, ok := src.Backend.(*elbv2.InMemoryBackend)
				require.True(t, ok)
				assert.Equal(t, home.Snapshot(context.Background()), snap)
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, tgNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(tgNames(t, dst, "eu-west-1")) == 1)

			old, ok := newRegionHandler(t).Backend.(*elbv2.InMemoryBackend)
			require.True(t, ok)
			require.NoError(t, old.Restore(context.Background(), snap))
			assert.Len(t, mustDescribe(t, old), 1)
		})
	}
}

func mustDescribe(t *testing.T, b *elbv2.InMemoryBackend) []string {
	t.Helper()

	tgs, err := b.DescribeTargetGroups(nil, nil, "")
	require.NoError(t, err)

	names := make([]string, 0, len(tgs))
	for _, g := range tgs {
		names = append(names, g.TargetGroupName)
	}

	return names
}
