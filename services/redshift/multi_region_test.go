package redshift_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/redshift"
)

const mrHome = "us-east-1"

func newRegionHandler() *redshift.Handler {
	h := redshift.NewHandler(redshift.NewInMemoryBackend("000000000000", mrHome))
	h.EnableRegions()

	return h
}

func regionForm(t *testing.T, h *redshift.Handler, region string, form url.Values) string {
	t.Helper()

	form.Set("Version", "2012-12-01")

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())

	return rec.Body.String()
}

func createSubnetGroup(t *testing.T, h *redshift.Handler, region, name string) {
	t.Helper()

	regionForm(t, h, region, url.Values{
		"Action": {"CreateClusterSubnetGroup"}, "ClusterSubnetGroupName": {name}, "Description": {"d"},
		"SubnetIdentifier.SubnetIdentifier.1": {"subnet-1"}, "SubnetIdentifier.SubnetIdentifier.2": {"subnet-2"},
	})
}

func subnetGroupsIn(t *testing.T, h *redshift.Handler, region string) string {
	t.Helper()

	return regionForm(t, h, region, url.Values{"Action": {"DescribeClusterSubnetGroups"}})
}

func createCluster(t *testing.T, h *redshift.Handler, region string) string {
	t.Helper()

	return regionForm(t, h, region, url.Values{
		"Action": {"CreateCluster"}, "ClusterIdentifier": {"shared"}, "NodeType": {"dc2.large"},
		"MasterUsername": {"admin"}, "MasterUserPassword": {"Passw0rdPassw0rd"}, "DBName": {"dev"},
	})
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

			h := newRegionHandler()

			for _, r := range tc.regions {
				createSubnetGroup(t, h, r, "shared")
				createSubnetGroup(t, h, r, "only-"+r)
				assert.Contains(t, createCluster(t, h, r), "."+r+".redshift.amazonaws.com")
			}

			for _, r := range tc.regions {
				out := subnetGroupsIn(t, h, r)
				assert.Contains(t, out, "only-"+r)

				for _, other := range tc.regions {
					if other != r {
						assert.NotContains(t, out, "only-"+other)
					}
				}
			}

			assert.Len(t, h.RegionBackends(), len(tc.regions))
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		extraRegion string
		wantRegions bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "with-sibling", extraRegion: "eu-west-1", wantRegions: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler()
			createSubnetGroup(t, src, mrHome, "shared")

			if tc.extraRegion != "" {
				createSubnetGroup(t, src, tc.extraRegion, "eu-only")
			}

			snap := src.Snapshot(t.Context())

			var doc map[string]json.RawMessage

			require.NoError(t, json.Unmarshal(snap, &doc))

			_, hasRegions := doc["regions"]
			assert.Equal(t, tc.wantRegions, hasRegions)

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Contains(t, subnetGroupsIn(t, dst, mrHome), "shared")

			if tc.extraRegion != "" {
				assert.Contains(t, subnetGroupsIn(t, dst, tc.extraRegion), "eu-only")
				assert.NotContains(t, subnetGroupsIn(t, dst, mrHome), "eu-only")
			}
		})
	}
}

func TestHandler_MultiRegionLegacyRestore(t *testing.T) {
	t.Parallel()

	legacy := redshift.NewHandler(redshift.NewInMemoryBackend("000000000000", mrHome))
	createSubnetGroup(t, legacy, mrHome, "shared")

	dst := newRegionHandler()
	createSubnetGroup(t, dst, "eu-west-1", "eu-only")

	require.NoError(t, dst.Restore(t.Context(), legacy.Snapshot(t.Context())))
	assert.Contains(t, subnetGroupsIn(t, dst, mrHome), "shared")
	assert.NotContains(t, subnetGroupsIn(t, dst, "eu-west-1"), "eu-only")
}
