package autoscaling_test

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
	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *autoscaling.Handler {
	t.Helper()

	h := autoscaling.NewHandler(autoscaling.NewInMemoryBackendWithConfig("000000000000", mrHome))
	h.EnableRegions(t.Context())
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	return h
}

func regionForm(t *testing.T, h *autoscaling.Handler, region string, form url.Values) string {
	t.Helper()

	form.Set("Version", "2011-01-01")

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return rec.Body.String()
}

func createGroup(t *testing.T, h *autoscaling.Handler, region, name string) {
	t.Helper()

	regionForm(t, h, region, url.Values{
		"Action": {"CreateLaunchConfiguration"}, "LaunchConfigurationName": {"lc-" + name},
		"ImageId": {"ami-1"}, "InstanceType": {"t2.micro"},
	})
	regionForm(t, h, region, url.Values{
		"Action": {"CreateAutoScalingGroup"}, "AutoScalingGroupName": {name},
		"LaunchConfigurationName": {"lc-" + name}, "MinSize": {"0"}, "MaxSize": {"1"},
	})
}

type groupList struct {
	Groups []struct {
		Name string   `xml:"AutoScalingGroupName"`
		ARN  string   `xml:"AutoScalingGroupARN"`
		AZs  []string `xml:"AvailabilityZones>member"`
	} `xml:"DescribeAutoScalingGroupsResult>AutoScalingGroups>member"`
}

func describeGroups(t *testing.T, h *autoscaling.Handler, region string) groupList {
	t.Helper()

	var out groupList

	body := regionForm(t, h, region, url.Values{"Action": {"DescribeAutoScalingGroups"}})
	require.NoError(t, xml.Unmarshal([]byte(body), &out), body)

	return out
}

func groupNames(t *testing.T, h *autoscaling.Handler, region string) []string {
	t.Helper()

	groups := describeGroups(t, h, region).Groups
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
				createGroup(t, h, r, "shared")
				createGroup(t, h, r, "only-"+r)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, groupNames(t, h, r))

				for _, g := range describeGroups(t, h, r).Groups {
					assert.Contains(t, g.ARN, ":"+r+":")
					assert.Equal(t, []string{r + "a"}, g.AZs)
				}
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
		{name: "home-only-snapshot-unchanged"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler(t)
			createGroup(t, src, mrHome, "home")

			if tc.remote {
				createGroup(t, src, "eu-west-1", "eu")
			}

			snap := src.Snapshot(t.Context())
			require.NotNil(t, snap)

			if !tc.remote {
				home, ok := src.Backend.(*autoscaling.InMemoryBackend)
				require.True(t, ok)
				assert.Equal(t, home.Snapshot(t.Context()), snap)
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Equal(t, []string{"home"}, groupNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(groupNames(t, dst, "eu-west-1")) == 1)

			old, ok := newRegionHandler(t).Backend.(*autoscaling.InMemoryBackend)
			require.True(t, ok)
			require.NoError(t, old.Restore(t.Context(), snap))

			groups, err := old.DescribeAutoScalingGroups(nil, nil)
			require.NoError(t, err)
			require.Len(t, groups, 1)
			assert.Equal(t, "home", groups[0].AutoScalingGroupName)
		})
	}
}
