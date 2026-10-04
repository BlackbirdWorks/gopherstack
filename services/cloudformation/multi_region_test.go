package cloudformation_test

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
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

const (
	mrHome     = "us-east-1"
	mrTemplate = `{"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`
)

func newRegionHandler() *cloudformation.Handler {
	h := cloudformation.NewHandler(cloudformation.NewInMemoryBackendWithConfig("000000000000", mrHome, nil))
	h.EnableRegions()

	return h
}

func regionForm(t *testing.T, h *cloudformation.Handler, region string, form url.Values) string {
	t.Helper()

	form.Set("Version", "2010-05-15")

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return rec.Body.String()
}

func createStack(t *testing.T, h *cloudformation.Handler, region, name string) {
	t.Helper()

	regionForm(t, h, region, url.Values{
		"Action": {"CreateStack"}, "StackName": {name}, "TemplateBody": {mrTemplate},
	})
}

type stackList struct {
	Stacks []struct {
		Name string `xml:"StackName"`
		ID   string `xml:"StackId"`
	} `xml:"DescribeStacksResult>Stacks>member"`
}

func describeStacks(t *testing.T, h *cloudformation.Handler, region string) stackList {
	t.Helper()

	var out stackList

	body := regionForm(t, h, region, url.Values{"Action": {"DescribeStacks"}})
	require.NoError(t, xml.Unmarshal([]byte(body), &out), body)

	return out
}

func stackNames(t *testing.T, h *cloudformation.Handler, region string) []string {
	t.Helper()

	stacks := describeStacks(t, h, region).Stacks
	names := make([]string, 0, len(stacks))

	for _, s := range stacks {
		names = append(names, s.Name)
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

			h := newRegionHandler()

			for _, r := range tc.regions {
				createStack(t, h, r, "shared")
				createStack(t, h, r, "only-"+r)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, stackNames(t, h, r))

				for _, s := range describeStacks(t, h, r).Stacks {
					assert.Contains(t, s.ID, ":"+r+":")
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

			src := newRegionHandler()
			createStack(t, src, mrHome, "home")

			if tc.remote {
				createStack(t, src, "eu-west-1", "eu")
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)

			if !tc.remote {
				home, ok := src.Backend.(*cloudformation.InMemoryBackend)
				require.True(t, ok)
				assert.Equal(t, home.Snapshot(context.Background()), snap)
			}

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, stackNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(stackNames(t, dst, "eu-west-1")) == 1)

			old, ok := newRegionHandler().Backend.(*cloudformation.InMemoryBackend)
			require.True(t, ok)
			require.NoError(t, old.Restore(context.Background(), snap))
			assert.Len(t, old.ListAll(), 1)
		})
	}
}
