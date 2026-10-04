package mq_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/mq"
)

const mrHome = "us-east-1"

func newRegionHandler() *mq.Handler {
	h := mq.NewHandler(mq.NewInMemoryBackend("000000000000", mrHome))
	h.EnableRegions()

	return h
}

func regionCall(t *testing.T, h *mq.Handler, region, method, path, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())

	out := map[string]any{}
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}

	return out
}

func listField(t *testing.T, h *mq.Handler, region, path, key, field string) []string {
	t.Helper()

	list, _ := regionCall(t, h, region, http.MethodGet, path, "")[key].([]any)

	out := make([]string, 0, len(list))

	for _, v := range list {
		m, _ := v.(map[string]any)
		s, _ := m[field].(string)
		out = append(out, s)
	}

	return out
}

const (
	cfgBody    = `{"name":"shared","engineType":"ACTIVEMQ","engineVersion":"5.17.6"}`
	brokerBody = `{"brokerName":"shared","engineType":"ACTIVEMQ","engineVersion":"5.17.6",` +
		`"hostInstanceType":"mq.t3.micro","deploymentMode":"SINGLE_INSTANCE","publiclyAccessible":false,` +
		`"autoMinorVersionUpgrade":false,"users":[{"username":"admin","password":"s3cretpassword12"}]}`
)

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
				regionCall(t, h, r, http.MethodPost, "/v1/configurations", cfgBody)
				regionCall(t, h, r, http.MethodPost, "/v1/brokers", brokerBody)
			}

			for _, r := range tc.regions {
				assert.Equal(t, []string{"shared"}, listField(t, h, r, "/v1/configurations", "configurations", "name"))

				arns := listField(t, h, r, "/v1/configurations", "configurations", "arn")
				require.Len(t, arns, 1)
				assert.Contains(t, arns[0], ":mq:"+r+":")

				brokers := listField(t, h, r, "/v1/brokers", "brokerSummaries", "brokerArn")
				require.Len(t, brokers, 1)
				assert.Contains(t, brokers[0], ":mq:"+r+":")
			}
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
			regionCall(t, src, mrHome, http.MethodPost, "/v1/configurations", cfgBody)

			if tc.extraRegion != "" {
				regionCall(t, src, tc.extraRegion, http.MethodPost, "/v1/configurations", cfgBody)
			}

			snap := src.Snapshot(t.Context())

			var doc map[string]json.RawMessage

			require.NoError(t, json.Unmarshal(snap, &doc))

			_, hasRegions := doc["regions"]
			assert.Equal(t, tc.wantRegions, hasRegions)

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(t.Context(), snap))
			names := listField(t, dst, mrHome, "/v1/configurations", "configurations", "name")
			assert.Equal(t, []string{"shared"}, names)

			if tc.extraRegion != "" {
				arns := listField(t, dst, tc.extraRegion, "/v1/configurations", "configurations", "arn")
				require.Len(t, arns, 1)
				assert.Contains(t, arns[0], ":mq:"+tc.extraRegion+":")
			}
		})
	}
}

func TestHandler_MultiRegionLegacyRestore(t *testing.T) {
	t.Parallel()

	legacy := mq.NewHandler(mq.NewInMemoryBackend("000000000000", mrHome))
	regionCall(t, legacy, mrHome, http.MethodPost, "/v1/configurations", cfgBody)

	dst := newRegionHandler()
	regionCall(t, dst, "eu-west-1", http.MethodPost, "/v1/configurations", cfgBody)

	require.NoError(t, dst.Restore(t.Context(), legacy.Snapshot(t.Context())))
	assert.Equal(t, []string{"shared"}, listField(t, dst, mrHome, "/v1/configurations", "configurations", "name"))
	assert.Empty(t, listField(t, dst, "eu-west-1", "/v1/configurations", "configurations", "name"))
}

func TestHandler_MultiRegionDockerBrokers(t *testing.T) {
	t.Parallel()

	rt := &fakeRuntime{}
	probe := newGatedProbe()

	home := mq.NewInMemoryBackend("000000000000", mrHome)
	home.EnableBrokers(mq.BrokerConfig{Runtime: rt, Probe: probe.probe, StartTimeout: time.Minute})

	h := mq.NewHandler(home)
	h.EnableRegions()
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	for _, r := range []string{mrHome, "eu-west-1"} {
		regionCall(t, h, r, http.MethodPost, "/v1/brokers", brokerBody)
	}

	assert.Eventually(t, func() bool { return len(rt.createdSpecs()) == 2 }, time.Minute, time.Millisecond)

	specs := rt.createdSpecs()
	assert.NotEqual(t, specs[0].Name, specs[1].Name)
	assert.Len(t, h.RegionBackends(), 2)
}
