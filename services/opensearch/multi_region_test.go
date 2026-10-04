package opensearch_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

const mrHome = "us-east-1"

func newMRHandler() *opensearch.Handler {
	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("000000000000", mrHome))
	h.AccountID, h.Region = "000000000000", mrHome
	h.EnableRegions()

	return h
}

func regionDo(t *testing.T, h *opensearch.Handler, region, method, path, body string) map[string]any {
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

func regionDomainNames(t *testing.T, h *opensearch.Handler, region string) []string {
	t.Helper()

	list, _ := regionDo(t, h, region, http.MethodGet, "/2021-01-01/domain", "")["DomainNames"].([]any)
	out := make([]string, 0, len(list))

	for _, v := range list {
		m, _ := v.(map[string]any)
		s, _ := m["DomainName"].(string)
		out = append(out, s)
	}

	return out
}

func regionDomainARN(t *testing.T, h *opensearch.Handler, region string) string {
	t.Helper()

	out := regionDo(t, h, region, http.MethodGet, "/2021-01-01/opensearch/domain/shared", "")
	d, _ := out["DomainStatus"].(map[string]any)
	arn, _ := d["ARN"].(string)

	return arn
}

const domainBody = `{"DomainName":"shared"}`

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

			h := newMRHandler()

			for _, r := range tc.regions {
				regionDo(t, h, r, http.MethodPost, "/2021-01-01/opensearch/domain", domainBody)
			}

			for _, r := range tc.regions {
				assert.Equal(t, []string{"shared"}, regionDomainNames(t, h, r))
				assert.Contains(t, regionDomainARN(t, h, r), ":es:"+r+":")
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

			src := newMRHandler()
			regionDo(t, src, mrHome, http.MethodPost, "/2021-01-01/opensearch/domain", domainBody)

			if tc.extraRegion != "" {
				regionDo(t, src, tc.extraRegion, http.MethodPost, "/2021-01-01/opensearch/domain", domainBody)
			}

			snap := src.Snapshot(t.Context())

			var doc map[string]json.RawMessage

			require.NoError(t, json.Unmarshal(snap, &doc))

			_, hasRegions := doc["regions"]
			assert.Equal(t, tc.wantRegions, hasRegions)

			dst := newMRHandler()
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Equal(t, []string{"shared"}, regionDomainNames(t, dst, mrHome))

			if tc.extraRegion != "" {
				assert.Contains(t, regionDomainARN(t, dst, tc.extraRegion), ":es:"+tc.extraRegion+":")
			}
		})
	}
}

func TestHandler_MultiRegionLegacyRestore(t *testing.T) {
	t.Parallel()

	legacy := opensearch.NewHandler(opensearch.NewInMemoryBackend("000000000000", mrHome))
	regionDo(t, legacy, mrHome, http.MethodPost, "/2021-01-01/opensearch/domain", domainBody)

	dst := newMRHandler()
	regionDo(t, dst, "eu-west-1", http.MethodPost, "/2021-01-01/opensearch/domain", domainBody)

	require.NoError(t, dst.Restore(t.Context(), legacy.Snapshot(t.Context())))
	assert.Equal(t, []string{"shared"}, regionDomainNames(t, dst, mrHome))
	assert.Empty(t, regionDomainNames(t, dst, "eu-west-1"))
}
