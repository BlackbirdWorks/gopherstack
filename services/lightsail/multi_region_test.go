package lightsail_test

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
	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

func newRegionHandler(t *testing.T) *lightsail.Handler {
	t.Helper()

	h := lightsail.NewHandler(lightsail.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion))
	h.EnableRegions(t.Context())

	return h
}

func lightsailCall(t *testing.T, h *lightsail.Handler, region, op, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "Lightsail_20161128."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: rtTestAccountID}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func keyPairs(t *testing.T, h *lightsail.Handler, region string) map[string]string {
	t.Helper()

	got := map[string]string{}

	list, _ := lightsailCall(t, h, region, "GetKeyPairs", `{}`)["keyPairs"].([]any)
	for _, v := range list {
		kp, _ := v.(map[string]any)
		name, _ := kp["name"].(string)
		arn, _ := kp["arn"].(string)
		got[name] = arn
	}

	return got
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
				lightsailCall(t, h, r, "CreateKeyPair", `{"keyPairName":"shared"}`)
				lightsailCall(t, h, r, "CreateKeyPair", `{"keyPairName":"only-`+r+`"}`)
			}

			for _, r := range tc.regions {
				got := keyPairs(t, h, r)
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
		prepare  func(t *testing.T, h *lightsail.Handler)
		name     string
		wantEU   bool
		wantSame bool
	}{
		{name: "home-only-snapshot-unchanged", prepare: func(t *testing.T, h *lightsail.Handler) {
			t.Helper()
			lightsailCall(t, h, rtTestRegion, "CreateKeyPair", `{"keyPairName":"home"}`)
		}, wantSame: true},
		{name: "peer-round-trips", prepare: func(t *testing.T, h *lightsail.Handler) {
			t.Helper()
			lightsailCall(t, h, rtTestRegion, "CreateKeyPair", `{"keyPairName":"home"}`)
			lightsailCall(t, h, "eu-west-1", "CreateKeyPair", `{"keyPairName":"eu"}`)
		}, wantEU: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler(t)
			tc.prepare(t, src)

			snap := src.Snapshot(t.Context())
			require.NotNil(t, snap)

			if tc.wantSame {
				legacy := src.Backend.Snapshot(t.Context())
				assert.Equal(t, legacy, snap, "single-region snapshot must keep the legacy bytes")
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Contains(t, keyPairs(t, dst, rtTestRegion), "home")

			eu := keyPairs(t, dst, "eu-west-1")
			assert.Equal(t, tc.wantEU, len(eu) == 1)

			old := newRegionHandler(t)
			require.NoError(t, old.Backend.Restore(t.Context(), snap), "older builds must ignore the regions key")
			assert.Contains(t, keyPairs(t, old, rtTestRegion), "home")
		})
	}
}
