package wafv2_test

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
	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

const mrHome = "us-east-1"

func regionCall(t *testing.T, h *wafv2.Handler, region, target, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "AWSWAF_20190729."+target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func ipSetARNs(t *testing.T, h *wafv2.Handler, region, scope string) []string {
	t.Helper()

	list, _ := regionCall(t, h, region, "ListIPSets", `{"Scope":"`+scope+`"}`)["IPSets"].([]any)
	out := make([]string, 0, len(list))

	for _, v := range list {
		m, _ := v.(map[string]any)
		s, _ := m["ARN"].(string)
		out = append(out, s)
	}

	return out
}

func createIPSet(t *testing.T, h *wafv2.Handler, region, scope string) {
	t.Helper()

	regionCall(t, h, region, "CreateIPSet",
		`{"Name":"shared","Scope":"`+scope+`","IPAddressVersion":"IPV4","Addresses":["10.0.0.0/8"]}`)
}

func TestHandler_MultiRegionRegionalScope(t *testing.T) {
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

			h := wafv2.NewHandler(wafv2.NewInMemoryBackend("000000000000", mrHome))

			for _, r := range tc.regions {
				createIPSet(t, h, r, "REGIONAL")
			}

			for _, r := range tc.regions {
				arns := ipSetARNs(t, h, r, "REGIONAL")
				require.Len(t, arns, 1)
				assert.Contains(t, arns[0], ":wafv2:"+r+":")
			}
		})
	}
}

func TestHandler_MultiRegionCloudFrontScopeIsGlobal(t *testing.T) {
	t.Parallel()

	h := wafv2.NewHandler(wafv2.NewInMemoryBackend("000000000000", mrHome))
	createIPSet(t, h, mrHome, "CLOUDFRONT")

	for _, r := range []string{mrHome, "eu-west-1"} {
		arns := ipSetARNs(t, h, r, "CLOUDFRONT")
		require.Len(t, arns, 1, r)
		assert.Contains(t, arns[0], "global/ipset/")
	}
}
