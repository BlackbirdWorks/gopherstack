package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func usageByName(t *testing.T, out map[string]any) map[string]map[string]any {
	t.Helper()

	got := map[string]map[string]any{}

	list, _ := out["value"].([]any)
	for _, v := range list {
		m, _ := v.(map[string]any)
		name, _ := m["name"].(string)
		got[name] = m
	}

	return got
}

func TestACRListUsages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantKey []string
		limit   float64
		premium bool
	}{
		{name: "basic", premium: false, wantKey: []string{"Size", "Webhooks"}, limit: 10 << 30},
		{name: "premium", premium: true,
			wantKey: []string{"Size", "Webhooks", "Geo-replications", "IPRules", "VNetRules"}, limit: 500 << 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, fake := newACRHandlerWithFake(t, tt.premium)
			fake.used = map[string]int64{"gopherreg": 12345}

			st, _ := doRequest(
				t,
				h,
				http.MethodPut,
				acrBase+"/webhooks/hook1"+acrVersion,
				[]byte(
					`{"location":"local","properties":{"serviceUri":"https://example.invalid/h","actions":["push"]}}`,
				),
			)
			require.Equal(t, http.StatusOK, st)

			st, out := doRequest(t, h, http.MethodGet, acrBase+"/listUsages"+acrVersion, nil)
			require.Equal(t, http.StatusOK, st, "%v", out)

			got := usageByName(t, out)
			assert.Len(t, got, len(tt.wantKey))

			for _, k := range tt.wantKey {
				assert.Contains(t, got, k)
			}

			assert.InDelta(t, 12345, got["Size"]["currentValue"], 0)
			assert.Equal(t, "Bytes", got["Size"]["unit"])
			assert.InDelta(t, tt.limit, got["Size"]["limit"], 0)
			assert.InDelta(t, 1, got["Webhooks"]["currentValue"], 0)
		})
	}

	t.Run("unknown registry", func(t *testing.T) {
		t.Parallel()

		h := newACRHandler(t, false)
		st, _ := doRequest(t, h, http.MethodGet,
			"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ContainerRegistry/registries/nonesuch/listUsages"+
				acrVersion, nil)
		assert.Equal(t, http.StatusNotFound, st)
	})
}

func TestACRExportPolicy(t *testing.T) {
	t.Parallel()

	const disable = `{"properties":{"policies":{"exportPolicy":{"status":"disabled"}}`

	tests := []struct {
		allowed *bool
		name    string
		body    string
		want    int
		premium bool
	}{
		{
			name:    "basic refuses",
			body:    disable + `,"publicNetworkAccess":"Disabled"}}`,
			want:    http.StatusBadRequest,
			allowed: new(true),
		},
		{name: "needs public access disabled", premium: true, body: disable + `}}`, want: http.StatusBadRequest,
			allowed: new(true)},
		{name: "disabled", premium: true, body: disable + `,"publicNetworkAccess":"Disabled"}}`,
			want: http.StatusOK, allowed: new(false)},
		{name: "enabled", premium: true, body: `{"properties":{"policies":{"exportPolicy":{"status":"enabled"}}}}`,
			want: http.StatusOK, allowed: new(true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, fake := newACRHandlerWithFake(t, tt.premium)

			st, out := doRequest(t, h, http.MethodPatch, acrBase+acrVersion, []byte(tt.body))
			require.Equal(t, tt.want, st, "%v", out)

			fake.mu.Lock()
			defer fake.mu.Unlock()

			assert.Equal(t, *tt.allowed, fake.exportAllowed["gopherreg"])
		})
	}
}

func TestACRPolicyPatchKeepsOtherPolicies(t *testing.T) {
	t.Parallel()

	h := newACRHandler(t, true)

	st, out := doRequest(t, h, http.MethodPatch, acrBase+acrVersion,
		[]byte(`{"properties":{"publicNetworkAccess":"Disabled","policies":{"exportPolicy":{"status":"disabled"}}}}`))
	require.Equal(t, http.StatusOK, st, "%v", out)

	st, out = doRequest(t, h, http.MethodPatch, acrBase+acrVersion,
		[]byte(`{"properties":{"policies":{"retentionPolicy":{"status":"enabled","days":3}}}}`))
	require.Equal(t, http.StatusOK, st, "%v", out)

	policies, _ := out["properties"].(map[string]any)["policies"].(map[string]any)
	assert.Equal(t, "disabled", policies["exportPolicy"].(map[string]any)["status"])
	assert.Equal(t, "enabled", policies["retentionPolicy"].(map[string]any)["status"])
}
