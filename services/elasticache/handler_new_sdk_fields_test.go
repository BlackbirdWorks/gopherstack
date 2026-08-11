package elasticache_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// newRawFieldTestServer spins up a fresh backend+server per case so raw-XML
// wire assertions (element present vs. entirely absent) aren't confused by
// SDK-side zero-value defaulting -- gopherstack-31dm needs the actual bytes
// on the wire, not the SDK client's parsed (and thus always-zero-valued for
// an absent element) view of them.
func newRawFieldTestServer(t *testing.T) string {
	t.Helper()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	handler := elasticache.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(handler))
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv.URL
}

func postFormRaw(t *testing.T, srvURL string, form url.Values) string {
	t.Helper()

	form.Set("Version", "2015-02-02")
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		srvURL,
		strings.NewReader(form.Encode()),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(body)
}

// TestHandler_NewSDKFields_WireShape covers the six ServerlessCache/
// ReplicationGroup/Snapshot fields the SDK gained between the pinned
// v1.51.11 audit and the actual v1.56.4 go.mod pin (gopherstack-31dm):
// ServerlessCache.NetworkType/StorageEncryptionType,
// ReplicationGroup.Durability/EffectiveDurability/StorageEncryptionType, and
// Snapshot.Durability. Only NetworkType (create-only) and Durability
// (create+modify) have a real Create/Modify input member to echo; the other
// three have none, so this asserts they stay entirely absent from the wire
// rather than a fabricated value -- per parity-principles.md's no-fabrication
// rule.
func TestHandler_NewSDKFields_WireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run   func(t *testing.T, srvURL string) string
		check func(t *testing.T, body string)
		name  string
	}{
		{
			name: "serverlesscache_networktype_echoed_from_create_input",
			run: func(t *testing.T, srvURL string) string {
				t.Helper()

				return postFormRaw(t, srvURL, url.Values{
					"Action":              {"CreateServerlessCache"},
					"ServerlessCacheName": {"sc-networktype"},
					"Engine":              {"redis"},
					"NetworkType":         {"dual_stack"},
				})
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "<NetworkType>dual_stack</NetworkType>")
			},
		},
		{
			name: "serverlesscache_networktype_absent_when_unset",
			run: func(t *testing.T, srvURL string) string {
				t.Helper()

				return postFormRaw(t, srvURL, url.Values{
					"Action":              {"CreateServerlessCache"},
					"ServerlessCacheName": {"sc-no-networktype"},
					"Engine":              {"redis"},
				})
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.NotContains(t, body, "<NetworkType>")
			},
		},
		{
			name: "serverlesscache_storageencryptiontype_always_absent",
			run: func(t *testing.T, srvURL string) string {
				t.Helper()

				return postFormRaw(t, srvURL, url.Values{
					"Action":              {"CreateServerlessCache"},
					"ServerlessCacheName": {"sc-storageenc"},
					"Engine":              {"redis"},
					"KmsKeyId":            {"kms-explicit"},
				})
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.NotContains(t, body, "<StorageEncryptionType>",
					"StorageEncryptionType has no Create/ModifyServerlessCache input member; must never be fabricated")
			},
		},
		{
			name: "replicationgroup_durability_echoed_on_create",
			run: func(t *testing.T, srvURL string) string {
				t.Helper()

				return postFormRaw(t, srvURL, url.Values{
					"Action":                      {"CreateReplicationGroup"},
					"ReplicationGroupId":          {"rg-durability-create"},
					"ReplicationGroupDescription": {"durability create test"},
					"Durability":                  {"sync"},
				})
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "<Durability>sync</Durability>")
			},
		},
		{
			name: "replicationgroup_durability_echoed_on_modify",
			run: func(t *testing.T, srvURL string) string {
				t.Helper()

				postFormRaw(t, srvURL, url.Values{
					"Action":                      {"CreateReplicationGroup"},
					"ReplicationGroupId":          {"rg-durability-modify"},
					"ReplicationGroupDescription": {"durability modify test"},
				})

				return postFormRaw(t, srvURL, url.Values{
					"Action":             {"ModifyReplicationGroup"},
					"ReplicationGroupId": {"rg-durability-modify"},
					"Durability":         {"async"},
				})
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "<Durability>async</Durability>")
			},
		},
		{
			name: "replicationgroup_durability_absent_when_unset",
			run: func(t *testing.T, srvURL string) string {
				t.Helper()

				return postFormRaw(t, srvURL, url.Values{
					"Action":                      {"CreateReplicationGroup"},
					"ReplicationGroupId":          {"rg-no-durability"},
					"ReplicationGroupDescription": {"no durability"},
				})
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.NotContains(t, body, "<Durability>")
			},
		},
		{
			name: "replicationgroup_effectivedurability_always_absent",
			run: func(t *testing.T, srvURL string) string {
				t.Helper()

				return postFormRaw(t, srvURL, url.Values{
					"Action":                      {"CreateReplicationGroup"},
					"ReplicationGroupId":          {"rg-effdurability"},
					"ReplicationGroupDescription": {"effective durability"},
					"Durability":                  {"default"},
				})
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.NotContains(t, body, "<EffectiveDurability>",
					"EffectiveDurability is server-resolved with no Create/Modify input; must never be fabricated")
			},
		},
		{
			name: "replicationgroup_storageencryptiontype_always_absent",
			run: func(t *testing.T, srvURL string) string {
				t.Helper()

				return postFormRaw(t, srvURL, url.Values{
					"Action":                      {"CreateReplicationGroup"},
					"ReplicationGroupId":          {"rg-storageenc"},
					"ReplicationGroupDescription": {"storage encryption"},
					"AtRestEncryptionEnabled":     {"true"},
					"KmsKeyId":                    {"kms-rg"},
				})
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.NotContains(t, body, "<StorageEncryptionType>",
					"StorageEncryptionType has no Create/ModifyReplicationGroup input member; must never be fabricated")
			},
		},
		{
			name: "snapshot_durability_always_absent",
			run: func(t *testing.T, srvURL string) string {
				t.Helper()

				postFormRaw(t, srvURL, url.Values{
					"Action":         {"CreateCacheCluster"},
					"CacheClusterId": {"snap-durability-cluster"},
					"Engine":         {"redis"},
				})

				return postFormRaw(t, srvURL, url.Values{
					"Action":         {"CreateSnapshot"},
					"SnapshotName":   {"snap-durability"},
					"CacheClusterId": {"snap-durability-cluster"},
				})
			},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.NotContains(t, body, "<Durability>",
					"CreateSnapshotInput has no Durability member and CacheCluster has "+
						"no durability concept; must never be fabricated")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srvURL := newRawFieldTestServer(t)
			body := tt.run(t, srvURL)
			tt.check(t, body)
		})
	}
}
