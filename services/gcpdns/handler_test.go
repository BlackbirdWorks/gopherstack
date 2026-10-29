package gcpdns_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/gcpdns"
)

func TestWireErrorsAndRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "z", "z.com.")

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		reason string
		status int
	}{
		{"missing zone", "GET", "/dns/v1/projects/p/managedZones/nope", "", "notFound", 404},
		{"unknown path", "GET", "/dns/v1/projects/p/bogus", "", "notFound", 404},
		{"wrong prefix", "GET", "/v9/projects/p/managedZones", "", "notFound", 404},
		{"unsupported verb", "DELETE", "/dns/v1/projects/p/managedZones/z/changes/0", "", "notFound", 404},
		{"bad json", "POST", "/dns/v1/projects/p/managedZones", "{", "invalid", 400},
		{"bad max results", "GET", "/dns/v1/projects/p/managedZones?maxResults=-1", "", "invalid", 400},
		{"iam not served", "POST", "/dns/v1/projects/p/managedZones/z:getIamPolicy", "{}", "notFound", 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(t.Context(), tt.method, e.url+tt.path, strings.NewReader(tt.body))
			require.NoError(t, err)

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.status, resp.StatusCode)
			assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")

			var body struct {
				Error struct {
					Message string `json:"message"`
					Errors  []struct {
						Domain string `json:"domain"`
						Reason string `json:"reason"`
					} `json:"errors"`
					Code int `json:"code"`
				} `json:"error"`
			}

			require.NoError(t, json.Unmarshal(raw, &body), string(raw))
			assert.Equal(t, tt.status, body.Error.Code)
			assert.NotEmpty(t, body.Error.Message)
			require.Len(t, body.Error.Errors, 1)
			assert.Equal(t, "global", body.Error.Errors[0].Domain)
			assert.Equal(t, tt.reason, body.Error.Errors[0].Reason)
		})
	}
}

func TestWireShapes(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "z", "z.com.")

	get := func(path string) map[string]any {
		resp, err := testclient.New(t).Get(e.url + path)
		require.NoError(t, err)

		defer resp.Body.Close()

		var m map[string]any

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&m))

		return m
	}

	zone := get("/dns/v1/projects/test-project/managedZones/z")
	assert.IsType(t, "", zone["id"], "zone ids are int64 strings on the wire")
	assert.Equal(t, "dns#managedZone", zone["kind"])

	list := get("/dns/v1/projects/test-project/managedZones")
	assert.Equal(t, "dns#managedZonesListResponse", list["kind"])
	assert.Len(t, list["managedZones"], 1)

	empty := get("/dns/v1/projects/other/managedZones")
	assert.NotContains(t, empty, "managedZones")
	assert.Equal(t, "dns#managedZonesListResponse", empty["kind"])

	rrs := get("/dns/v1/projects/test-project/managedZones/z/rrsets")
	assert.Equal(t, "dns#resourceRecordSetsListResponse", rrs["kind"])

	proj := get("/dns/v1/projects/test-project")
	assert.IsType(t, "", proj["number"], "project numbers are uint64 strings on the wire")
}

func TestHandlerMetadata(t *testing.T) {
	t.Parallel()

	h := gcpdns.NewHandler(gcpdns.NewInMemoryBackend())
	assert.Equal(t, "GCPDNS", h.Name())
	assert.Equal(t, gcpdns.DefaultPort, h.Port)
	assert.Equal(t, gcpdns.DefaultDNSPort, h.DNSPort)
	assert.GreaterOrEqual(t, len(h.GetSupportedOperations()), 37)
	assert.Contains(t, h.GetSupportedOperations(), "Changes.Create")
	assert.Equal(t, gcpdns.Settings{Port: 8140, DNSPort: 8141}, gcpdns.DefaultSettings())

	_, err := (&gcpdns.Provider{}).Init(nil)
	require.ErrorIs(t, err, gcpdns.ErrNilAppContext)
}

func TestStartWorkerPortConflict(t *testing.T) {
	t.Parallel()

	first := gcpdns.NewHandler(gcpdns.NewInMemoryBackend())
	first.Port, first.DNSPort = freePort(t), freePort(t)
	require.NoError(t, first.StartWorker(t.Context()))
	t.Cleanup(func() { first.Shutdown(t.Context()) })

	for name, ports := range map[string][2]int{
		"rest port taken": {first.Port, freePort(t)},
		"dns port taken":  {freePort(t), first.DNSPort},
	} {
		second := gcpdns.NewHandler(gcpdns.NewInMemoryBackend())
		second.Port, second.DNSPort = ports[0], ports[1]
		require.Error(t, second.StartWorker(t.Context()), name)
		second.Shutdown(t.Context())
	}
}
