package gcpredis_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	base := "/v1/projects/p/locations/us-central1"

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "create", Method: http.MethodPost, Path: base + "/instances?instanceId=r1",
			Body: `{"tier":"BASIC","memorySizeGb":1,"authEnabled":true}`, Status: 200,
			Contains: []string{`"name":"projects/p/locations/us-central1/operations/operation-`, `"verb":"create"`},
		},
		{
			Name: "list locations", Method: http.MethodGet, Path: "/v1/projects/p/locations?pageSize=3", Status: 200,
			Contains: []string{`"locationId":"africa-south1"`, `"nextPageToken"`, `"availableZones"`},
		},
		{
			Name: "get location", Method: http.MethodGet, Path: base, Status: 200,
			Contains: []string{`"displayName":"Iowa"`},
		},
		{
			Name: "missing instance", Method: http.MethodGet, Path: base + "/instances/nope", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
		{
			Name: "invalid tier", Method: http.MethodPost, Path: base + "/instances?instanceId=bad",
			Body: `{"memorySizeGb":1}`, Status: 400, Contains: []string{`"status":"INVALID_ARGUMENT"`},
		},
		{
			Name: "unknown operation", Method: http.MethodGet, Path: base + "/operations/none", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
	})

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
			"http://"+e.handler.Addr()+base+"/instances/r1", nil)
		if err != nil {
			return false
		}

		resp, err := testclient.New(t).Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		var inst struct {
			State string `json:"state"`
			Host  string `json:"host"`
			Auth  bool   `json:"authEnabled"`
		}

		return json.Unmarshal(body, &inst) == nil && inst.State == "READY" && inst.Host != "" &&
			assert.True(t, inst.Auth)
	}, 10*time.Second, 5*time.Millisecond)
}
