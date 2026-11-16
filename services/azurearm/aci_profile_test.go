package azurearm_test

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	aciProfilesBase = "/subscriptions/" + aciSub +
		"/resourceGroups/rg/providers/Microsoft.ContainerInstance/containerGroupProfiles"
	aciProfileAPI = "?api-version=2025-09-01"
)

func (s *aciStack) rest(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()

	var rdr io.Reader = http.NoBody
	if body != "" {
		rdr = strings.NewReader(body)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, s.url+path, rdr)
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)

	var out map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out, jsonx.V1())
	}

	return resp.StatusCode, out
}

func profileBody(image string, extra string) string {
	return `{"location":"westus","tags":{"a":"b"},"properties":{"osType":"Linux","restartPolicy":"OnFailure",` + extra +
		`"containers":[{"name":"c1","properties":{"image":"` + image + `","environmentVariables":[` +
		`{"name":"S","secureValue":"hunter2"}],"resources":{"requests":{"cpu":1,"memoryInGB":1.5}}}}]}}`
}

func revisionsOf(t *testing.T, body map[string]any) []any {
	t.Helper()

	props, ok := body["properties"].(map[string]any)
	require.True(t, ok)

	regs, _ := props["registeredRevisions"].([]any)

	return regs
}

func TestACIProfileLifecycle(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	url := aciProfilesBase + "/prof1" + aciProfileAPI

	status, body := s.rest(
		t,
		http.MethodPut,
		url,
		profileBody("img:1", `"shutdownGracePeriod":"2026-01-01T00:00:00Z","useKrypton":true,`),
	)
	require.Equal(t, http.StatusCreated, status)
	props, _ := body["properties"].(map[string]any)
	assert.EqualValues(t, 1, props["revision"])
	assert.Equal(t, "Standard", props["sku"])
	assert.Equal(t, true, props["useKrypton"])
	assert.Equal(t, "2026-01-01T00:00:00Z", props["shutdownGracePeriod"])
	assert.Equal(t, "Microsoft.ContainerInstance/containerGroupProfiles", body["type"])
	assert.NotContains(t, mustJSON(t, body), "hunter2", "secure values are write-only")

	status, body = s.rest(
		t,
		http.MethodPut,
		url,
		profileBody("img:1", `"shutdownGracePeriod":"2026-01-01T00:00:00Z","useKrypton":true,`),
	)
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, revisionsOf(t, body), 1, "an unchanged PUT keeps the revision")

	status, body = s.rest(t, http.MethodPut, url, profileBody("img:2", ""))
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, revisionsOf(t, body), 2)
	assert.EqualValues(t, 2, body["properties"].(map[string]any)["revision"])

	status, body = s.rest(t, http.MethodGet, aciProfilesBase+"/prof1/revisions/1"+aciProfileAPI, "")
	require.Equal(t, http.StatusOK, status)
	first := body["properties"].(map[string]any)["containers"].([]any)[0].(map[string]any)["properties"].(map[string]any)
	assert.Equal(t, "img:1", first["image"])

	status, body = s.rest(t, http.MethodGet, aciProfilesBase+"/prof1/revisions"+aciProfileAPI, "")
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, body["value"], 2)

	status, _ = s.rest(t, http.MethodGet, aciProfilesBase+"/prof1/revisions/9"+aciProfileAPI, "")
	assert.Equal(t, http.StatusNotFound, status)

	status, body = s.rest(t, http.MethodPatch, url, `{"tags":{"x":"y"}}`)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, map[string]any{"x": "y"}, body["tags"])
	assert.Len(t, revisionsOf(t, body), 2, "a tag patch does not add a revision")

	status, body = s.rest(t, http.MethodGet, aciProfilesBase+aciProfileAPI, "")
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, body["value"], 1)

	status, body = s.rest(
		t,
		http.MethodGet,
		"/subscriptions/"+aciSub+"/providers/Microsoft.ContainerInstance/containerGroupProfiles"+aciProfileAPI,
		"",
	)
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, body["value"], 1)

	snap := s.handler.Snapshot(t.Context())
	dst := newACIStack(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	status, body = dst.rest(t, http.MethodGet, url, "")
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, revisionsOf(t, body), 2)

	status, _ = s.rest(t, http.MethodDelete, url, "")
	assert.Equal(t, http.StatusOK, status)
	status, _ = s.rest(t, http.MethodDelete, url, "")
	assert.Equal(t, http.StatusNoContent, status)
	status, _ = s.rest(t, http.MethodGet, url, "")
	assert.Equal(t, http.StatusNotFound, status)
}

func TestACIProfileValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		body     string
		wantCode string
	}{
		{
			name:     "no containers",
			path:     "/p1",
			body:     `{"properties":{"osType":"Linux"}}`,
			wantCode: "InvalidRequestContent",
		},
		{name: "bad name", path: "/-bad", body: profileBody("i", ""), wantCode: "InvalidRequestContent"},
		{
			name:     "subnet ids are group only",
			path:     "/p2",
			body:     profileBody("i", `"subnetIds":[{"id":"/x"}],`),
			wantCode: "InvalidRequestContent",
		},
		{
			name:     "bad security context",
			path:     "/p3",
			body:     profileBody("i", `"securityContext":{"privileged":true},`),
			wantCode: "InvalidRequestContent",
		},
		{
			name:     "bad time to live type",
			path:     "/p4",
			body:     profileBody("i", `"timeToLive":5,`),
			wantCode: "InvalidRequestContent",
		},
		{name: "security context accepted", path: "/p5", body: profileBody("i", `"securityContext":{"runAsUser":5},`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			status, body := s.rest(t, http.MethodPut, aciProfilesBase+tt.path+aciProfileAPI, tt.body)

			if tt.wantCode == "" {
				assert.Equal(t, http.StatusCreated, status)

				return
			}

			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tt.wantCode, body["error"].(map[string]any)["code"])
		})
	}
}
