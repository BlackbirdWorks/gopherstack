package azurearm_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const aciGroupsBase = "/subscriptions/" + aciSub +
	"/resourceGroups/rg/providers/Microsoft.ContainerInstance/containerGroups"

const aciProfileRefID = "/subscriptions/" + aciSub +
	"/resourceGroups/rg/providers/Microsoft.ContainerInstance/containerGroupProfiles/prof1"

func refGroup(ref, containers string) string {
	body := `{"location":"westus","properties":{"containerGroupProfile":` + ref
	if containers != "" {
		body += `,"containers":` + containers
	}

	return body + `}}`
}

func TestACIContainerGroupProfileReference(t *testing.T) {
	t.Parallel()

	two := `{"location":"westus","properties":{"osType":"Linux","restartPolicy":"Never","containers":[` +
		`{"name":"c1","properties":{"image":"img:1","resources":{"requests":{"cpu":1,"memoryInGB":1.5}}}},` +
		`{"name":"side","properties":{"image":"side:1","resources":{"requests":{"cpu":1,"memoryInGB":1}}}}]}}`

	tests := []struct {
		check      func(t *testing.T, body map[string]any)
		name       string
		body       string
		wantCode   string
		wantStatus int
	}{
		{
			name: "latest revision when none given", wantStatus: http.StatusOK,
			body: refGroup(`{"id":"`+aciProfileRefID+`"}`, ""),
			check: func(t *testing.T, body map[string]any) {
				t.Helper()

				props := body["properties"].(map[string]any)
				assert.EqualValues(t, 2, props["containerGroupProfile"].(map[string]any)["revision"])
				assert.Equal(t, "Never", props["restartPolicy"])
				assert.Len(t, props["containers"], 2)
			},
		},
		{
			name: "pinned revision and container override", wantStatus: http.StatusOK,
			body: refGroup(`{"id":"`+aciProfileRefID+`","revision":1}`,
				`[{"name":"c1","properties":{"image":"override:9","configMap":{"keyValuePairs":{"k":"v"}}}}]`),
			check: func(t *testing.T, body map[string]any) {
				t.Helper()

				props := body["properties"].(map[string]any)
				conts := props["containers"].([]any)
				assert.Len(t, conts, 2, "profile containers the group does not name are kept")

				c := conts[0].(map[string]any)["properties"].(map[string]any)
				assert.Equal(t, "override:9", c["image"])
				assert.Equal(t, map[string]any{"keyValuePairs": map[string]any{"k": "v"}}, c["configMap"])
				assert.EqualValues(t, 1, props["containerGroupProfile"].(map[string]any)["revision"])
			},
		},
		{
			name: "group property overrides profile", wantStatus: http.StatusOK,
			body: `{"location":"westus","properties":{"restartPolicy":"Always","containerGroupProfile":{"id":"` +
				aciProfileRefID + `","revision":2}}}`,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "Always", body["properties"].(map[string]any)["restartPolicy"])
			},
		},
		{
			name: "unknown profile", wantStatus: http.StatusNotFound, wantCode: "ResourceNotFound",
			body: refGroup(`{"id":"`+aciProfileRefID+`x"}`, ""),
		},
		{
			name: "unknown revision", wantStatus: http.StatusNotFound, wantCode: "ResourceNotFound",
			body: refGroup(`{"id":"`+aciProfileRefID+`","revision":7}`, ""),
		},
		{
			name: "missing id", wantStatus: http.StatusBadRequest, wantCode: "InvalidRequestContent",
			body: refGroup(`{"revision":1}`, ""),
		},
		{
			name: "wrong resource type", wantStatus: http.StatusBadRequest, wantCode: "InvalidRequestContent",
			body: refGroup(`{"id":"/subscriptions/`+aciSub+
				`/resourceGroups/rg/providers/Microsoft.ContainerInstance/containerGroups/x"}`, ""),
		},
		{
			name: "unknown member", wantStatus: http.StatusBadRequest, wantCode: "InvalidRequestContent",
			body: refGroup(`{"id":"`+aciProfileRefID+`","extra":1}`, ""),
		},
		{
			name:       "standby pool still unsupported",
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidRequestContent",
			body: `{"location":"westus","properties":{"standbyPoolProfile":{"id":"x"},"containerGroupProfile":{"id":"` +
				aciProfileRefID + `"}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)

			status, _ := s.rest(t, http.MethodPut, aciProfilesBase+"/prof1"+aciProfileAPI, two)
			require.Equal(t, http.StatusCreated, status)

			status, _ = s.rest(t, http.MethodPut, aciProfilesBase+"/prof1"+aciProfileAPI,
				`{"location":"westus","properties":{"osType":"Linux","restartPolicy":"Never","containers":[`+
					`{"name":"c1","properties":{"image":"img:2","resources":{"requests":{"cpu":1,"memoryInGB":1.5}}}},`+
					`{"name":"side","properties":{"image":"side:1","resources":{"requests":{"cpu":1,"memoryInGB":1}}}}]}}`)
			require.Equal(t, http.StatusOK, status)

			status, body := s.rest(t, http.MethodPut, aciGroupsBase+"/g1"+aciProfileAPI, tt.body)
			require.Equal(t, tt.wantStatus, status, "%v", body)

			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, body["error"].(map[string]any)["code"])

				return
			}

			tt.check(t, body)

			status, got := s.rest(t, http.MethodGet, aciGroupsBase+"/g1"+aciProfileAPI, "")
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, body["properties"].(map[string]any)["containerGroupProfile"],
				got["properties"].(map[string]any)["containerGroupProfile"])

			snap := s.handler.Snapshot(t.Context())
			dst := newACIStack(t)
			require.NoError(t, dst.handler.Restore(t.Context(), snap))

			status, got = dst.rest(t, http.MethodGet, aciGroupsBase+"/g1"+aciProfileAPI, "")
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, body["properties"].(map[string]any)["containerGroupProfile"],
				got["properties"].(map[string]any)["containerGroupProfile"])
		})
	}
}

func TestACIContainerGroupProfileReferenceRevisionInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		revision   string
		wantStatus int
	}{
		{name: "valid", revision: `1`, wantStatus: http.StatusOK},
		{name: "null means latest", revision: `null`, wantStatus: http.StatusOK},
		{name: "zero", revision: `0`, wantStatus: http.StatusNotFound},
		{name: "negative", revision: `-1`, wantStatus: http.StatusNotFound},
		{name: "huge", revision: `99999999999999999999999`, wantStatus: http.StatusBadRequest},
		{name: "string", revision: `"1"`, wantStatus: http.StatusBadRequest},
		{name: "fraction", revision: `1.5`, wantStatus: http.StatusBadRequest},
		{name: "object", revision: `{}`, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)

			status, _ := s.rest(t, http.MethodPut, aciProfilesBase+"/prof1"+aciProfileAPI,
				`{"location":"westus","properties":{"osType":"Linux","containers":[{"name":"c1","properties":`+
					`{"image":"img:1","resources":{"requests":{"cpu":1,"memoryInGB":1}}}}]}}`)
			require.Equal(t, http.StatusCreated, status)

			status, body := s.rest(t, http.MethodPut, aciGroupsBase+"/g1"+aciProfileAPI,
				refGroup(`{"id":"`+aciProfileRefID+`","revision":`+tt.revision+`}`, ""))
			assert.Equal(t, tt.wantStatus, status, "%v", body)
		})
	}
}

func TestACIContainerGroupProfileReferenceKeepsSecretsOut(t *testing.T) {
	t.Parallel()

	const secret = "s3cr3t-value-9f2"

	s := newACIStack(t)

	status, pbody := s.rest(t, http.MethodPut, aciProfilesBase+"/prof1"+aciProfileAPI,
		`{"location":"westus","properties":{"osType":"Linux","imageRegistryCredentials":[`+
			`{"server":"r.io","username":"u","password":"`+secret+`"}],"containers":[{"name":"c1","properties":`+
			`{"image":"img:1","environmentVariables":[{"name":"K","secureValue":"`+secret+`"}],`+
			`"resources":{"requests":{"cpu":1,"memoryInGB":1}}}}]}}`)
	require.Equal(t, http.StatusCreated, status)
	assert.NotContains(t, fmt.Sprint(pbody), secret)

	status, body := s.rest(t, http.MethodPut, aciGroupsBase+"/g1"+aciProfileAPI,
		refGroup(`{"id":"`+aciProfileRefID+`"}`, ""))
	require.Equal(t, http.StatusOK, status, "%v", body)
	assert.NotContains(t, fmt.Sprint(body), secret)

	status, got := s.rest(t, http.MethodGet, aciGroupsBase+"/g1"+aciProfileAPI, "")
	require.Equal(t, http.StatusOK, status)
	assert.NotContains(t, fmt.Sprint(got), secret)
}
