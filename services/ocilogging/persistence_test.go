package ocilogging_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocilogging"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	g := e.mkGroup(t, "persist")
	l := e.mkLog(t, g, "app", nil)
	e.put(t, l, 0, `{"msg":"kept"}`)

	snap := e.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := oci.NewInMemoryBackend()
	restored.SetClock(e.clock.Now)
	require.NoError(t, restored.Restore(t.Context(), snap))

	got, _, err := restored.GetLog(*g.Id, *l.Id)
	require.NoError(t, err)
	assert.Equal(t, "app", got.Name)

	now := e.clock.Now()
	start, end := now.Add(-time.Hour), now.Add(time.Hour)

	out, _, err := restored.SearchLogs(oci.SearchIn{
		TimeStart: &start, TimeEnd: &end, SearchQuery: fmt.Sprintf(`search "%s/%s/%s"`, testCompartment, *g.Id, *l.Id),
	}, "", 0)
	require.NoError(t, err)
	assert.Equal(t, 1, out.Summary.ResultCount)

	e.backend.Reset()

	_, _, err = e.backend.GetLog(*g.Id, *l.Id)
	require.Error(t, err)
}

func TestRestoreValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"null group", `{"version":1,"logGroups":{"a":null}}`, true},
		{"null log", `{"version":1,"logs":{"a":null}}`, true},
		{"null agent", `{"version":1,"agentConfigurations":{"a":null}}`, true},
		{"null workreq", `{"version":1,"workRequests":{"a":null}}`, true},
		{"null entry", `{"version":1,"entries":{"a":[null]}}`, true},
		{"incompatible version resets", `{"version":99}`, false},
		{"empty ok", `{"version":1}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := oci.NewInMemoryBackend().Restore(t.Context(), []byte(tt.data))
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestEmitServiceLog(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	g := e.mkGroup(t, "grp")
	svc := e.mkLog(t, g, "fn", func(d *logging.CreateLogDetails) {
		d.LogType = logging.CreateLogDetailsLogTypeService
		d.Configuration = svcConfig("functions", "app-1", "invoke")
	})
	off := e.mkLog(t, g, "fn-off", func(d *logging.CreateLogDetails) {
		d.LogType = logging.CreateLogDetailsLogTypeService
		d.IsEnabled = new(false)
		d.Configuration = svcConfig("functions", "app-2", "invoke")
	})

	tests := []struct {
		name     string
		resource string
		want     int
	}{
		{"matching log", "app-1", 1},
		{"other resource", "app-3", 0},
		{"disabled log", "app-2", 0},
	}

	for _, tt := range tests {
		got := e.backend.EmitServiceLog("functions", tt.resource, "invoke", oci.ServiceEvent{
			Type: "com.oraclecloud.functions.invoke", Data: map[string]any{"msg": "ran"},
		})
		assert.Equal(t, tt.want, got, tt.name)
	}

	assert.Zero(t, e.backend.EmitServiceLog("functions", "app-1", "other", oci.ServiceEvent{}))

	resp := e.query(
		t,
		fmt.Sprintf(`search "%s/%s/%s" | where data.msg = 'ran'`, testCompartment, *g.Id, *svc.Id),
		nil,
		nil,
	)
	assert.Equal(t, []string{"ran"}, labels(resp))

	resp = e.query(t, fmt.Sprintf(`search "%s/%s/%s"`, testCompartment, *g.Id, *off.Id), nil, nil)
	assert.Empty(t, resp.Results)
}

func TestErrorEnvelope(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		code   string
		status int
	}{
		{
			"missing group",
			http.MethodGet,
			"/20200531/logGroups/ocid1.loggroup.oc1..nope",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{"bad json", http.MethodPost, "/20200531/logGroups", "{", "CannotParseRequest", 400},
		{"no body", http.MethodPost, "/20200531/logGroups", "", "MissingParameter", 400},
		{"no compartment", http.MethodPost, "/20200531/logGroups", `{"displayName":"x"}`, "MissingParameter", 400},
		{"search no body", http.MethodPost, "/20190909/search", "", "MissingParameter", 400},
		{"search alias", http.MethodPost, "/20190909/actions/search", `{"searchQuery":"x"}`, "MissingParameter", 400},
		{
			"missing work request",
			http.MethodGet,
			"/20200531/workRequests/ocid1.loggingworkrequest.oc1..nope/errors",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{"unknown path", http.MethodGet, "/20200531/zzz", "", "NotFound", 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := rawDo(t, tt.method, e.base+tt.path, tt.body)
			assert.Equal(t, tt.status, status)
			assert.Contains(t, body, `"code":"`+tt.code+`"`)
			assert.Contains(t, body, `"message"`)
		})
	}
}

func TestWorkRequestSubresources(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	resp, err := e.mgmt.CreateLogGroup(t.Context(), logging.CreateLogGroupRequest{
		CompartmentId: new(testCompartment), DisplayName: new("g"),
	})
	require.NoError(t, err)

	_, err = e.mgmt.DeleteWorkRequest(
		t.Context(),
		logging.DeleteWorkRequestRequest{WorkRequestId: resp.OpcWorkRequestId},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "in-progress work request")

	logs, err := e.mgmt.ListWorkRequestLogs(
		t.Context(),
		logging.ListWorkRequestLogsRequest{WorkRequestId: resp.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Len(t, logs.Items, 1)

	e.settle()

	logs, err = e.mgmt.ListWorkRequestLogs(
		t.Context(),
		logging.ListWorkRequestLogsRequest{WorkRequestId: resp.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Len(t, logs.Items, 2)

	errs, err := e.mgmt.ListWorkRequestErrors(
		t.Context(),
		logging.ListWorkRequestErrorsRequest{WorkRequestId: resp.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Empty(t, errs.Items)

	list, err := e.mgmt.ListWorkRequests(t.Context(), logging.ListWorkRequestsRequest{
		CompartmentId: new(testCompartment), Status: logging.ListWorkRequestsStatusSucceeded,
	})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)
}

func TestAdminKeyEndpoint(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	pub, err := json.Marshal(e.kp.pubPEM)
	require.NoError(t, err)

	status, _ := rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys",
		`{"tenancy":"`+testTenancy+`","user":"`+testUser+`","publicKey":`+string(pub)+`}`)
	assert.Equal(t, http.StatusCreated, status)

	status, body := rawDo(t, http.MethodGet, e.base+"/_gopherstack/oci/keys", "")
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, testTenancy)
}
