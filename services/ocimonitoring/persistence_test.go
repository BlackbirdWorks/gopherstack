package ocimonitoring_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocimonitoring"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mkAlarm(t, topicOCID, nil)
	e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 95, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t))

	snap := e.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := oci.NewInMemoryBackend()
	restored.SetClock(e.clock.Now)
	require.NoError(t, restored.Restore(t.Context(), snap))

	got, _, err := restored.GetAlarm(*a.Id)
	require.NoError(t, err)
	assert.Equal(t, "cpu-high", got.Name)

	st, err := restored.ListAlarmsStatus(oci.AlarmFilter{Compartment: testCompartment}, oci.ListOpts{})
	require.NoError(t, err)
	require.Len(t, st.Data, 1)
	assert.Equal(t, "FIRING", st.Data[0].Status, "evaluation state survives")

	metrics, err := restored.ListMetrics(testCompartment, oci.ListMetricsIn{}, "", 0)
	require.NoError(t, err)
	assert.Len(t, metrics.Data, 1)

	e.backend.Reset()

	_, _, err = e.backend.GetAlarm(*a.Id)
	require.Error(t, err)
}

func TestRestoreValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"null stream", `{"version":1,"streams":{"a":null}}`, true},
		{"null alarm", `{"version":1,"alarms":{"a":null}}`, true},
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

func TestAuthAndErrorEnvelope(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true, realClock: true})

	_, err := e.mon.ListAlarms(t.Context(), monitoring.ListAlarmsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)

	open := newEnv(t, envOpts{enforce: true, realClock: true})
	_, err = open.mon.ListAlarms(t.Context(), monitoring.ListAlarmsRequest{CompartmentId: new(testCompartment)})
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		code   string
		status int
	}{
		{"unsigned rejected", http.MethodGet, "/20180401/alarms?compartmentId=x", "", "NotAuthenticated", 401},
		{"unknown path", http.MethodGet, "/20180401/zzz", "", "NotFound", 404},
	}

	for _, tt := range tests {
		status, body := rawDo(t, tt.method, e.base+tt.path, tt.body)
		assert.Equal(t, tt.status, status, tt.name)
		assert.Contains(t, body, `"code":"`+tt.code+`"`, tt.name)
	}

	plain := newEnv(t, envOpts{})
	plainTests := []struct {
		name   string
		method string
		path   string
		body   string
		code   string
		status int
	}{
		{"missing alarm", http.MethodGet, "/20180401/alarms/ocid1.alarm.oc1..nope", "", "NotAuthorizedOrNotFound", 404},
		{
			"missing alarm history",
			http.MethodGet,
			"/20180401/alarms/ocid1.alarm.oc1..nope/history",
			"",
			"NotAuthorizedOrNotFound",
			404,
		},
		{"bad json", http.MethodPost, "/20180401/alarms", "{", "CannotParseRequest", 400},
		{"no body", http.MethodPost, "/20180401/alarms", "", "MissingParameter", 400},
		{"no compartment on list", http.MethodGet, "/20180401/alarms", "", "MissingParameter", 400},
		{
			"list metrics no compartment",
			http.MethodPost,
			"/20180401/metrics/actions/listMetrics",
			`{}`,
			"MissingParameter",
			400,
		},
	}

	for _, tt := range plainTests {
		status, body := rawDo(t, tt.method, plain.base+tt.path, tt.body)
		assert.Equal(t, tt.status, status, tt.name)
		assert.Contains(t, body, `"code":"`+tt.code+`"`, tt.name)
	}

	a := plain.mkAlarm(t, topicOCID, nil)
	status, body := rawDo(t, http.MethodGet, plain.base+"/20180401/alarms/"+*a.Id+"/history?alarmHistorytype=BOGUS", "")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "InvalidParameter")

	status, _ = rawDo(t, http.MethodGet, plain.base+"/20180401/alarms/"+*a.Id+"/history?timestampLessThan=notatime", "")
	assert.Equal(t, http.StatusBadRequest, status)
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
