package redshiftdata_test

import (
	"maps"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

const statementDelay = 10 * time.Second

func newDelayedHandler(t *testing.T) *redshiftdata.Handler {
	t.Helper()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	b.SetLifecycleDelay(statementDelay)

	return redshiftdata.NewHandler(b)
}

func describeStatus(t *testing.T, h *redshiftdata.Handler, id string) string {
	t.Helper()

	rec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	status, _ := decodeBody(t, rec.Body)["Status"].(string)

	return status
}

func TestStatementLifecycle_Progression(t *testing.T) {
	t.Parallel()

	const delay = statementDelay

	single := map[string]any{"Sql": "select 1"}
	batch := map[string]any{"Sqls": []string{"select 1", "select 2"}}

	cases := []struct {
		body    map[string]any
		name    string
		op      string
		want    string
		advance time.Duration
	}{
		{name: "submitted", op: "ExecuteStatement", body: single, advance: 0, want: "SUBMITTED"},
		{name: "started", op: "ExecuteStatement", body: single, advance: delay / 2, want: "STARTED"},
		{name: "finished", op: "ExecuteStatement", body: single, advance: delay, want: "FINISHED"},
		{name: "batch_started", op: "BatchExecuteStatement", body: batch, advance: delay / 2, want: "STARTED"},
		{name: "batch_finished", op: "BatchExecuteStatement", body: batch, advance: delay, want: "FINISHED"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newDelayedHandler(t)
				body := map[string]any{"ClusterIdentifier": "c"}
				maps.Copy(body, tc.body)
				rec := doRequest(t, h, tc.op, body)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				created := decodeBody(t, rec.Body)
				assert.Equal(t, "SUBMITTED", created["Status"])
				id, _ := created["Id"].(string)

				time.Sleep(tc.advance)

				assert.Equal(t, tc.want, describeStatus(t, h, id))
			})
		})
	}
}

func TestStatementLifecycle_ResultAndCancel(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newDelayedHandler(t)
		rec := doRequest(t, h, "ExecuteStatement", map[string]any{"Sql": "select 1", "ClusterIdentifier": "c"})
		id, _ := decodeBody(t, rec.Body)["Id"].(string)

		rec = doRequest(t, h, "GetStatementResult", map[string]any{"Id": id})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "ValidationException")

		rec = doRequest(t, h, "ListSessions", map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code)

		rec = doRequest(t, h, "GetStatementResult", map[string]any{"Id": id, "WaitTimeSeconds": 30})
		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		rec = doRequest(t, h, "ExecuteStatement", map[string]any{"Sql": "select 2", "ClusterIdentifier": "c"})
		id2, _ := decodeBody(t, rec.Body)["Id"].(string)
		rec = doRequest(t, h, "CancelStatement", map[string]any{"Id": id2})
		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "ABORTED", describeStatus(t, h, id2))
	})
}

func TestStatementLifecycle_WaitTimeSeconds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		want     string
		wait     int
		wantCode int
	}{
		{name: "waits_to_finish", wait: 30, wantCode: http.StatusOK, want: "FINISHED"},
		{name: "too_short_returns_current", wait: 2, wantCode: http.StatusOK, want: "SUBMITTED"},
		{name: "over_max", wait: 31, wantCode: http.StatusBadRequest},
		{name: "negative", wait: -1, wantCode: http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newDelayedHandler(t)
				rec := doRequest(t, h, "ExecuteStatement", map[string]any{
					"Sql": "select 1", "ClusterIdentifier": "c", "WaitTimeSeconds": tc.wait,
				})
				require.Equal(t, tc.wantCode, rec.Code, rec.Body.String())

				if tc.want != "" {
					assert.Equal(t, tc.want, decodeBody(t, rec.Body)["Status"])
				}
			})
		})
	}
}

func TestStatementLifecycle_BusySession(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newDelayedHandler(t)
		rec := doRequest(t, h, "ExecuteStatement", map[string]any{
			"Sql": "select 1", "ClusterIdentifier": "c", "SessionKeepAliveSeconds": 600,
		})
		body := decodeBody(t, rec.Body)
		sid, _ := body["SessionId"].(string)
		id, _ := body["Id"].(string)

		list := func() map[string]any {
			listRec := doRequest(t, h, "ListSessions", map[string]any{"SessionId": sid})
			sessions, _ := decodeBody(t, listRec.Body)["Sessions"].([]any)
			require.Len(t, sessions, 1)
			s, _ := sessions[0].(map[string]any)

			return s
		}

		busy := list()
		assert.Equal(t, "BUSY", busy["Status"])
		assert.Equal(t, id, busy["CurrentStatementId"])

		time.Sleep(10 * time.Second)

		idle := list()
		assert.Equal(t, "AVAILABLE", idle["Status"])
		assert.NotContains(t, idle, "CurrentStatementId")
	})
}

func TestErrorMessageHasNoCodePrefix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": "missing"})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.JSONEq(
		t,
		`{"__type":"ResourceNotFoundException","message":"statement missing not found"}`,
		rec.Body.String(),
	)
}
