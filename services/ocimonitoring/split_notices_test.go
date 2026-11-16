package ocimonitoring_test

import (
	"context"
	"encoding/json/v2"
	"sync"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	oci "github.com/blackbirdworks/gopherstack/services/ocimonitoring"
)

type alarmMsg struct {
	DedupeKey        string `json:"dedupeKey"`
	Type             string `json:"type"`
	NotificationType string `json:"notificationType"`
	AlarmMetaData    []struct {
		Status             string              `json:"status"`
		Dimensions         []map[string]string `json:"dimensions"`
		TotalMetricsFiring int                 `json:"totalMetricsFiring"`
	} `json:"alarmMetaData"`
}

type recorder struct {
	bodies []string
	mu     sync.Mutex
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.bodies...)
}

// recordMessages delivers alarm messages straight to the test: delivery runs inside the request that swept the
// alarms, so assertions never wait on a network hop.
func recordMessages(e *env) *recorder {
	r := &recorder{}

	e.backend.SetPublisher(func(_ context.Context, _, _, body string, _ bool) error {
		r.mu.Lock()
		r.bodies = append(r.bodies, body)
		r.mu.Unlock()

		return nil
	})
	e.backend.SetTopicChecker(func(string) bool { return true })

	return r
}

func decodeMsgs(t *testing.T, raw []string) []alarmMsg {
	t.Helper()

	out := make([]alarmMsg, 0, len(raw))

	for _, b := range raw {
		var m alarmMsg

		require.NoError(t, json.Unmarshal([]byte(b), &m, jsonx.V1()))
		out = append(out, m)
	}

	return out
}

func TestAlarmMessagesPerMetricStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hosts     map[string]bool
		name      string
		kind      string
		messages  int
		total     int
		perStream bool
	}{
		{map[string]bool{"a": true}, "grouped", "Grouped messages across metric streams", 1, 1, false},
		{map[string]bool{"a": true, "b": true}, "split", "Split messages per metric stream", 2, 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			rec := recordMessages(e)
			a := e.mkAlarm(t, topicOCID, func(d *monitoring.CreateAlarmDetails) {
				d.IsNotificationsPerMetricDimensionEnabled = new(tt.perStream)
			})

			e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 95, 0})
			e.post(t, ns, metric, map[string]string{"host": "b"}, pt{0, 96, 0})
			assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t))

			msgs := decodeMsgs(t, rec.all())
			require.Len(t, msgs, tt.messages)

			keys := map[string]bool{}
			hosts := map[string]bool{}

			for _, m := range msgs {
				assert.Equal(t, "OK_TO_FIRING", m.Type)
				require.Len(t, m.AlarmMetaData, 1)
				assert.Equal(t, tt.kind, m.NotificationType)
				assert.Equal(t, tt.total, m.AlarmMetaData[0].TotalMetricsFiring)

				keys[m.DedupeKey] = true

				for _, d := range m.AlarmMetaData[0].Dimensions {
					hosts[d["host"]] = true
				}
			}

			assert.Len(t, keys, tt.messages, "one dedupe key per message")
			assert.Equal(t, tt.hosts, hosts)

			if tt.perStream {
				assert.NotContains(t, keys, *a.Id)
			}

			e.clock.Advance(5 * time.Minute)
			assert.Equal(t, monitoring.AlarmStatusSummaryStatusOk, e.status(t))

			msgs = decodeMsgs(t, rec.all()[tt.messages:])
			require.Len(t, msgs, tt.messages)

			for _, m := range msgs {
				assert.Equal(t, "FIRING_TO_OK", m.Type)
				assert.Equal(t, 0, m.AlarmMetaData[0].TotalMetricsFiring)
			}
		})
	}
}

func TestSplitNewStreamAtRepeatIsNotDuplicated(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	rec := recordMessages(e)
	e.mkAlarm(t, topicOCID, func(d *monitoring.CreateAlarmDetails) {
		d.IsNotificationsPerMetricDimensionEnabled = new(true)
		d.RepeatNotificationDuration = new("PT2M")
		d.Query = new(metric + "[3m].max() > 80")
	})

	e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 95, 0})
	e.status(t)
	require.Len(t, rec.all(), 1)

	e.clock.Advance(2 * time.Minute)

	now, val := e.clock.Now(), 96.0
	_, err := e.backend.PostMetricData(oci.PostMetricDataIn{MetricData: []oci.MetricDataIn{{
		Namespace: ns, CompartmentID: testCompartment, Name: metric, Dimensions: map[string]string{"host": "b"},
		Datapoints: []oci.DatapointIn{{Timestamp: &now, Value: &val}},
	}}})
	require.NoError(t, err)
	e.status(t)

	perHost := map[string][]string{}

	for _, m := range decodeMsgs(t, rec.all()[1:]) {
		require.Len(t, m.AlarmMetaData, 1)

		for _, d := range m.AlarmMetaData[0].Dimensions {
			perHost[d["host"]] = append(perHost[d["host"]], m.Type)
		}
	}

	assert.Equal(t, map[string][]string{"a": {"REPEAT"}, "b": {"OK_TO_FIRING"}}, perHost)
}
