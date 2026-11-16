package ocimonitoring_test

import (
	"encoding/json/v2"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
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
			o := newONS(t, e)
			a := e.mkAlarm(t, o.topicID, func(d *monitoring.CreateAlarmDetails) {
				d.IsNotificationsPerMetricDimensionEnabled = new(tt.perStream)
			})

			e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 95, 0})
			e.post(t, ns, metric, map[string]string{"host": "b"}, pt{0, 96, 0})
			assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t))

			keys := map[string]bool{}
			hosts := map[string]bool{}

			for range tt.messages {
				var m alarmMsg

				require.NoError(t, json.Unmarshal([]byte(o.sink.next(t).body), &m, jsonx.V1()))
				assert.Equal(t, "OK_TO_FIRING", m.Type)
				require.Len(t, m.AlarmMetaData, 1)
				assert.Equal(t, tt.kind, m.NotificationType)
				assert.Equal(t, tt.total, m.AlarmMetaData[0].TotalMetricsFiring)

				keys[m.DedupeKey] = true

				for _, d := range m.AlarmMetaData[0].Dimensions {
					hosts[d["host"]] = true
				}
			}

			assert.True(t, o.sink.none(t))
			assert.Len(t, keys, tt.messages, "one dedupe key per message")
			assert.Equal(t, tt.hosts, hosts)

			if tt.perStream {
				assert.NotContains(t, keys, *a.Id)
			}

			e.clock.Advance(5 * time.Minute)
			assert.Equal(t, monitoring.AlarmStatusSummaryStatusOk, e.status(t))

			for range tt.messages {
				var m alarmMsg

				require.NoError(t, json.Unmarshal([]byte(o.sink.next(t).body), &m, jsonx.V1()))
				assert.Equal(t, "FIRING_TO_OK", m.Type)
				assert.Equal(t, 0, m.AlarmMetaData[0].TotalMetricsFiring)
			}

			assert.True(t, o.sink.none(t))
		})
	}
}
