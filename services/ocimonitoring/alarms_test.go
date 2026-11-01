package ocimonitoring_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) alarmDetails(topic string, mod func(*monitoring.CreateAlarmDetails)) monitoring.CreateAlarmDetails {
	d := monitoring.CreateAlarmDetails{
		DisplayName: new("cpu-high"), CompartmentId: new(testCompartment), MetricCompartmentId: new(testCompartment),
		Namespace: new(ns), Query: new(metric + "[1m].max() > 80"), Severity: monitoring.AlarmSeverityCritical,
		Destinations: []string{topic}, IsEnabled: new(true), Body: new("CPU is high"),
	}
	if mod != nil {
		mod(&d)
	}

	return d
}

func (e *env) mkAlarm(t *testing.T, topic string, mod func(*monitoring.CreateAlarmDetails)) monitoring.Alarm {
	t.Helper()

	resp, err := e.mon.CreateAlarm(
		t.Context(),
		monitoring.CreateAlarmRequest{CreateAlarmDetails: e.alarmDetails(topic, mod)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, *resp.Etag)

	return resp.Alarm
}

func (e *env) status(t *testing.T) monitoring.AlarmStatusSummaryStatusEnum {
	t.Helper()

	resp, err := e.mon.ListAlarmsStatus(
		t.Context(),
		monitoring.ListAlarmsStatusRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)

	return resp.Items[0].Status
}

const topicOCID = "ocid1.onstopic.oc1..topic"

func TestAlarmValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mod  func(*monitoring.CreateAlarmDetails)
		code string
	}{
		{"valid", nil, ""},
		{
			"absent without threshold",
			func(d *monitoring.CreateAlarmDetails) { d.Query = new(metric + "[1m].absent()") },
			"",
		},
		{
			"query without threshold",
			func(d *monitoring.CreateAlarmDetails) { d.Query = new(metric + "[1m].mean()") },
			"InvalidParameter",
		},
		{"unparsable query", func(d *monitoring.CreateAlarmDetails) { d.Query = new("nonsense") }, "InvalidParameter"},
		{
			"in range threshold",
			func(d *monitoring.CreateAlarmDetails) { d.Query = new(metric + "[5m].mean() in (1, 5)") },
			"",
		},
		{"blank name", func(d *monitoring.CreateAlarmDetails) { d.DisplayName = new("") }, "MissingParameter"},
		{"no destinations", func(d *monitoring.CreateAlarmDetails) { d.Destinations = []string{} }, "MissingParameter"},
		{
			"bad destination",
			func(d *monitoring.CreateAlarmDetails) { d.Destinations = []string{"ocid1.queue.oc1..q"} },
			"InvalidParameter",
		},
		{"two topics", func(d *monitoring.CreateAlarmDetails) {
			d.Destinations = []string{topicOCID, "ocid1.onstopic.oc1..other"}
		}, "InvalidParameter"},
		{"topic and stream", func(d *monitoring.CreateAlarmDetails) {
			d.Destinations = []string{topicOCID, "ocid1.stream.oc1..s"}
		}, ""},
		{"pretty json with stream", func(d *monitoring.CreateAlarmDetails) {
			d.Destinations = []string{"ocid1.stream.oc1..s"}
			d.MessageFormat = monitoring.CreateAlarmDetailsMessageFormatPrettyJson
		}, "InvalidParameter"},
		{
			"pending too long",
			func(d *monitoring.CreateAlarmDetails) { d.PendingDuration = new("PT2H") },
			"InvalidParameter",
		},
		{
			"pending not iso",
			func(d *monitoring.CreateAlarmDetails) { d.PendingDuration = new("5 minutes") },
			"InvalidParameter",
		},
		{"pending ok", func(d *monitoring.CreateAlarmDetails) { d.PendingDuration = new("PT10M") }, ""},
		{
			"repeat too short",
			func(d *monitoring.CreateAlarmDetails) { d.RepeatNotificationDuration = new("PT30S") },
			"InvalidParameter",
		},
		{"repeat ok", func(d *monitoring.CreateAlarmDetails) { d.RepeatNotificationDuration = new("P2D") }, ""},
		{"resolution 5m", func(d *monitoring.CreateAlarmDetails) { d.Resolution = new("5m") }, "InvalidParameter"},
		{"suppression reversed", func(d *monitoring.CreateAlarmDetails) {
			d.Suppression = &monitoring.Suppression{
				TimeSuppressFrom:  &common.SDKTime{Time: epoch().Add(time.Hour)},
				TimeSuppressUntil: &common.SDKTime{Time: epoch()},
			}
		}, "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			_, err := e.mon.CreateAlarm(t.Context(), monitoring.CreateAlarmRequest{
				CreateAlarmDetails: e.alarmDetails(topicOCID, tt.mod),
			})

			if tt.code == "" {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestAlarmCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mkAlarm(t, topicOCID, func(d *monitoring.CreateAlarmDetails) {
		d.FreeformTags = map[string]string{"k": "v"}
		d.NotificationTitle = new("custom title")
		d.RuleName = new("rule-1")
	})

	assert.Equal(t, monitoring.AlarmLifecycleStateActive, a.LifecycleState)
	assert.Equal(t, "PT1M", *a.PendingDuration)
	assert.Equal(t, "1m", *a.Resolution)
	assert.Equal(t, monitoring.AlarmMessageFormatRaw, a.MessageFormat)
	assert.Equal(t, "custom title", *a.NotificationTitle)

	got, err := e.mon.GetAlarm(t.Context(), monitoring.GetAlarmRequest{AlarmId: a.Id})
	require.NoError(t, err)
	assert.Equal(t, a.Query, got.Query)
	assert.Equal(t, map[string]string{"k": "v"}, got.FreeformTags)

	_, err = e.mon.UpdateAlarm(t.Context(), monitoring.UpdateAlarmRequest{
		AlarmId: a.Id, IfMatch: new("stale"), Body: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.mon.UpdateAlarm(t.Context(), monitoring.UpdateAlarmRequest{
		AlarmId: a.Id, IfMatch: got.Etag,
		DisplayName: new("renamed"), Severity: monitoring.AlarmSeverityWarning, IsEnabled: new(false),
		Query: new(metric + "[1m].min() < 5"), MessageFormat: monitoring.UpdateAlarmDetailsMessageFormatPrettyJson,
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *upd.DisplayName)
	assert.Equal(t, monitoring.AlarmSeverityWarning, upd.Severity)
	assert.False(t, *upd.IsEnabled)
	assert.NotEqual(t, *got.Etag, *upd.Etag)

	_, err = e.mon.UpdateAlarm(t.Context(), monitoring.UpdateAlarmRequest{
		AlarmId: a.Id, Query: new(metric + "[1m].mean()"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	list, err := e.mon.ListAlarms(t.Context(), monitoring.ListAlarmsRequest{
		CompartmentId: new(testCompartment), DisplayName: new("renamed"),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	_, err = e.mon.ChangeAlarmCompartment(t.Context(), monitoring.ChangeAlarmCompartmentRequest{
		AlarmId:       a.Id,
		CompartmentId: new("ocid1.compartment.oc1..other"),
	})
	require.NoError(t, err)

	list, err = e.mon.ListAlarms(t.Context(), monitoring.ListAlarmsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, list.Items)

	_, err = e.mon.DeleteAlarm(t.Context(), monitoring.DeleteAlarmRequest{AlarmId: a.Id})
	require.NoError(t, err)

	_, err = e.mon.GetAlarm(t.Context(), monitoring.GetAlarmRequest{AlarmId: a.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mon.DeleteAlarm(t.Context(), monitoring.DeleteAlarmRequest{AlarmId: a.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestAlarmListSorting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	seeds := []struct {
		name string
		sev  monitoring.AlarmSeverityEnum
	}{
		{"b", monitoring.AlarmSeverityInfo},
		{"a", monitoring.AlarmSeverityCritical},
		{"c", monitoring.AlarmSeverityWarning},
	}

	for _, s := range seeds {
		e.mkAlarm(t, topicOCID, func(d *monitoring.CreateAlarmDetails) {
			d.DisplayName, d.Severity = new(s.name), s.sev
		})
	}

	tests := []struct {
		name string
		req  monitoring.ListAlarmsRequest
		want []string
	}{
		{
			"by name",
			monitoring.ListAlarmsRequest{SortBy: monitoring.ListAlarmsSortByDisplayname},
			[]string{"a", "b", "c"},
		},
		{"by name desc", monitoring.ListAlarmsRequest{
			SortBy: monitoring.ListAlarmsSortByDisplayname, SortOrder: monitoring.ListAlarmsSortOrderDesc,
		}, []string{"c", "b", "a"}},
		{
			"by severity",
			monitoring.ListAlarmsRequest{SortBy: monitoring.ListAlarmsSortBySeverity},
			[]string{"a", "c", "b"},
		},
	}

	for _, tt := range tests {
		tt.req.CompartmentId = new(testCompartment)

		resp, err := e.mon.ListAlarms(t.Context(), tt.req)
		require.NoError(t, err)

		var got []string
		for _, a := range resp.Items {
			got = append(got, *a.DisplayName)
		}

		assert.Equal(t, tt.want, got, tt.name)
	}
}

func TestAlarmFiringDeliversToNotifications(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	o := newONS(t, e)
	a := e.mkAlarm(t, o.topicID, nil)
	dims := map[string]string{"host": "a"}

	assert.Equal(t, monitoring.AlarmStatusSummaryStatusOk, e.status(t))
	assert.True(t, o.sink.none(t), "an OK alarm sends nothing")

	e.post(t, ns, metric, dims, pt{0, 95, 0})
	assert.Equal(
		t,
		monitoring.AlarmStatusSummaryStatusFiring,
		e.status(t),
		"PT1M fires on the first breaching evaluation",
	)

	msg := o.sink.next(t)
	assert.Equal(t, "alarms", msg.header.Get("X-Oci-Ns-Topicname"))
	assert.Equal(t, o.topicID, msg.header.Get("X-Oci-Ns-Topicocid"))
	assert.Contains(t, msg.header.Get("Content-Type"), "application/json")

	var n struct {
		Type          string `json:"type"`
		Severity      string `json:"severity"`
		Title         string `json:"title"`
		Body          string `json:"body"`
		DedupeKey     string `json:"dedupeKey"`
		AlarmMetaData []struct {
			ID                 string               `json:"id"`
			Status             string               `json:"status"`
			Query              string               `json:"query"`
			Dimensions         []map[string]string  `json:"dimensions"`
			MetricValues       []map[string]float64 `json:"metricValues"`
			TotalMetricsFiring int                  `json:"totalMetricsFiring"`
		} `json:"alarmMetaData"`
	}

	require.NoError(t, json.Unmarshal([]byte(msg.body), &n))
	assert.Equal(t, "OK_TO_FIRING", n.Type)
	assert.Equal(t, "CRITICAL", n.Severity)
	assert.Equal(t, "cpu-high", n.Title)
	assert.Equal(t, "CPU is high", n.Body)
	assert.Equal(t, *a.Id, n.DedupeKey)
	require.Len(t, n.AlarmMetaData, 1)
	assert.Equal(t, "FIRING", n.AlarmMetaData[0].Status)
	assert.Equal(t, 1, n.AlarmMetaData[0].TotalMetricsFiring)
	assert.Equal(t, []map[string]string{dims}, n.AlarmMetaData[0].Dimensions)
	assert.InDelta(t, 95, n.AlarmMetaData[0].MetricValues[0][metric], 1e-9)

	assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t))
	assert.True(t, o.sink.none(t), "no repeat without repeatNotificationDuration")

	e.clock.Advance(5 * time.Minute)
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusOk, e.status(t), "no data in the window clears the alarm")

	var ok struct {
		Type string `json:"type"`
	}

	require.NoError(t, json.Unmarshal([]byte(o.sink.next(t).body), &ok))
	assert.Equal(t, "FIRING_TO_OK", ok.Type)

	hist, err := e.mon.GetAlarmHistory(t.Context(), monitoring.GetAlarmHistoryRequest{AlarmId: a.Id})
	require.NoError(t, err)
	assert.True(t, *hist.IsEnabled)

	summaries := make([]string, 0, len(hist.Entries))
	for _, h := range hist.Entries {
		summaries = append(summaries, *h.Summary)
	}

	assert.Equal(t, []string{
		"The alarm state is OK",
		"The alarm state is FIRING", "State transitioned from Ok to Firing",
		"The alarm state is OK", "State transitioned from Firing to Ok",
	}, summaries)

	trans, err := e.mon.GetAlarmHistory(t.Context(), monitoring.GetAlarmHistoryRequest{
		AlarmId: a.Id, AlarmHistorytype: monitoring.GetAlarmHistoryAlarmHistorytypeStateTransitionHistory,
	})
	require.NoError(t, err)
	require.Len(t, trans.Entries, 2)
	assert.NotNil(t, trans.Entries[0].TimestampTriggered)

	state, err := e.mon.GetAlarmHistory(t.Context(), monitoring.GetAlarmHistoryRequest{
		AlarmId: a.Id, AlarmHistorytype: monitoring.GetAlarmHistoryAlarmHistorytypeStateHistory,
	})
	require.NoError(t, err)
	assert.Len(t, state.Entries, 3)
}

func TestAlarmPendingDuration(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	o := newONS(t, e)
	e.mkAlarm(t, o.topicID, func(d *monitoring.CreateAlarmDetails) { d.PendingDuration = new("PT3M") })

	dims := map[string]string{"host": "a"}

	steps := []struct {
		want    monitoring.AlarmStatusSummaryStatusEnum
		advance time.Duration
	}{
		{monitoring.AlarmStatusSummaryStatusOk, 0},
		{monitoring.AlarmStatusSummaryStatusOk, time.Minute},
		{monitoring.AlarmStatusSummaryStatusFiring, time.Minute},
	}

	for i, s := range steps {
		e.clock.Advance(s.advance)
		e.post(t, ns, metric, dims, pt{0, 90, 0})
		assert.Equal(t, s.want, e.status(t), "step %d", i)
	}

	msg := o.sink.next(t)
	assert.Contains(t, msg.body, "OK_TO_FIRING")
	assert.True(t, o.sink.none(t))
}

func TestAlarmPendingResetsWhenConditionClears(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	o := newONS(t, e)
	e.mkAlarm(t, o.topicID, func(d *monitoring.CreateAlarmDetails) { d.PendingDuration = new("PT3M") })

	dims := map[string]string{"host": "a"}

	e.post(t, ns, metric, dims, pt{0, 90, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusOk, e.status(t))

	e.clock.Advance(90 * time.Second)
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusOk, e.status(t), "the window is empty, pending resets")

	e.clock.Advance(30 * time.Second)
	e.post(t, ns, metric, dims, pt{0, 90, 0})
	e.clock.Advance(time.Minute)
	e.post(t, ns, metric, dims, pt{0, 90, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusOk, e.status(t), "only one minute of breach since the reset")
	assert.True(t, o.sink.none(t))
}

func TestAlarmRepeatNotification(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	o := newONS(t, e)
	e.mkAlarm(t, o.topicID, func(d *monitoring.CreateAlarmDetails) { d.RepeatNotificationDuration = new("PT2M") })

	dims := map[string]string{"host": "a"}
	e.post(t, ns, metric, dims, pt{0, 90, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t))
	assert.Contains(t, o.sink.next(t).body, "OK_TO_FIRING")

	e.clock.Advance(time.Minute)
	e.post(t, ns, metric, dims, pt{0, 90, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t))
	assert.True(t, o.sink.none(t), "repeat interval not reached")

	e.clock.Advance(time.Minute)
	e.post(t, ns, metric, dims, pt{0, 90, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t))
	assert.Contains(t, o.sink.next(t).body, "REPEAT")
}

func TestAlarmSuppressionAndDisable(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	o := newONS(t, e)
	a := e.mkAlarm(t, o.topicID, func(d *monitoring.CreateAlarmDetails) {
		d.Suppression = &monitoring.Suppression{
			TimeSuppressFrom:  &common.SDKTime{Time: epoch().Add(-time.Hour)},
			TimeSuppressUntil: &common.SDKTime{Time: epoch().Add(time.Hour)},
			Description:       new("maintenance"),
		}
	})

	e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 90, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusSuspended, e.status(t))
	assert.True(t, o.sink.none(t), "suppressed alarms send nothing")

	got, err := e.mon.GetAlarm(t.Context(), monitoring.GetAlarmRequest{AlarmId: a.Id})
	require.NoError(t, err)
	assert.Equal(t, "maintenance", *got.Suppression.Description)

	_, err = e.mon.UpdateAlarm(t.Context(), monitoring.UpdateAlarmRequest{
		AlarmId: a.Id, IsEnabled: new(false),
	})
	require.NoError(t, err)

	e.clock.Advance(3 * time.Hour)
	e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 95, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t), "state is unchanged while disabled")
	assert.True(t, o.sink.none(t))
}

func TestAlarmAbsentAndGrouping(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	o := newONS(t, e)
	e.mkAlarm(t, o.topicID, func(d *monitoring.CreateAlarmDetails) {
		d.Query = new(metric + "[1m].absent()")
		d.MessageFormat = monitoring.CreateAlarmDetailsMessageFormatOnsOptimized
	})

	assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t), "no data at all fires absent()")

	msg := o.sink.next(t)
	assert.Contains(t, msg.header.Get("Content-Type"), "text/plain")
	assert.Contains(t, msg.body, "Alarm: cpu-high")
	assert.Contains(t, msg.body, "Status: FIRING")

	e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 1, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusOk, e.status(t))
	assert.Contains(t, o.sink.next(t).body, "Status: OK")
}

func TestAlarmGroupedThreshold(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	o := newONS(t, e)
	e.mkAlarm(t, o.topicID, func(d *monitoring.CreateAlarmDetails) {
		d.Query = new(metric + "[1m]{host =~ \"web*\"}.groupBy(host).sum() >= 100")
	})

	e.post(t, ns, metric, map[string]string{"host": "web1"}, pt{0, 60, 0})
	e.post(t, ns, metric, map[string]string{"host": "web2"}, pt{0, 120, 0})
	e.post(t, ns, metric, map[string]string{"host": "db1"}, pt{0, 500, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t))

	var n struct {
		AlarmMetaData []struct {
			Dimensions         []map[string]string `json:"dimensions"`
			TotalMetricsFiring int                 `json:"totalMetricsFiring"`
		} `json:"alarmMetaData"`
	}

	require.NoError(t, json.Unmarshal([]byte(o.sink.next(t).body), &n))
	assert.Equal(t, 1, n.AlarmMetaData[0].TotalMetricsFiring, "only web2 breaches; db1 is filtered out")
	assert.Equal(t, []map[string]string{{"host": "web2"}}, n.AlarmMetaData[0].Dimensions)
}

func TestAlarmDestinationChecked(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	newONS(t, e)

	_, err := e.mon.CreateAlarm(t.Context(), monitoring.CreateAlarmRequest{
		CreateAlarmDetails: e.alarmDetails("ocid1.onstopic.oc1..ghost", nil),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestAlarmWithoutPublisherStillTransitions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkAlarm(t, topicOCID, nil)
	e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 95, 0})
	assert.Equal(t, monitoring.AlarmStatusSummaryStatusFiring, e.status(t))
}
