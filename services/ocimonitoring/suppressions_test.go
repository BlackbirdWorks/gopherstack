package ocimonitoring_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type suppDetails = monitoring.CreateAlarmSuppressionDetails

const halfHour = 30 * time.Minute

func sdkTime(t time.Time) *common.SDKTime { return &common.SDKTime{Time: t} }

func alarmTarget(id *string) monitoring.AlarmSuppressionAlarmTarget {
	return monitoring.AlarmSuppressionAlarmTarget{AlarmId: id}
}

func (e *env) suppress(
	t *testing.T,
	mod func(*suppDetails),
	target monitoring.AlarmSuppressionTarget,
) monitoring.CreateAlarmSuppressionResponse {
	t.Helper()

	resp, err := e.createSuppression(t, mod, target)
	require.NoError(t, err)

	return resp
}

func (e *env) createSuppression(
	t *testing.T,
	mod func(*suppDetails),
	target monitoring.AlarmSuppressionTarget,
) (monitoring.CreateAlarmSuppressionResponse, error) {
	t.Helper()

	d := suppDetails{
		AlarmSuppressionTarget: target, DisplayName: new("maint"), Level: monitoring.AlarmSuppressionLevelAlarm,
		TimeSuppressFrom: sdkTime(epoch().Add(-time.Hour)), TimeSuppressUntil: sdkTime(epoch().Add(24 * time.Hour)),
	}
	if mod != nil {
		mod(&d)
	}

	return e.mon.CreateAlarmSuppression(t.Context(), monitoring.CreateAlarmSuppressionRequest{
		CreateAlarmSuppressionDetails: d,
	})
}

func TestAlarmSuppressionCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mkAlarm(t, topicOCID, nil)

	created := e.suppress(t, func(d *suppDetails) {
		d.Description = new("planned")
		d.FreeformTags = map[string]string{"k": "v"}
	}, alarmTarget(a.Id))
	s := created.AlarmSuppression

	assert.NotEmpty(t, *created.Etag)
	assert.Equal(t, monitoring.AlarmSuppressionLifecycleStateActive, s.LifecycleState)
	assert.Equal(t, monitoring.AlarmSuppressionLevelAlarm, s.Level)
	assert.Equal(t, testCompartment, *s.CompartmentId)
	assert.Equal(t, *a.Id, *s.AlarmSuppressionTarget.(monitoring.AlarmSuppressionAlarmTarget).AlarmId)

	got, err := e.mon.GetAlarmSuppression(t.Context(), monitoring.GetAlarmSuppressionRequest{AlarmSuppressionId: s.Id})
	require.NoError(t, err)
	assert.Equal(t, "planned", *got.Description)
	assert.Equal(t, "v", got.FreeformTags["k"])

	byAlarm, err := e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{AlarmId: a.Id})
	require.NoError(t, err)
	require.Len(t, byAlarm.Items, 1)
	assert.Equal(t, *s.Id, *byAlarm.Items[0].Id)

	comp := e.suppress(t, func(d *suppDetails) { d.DisplayName = new("zone") },
		monitoring.AlarmSuppressionCompartmentTarget{CompartmentId: new(testCompartment)})

	direct, err := e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{AlarmId: a.Id})
	require.NoError(t, err)
	assert.Len(t, direct.Items, 1, "compartment suppressions only join with isAllSuppressions")

	all, err := e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{
		AlarmId: a.Id, IsAllSuppressions: new(true),
	})
	require.NoError(t, err)
	assert.Len(t, all.Items, 2)

	byComp, err := e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{
		CompartmentId: new(testCompartment), TargetType: monitoring.ListAlarmSuppressionsTargetTypeCompartment,
	})
	require.NoError(t, err)
	require.Len(t, byComp.Items, 1)
	assert.Equal(t, *comp.Id, *byComp.Items[0].Id)

	named, err := e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{
		CompartmentId: new(
			testCompartment,
		),
		DisplayName: new("zone"),
		SortBy:      monitoring.ListAlarmSuppressionsSortByDisplayname,
	})
	require.NoError(t, err)
	assert.Len(t, named.Items, 1)

	snap := e.backend.Snapshot(t.Context())
	fresh := newEnv(t, envOpts{})
	require.NoError(t, fresh.backend.Restore(t.Context(), snap))

	restored, err := fresh.mon.GetAlarmSuppression(
		t.Context(),
		monitoring.GetAlarmSuppressionRequest{AlarmSuppressionId: s.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "planned", *restored.Description)

	_, err = e.mon.DeleteAlarmSuppression(
		t.Context(),
		monitoring.DeleteAlarmSuppressionRequest{AlarmSuppressionId: s.Id},
	)
	require.NoError(t, err)

	gone, err := e.mon.GetAlarmSuppression(t.Context(), monitoring.GetAlarmSuppressionRequest{AlarmSuppressionId: s.Id})
	require.NoError(t, err)
	assert.Equal(t, monitoring.AlarmSuppressionLifecycleStateDeleted, gone.LifecycleState)

	active, err := e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{AlarmId: a.Id})
	require.NoError(t, err)
	assert.Empty(t, active.Items)

	deleted, err := e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{
		AlarmId: a.Id, LifecycleState: monitoring.AlarmSuppressionLifecycleStateDeleted,
	})
	require.NoError(t, err)
	assert.Len(t, deleted.Items, 1)

	_, err = e.mon.DeleteAlarmSuppression(
		t.Context(),
		monitoring.DeleteAlarmSuppressionRequest{AlarmSuppressionId: s.Id},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestAlarmSuppressionErrors(t *testing.T) {
	t.Parallel()

	rec := func(rule, dur string) func(*suppDetails) {
		return func(d *suppDetails) {
			d.SuppressionConditions = []monitoring.SuppressionCondition{
				monitoring.Recurrence{SuppressionRecurrence: new(rule), SuppressionDuration: new(dur)},
			}
		}
	}

	tests := []struct {
		mod    func(*suppDetails)
		target func(a *string) monitoring.AlarmSuppressionTarget
		name   string
		code   string
		status int
	}{
		{name: "unknown alarm", status: 404, code: "NotAuthorizedOrNotFound",
			target: func(*string) monitoring.AlarmSuppressionTarget { return alarmTarget(new("ocid1.alarm.oc1..x")) }},
		{name: "window", status: 400, code: "InvalidParameter", mod: func(d *suppDetails) {
			d.TimeSuppressUntil = d.TimeSuppressFrom
		}},
		{
			name:   "dimension without dims",
			status: 400,
			code:   "MissingParameter",
			mod:    func(d *suppDetails) { d.Level = monitoring.AlarmSuppressionLevelDimension },
		},
		{
			name:   "long dims",
			status: 400,
			code:   "InvalidParameter",
			mod: func(d *suppDetails) {
				d.Level = monitoring.AlarmSuppressionLevelDimension
				d.Dimensions = map[string]string{"k": string(make([]byte, 4100))}
			},
		},
		{name: "interval", status: 400, code: "InvalidParameter", mod: rec("FREQ=DAILY;INTERVAL=2", "PT1H")},
		{name: "monthly", status: 400, code: "InvalidParameter", mod: rec("FREQ=MONTHLY", "PT1H")},
		{name: "byday daily", status: 400, code: "InvalidParameter", mod: rec("FREQ=DAILY;BYDAY=MO", "PT1H")},
		{name: "bad day", status: 400, code: "InvalidParameter", mod: rec("FREQ=WEEKLY;BYDAY=XX", "PT1H")},
		{name: "bad hour", status: 400, code: "InvalidParameter", mod: rec("FREQ=DAILY;BYHOUR=24", "PT1H")},
		{name: "no freq", status: 400, code: "InvalidParameter", mod: rec("BYHOUR=1", "PT1H")},
		{name: "too short", status: 400, code: "InvalidParameter", mod: rec("FREQ=DAILY", "PT30S")},
		{name: "too long", status: 400, code: "InvalidParameter", mod: rec("FREQ=DAILY", "PT25H")},
		{
			name:   "two conditions",
			status: 400,
			code:   "InvalidParameter",
			mod: func(d *suppDetails) {
				c := monitoring.Recurrence{SuppressionRecurrence: new("FREQ=DAILY"), SuppressionDuration: new("PT1H")}
				d.SuppressionConditions = []monitoring.SuppressionCondition{c, c}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			a := e.mkAlarm(t, topicOCID, nil)

			var target monitoring.AlarmSuppressionTarget = alarmTarget(a.Id)
			if tt.target != nil {
				target = tt.target(a.Id)
			}

			_, err := e.createSuppression(t, tt.mod, target)
			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestAlarmSuppressionRequestErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mkAlarm(t, topicOCID, nil)
	s := e.suppress(t, nil, alarmTarget(a.Id))

	_, err := e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{
		CompartmentId: new(testCompartment), IsAllSuppressions: new(true),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mon.GetAlarmSuppression(
		t.Context(),
		monitoring.GetAlarmSuppressionRequest{AlarmSuppressionId: new("nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mon.DeleteAlarmSuppression(t.Context(), monitoring.DeleteAlarmSuppressionRequest{
		AlarmSuppressionId: s.Id, IfMatch: new("stale"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mon.SummarizeAlarmSuppressionHistory(t.Context(), monitoring.SummarizeAlarmSuppressionHistoryRequest{
		AlarmId:                  a.Id,
		TimeSuppressFromLessThan: sdkTime(epoch().Add(time.Hour)),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	for _, fn := range []func() error{
		func() error {
			_, e2 := e.mon.RemoveAlarmSuppression(t.Context(), monitoring.RemoveAlarmSuppressionRequest{AlarmId: new("nope")})

			return e2
		},
		func() error {
			_, e2 := e.mon.RetrieveDimensionStates(t.Context(), monitoring.RetrieveDimensionStatesRequest{AlarmId: new("nope")})

			return e2
		},
		func() error {
			_, e2 := e.mon.SummarizeAlarmSuppressionHistory(t.Context(),
				monitoring.SummarizeAlarmSuppressionHistoryRequest{AlarmId: new("nope")})

			return e2
		},
	} {
		assert.Equal(t, http.StatusNotFound, svcErr(t, fn()).GetHTTPStatusCode())
	}
}

func TestAlarmSuppressionRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mkAlarm(t, topicOCID, nil)

	send := func() monitoring.AlarmSuppression {
		resp, err := e.mon.CreateAlarmSuppression(t.Context(), monitoring.CreateAlarmSuppressionRequest{
			OpcRetryToken: new("tok"),
			AlarmSuppressionTarget: alarmTarget(
				a.Id,
			),
			DisplayName:       new("m"),
			Level:             monitoring.AlarmSuppressionLevelAlarm,
			TimeSuppressFrom:  sdkTime(epoch()),
			TimeSuppressUntil: sdkTime(epoch().Add(time.Hour)),
		})
		require.NoError(t, err)

		return resp.AlarmSuppression
	}

	first, second := send(), send()
	assert.Equal(t, *first.Id, *second.Id)

	list, err := e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{AlarmId: a.Id})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)
}

func TestSuppressionAppliedInEvaluation(t *testing.T) {
	t.Parallel()

	hostDims := func(h string) map[string]string { return map[string]string{"host": h} }

	tests := []struct {
		mod        func(*suppDetails)
		target     func(a *monitoring.Alarm) monitoring.AlarmSuppressionTarget
		name       string
		advance    time.Duration
		wantNotice bool
		wantHostA  bool
		wantHostB  bool
	}{
		{
			name:       "none",
			wantNotice: true,
			wantHostA:  true,
			wantHostB:  true,
			mod: func(d *suppDetails) {
				d.TimeSuppressFrom = sdkTime(epoch().Add(time.Hour))
			},
		},
		{name: "alarm level", wantNotice: false},
		{
			name:       "compartment target",
			wantNotice: false,
			target: func(*monitoring.Alarm) monitoring.AlarmSuppressionTarget {
				return monitoring.AlarmSuppressionCompartmentTarget{CompartmentId: new(testCompartment)}
			},
		},
		{name: "other compartment", wantNotice: true, wantHostA: true, wantHostB: true,
			target: func(*monitoring.Alarm) monitoring.AlarmSuppressionTarget {
				return monitoring.AlarmSuppressionCompartmentTarget{CompartmentId: new("ocid1.compartment.oc1..other")}
			}},
		{
			name:       "dimension one host",
			wantNotice: true,
			wantHostB:  true,
			mod: func(d *suppDetails) {
				d.Level, d.Dimensions = monitoring.AlarmSuppressionLevelDimension, hostDims("a")
			},
		},
		{name: "dimension both", wantNotice: false, mod: func(d *suppDetails) {
			d.Level, d.Dimensions = monitoring.AlarmSuppressionLevelDimension, map[string]string{}
		}},
		{name: "dimension other", wantNotice: true, wantHostA: true, wantHostB: true,
			mod: func(d *suppDetails) {
				d.Level, d.Dimensions = monitoring.AlarmSuppressionLevelDimension, hostDims("zzz")
			}},
		{name: "expired", wantNotice: true, wantHostA: true, wantHostB: true, advance: 48 * time.Hour,
			mod: func(*suppDetails) {}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			o := newONS(t, e)
			a := e.mkAlarm(t, o.topicID, nil)

			var target monitoring.AlarmSuppressionTarget = alarmTarget(a.Id)
			if tt.target != nil {
				target = tt.target(&a)
			}

			mod := tt.mod
			if tt.name == "dimension both" {
				mod = func(d *suppDetails) {
					d.Level = monitoring.AlarmSuppressionLevelDimension
					d.Dimensions = map[string]string{"host": "a"}
				}
				e.suppress(t, func(d *suppDetails) {
					d.Level, d.Dimensions = monitoring.AlarmSuppressionLevelDimension, hostDims("b")
				}, target)
			}

			e.suppress(t, mod, target)
			e.clock.Advance(tt.advance)
			e.postHosts(t, 95, 96)

			if !tt.wantNotice {
				e.requireSuppressed(t, o, a.Id)

				return
			}

			body := o.sink.next(t).body
			assert.Equal(t, tt.wantHostA, containsHost(body, "a"), body)
			assert.Equal(t, tt.wantHostB, containsHost(body, "b"), body)
		})
	}
}

func (e *env) postHosts(t *testing.T, a, b float64) {
	t.Helper()

	_, err := e.mon.PostMetricData(t.Context(), monitoring.PostMetricDataRequest{
		MetricData: []monitoring.MetricDataDetails{
			e.metric(ns, metric, map[string]string{"host": "a"}, pt{0, a, 0}),
			e.metric(ns, metric, map[string]string{"host": "b"}, pt{0, b, 0}),
		},
	})
	require.NoError(t, err)

	e.status(t)
}

// requireSuppressed proves the firing notice was withheld: once the suppressions are deleted and the alarm
// clears, the first message delivered is the clearing one, not the firing one.
func (e *env) requireSuppressed(t *testing.T, o *onsEnv, alarmID *string) {
	t.Helper()

	list, err := e.mon.ListAlarmSuppressions(t.Context(), monitoring.ListAlarmSuppressionsRequest{
		AlarmId: alarmID, IsAllSuppressions: new(true),
	})
	require.NoError(t, err)

	for _, s := range list.Items {
		_, err = e.mon.DeleteAlarmSuppression(
			t.Context(),
			monitoring.DeleteAlarmSuppressionRequest{AlarmSuppressionId: s.Id},
		)
		require.NoError(t, err)
	}

	e.clock.Advance(2 * time.Minute)
	e.postHosts(t, 1, 1)

	assert.Contains(t, o.sink.next(t).body, `"type":"FIRING_TO_OK"`)
}

func containsHost(body, host string) bool {
	return len(body) > 0 && (contains(body, `"host":"`+host+`"`) || contains(body, `\"host\":\"`+host+`\"`))
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}

	return false
}

func TestRecurrenceSuppressionWindows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rule       string
		dur        string
		advance    time.Duration
		suppressed bool
	}{
		{name: "before window", rule: "FREQ=DAILY;BYHOUR=13", dur: "PT1H", advance: 0, suppressed: false},
		{name: "window start", rule: "FREQ=DAILY;BYHOUR=13", dur: "PT1H", advance: time.Hour, suppressed: true},
		{name: "inside window", rule: "FREQ=DAILY;BYHOUR=13", dur: "PT1H", advance: 90 * time.Minute, suppressed: true},
		{
			name:       "after window",
			rule:       "FREQ=DAILY;BYHOUR=13",
			dur:        "PT1H",
			advance:    150 * time.Minute,
			suppressed: false,
		},
		{
			name:       "next day",
			rule:       "FREQ=DAILY;BYHOUR=13",
			dur:        "PT1H",
			advance:    25*time.Hour + 30*time.Minute,
			suppressed: true,
		},
		{
			name:       "default midnight",
			rule:       "FREQ=DAILY",
			dur:        "PT2H",
			advance:    12*time.Hour + 30*time.Minute,
			suppressed: true,
		},
		{name: "multi hours", rule: "FREQ=DAILY;BYHOUR=3,12", dur: "PT1H", advance: halfHour, suppressed: true},
		{
			name:       "weekly match",
			rule:       "FREQ=WEEKLY;BYDAY=TH;BYHOUR=12",
			dur:        "PT1H",
			advance:    30 * time.Minute,
			suppressed: true,
		},
		{
			name:       "weekly other day",
			rule:       "FREQ=WEEKLY;BYDAY=FR;BYHOUR=12",
			dur:        "PT1H",
			advance:    30 * time.Minute,
			suppressed: false,
		},
		{
			name:       "weekly next",
			rule:       "FREQ=WEEKLY;BYDAY=FR,TH;BYHOUR=12",
			dur:        "PT1H",
			advance:    24*time.Hour + 30*time.Minute,
			suppressed: true,
		},
		{
			name:       "past until",
			rule:       "FREQ=DAILY;BYHOUR=12",
			dur:        "PT1H",
			advance:    5*24*time.Hour + 30*time.Minute,
			suppressed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			o := newONS(t, e)
			a := e.mkAlarm(t, o.topicID, nil)

			e.suppress(t, func(d *suppDetails) {
				d.TimeSuppressFrom = sdkTime(epoch().Add(-24 * time.Hour))
				d.TimeSuppressUntil = sdkTime(epoch().Add(4 * 24 * time.Hour))
				d.SuppressionConditions = []monitoring.SuppressionCondition{
					monitoring.Recurrence{SuppressionRecurrence: new(tt.rule), SuppressionDuration: new(tt.dur)},
				}
			}, alarmTarget(a.Id))

			e.clock.Advance(tt.advance)
			e.postHosts(t, 95, 10)

			if tt.suppressed {
				e.requireSuppressed(t, o, a.Id)

				return
			}

			assert.Contains(t, o.sink.next(t).body, `"type":"OK_TO_FIRING"`)
		})
	}
}

func (e *env) history(
	t *testing.T,
	alarmID *string,
	d monitoring.SummarizeAlarmSuppressionHistoryDetails,
) monitoring.SummarizeAlarmSuppressionHistoryResponse {
	t.Helper()

	resp, err := e.mon.SummarizeAlarmSuppressionHistory(t.Context(), monitoring.SummarizeAlarmSuppressionHistoryRequest{
		AlarmId: alarmID, SummarizeAlarmSuppressionHistoryDetails: d,
	})
	require.NoError(t, err)

	return resp
}

func TestAlarmSuppressionHistory(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mkAlarm(t, topicOCID, nil)

	rec := e.suppress(t, func(d *suppDetails) {
		d.DisplayName = new("nightly")
		d.TimeSuppressFrom = sdkTime(epoch().Add(-48 * time.Hour))
		d.TimeSuppressUntil = sdkTime(epoch().Add(48 * time.Hour))
		d.SuppressionConditions = []monitoring.SuppressionCondition{
			monitoring.Recurrence{SuppressionRecurrence: new("FREQ=DAILY;BYHOUR=1"), SuppressionDuration: new("PT2H")},
		}
	}, alarmTarget(a.Id))

	dim := e.suppress(t, func(d *suppDetails) {
		d.DisplayName = new("host-a")
		d.Level = monitoring.AlarmSuppressionLevelDimension
		d.Dimensions = map[string]string{"host": "a"}
		d.TimeSuppressFrom = sdkTime(epoch().Add(-time.Hour))
	}, monitoring.AlarmSuppressionCompartmentTarget{CompartmentId: new(testCompartment)})

	e.clock.Advance(time.Hour)

	_, err := e.mon.DeleteAlarmSuppression(
		t.Context(),
		monitoring.DeleteAlarmSuppressionRequest{AlarmSuppressionId: dim.Id},
	)
	require.NoError(t, err)

	resp, err := e.mon.SummarizeAlarmSuppressionHistory(
		t.Context(),
		monitoring.SummarizeAlarmSuppressionHistoryRequest{AlarmId: a.Id},
	)
	require.NoError(t, err)
	require.Len(t, resp.Items, 3, "two nightly occurrences and the deleted dimension suppression")

	var nightly int

	for _, it := range resp.Items {
		switch *it.SuppressionId {
		case *rec.Id:
			nightly++

			assert.Equal(t, 2*time.Hour, it.TimeEffectiveUntil.Sub(it.TimeEffectiveFrom.Time))
			assert.Equal(t, monitoring.AlarmSuppressionHistoryItemLevelAlarm, it.Level)
		case *dim.Id:
			assert.Equal(t, epoch().Add(time.Hour), it.TimeEffectiveUntil.Time, "deletion ends the effect")
			assert.Equal(t, "a", it.Dimensions["host"])
		}
	}

	assert.Equal(t, 2, nightly)

	filtered := e.history(t, a.Id, monitoring.SummarizeAlarmSuppressionHistoryDetails{
		Dimensions: map[string]string{"host": "b"},
	})
	assert.Len(t, filtered.Items, 2, "the host=a suppression does not apply to host=b")

	windowed := e.history(t, a.Id, monitoring.SummarizeAlarmSuppressionHistoryDetails{
		TimeSuppressFromGreaterThanOrEqualTo: sdkTime(epoch().Add(-time.Hour)),
	})
	assert.Len(t, windowed.Items, 1)
}

func TestRetrieveDimensionStates(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	o := newONS(t, e)
	a := e.mkAlarm(t, o.topicID, func(d *monitoring.CreateAlarmDetails) {
		d.IsNotificationsPerMetricDimensionEnabled = new(true)
	})

	e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 95, 0})
	e.post(t, ns, metric, map[string]string{"host": "b"}, pt{0, 10, 0})

	get := func(d monitoring.RetrieveDimensionStatesDetails) monitoring.AlarmDimensionStatesCollection {
		resp, err := e.mon.RetrieveDimensionStates(t.Context(), monitoring.RetrieveDimensionStatesRequest{
			AlarmId: a.Id, RetrieveDimensionStatesDetails: d,
		})
		require.NoError(t, err)

		return resp.AlarmDimensionStatesCollection
	}

	all := get(monitoring.RetrieveDimensionStatesDetails{})
	require.Len(t, all.Items, 2)
	assert.Equal(t, *a.Id, *all.AlarmId)
	assert.True(t, *all.IsEnabled)
	assert.True(t, *all.IsNotificationsPerMetricDimensionEnabled)
	assert.Equal(t, "a", all.Items[0].Dimensions["host"])
	assert.Equal(t, monitoring.AlarmDimensionStatesEntryStatusFiring, all.Items[0].Status)
	assert.Equal(t, monitoring.AlarmDimensionStatesEntryStatusOk, all.Items[1].Status)
	assert.Equal(t, "BASE", *all.Items[0].RuleName)
	assert.Equal(t, epoch(), all.Items[0].Timestamp.Time)

	firing := get(monitoring.RetrieveDimensionStatesDetails{Status: monitoring.AlarmDimensionStatesEntryStatusFiring})
	require.Len(t, firing.Items, 1)
	assert.Equal(t, "a", firing.Items[0].Dimensions["host"])

	exact := get(monitoring.RetrieveDimensionStatesDetails{DimensionFilters: map[string]string{"host": "b"}})
	require.Len(t, exact.Items, 1)

	none := get(monitoring.RetrieveDimensionStatesDetails{DimensionFilters: map[string]string{"other": "b"}})
	assert.Empty(t, none.Items)

	e.clock.Advance(2 * time.Minute)
	e.post(t, ns, metric, map[string]string{"host": "a"}, pt{0, 5, 0})

	cleared := get(monitoring.RetrieveDimensionStatesDetails{})
	for _, it := range cleared.Items {
		assert.Equal(t, monitoring.AlarmDimensionStatesEntryStatusOk, it.Status)
	}

	fresh := newEnv(t, envOpts{})
	require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	resp, err := fresh.mon.RetrieveDimensionStates(
		t.Context(),
		monitoring.RetrieveDimensionStatesRequest{AlarmId: a.Id},
	)
	require.NoError(t, err)
	assert.Len(t, resp.Items, 2)

	_, err = e.mon.RetrieveDimensionStates(t.Context(), monitoring.RetrieveDimensionStatesRequest{
		AlarmId: a.Id, Status: "BOGUS",
	})
	assert.Error(t, err)
}

func TestRemoveAlarmSuppression(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mkAlarm(t, topicOCID, func(d *monitoring.CreateAlarmDetails) {
		d.Suppression = &monitoring.Suppression{
			TimeSuppressFrom: sdkTime(epoch().Add(-time.Hour)), TimeSuppressUntil: sdkTime(epoch().Add(time.Hour)),
		}
	})

	_, err := e.mon.RemoveAlarmSuppression(t.Context(), monitoring.RemoveAlarmSuppressionRequest{
		AlarmId: a.Id, IfMatch: new("stale"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.mon.RemoveAlarmSuppression(t.Context(), monitoring.RemoveAlarmSuppressionRequest{AlarmId: a.Id})
	require.NoError(t, err)

	got, err := e.mon.GetAlarm(t.Context(), monitoring.GetAlarmRequest{AlarmId: a.Id})
	require.NoError(t, err)
	assert.Nil(t, got.Suppression)
}
