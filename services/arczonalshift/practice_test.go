package arczonalshift_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/arczonalshift"
	"github.com/aws/aws-sdk-go-v2/service/arczonalshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	zs "github.com/blackbirdworks/gopherstack/services/arczonalshift"
	cw "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

func cond(arn string) []zs.Alarm { return []zs.Alarm{{AlarmIdentifier: arn, Type: "CLOUDWATCH"}} }

func TestPracticeRunConfigurationCRUD(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	lb := f.loadBalancer(t, "web", 2)
	require.NoError(t, f.cw.PutMetricAlarm(&cw.MetricAlarm{AlarmName: "outcome", StateValue: "OK"}))

	outcome := &types.ControlCondition{
		AlarmIdentifier: aws.String("arn:aws:cloudwatch:us-east-1:123456789012:alarm:outcome"),
		Type:            types.ControlConditionTypeCloudwatch,
	}

	created, err := f.client.CreatePracticeRunConfiguration(ctx, &sdk.CreatePracticeRunConfigurationInput{
		ResourceIdentifier: aws.String(lb),
		OutcomeAlarms:      []types.ControlCondition{*outcome},
		AllowedWindows: []string{
			"MON:08:00-MON:17:00",
		},
		BlockedDates:   []string{"2030-12-25"},
		BlockedWindows: []string{"TUE:00:00-TUE:23:59"},
	})
	require.NoError(t, err)
	assert.Equal(t, types.ZonalAutoshiftStatusDisabled, created.ZonalAutoshiftStatus)
	assert.Equal(t, lb, aws.ToString(created.Arn))
	assert.Equal(t, []string{"MON:08:00-MON:17:00"}, created.PracticeRunConfiguration.AllowedWindows)

	_, err = f.client.CreatePracticeRunConfiguration(ctx, &sdk.CreatePracticeRunConfigurationInput{
		ResourceIdentifier: aws.String(lb), OutcomeAlarms: []types.ControlCondition{*outcome},
	})
	var ce *types.ConflictException
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, types.ConflictExceptionReasonPracticeConfigurationAlreadyExists, ce.Reason)

	got, err := f.client.GetManagedResource(ctx, &sdk.GetManagedResourceInput{ResourceIdentifier: aws.String(lb)})
	require.NoError(t, err)
	require.NotNil(t, got.PracticeRunConfiguration)
	assert.Equal(t, []string{"2030-12-25"}, got.PracticeRunConfiguration.BlockedDates)

	list, err := f.client.ListManagedResources(ctx, &sdk.ListManagedResourcesInput{})
	require.NoError(t, err)
	assert.Equal(
		t,
		types.ZonalAutoshiftStatusEnabled,
		list.Items[0].PracticeRunStatus,
		"a practice configuration exists",
	)

	upd, err := f.client.UpdatePracticeRunConfiguration(ctx, &sdk.UpdatePracticeRunConfigurationInput{
		ResourceIdentifier: aws.String(lb), BlockedDates: []string{"2031-01-01"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"2031-01-01"}, upd.PracticeRunConfiguration.BlockedDates)
	assert.Equal(
		t,
		[]string{"MON:08:00-MON:17:00"},
		upd.PracticeRunConfiguration.AllowedWindows,
		"unspecified fields are kept",
	)

	en, err := f.client.UpdateZonalAutoshiftConfiguration(ctx, &sdk.UpdateZonalAutoshiftConfigurationInput{
		ResourceIdentifier: aws.String(lb), ZonalAutoshiftStatus: types.ZonalAutoshiftStatusEnabled,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ZonalAutoshiftStatusEnabled, en.ZonalAutoshiftStatus)

	_, err = f.client.DeletePracticeRunConfiguration(
		ctx,
		&sdk.DeletePracticeRunConfigurationInput{ResourceIdentifier: aws.String(lb)},
	)
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, types.ConflictExceptionReasonZonalAutoshiftActive, ce.Reason)

	_, err = f.client.UpdateZonalAutoshiftConfiguration(ctx, &sdk.UpdateZonalAutoshiftConfigurationInput{
		ResourceIdentifier: aws.String(lb), ZonalAutoshiftStatus: types.ZonalAutoshiftStatusDisabled,
	})
	require.NoError(t, err)

	del, err := f.client.DeletePracticeRunConfiguration(
		ctx,
		&sdk.DeletePracticeRunConfigurationInput{ResourceIdentifier: aws.String(lb)},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ZonalAutoshiftStatusDisabled, del.ZonalAutoshiftStatus)

	_, err = f.client.DeletePracticeRunConfiguration(
		ctx,
		&sdk.DeletePracticeRunConfigurationInput{ResourceIdentifier: aws.String(lb)},
	)
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, types.ConflictExceptionReasonPracticeConfigurationDoesNotExist, ce.Reason)
}

func TestPracticeRunConfigurationValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	lb := f.loadBalancer(t, "web", 2)
	require.NoError(t, f.cw.PutMetricAlarm(&cw.MetricAlarm{AlarmName: "ok", StateValue: "OK"}))

	good := types.ControlCondition{
		AlarmIdentifier: aws.String("arn:aws:cloudwatch:us-east-1:123456789012:alarm:ok"),
		Type:            types.ControlConditionTypeCloudwatch,
	}
	missing := types.ControlCondition{
		AlarmIdentifier: aws.String("arn:aws:cloudwatch:us-east-1:123456789012:alarm:ghost"),
		Type:            types.ControlConditionTypeCloudwatch,
	}

	tests := []struct {
		mutate     func(*sdk.CreatePracticeRunConfigurationInput)
		name       string
		wantReason types.ValidationExceptionReason
	}{
		{func(in *sdk.CreatePracticeRunConfigurationInput) {
			in.OutcomeAlarms = []types.ControlCondition{missing}
		}, "alarm does not exist", types.ValidationExceptionReasonInvalidAlarmCondition},
		{func(in *sdk.CreatePracticeRunConfigurationInput) {
			in.BlockingAlarms = []types.ControlCondition{missing}
		}, "blocking alarm does not exist", types.ValidationExceptionReasonInvalidAlarmCondition},
		{
			func(in *sdk.CreatePracticeRunConfigurationInput) { in.AllowedWindows = []string{"MON-08:00"} },
			"malformed allowed window",
			types.ValidationExceptionReasonInvalidPracticeAllowedWindow,
		},
		{
			func(in *sdk.CreatePracticeRunConfigurationInput) { in.AllowedWindows = []string{"MON:08:00-TUE:09:00"} },
			"allowed window spans days",
			types.ValidationExceptionReasonInvalidPracticeAllowedWindow,
		},
		{
			func(in *sdk.CreatePracticeRunConfigurationInput) { in.BlockedWindows = []string{"XXX:08:00-XXX:09:00"} },
			"bad weekday",
			types.ValidationExceptionReasonInvalidPracticeBlocker,
		},
		{
			func(in *sdk.CreatePracticeRunConfigurationInput) { in.BlockedDates = []string{"2030-13-45"} },
			"bad date",
			types.ValidationExceptionReasonInvalidPracticeBlocker,
		},
		{func(in *sdk.CreatePracticeRunConfigurationInput) {
			in.AllowedWindows = []string{"MON:08:00-MON:12:00"}
			in.BlockedWindows = []string{"MON:11:00-MON:13:00"}
		}, "overlapping windows", types.ValidationExceptionReasonConflictingPracticeWindows},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.CreatePracticeRunConfigurationInput{
				ResourceIdentifier: aws.String(lb),
				OutcomeAlarms:      []types.ControlCondition{good},
			}
			tt.mutate(in)

			_, err := f.client.CreatePracticeRunConfiguration(t.Context(), in)

			var ve *types.ValidationException
			require.ErrorAs(t, err, &ve)
			assert.Equal(t, tt.wantReason, ve.Reason)
		})
	}

	t.Run("autoshift needs a configuration", func(t *testing.T) {
		t.Parallel()

		_, err := f.client.UpdateZonalAutoshiftConfiguration(t.Context(), &sdk.UpdateZonalAutoshiftConfigurationInput{
			ResourceIdentifier: aws.String(lb), ZonalAutoshiftStatus: types.ZonalAutoshiftStatusEnabled,
		})

		var ce *types.ConflictException
		require.ErrorAs(t, err, &ce)
		assert.Equal(t, types.ConflictExceptionReasonPracticeConfigurationDoesNotExist, ce.Reason)
	})
}

func TestStartPracticeRunBlockers(t *testing.T) {
	t.Parallel()

	// synctest starts at Saturday 2000-01-01 00:00 UTC.
	tests := []struct {
		cfg        func(outcome, blocking string) zs.PracticeConfig
		name       string
		alarm      string
		alarmState string
		want       string
		sleep      time.Duration
	}{
		{
			name: "allowed",
			cfg:  func(o, _ string) zs.PracticeConfig { return zs.PracticeConfig{OutcomeAlarms: cond(o)} },
		},
		{
			name: "outcome alarm red", alarm: "outcome", alarmState: "ALARM", want: "PracticeOutcomeAlarmsRed",
			cfg: func(o, _ string) zs.PracticeConfig { return zs.PracticeConfig{OutcomeAlarms: cond(o)} },
		},
		{
			name: "blocking alarm red", alarm: "blocking", alarmState: "ALARM", want: "PracticeBlockingAlarmsRed",
			cfg: func(o, b string) zs.PracticeConfig {
				return zs.PracticeConfig{OutcomeAlarms: cond(o), BlockingAlarms: cond(b)}
			},
		},
		{
			name: "blocked date", want: "PracticeInBlockedDates",
			cfg: func(o, _ string) zs.PracticeConfig {
				return zs.PracticeConfig{OutcomeAlarms: cond(o), BlockedDates: []string{"2000-01-01"}}
			},
		},
		{
			name: "blocked window", sleep: 10 * time.Hour, want: "PracticeInBlockedWindows",
			cfg: func(o, _ string) zs.PracticeConfig {
				return zs.PracticeConfig{OutcomeAlarms: cond(o), BlockedWindows: []string{"SAT:09:00-SAT:11:00"}}
			},
		},
		{
			name: "outside allowed window", sleep: 10 * time.Hour, want: "PracticeOutsideAllowedWindows",
			cfg: func(o, _ string) zs.PracticeConfig {
				return zs.PracticeConfig{OutcomeAlarms: cond(o), AllowedWindows: []string{"MON:09:00-MON:11:00"}}
			},
		},
		{
			name: "inside allowed window", sleep: 10 * time.Hour,
			cfg: func(o, _ string) zs.PracticeConfig {
				return zs.PracticeConfig{OutcomeAlarms: cond(o), AllowedWindows: []string{"SAT:09:00-SAT:11:00"}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				x := newBubble(t)
				outcome, blocking := x.alarm(t, "outcome"), x.alarm(t, "blocking")

				cfg := tt.cfg(outcome, blocking)
				cfg.Resource = x.lb
				_, err := x.b.CreatePracticeRunConfiguration(cfg)
				require.NoError(t, err)

				if tt.alarm != "" {
					require.NoError(t, x.alarms.SetAlarmState(t.Context(), tt.alarm, tt.alarmState, "test", ""))
				}

				time.Sleep(tt.sleep)

				run, err := x.b.StartPracticeRun(x.lb, "us-east-1a", "drill")
				if tt.want == "" {
					require.NoError(t, err)
					assert.Equal(t, "PRACTICE_RUN", run.ShiftType)
					assert.Equal(t, "PENDING", run.PracticeRunOutcome)

					return
				}

				requireReason(t, err, tt.want)
			})
		})
	}
}

func TestPracticeRunOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStatus   string
		wantOutcome  string
		wantInFlight string
		sleepAfter   time.Duration
		alarmDuring  bool
		cancel       bool
	}{
		{
			name:         "succeeds when the alarm stays green",
			sleepAfter:   31 * time.Minute,
			wantStatus:   "EXPIRED",
			wantOutcome:  "SUCCEEDED",
			wantInFlight: "PENDING",
		},
		{
			name:         "fails when the outcome alarm goes red",
			alarmDuring:  true,
			sleepAfter:   31 * time.Minute,
			wantStatus:   "EXPIRED",
			wantOutcome:  "FAILED",
			wantInFlight: "PENDING",
		},
		{
			name:         "interrupted when canceled",
			cancel:       true,
			sleepAfter:   time.Minute,
			wantStatus:   "CANCELED",
			wantOutcome:  "INTERRUPTED",
			wantInFlight: "PENDING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				x := newBubble(t)
				outcome := x.alarm(t, "outcome")
				_, err := x.b.CreatePracticeRunConfiguration(
					zs.PracticeConfig{Resource: x.lb, OutcomeAlarms: cond(outcome)},
				)
				require.NoError(t, err)

				run, err := x.b.StartPracticeRun(x.lb, "us-east-1a", "drill")
				require.NoError(t, err)
				assert.Equal(t, tt.wantInFlight, run.PracticeRunOutcome)

				time.Sleep(time.Minute)

				if tt.alarmDuring {
					require.NoError(t, x.alarms.SetAlarmState(t.Context(), "outcome", "ALARM", "test", ""))
				}

				if tt.cancel {
					_, err = x.b.CancelZonalShift(run.ZonalShiftID)
					require.Error(t, err, "a practice run is canceled with CancelPracticeRun")

					canceled, cErr := x.b.CancelPracticeRun(run.ZonalShiftID)
					require.NoError(t, cErr)
					assert.Equal(t, "CANCELED", canceled.Status)
				}

				time.Sleep(tt.sleepAfter)

				shifts, _, err := x.b.ListZonalShifts("", "", "", 0)
				require.NoError(t, err)
				require.Len(t, shifts, 1)
				assert.Equal(t, tt.wantStatus, shifts[0].Status)
				assert.Equal(t, tt.wantOutcome, shifts[0].PracticeRunOutcome)
			})
		})
	}
}

func TestPracticeRunDurationKnob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		want     string
		duration time.Duration
		sleep    time.Duration
	}{
		{name: "default is 30 minutes", sleep: 29 * time.Minute, want: "ACTIVE"},
		{name: "default expires after 30 minutes", sleep: 31 * time.Minute, want: "EXPIRED"},
		{name: "shortened", duration: 2 * time.Minute, sleep: 3 * time.Minute, want: "EXPIRED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				x := newBubble(t)
				x.b.SetPracticeRunDuration(tt.duration)

				outcome := x.alarm(t, "outcome")
				_, err := x.b.CreatePracticeRunConfiguration(
					zs.PracticeConfig{Resource: x.lb, OutcomeAlarms: cond(outcome)},
				)
				require.NoError(t, err)

				run, err := x.b.StartPracticeRun(x.lb, "us-east-1a", "drill")
				require.NoError(t, err)

				time.Sleep(tt.sleep)
				assert.Equal(t, tt.want, statusOf(t, x.b, run.ZonalShiftID))
			})
		})
	}
}

func TestPracticeRunViaSDK(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	lb := f.loadBalancer(t, "web", 2)
	require.NoError(t, f.cw.PutMetricAlarm(&cw.MetricAlarm{AlarmName: "outcome", StateValue: "OK"}))

	_, err := f.client.StartPracticeRun(ctx, &sdk.StartPracticeRunInput{
		ResourceIdentifier: aws.String(lb), AwayFrom: aws.String("us-east-1a"), Comment: aws.String("c"),
	})
	var ce *types.ConflictException
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, types.ConflictExceptionReasonPracticeConfigurationDoesNotExist, ce.Reason)

	_, err = f.client.CreatePracticeRunConfiguration(ctx, &sdk.CreatePracticeRunConfigurationInput{
		ResourceIdentifier: aws.String(lb),
		OutcomeAlarms: []types.ControlCondition{
			{
				AlarmIdentifier: aws.String(
					"arn:aws:cloudwatch:us-east-1:123456789012:alarm:outcome",
				),
				Type: types.ControlConditionTypeCloudwatch,
			},
		},
	})
	require.NoError(t, err)

	run, err := f.client.StartPracticeRun(ctx, &sdk.StartPracticeRunInput{
		ResourceIdentifier: aws.String(lb), AwayFrom: aws.String("us-east-1a"), Comment: aws.String("c"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ZonalShiftStatusActive, run.Status)

	managed, err := f.client.GetManagedResource(ctx, &sdk.GetManagedResourceInput{ResourceIdentifier: aws.String(lb)})
	require.NoError(t, err)
	require.Len(t, managed.ZonalShifts, 1)
	assert.Equal(t, types.ShiftTypePracticeRun, managed.ZonalShifts[0].ShiftType)
	assert.Equal(t, types.PracticeRunOutcomePending, managed.ZonalShifts[0].PracticeRunOutcome)

	manual := f.start(t, lb, "us-east-1b", "10m")

	managed, err = f.client.GetManagedResource(ctx, &sdk.GetManagedResourceInput{ResourceIdentifier: aws.String(lb)})
	require.NoError(t, err)

	applied := map[string]types.AppliedStatus{}
	for _, s := range managed.ZonalShifts {
		applied[aws.ToString(s.ZonalShiftId)] = s.AppliedStatus
	}

	assert.Equal(
		t,
		types.AppliedStatusApplied,
		applied[aws.ToString(manual.ZonalShiftId)],
		"customer shifts take precedence",
	)
	assert.Equal(t, types.AppliedStatusNotApplied, applied[aws.ToString(run.ZonalShiftId)])

	_, err = f.client.CancelPracticeRun(ctx, &sdk.CancelPracticeRunInput{ZonalShiftId: manual.ZonalShiftId})
	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, types.ValidationExceptionReasonUnsupportedPracticeCancelShiftType, ve.Reason)

	canceled, err := f.client.CancelPracticeRun(ctx, &sdk.CancelPracticeRunInput{ZonalShiftId: run.ZonalShiftId})
	require.NoError(t, err)
	assert.Equal(t, types.ZonalShiftStatusCanceled, canceled.Status)
}

func TestAutoshiftObserverNotification(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	got, err := f.client.GetAutoshiftObserverNotificationStatus(ctx, &sdk.GetAutoshiftObserverNotificationStatusInput{})
	require.NoError(t, err)
	assert.Equal(t, types.AutoshiftObserverNotificationStatusDisabled, got.Status)

	upd, err := f.client.UpdateAutoshiftObserverNotificationStatus(
		ctx,
		&sdk.UpdateAutoshiftObserverNotificationStatusInput{Status: types.AutoshiftObserverNotificationStatusEnabled},
	)
	require.NoError(t, err)
	assert.Equal(t, types.AutoshiftObserverNotificationStatusEnabled, upd.Status)

	got, err = f.client.GetAutoshiftObserverNotificationStatus(ctx, &sdk.GetAutoshiftObserverNotificationStatusInput{})
	require.NoError(t, err)
	assert.Equal(t, types.AutoshiftObserverNotificationStatusEnabled, got.Status)

	_, err = f.client.UpdateAutoshiftObserverNotificationStatus(
		ctx,
		&sdk.UpdateAutoshiftObserverNotificationStatusInput{Status: "MAYBE"},
	)
	assert.Equal(t, "ValidationException", apiCode(err))

	list, err := f.client.ListAutoshifts(ctx, &sdk.ListAutoshiftsInput{Status: types.AutoshiftExecutionStatusActive})
	require.NoError(t, err)
	assert.Empty(t, list.Items, "this emulator never initiates an autoshift")

	_, err = f.client.ListAutoshifts(ctx, &sdk.ListAutoshiftsInput{Status: "BOGUS"})
	assert.Equal(t, "ValidationException", apiCode(err))
}
