package secretsmanager_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// TestRotateSecret_RotationRulesValidation exercises Duration format/range validation and the
// AutomaticallyAfterDays/ScheduleExpression mutual exclusivity (API_RotationRulesType.html).
func TestRotateSecret_RotationRulesValidation(t *testing.T) {
	t.Parallel()

	sevenDays := int64(7)

	tests := []struct {
		rules   *secretsmanager.RotationRulesType
		name    string
		wantErr bool
	}{
		{
			name:  "duration_fits_hourly_rate_window",
			rules: &secretsmanager.RotationRulesType{ScheduleExpression: "rate(4 hours)", Duration: "1h"},
		},
		{
			name:  "duration_fits_daily_cron_window",
			rules: &secretsmanager.RotationRulesType{ScheduleExpression: "cron(0 8 * * ? *)", Duration: "3h"},
		},
		{
			name:  "duration_equal_to_hourly_window_limit_allowed",
			rules: &secretsmanager.RotationRulesType{ScheduleExpression: "rate(4 hours)", Duration: "4h"},
		},
		{
			name:    "duration_missing_h_suffix",
			rules:   &secretsmanager.RotationRulesType{ScheduleExpression: "rate(1 day)", Duration: "3"},
			wantErr: true,
		},
		{
			name:    "duration_non_numeric",
			rules:   &secretsmanager.RotationRulesType{ScheduleExpression: "rate(1 day)", Duration: "abh"},
			wantErr: true,
		},
		{
			name:    "duration_zero_hours",
			rules:   &secretsmanager.RotationRulesType{ScheduleExpression: "rate(1 day)", Duration: "0h"},
			wantErr: true,
		},
		{
			name:    "duration_too_long_for_length_constraint",
			rules:   &secretsmanager.RotationRulesType{ScheduleExpression: "rate(1 day)", Duration: "100h"},
			wantErr: true,
		},
		{
			name:    "duration_exceeds_hourly_rate_window",
			rules:   &secretsmanager.RotationRulesType{ScheduleExpression: "rate(1 hour)", Duration: "2h"},
			wantErr: true,
		},
		{
			name:    "duration_extends_past_daily_cron_window",
			rules:   &secretsmanager.RotationRulesType{ScheduleExpression: "cron(0 10 * * ? *)", Duration: "20h"},
			wantErr: true,
		},
		{
			name: "duration_not_validated_against_window_without_scheduleexpression",
			rules: &secretsmanager.RotationRulesType{
				AutomaticallyAfterDays: &sevenDays,
				Duration:               "50h",
			},
		},
		{
			name: "automaticallyafterdays_and_scheduleexpression_mutually_exclusive",
			rules: &secretsmanager.RotationRulesType{
				AutomaticallyAfterDays: &sevenDays,
				ScheduleExpression:     "rate(1 day)",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			t.Cleanup(b.StopRotationScheduler)
			ctx := context.Background()

			secretName := "rules-" + tt.name
			_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
				Name:         secretName,
				SecretString: "v",
			})
			require.NoError(t, err)

			rotateImmediately := false
			_, err = b.RotateSecret(ctx, &secretsmanager.RotateSecretInput{
				SecretID:          secretName,
				RotationLambdaARN: testLambdaARN,
				RotationRules:     tt.rules,
				RotateImmediately: &rotateImmediately,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestComputeNextRotationDate_WindowAlignment verifies rate() window starts align to midnight
// UTC (days) or the top of the hour (hours), per rotate-secrets_schedule.html.
func TestComputeNextRotationDate_WindowAlignment(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 3, 15, 15, 30, 0, 0, time.UTC)

	tests := []struct {
		want  time.Time
		rules *secretsmanager.RotationRulesType
		name  string
	}{
		{
			name:  "daily_rate_aligns_to_midnight_utc",
			rules: &secretsmanager.RotationRulesType{ScheduleExpression: "rate(2 days)"},
			want:  time.Date(2024, 3, 17, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "hourly_rate_aligns_to_the_hour",
			rules: &secretsmanager.RotationRulesType{ScheduleExpression: "rate(6 hours)"},
			want:  time.Date(2024, 3, 15, 21, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			t.Cleanup(b.StopRotationScheduler)
			b.SetNowForTest(func() time.Time { return base })
			t.Cleanup(func() { b.SetNowForTest(time.Now) })

			ctx := context.Background()
			secretName := "align-" + tt.name
			_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
				Name:         secretName,
				SecretString: "v",
			})
			require.NoError(t, err)

			rotateImmediately := false
			_, err = b.RotateSecret(ctx, &secretsmanager.RotateSecretInput{
				SecretID:          secretName,
				RotationLambdaARN: testLambdaARN,
				RotationRules:     tt.rules,
				RotateImmediately: &rotateImmediately,
			})
			require.NoError(t, err)

			desc, err := b.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretID: secretName})
			require.NoError(t, err)
			require.NotNil(t, desc.NextRotationDate)
			assert.InDelta(t, secretsmanager.UnixTimeFloat(tt.want), *desc.NextRotationDate, 1)
		})
	}
}

// TestRotateSecret_RateScheduleFiresAtAlignedWindowStart proves the scheduler fires at the
// aligned window start, not the literal unaligned base+interval instant.
func TestRotateSecret_RateScheduleFiresAtAlignedWindowStart(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := secretsmanager.NewInMemoryBackend()
		t.Cleanup(b.StopRotationScheduler)
		ctx := context.Background()

		now0 := time.Now().UTC()
		_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
			Name:         "rate-align-fire",
			SecretString: "initial",
		})
		require.NoError(t, err)

		before, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "rate-align-fire"})
		require.NoError(t, err)

		rotateImmediately := false
		_, err = b.RotateSecret(ctx, &secretsmanager.RotateSecretInput{
			SecretID:          "rate-align-fire",
			RotationLambdaARN: testLambdaARN,
			RotateImmediately: &rotateImmediately,
			RotationRules:     &secretsmanager.RotationRulesType{ScheduleExpression: "rate(2 hours)"},
		})
		require.NoError(t, err)

		candidate := now0.Add(2 * time.Hour)
		aligned := time.Date(
			candidate.Year(), candidate.Month(), candidate.Day(), candidate.Hour(), 0, 0, 0, time.UTC,
		)

		time.Sleep(aligned.Sub(now0) + time.Second)
		synctest.Wait()

		after, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "rate-align-fire"})
		require.NoError(t, err)
		assert.NotEqual(t, before.VersionID, after.VersionID,
			"rotation must fire once the virtual clock passes the aligned window start")

		desc, err := b.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretID: "rate-align-fire"})
		require.NoError(t, err)
		require.NotNil(t, desc.LastRotatedDate)
		assert.InDelta(t, secretsmanager.UnixTimeFloat(aligned), *desc.LastRotatedDate, 2)
	})
}
