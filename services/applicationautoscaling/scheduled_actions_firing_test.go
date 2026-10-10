package applicationautoscaling_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aassdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

const (
	testResourceID = "service/c/s"
	testDimension  = "ecs:service:DesiredCount"
)

func newTargetClient(t *testing.T) (*aassdk.Client, *applicationautoscaling.Handler) {
	t.Helper()

	h := applicationautoscaling.NewHandler(applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1"))
	c := newTestAASSDKClient(t, h)

	_, err := c.RegisterScalableTarget(t.Context(), &aassdk.RegisterScalableTargetInput{
		ServiceNamespace:  aastypes.ServiceNamespaceEcs,
		ResourceId:        aws.String(testResourceID),
		ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
		MinCapacity:       aws.Int32(1),
		MaxCapacity:       aws.Int32(10),
	})
	require.NoError(t, err)

	return c, h
}

func putAction(t *testing.T, c *aassdk.Client, name, schedule string, tz *string) error {
	t.Helper()

	_, err := c.PutScheduledAction(t.Context(), &aassdk.PutScheduledActionInput{
		ServiceNamespace:     aastypes.ServiceNamespaceEcs,
		ResourceId:           aws.String(testResourceID),
		ScalableDimension:    aastypes.ScalableDimensionECSServiceDesiredCount,
		ScheduledActionName:  aws.String(name),
		Schedule:             aws.String(schedule),
		Timezone:             tz,
		ScalableTargetAction: &aastypes.ScalableTargetAction{MinCapacity: aws.Int32(4), MaxCapacity: aws.Int32(8)},
	})

	return err
}

func TestScheduledActions_FireOnSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		schedule  string
		wantFired bool
		advanceBy time.Duration
	}{
		{name: "at_due", schedule: "at(%s)", advanceBy: 2 * time.Minute, wantFired: true},
		{name: "at_in_past", schedule: "at(%s)", advanceBy: -time.Minute, wantFired: false},
		{name: "rate_elapsed", schedule: "rate(5 minutes)", advanceBy: 6 * time.Minute, wantFired: true},
		{name: "rate_not_elapsed", schedule: "rate(5 minutes)", advanceBy: time.Minute, wantFired: false},
		{name: "cron_every_minute", schedule: "cron(* * * * ? *)", advanceBy: 3 * time.Minute, wantFired: true},
		{name: "cron_other_year", schedule: "cron(* * * * ? 2099)", advanceBy: 3 * time.Minute, wantFired: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, h := newTargetClient(t)
			now := time.Now().UTC()

			schedule := tt.schedule
			if schedule == "at(%s)" {
				schedule = "at(" + now.Add(tt.advanceBy/2).Format("2006-01-02T15:04:05") + ")"
			}

			require.NoError(t, putAction(t, c, "sa", schedule, nil))

			h.Backend.FireDueScheduledActions(t.Context(), now.Add(time.Second))
			h.Backend.FireDueScheduledActions(t.Context(), now.Add(tt.advanceBy))

			out, err := c.DescribeScalableTargets(t.Context(), &aassdk.DescribeScalableTargetsInput{
				ServiceNamespace: aastypes.ServiceNamespaceEcs,
			})
			require.NoError(t, err)
			require.Len(t, out.ScalableTargets, 1)

			acts, err := c.DescribeScalingActivities(t.Context(), &aassdk.DescribeScalingActivitiesInput{
				ServiceNamespace: aastypes.ServiceNamespaceEcs,
			})
			require.NoError(t, err)

			if !tt.wantFired {
				assert.Equal(t, int32(1), aws.ToInt32(out.ScalableTargets[0].MinCapacity))

				return
			}

			assert.Equal(t, int32(4), aws.ToInt32(out.ScalableTargets[0].MinCapacity))
			assert.Equal(t, int32(8), aws.ToInt32(out.ScalableTargets[0].MaxCapacity))

			found := false
			for _, a := range acts.ScalingActivities {
				if aws.ToString(a.Cause) == "scheduled action name sa was triggered" {
					found = true

					assert.Equal(t, "Setting min capacity to 4 and max capacity to 8", aws.ToString(a.Description))
					assert.Equal(t, aastypes.ScalingActivityStatusCodeSuccessful, a.StatusCode)
				}
			}
			assert.True(t, found)
		})
	}
}

func TestScheduledActions_AtFiresOnce(t *testing.T) {
	t.Parallel()

	c, h := newTargetClient(t)
	now := time.Now().UTC()

	require.NoError(t, putAction(t, c, "once", "at("+now.Add(time.Minute).Format("2006-01-02T15:04:05")+")", nil))

	h.Backend.FireDueScheduledActions(t.Context(), now)
	h.Backend.FireDueScheduledActions(t.Context(), now.Add(2*time.Minute))
	h.Backend.FireDueScheduledActions(t.Context(), now.Add(3*time.Minute))

	acts, err := c.DescribeScalingActivities(t.Context(), &aassdk.DescribeScalingActivitiesInput{
		ServiceNamespace: aastypes.ServiceNamespaceEcs,
	})
	require.NoError(t, err)

	fired := 0
	for _, a := range acts.ScalingActivities {
		if aws.ToString(a.Cause) == "scheduled action name once was triggered" {
			fired++
		}
	}
	assert.Equal(t, 1, fired)
}

func TestScheduledActions_ScheduleValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tz       *string
		name     string
		schedule string
		wantErr  bool
	}{
		{name: "at_ok", schedule: "at(2030-01-01T00:00:00)"},
		{name: "rate_ok", schedule: "rate(1 hour)"},
		{name: "rate_singular_mismatch", schedule: "rate(2 hour)", wantErr: true},
		{name: "rate_zero", schedule: "rate(0 minutes)", wantErr: true},
		{name: "cron_ok", schedule: "cron(0 12 * * ? *)"},
		{name: "cron_names_ok", schedule: "cron(0 12 ? JAN MON-FRI *)"},
		{name: "cron_five_fields", schedule: "cron(0 12 * * ?)", wantErr: true},
		{name: "cron_both_day_fields", schedule: "cron(0 12 1 * 2 *)", wantErr: true},
		{name: "cron_minute_range", schedule: "cron(75 12 * * ? *)", wantErr: true},
		{name: "unknown_kind", schedule: "bogus(5)", wantErr: true},
		{name: "at_bad_format", schedule: "at(2030-01-01)", wantErr: true},
		{name: "timezone_ok", schedule: "cron(0 12 * * ? *)", tz: aws.String("America/New_York")},
		{name: "timezone_bad", schedule: "cron(0 12 * * ? *)", tz: aws.String("Mars/Base"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, _ := newTargetClient(t)
			err := putAction(t, c, "sa", tt.schedule, tt.tz)

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr, "got %v", err)
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "ValidationException:")
		})
	}
}

func TestErrorWording(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(*aassdk.Client) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "deregister_unknown_target", wantCode: "ObjectNotFoundException",
			wantMsg: "No scalable target found for service namespace: ecs, resource ID: service/c/nope, " +
				"scalable dimension: ecs:service:DesiredCount",
			call: func(c *aassdk.Client) error {
				_, err := c.DeregisterScalableTarget(t.Context(), &aassdk.DeregisterScalableTargetInput{
					ServiceNamespace:  aastypes.ServiceNamespaceEcs,
					ResourceId:        aws.String("service/c/nope"),
					ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
				})

				return err
			},
		},
		{
			name: "delete_unknown_policy", wantCode: "ObjectNotFoundException",
			wantMsg: "No scaling policy found for service namespace: ecs, resource ID: service/c/s, " +
				"scalable dimension: ecs:service:DesiredCount, policy name: nope",
			call: func(c *aassdk.Client) error {
				_, err := c.DeleteScalingPolicy(t.Context(), &aassdk.DeleteScalingPolicyInput{
					ServiceNamespace:  aastypes.ServiceNamespaceEcs,
					ResourceId:        aws.String(testResourceID),
					ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
					PolicyName:        aws.String("nope"),
				})

				return err
			},
		},
		{
			name: "delete_unknown_scheduled_action", wantCode: "ObjectNotFoundException",
			wantMsg: "No scheduled action found for service namespace: ecs, resource ID: service/c/s, " +
				"scalable dimension: ecs:service:DesiredCount, scheduled action name: nope",
			call: func(c *aassdk.Client) error {
				_, err := c.DeleteScheduledAction(t.Context(), &aassdk.DeleteScheduledActionInput{
					ServiceNamespace:    aastypes.ServiceNamespaceEcs,
					ResourceId:          aws.String(testResourceID),
					ScalableDimension:   aastypes.ScalableDimensionECSServiceDesiredCount,
					ScheduledActionName: aws.String("nope"),
				})

				return err
			},
		},
		{
			name: "negative_min_capacity", wantCode: "ValidationException",
			wantMsg: "Capacity values must be 0 or greater",
			call: func(c *aassdk.Client) error {
				_, err := c.RegisterScalableTarget(t.Context(), &aassdk.RegisterScalableTargetInput{
					ServiceNamespace:  aastypes.ServiceNamespaceEcs,
					ResourceId:        aws.String("service/c/other"),
					ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
					MinCapacity:       aws.Int32(-1),
					MaxCapacity:       aws.Int32(5),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, _ := newTargetClient(t)
			err := tt.call(c)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr, "got %v", err)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Equal(t, tt.wantMsg, apiErr.ErrorMessage())
		})
	}
}

func TestHandler_SchedulerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions bool
	}{
		{name: "home_only"},
		{name: "with_regional_sibling", regions: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := applicationautoscaling.NewHandler(
				applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1"),
			)
			if tt.regions {
				h.EnableRegions()
			}

			require.NoError(t, h.StartWorker(t.Context()))

			if tt.regions {
				require.NotNil(t, h.BackendFor("eu-west-1"))
			}

			h.Shutdown(t.Context())
		})
	}
}
