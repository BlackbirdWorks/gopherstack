package applicationautoscaling_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aassdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

type ttEnv struct {
	aas *aassdk.Client
	cw  *cwsdk.Client
}

func newTTEnv(t *testing.T) *ttEnv {
	t.Helper()

	cwH := cwbackend.NewHandler(cwbackend.NewInMemoryBackendWithConfig("123456789012", "us-east-1"))
	bk := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")
	bk.SetAppConfig(&fakeCloudWatchSibling{h: cwH})

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(applicationautoscaling.NewHandler(bk)))
	require.NoError(t, registry.Register(cwH))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	env := &ttEnv{
		aas: aassdk.NewFromConfig(cfg, func(o *aassdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
		cw:  cwsdk.NewFromConfig(cfg, func(o *cwsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
	}

	_, err = env.aas.RegisterScalableTarget(t.Context(), &aassdk.RegisterScalableTargetInput{
		ServiceNamespace:  aastypes.ServiceNamespaceEcs,
		ResourceId:        aws.String("service/c/s"),
		ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
		MinCapacity:       aws.Int32(1),
		MaxCapacity:       aws.Int32(5),
	})
	require.NoError(t, err)

	return env
}

func (e *ttEnv) put(
	t *testing.T, cfg *aastypes.TargetTrackingScalingPolicyConfiguration,
) *aassdk.PutScalingPolicyOutput {
	t.Helper()

	out, err := e.aas.PutScalingPolicy(t.Context(), &aassdk.PutScalingPolicyInput{
		ServiceNamespace:                         aastypes.ServiceNamespaceEcs,
		ResourceId:                               aws.String("service/c/s"),
		ScalableDimension:                        aastypes.ScalableDimensionECSServiceDesiredCount,
		PolicyName:                               aws.String("tt"),
		PolicyType:                               aastypes.PolicyTypeTargetTrackingScaling,
		TargetTrackingScalingPolicyConfiguration: cfg,
	})
	require.NoError(t, err)

	return out
}

func (e *ttEnv) alarms(t *testing.T) map[string]cwtypes.MetricAlarm {
	t.Helper()

	out, err := e.cw.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{
		AlarmNamePrefix: aws.String("TargetTracking-"),
	})
	require.NoError(t, err)

	got := map[string]cwtypes.MetricAlarm{}
	for _, a := range out.MetricAlarms {
		got[aws.ToString(a.AlarmName)] = a
	}

	return got
}

func ecsCPUConfig(target float64) *aastypes.TargetTrackingScalingPolicyConfiguration {
	return &aastypes.TargetTrackingScalingPolicyConfiguration{
		TargetValue: aws.Float64(target),
		PredefinedMetricSpecification: &aastypes.PredefinedMetricSpecification{
			PredefinedMetricType: aastypes.MetricTypeECSServiceAverageCPUUtilization,
		},
	}
}

func TestTargetTracking_ManagedAlarms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg        *aastypes.TargetTrackingScalingPolicyConfiguration
		check      func(t *testing.T, high, low cwtypes.MetricAlarm)
		name       string
		wantAlarms int
	}{
		{
			name: "predefined_ecs_cpu", cfg: ecsCPUConfig(60), wantAlarms: 2,
			check: func(t *testing.T, high, low cwtypes.MetricAlarm) {
				t.Helper()
				assert.Equal(t, "AWS/ECS", aws.ToString(high.Namespace))
				assert.Equal(t, "CPUUtilization", aws.ToString(high.MetricName))
				assert.InDelta(t, 60, aws.ToFloat64(high.Threshold), 0.001)
				assert.InDelta(t, 54, aws.ToFloat64(low.Threshold), 0.001)
				assert.Equal(t, cwtypes.ComparisonOperatorGreaterThanThreshold, high.ComparisonOperator)
				assert.Equal(t, cwtypes.ComparisonOperatorLessThanThreshold, low.ComparisonOperator)
				assert.Equal(t, int32(3), aws.ToInt32(high.EvaluationPeriods))
				assert.Equal(t, int32(15), aws.ToInt32(low.EvaluationPeriods))

				dims := map[string]string{}
				for _, d := range high.Dimensions {
					dims[aws.ToString(d.Name)] = aws.ToString(d.Value)
				}

				assert.Equal(t, map[string]string{"ClusterName": "c", "ServiceName": "s"}, dims)
			},
		},
		{
			name: "customized_metric", wantAlarms: 2,
			cfg: &aastypes.TargetTrackingScalingPolicyConfiguration{
				TargetValue: aws.Float64(10),
				CustomizedMetricSpecification: &aastypes.CustomizedMetricSpecification{
					MetricName: aws.String(
						"QueueDepth",
					),
					Namespace:  aws.String("App"),
					Statistic:  aastypes.MetricStatisticSum,
					Dimensions: []aastypes.MetricDimension{{Name: aws.String("Q"), Value: aws.String("jobs")}},
				},
			},
			check: func(t *testing.T, high, _ cwtypes.MetricAlarm) {
				t.Helper()
				assert.Equal(t, "App", aws.ToString(high.Namespace))
				assert.Equal(t, "QueueDepth", aws.ToString(high.MetricName))
				assert.Equal(t, cwtypes.StatisticSum, high.Statistic)
			},
		},
		{
			name: "scale_in_disabled", wantAlarms: 1,
			cfg: func() *aastypes.TargetTrackingScalingPolicyConfiguration {
				c := ecsCPUConfig(60)
				c.DisableScaleIn = aws.Bool(true)

				return c
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newTTEnv(t)
			put := env.put(t, tt.cfg)
			require.Len(t, put.Alarms, tt.wantAlarms)

			got := env.alarms(t)
			require.Len(t, got, tt.wantAlarms)

			var high, low cwtypes.MetricAlarm

			for _, a := range put.Alarms {
				name := aws.ToString(a.AlarmName)
				alarm, ok := got[name]
				require.True(t, ok, name)
				assert.Equal(t, aws.ToString(a.AlarmARN), aws.ToString(alarm.AlarmArn))
				assert.Equal(t, []string{aws.ToString(put.PolicyARN)}, alarm.AlarmActions)
				assert.True(t, strings.HasPrefix(name, "TargetTracking-service/c/s-"), name)

				switch {
				case strings.Contains(name, "-AlarmHigh-"):
					high = alarm
				case strings.Contains(name, "-AlarmLow-"):
					low = alarm
				}
			}

			require.NotEmpty(t, aws.ToString(high.AlarmName), "AlarmHigh exists")

			if tt.check != nil {
				tt.check(t, high, low)
			}

			desc, err := env.aas.DescribeScalingPolicies(t.Context(), &aassdk.DescribeScalingPoliciesInput{
				ServiceNamespace: aastypes.ServiceNamespaceEcs,
			})
			require.NoError(t, err)
			require.Len(t, desc.ScalingPolicies, 1)
			assert.Len(t, desc.ScalingPolicies[0].Alarms, tt.wantAlarms)
		})
	}
}

func TestTargetTracking_ReplaceAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		finish func(t *testing.T, env *ttEnv)
		name   string
		want   int
	}{
		{
			name: "update_keeps_alarm_names_and_moves_threshold", want: 2,
			finish: func(t *testing.T, env *ttEnv) {
				t.Helper()

				before := env.alarms(t)
				env.put(t, ecsCPUConfig(80))
				after := env.alarms(t)

				require.Len(t, after, 2)

				for name, a := range after {
					_, existed := before[name]
					assert.True(t, existed, "alarm %s reused", name)

					if strings.Contains(name, "-AlarmHigh-") {
						assert.InDelta(t, 80, aws.ToFloat64(a.Threshold), 0.001)
					}
				}
			},
		},
		{
			name: "enabling_disable_scale_in_drops_low_alarm", want: 1,
			finish: func(t *testing.T, env *ttEnv) {
				t.Helper()

				c := ecsCPUConfig(60)
				c.DisableScaleIn = aws.Bool(true)
				env.put(t, c)
				assert.Len(t, env.alarms(t), 1)
			},
		},
		{
			name: "delete_policy_removes_alarms", want: 0,
			finish: func(t *testing.T, env *ttEnv) {
				t.Helper()

				_, err := env.aas.DeleteScalingPolicy(t.Context(), &aassdk.DeleteScalingPolicyInput{
					ServiceNamespace:  aastypes.ServiceNamespaceEcs,
					ResourceId:        aws.String("service/c/s"),
					ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
					PolicyName:        aws.String("tt"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "deregister_target_removes_alarms", want: 0,
			finish: func(t *testing.T, env *ttEnv) {
				t.Helper()

				_, err := env.aas.DeregisterScalableTarget(t.Context(), &aassdk.DeregisterScalableTargetInput{
					ServiceNamespace:  aastypes.ServiceNamespaceEcs,
					ResourceId:        aws.String("service/c/s"),
					ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newTTEnv(t)
			env.put(t, ecsCPUConfig(60))
			require.Len(t, env.alarms(t), 2)

			tt.finish(t, env)
			assert.Len(t, env.alarms(t), tt.want)
		})
	}
}
