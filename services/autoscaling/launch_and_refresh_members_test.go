package autoscaling_test

import (
	"net/url"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func createZonedGroup(t *testing.T, c *assdk.Client, name string, zones ...string) {
	t.Helper()

	_, err := c.CreateLaunchConfiguration(t.Context(), &assdk.CreateLaunchConfigurationInput{
		LaunchConfigurationName: aws.String(name + "-lc"),
		ImageId:                 aws.String("ami-1"),
		InstanceType:            aws.String("t3.micro"),
	})
	require.NoError(t, err)

	_, err = c.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName:    aws.String(name),
		LaunchConfigurationName: aws.String(name + "-lc"),
		MinSize:                 aws.Int32(0),
		MaxSize:                 aws.Int32(20),
		DesiredCapacity:         aws.Int32(0),
		AvailabilityZones:       zones,
	})
	require.NoError(t, err)
}

func instanceZones(t *testing.T, c *assdk.Client, group string) map[string]int {
	t.Helper()

	out, err := c.DescribeAutoScalingGroups(t.Context(), &assdk.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{group},
	})
	require.NoError(t, err)
	require.Len(t, out.AutoScalingGroups, 1)

	zones := map[string]int{}
	for _, in := range out.AutoScalingGroups[0].Instances {
		zones[aws.ToString(in.AvailabilityZone)]++
	}

	return zones
}

func TestLaunchInstances_ClientTokenAndZones(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantZones map[string]int
		name      string
		zones     []string
		wantErr   bool
	}{
		{name: "default_zone", wantZones: map[string]int{"us-east-1a": 2}},
		{name: "requested_zone", zones: []string{"us-east-1b"}, wantZones: map[string]int{"us-east-1b": 2}},
		{
			name: "spread_over_zones", zones: []string{"us-east-1a", "us-east-1b"},
			wantZones: map[string]int{"us-east-1a": 1, "us-east-1b": 1},
		},
		{name: "foreign_zone_rejected", zones: []string{"us-east-1c"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			createZonedGroup(t, client, "grp", "us-east-1a", "us-east-1b")

			launch := func(token string, capacity int32) (*assdk.LaunchInstancesOutput, error) {
				return client.LaunchInstances(t.Context(), &assdk.LaunchInstancesInput{
					AutoScalingGroupName: aws.String("grp"),
					ClientToken:          aws.String(token),
					RequestedCapacity:    aws.Int32(capacity),
					AvailabilityZones:    tt.zones,
				})
			}

			first, err := launch("tok", 2)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			replay, err := launch("tok", 2)
			require.NoError(t, err)

			ids := func(o *assdk.LaunchInstancesOutput) []string {
				var all []string
				for _, c := range o.Instances {
					all = append(all, c.InstanceIds...)
				}

				return all
			}

			assert.ElementsMatch(t, ids(first), ids(replay), "same token replays the same instances")
			assert.Equal(t, tt.wantZones, instanceZones(t, client, "grp"), "replay launches nothing extra")

			_, err = launch("tok", 3)
			require.Error(t, err, "same token with different parameters is rejected")
		})
	}
}

func TestDescribeScalingActivities_ActivityIds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pick  func(ids []string) []string
		name  string
		count int
	}{
		{name: "one", pick: func(ids []string) []string { return ids[:1] }, count: 1},
		{
			name:  "unknown_ignored",
			pick:  func(ids []string) []string { return append([]string{"nope"}, ids[:1]...) },
			count: 1,
		},
		{name: "none_requested", pick: func([]string) []string { return nil }, count: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			createZonedGroup(t, client, "grp", "us-east-1a")

			launched, err := client.LaunchInstances(t.Context(), &assdk.LaunchInstancesInput{
				AutoScalingGroupName: aws.String("grp"), ClientToken: aws.String("t"), RequestedCapacity: aws.Int32(1),
			})
			require.NoError(t, err)

			_, err = client.TerminateInstanceInAutoScalingGroup(
				t.Context(),
				&assdk.TerminateInstanceInAutoScalingGroupInput{
					InstanceId:                     aws.String(launched.Instances[0].InstanceIds[0]),
					ShouldDecrementDesiredCapacity: aws.Bool(true),
				},
			)
			require.NoError(t, err)

			all, err := client.DescribeScalingActivities(t.Context(), &assdk.DescribeScalingActivitiesInput{
				AutoScalingGroupName: aws.String("grp"),
			})
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(all.Activities), 2)

			ids := make([]string, 0, len(all.Activities))
			for _, a := range all.Activities {
				ids = append(ids, aws.ToString(a.ActivityId))
			}

			got, err := client.DescribeScalingActivities(t.Context(), &assdk.DescribeScalingActivitiesInput{
				AutoScalingGroupName: aws.String("grp"), ActivityIds: tt.pick(ids),
			})
			require.NoError(t, err)

			want := tt.count
			if want < 0 {
				want = len(all.Activities)
			}

			assert.Len(t, got.Activities, want)
		})
	}
}

func TestCreateLaunchConfiguration_MetadataOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		opts    *types.InstanceMetadataOptions
		name    string
		wantErr bool
	}{
		{name: "absent"},
		{
			name: "required_tokens",
			opts: &types.InstanceMetadataOptions{
				HttpEndpoint:            types.InstanceMetadataEndpointStateEnabled,
				HttpTokens:              types.InstanceMetadataHttpTokensStateRequired,
				HttpPutResponseHopLimit: aws.Int32(2),
			},
		},
		{
			name:    "hop_limit_out_of_range",
			opts:    &types.InstanceMetadataOptions{HttpPutResponseHopLimit: aws.Int32(65)},
			wantErr: true,
		},
		{name: "bad_tokens", opts: &types.InstanceMetadataOptions{HttpTokens: "sometimes"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			_, err := client.CreateLaunchConfiguration(t.Context(), &assdk.CreateLaunchConfigurationInput{
				LaunchConfigurationName: aws.String("lc"),
				ImageId:                 aws.String("ami-1"),
				InstanceType:            aws.String("t3.micro"),
				MetadataOptions:         tt.opts,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := client.DescribeLaunchConfigurations(t.Context(), &assdk.DescribeLaunchConfigurationsInput{
				LaunchConfigurationNames: []string{"lc"},
			})
			require.NoError(t, err)
			require.Len(t, got.LaunchConfigurations, 1)

			if tt.opts == nil {
				assert.Nil(t, got.LaunchConfigurations[0].MetadataOptions)

				return
			}

			require.NotNil(t, got.LaunchConfigurations[0].MetadataOptions)
			assert.Equal(t, *tt.opts, *got.LaunchConfigurations[0].MetadataOptions)
		})
	}
}

func TestStartInstanceRefresh_DesiredConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc *autoscaling.DesiredConfiguration
		name string
	}{
		{
			name: "launch_template",
			desc: &autoscaling.DesiredConfiguration{
				LaunchTemplate: &autoscaling.LaunchTemplateSpecification{LaunchTemplateName: "lt-new", Version: "2"},
			},
		},
		{name: "absent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := autoscaling.NewInMemoryBackend()
				defer b.Close()

				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "grp",
					MinSize:              0,
					MaxSize:              5,
					LaunchTemplate: &autoscaling.LaunchTemplateSpecification{
						LaunchTemplateName: "lt-old", Version: "1",
					},
				})
				require.NoError(t, err)

				_, err = b.StartInstanceRefreshWithInput(autoscaling.StartInstanceRefreshInput{
					AutoScalingGroupName: "grp", DesiredConfiguration: tt.desc,
				})
				require.NoError(t, err)

				groups, err := b.DescribeAutoScalingGroups([]string{"grp"}, nil)
				require.NoError(t, err)
				assert.Equal(t, "lt-old", groups[0].LaunchTemplate.LaunchTemplateName, "unchanged while in progress")

				time.Sleep(time.Hour)
				synctest.Wait()

				groups, err = b.DescribeAutoScalingGroups([]string{"grp"}, nil)
				require.NoError(t, err)

				want := "lt-old"
				if tt.desc != nil {
					want = "lt-new"
				}

				assert.Equal(t, want, groups[0].LaunchTemplate.LaunchTemplateName)
			})
		})
	}
}

func TestDescribeInstanceRefreshes_DesiredConfiguration(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("grp"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(5),
		AvailabilityZones:    []string{"us-east-1a"},
		LaunchTemplate: &types.LaunchTemplateSpecification{
			LaunchTemplateName: aws.String("lt-old"),
			Version:            aws.String("1"),
		},
	})
	require.NoError(t, err)

	started, err := client.StartInstanceRefresh(t.Context(), &assdk.StartInstanceRefreshInput{
		AutoScalingGroupName: aws.String("grp"),
		DesiredConfiguration: &types.DesiredConfiguration{
			LaunchTemplate: &types.LaunchTemplateSpecification{
				LaunchTemplateName: aws.String("lt-new"),
				Version:            aws.String("2"),
			},
		},
	})
	require.NoError(t, err)

	got, err := client.DescribeInstanceRefreshes(t.Context(), &assdk.DescribeInstanceRefreshesInput{
		AutoScalingGroupName: aws.String("grp"), InstanceRefreshIds: []string{aws.ToString(started.InstanceRefreshId)},
	})
	require.NoError(t, err)
	require.Len(t, got.InstanceRefreshes, 1)
	require.NotNil(t, got.InstanceRefreshes[0].DesiredConfiguration)
	assert.Equal(
		t,
		"lt-new",
		aws.ToString(got.InstanceRefreshes[0].DesiredConfiguration.LaunchTemplate.LaunchTemplateName),
	)
}

func TestGetPredictiveScalingForecast_Window(t *testing.T) {
	t.Parallel()

	start := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		end       time.Time
		name      string
		wantCount int
		wantErr   bool
	}{
		{name: "six_hours", end: start.Add(6 * time.Hour), wantCount: 6},
		{name: "end_before_start", end: start.Add(-time.Hour), wantErr: true},
		{name: "over_thirty_days", end: start.Add(31 * 24 * time.Hour), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			createZonedGroup(t, client, "grp", "us-east-1a")

			got, err := client.GetPredictiveScalingForecast(t.Context(), &assdk.GetPredictiveScalingForecastInput{
				AutoScalingGroupName: aws.String("grp"), PolicyName: aws.String("p"),
				StartTime: aws.Time(start), EndTime: aws.Time(tt.end),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, got.CapacityForecast.Timestamps, tt.wantCount)
			assert.Equal(t, start, got.CapacityForecast.Timestamps[0].UTC())
		})
	}
}

func TestPutScalingPolicy_PredictiveSpecRequiresTargetValue(t *testing.T) {
	t.Parallel()

	const spec = "PredictiveScalingConfiguration.MetricSpecifications.member.1."

	tests := []struct {
		name       string
		target     string
		wantStatus int
	}{
		{name: "with_target", target: "40", wantStatus: 200},
		{name: "missing_target", wantStatus: 400},
		{name: "non_numeric_target", target: "high", wantStatus: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName": {"grp"}, "MinSize": {"1"}, "MaxSize": {"10"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			form := url.Values{
				"AutoScalingGroupName": {"grp"}, "PolicyName": {"p"}, "PolicyType": {"PredictiveScaling"},
				spec + "PredefinedMetricPairSpecification.PredefinedMetricType": {"ASGCPUUtilization"},
			}
			if tt.target != "" {
				form.Set(spec+"TargetValue", tt.target)
			}

			code, body = doAS(t, h, "PutScalingPolicy", form)
			assert.Equal(t, tt.wantStatus, code, body)
		})
	}
}
