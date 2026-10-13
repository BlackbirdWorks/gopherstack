package autoscaling_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

type fakeEC2 struct {
	subnets   map[string]string
	instances map[string]autoscaling.InstanceAttributes
}

func (f fakeEC2) SubnetAvailabilityZone(_ context.Context, id string) (string, bool) {
	az, ok := f.subnets[id]

	return az, ok
}

func (f fakeEC2) InstanceAttributes(_ context.Context, id string) (autoscaling.InstanceAttributes, bool) {
	a, ok := f.instances[id]

	return a, ok
}

func newClientWithEC2(t *testing.T, lookup autoscaling.EC2Lookup) *assdk.Client {
	t.Helper()

	backend := autoscaling.NewInMemoryBackend()
	backend.SetEC2Lookup(lookup)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(autoscaling.NewHandler(backend)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return assdk.NewFromConfig(cfg, func(o *assdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

func TestAvailabilityZoneIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ids       []string
		zones     []string
		wantZones []string
		wantErr   bool
	}{
		{name: "ids_resolve", ids: []string{"use1-az1", "use1-az2"}, wantZones: []string{"us-east-1a", "us-east-1b"}},
		{name: "names_report_ids", zones: []string{"us-east-1b"}, wantZones: []string{"us-east-1b"}},
		{name: "both_rejected", ids: []string{"use1-az1"}, zones: []string{"us-east-1a"}, wantErr: true},
		{name: "unknown_id_rejected", ids: []string{"euw1-az1"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			_, err := client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String("g"),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(2),
				AvailabilityZoneIds:  tt.ids,
				AvailabilityZones:    tt.zones,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			out, err := client.DescribeAutoScalingGroups(t.Context(), &assdk.DescribeAutoScalingGroupsInput{})
			require.NoError(t, err)
			require.Len(t, out.AutoScalingGroups, 1)

			g := out.AutoScalingGroups[0]
			assert.Equal(t, tt.wantZones, g.AvailabilityZones)
			assert.Len(t, g.AvailabilityZoneIds, len(tt.wantZones))

			_, err = client.UpdateAutoScalingGroup(t.Context(), &assdk.UpdateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String("g"),
				AvailabilityZoneIds:  []string{"use1-az3"},
			})
			require.NoError(t, err)

			out, err = client.DescribeAutoScalingGroups(t.Context(), &assdk.DescribeAutoScalingGroupsInput{})
			require.NoError(t, err)
			assert.Equal(t, []string{"us-east-1c"}, out.AutoScalingGroups[0].AvailabilityZones)
			assert.Equal(t, []string{"use1-az3"}, out.AutoScalingGroups[0].AvailabilityZoneIds)
		})
	}
}

func TestLaunchInstances_Placement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantAZ     string
		wantAZID   string
		wantSubnet string
		zoneIDs    []string
		subnetIDs  []string
		wantErr    bool
	}{
		{
			name:       "zone_id",
			zoneIDs:    []string{"use1-az2"},
			wantAZ:     "us-east-1b",
			wantAZID:   "use1-az2",
			wantSubnet: "subnet-b",
		},
		{
			name:       "subnet",
			subnetIDs:  []string{"subnet-b"},
			wantAZ:     "us-east-1b",
			wantAZID:   "use1-az2",
			wantSubnet: "subnet-b",
		},
		{name: "default", wantAZ: "us-east-1a", wantAZID: "use1-az1", wantSubnet: "subnet-a"},
		{name: "foreign_subnet", subnetIDs: []string{"subnet-x"}, wantErr: true},
		{name: "subnet_outside_zone", subnetIDs: []string{"subnet-b"}, zoneIDs: []string{"use1-az1"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newClientWithEC2(t, fakeEC2{subnets: map[string]string{
				"subnet-a": "us-east-1a", "subnet-b": "us-east-1b", "subnet-x": "us-east-1a",
			}})

			_, err := client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String("g"),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(5),
				AvailabilityZones:    []string{"us-east-1a", "us-east-1b"},
				VPCZoneIdentifier:    aws.String("subnet-a,subnet-b"),
			})
			require.NoError(t, err)

			out, err := client.LaunchInstances(t.Context(), &assdk.LaunchInstancesInput{
				AutoScalingGroupName: aws.String("g"),
				ClientToken:          aws.String("tok"),
				RequestedCapacity:    aws.Int32(1),
				AvailabilityZoneIds:  tt.zoneIDs,
				SubnetIds:            tt.subnetIDs,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Len(t, out.Instances, 1)

			c := out.Instances[0]
			assert.Equal(t, tt.wantAZ, aws.ToString(c.AvailabilityZone))
			assert.Equal(t, tt.wantAZID, aws.ToString(c.AvailabilityZoneId))
			assert.Equal(t, tt.wantSubnet, aws.ToString(c.SubnetId))

			desc, err := client.DescribeAutoScalingInstances(t.Context(), &assdk.DescribeAutoScalingInstancesInput{
				InstanceIds: c.InstanceIds,
			})
			require.NoError(t, err)
			require.Len(t, desc.AutoScalingInstances, 1)
			assert.Equal(t, tt.wantAZID, aws.ToString(desc.AutoScalingInstances[0].AvailabilityZoneId))
		})
	}
}

func TestDescribeScalingActivities_IncludeDeletedGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		includeGroup *bool
		name         string
		byName       bool
		wantErr      bool
		wantActs     bool
	}{
		{name: "excluded_all"},
		{name: "included_all", includeGroup: aws.Bool(true), wantActs: true},
		{name: "included_by_name", includeGroup: aws.Bool(true), byName: true, wantActs: true},
		{name: "excluded_by_name", byName: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			createZonedGroup(t, client, "deleted-grp", "us-east-1a")

			_, err := client.DeleteAutoScalingGroup(t.Context(), &assdk.DeleteAutoScalingGroupInput{
				AutoScalingGroupName: aws.String("deleted-grp"), ForceDelete: aws.Bool(true),
			})
			require.NoError(t, err)

			in := &assdk.DescribeScalingActivitiesInput{IncludeDeletedGroups: tt.includeGroup}
			if tt.byName {
				in.AutoScalingGroupName = aws.String("deleted-grp")
			}

			out, err := client.DescribeScalingActivities(t.Context(), in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantActs, len(out.Activities) > 0)
		})
	}
}

func TestInstanceIDDerivedLaunchSettings(t *testing.T) {
	t.Parallel()

	attrs := autoscaling.InstanceAttributes{
		ImageID: "ami-from-instance", InstanceType: "m5.large", KeyName: "key", SecurityGroups: []string{"sg-1"},
	}

	tests := []struct {
		name      string
		instance  string
		overrides string
		wantType  string
		wantErr   bool
	}{
		{name: "derives", instance: "i-1", wantType: "m5.large"},
		{name: "override_wins", instance: "i-1", overrides: "c5.large", wantType: "c5.large"},
		{name: "unknown_instance", instance: "i-missing", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newClientWithEC2(t, fakeEC2{instances: map[string]autoscaling.InstanceAttributes{"i-1": attrs}})

			_, err := client.CreateLaunchConfiguration(t.Context(), &assdk.CreateLaunchConfigurationInput{
				LaunchConfigurationName: aws.String("lc"),
				InstanceId:              aws.String(tt.instance),
				InstanceType:            aws.String(tt.overrides),
			})

			if tt.wantErr {
				require.Error(t, err)

				_, err = client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
					AutoScalingGroupName: aws.String("g"),
					InstanceId:           aws.String(tt.instance),
					MinSize:              aws.Int32(0),
					MaxSize:              aws.Int32(1),
				})
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			lcs, err := client.DescribeLaunchConfigurations(t.Context(), &assdk.DescribeLaunchConfigurationsInput{})
			require.NoError(t, err)
			require.Len(t, lcs.LaunchConfigurations, 1)
			assert.Equal(t, "ami-from-instance", aws.ToString(lcs.LaunchConfigurations[0].ImageId))
			assert.Equal(t, tt.wantType, aws.ToString(lcs.LaunchConfigurations[0].InstanceType))
			assert.Equal(t, []string{"sg-1"}, lcs.LaunchConfigurations[0].SecurityGroups)

			_, err = client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String("g"),
				InstanceId:           aws.String(tt.instance),
				MinSize:              aws.Int32(0),
				MaxSize:              aws.Int32(1),
			})
			require.NoError(t, err)

			groups, err := client.DescribeAutoScalingGroups(t.Context(), &assdk.DescribeAutoScalingGroupsInput{})
			require.NoError(t, err)
			require.Len(t, groups.AutoScalingGroups, 1)
			assert.Equal(t, "g", aws.ToString(groups.AutoScalingGroups[0].LaunchConfigurationName))
		})
	}
}
