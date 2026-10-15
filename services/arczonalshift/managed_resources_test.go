package arczonalshift_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/arczonalshift"
	"github.com/aws/aws-sdk-go-v2/service/arczonalshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	asg "github.com/blackbirdworks/gopherstack/services/autoscaling"
	cw "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	ec2bk "github.com/blackbirdworks/gopherstack/services/ec2"
	eksbk "github.com/blackbirdworks/gopherstack/services/eks"
	elbv2 "github.com/blackbirdworks/gopherstack/services/elbv2"
)

type extSiblings struct {
	*siblings
	asg, eks, ec2 service.Registerable
}

func (s *extSiblings) GetAutoscalingHandler() service.Registerable { return s.asg }
func (s *extSiblings) GetEKSHandler() service.Registerable         { return s.eks }
func (s *extSiblings) GetEC2Handler() service.Registerable         { return s.ec2 }

type managedWorld struct {
	asg *asg.InMemoryBackend
	eks *eksbk.InMemoryBackend
	ec2 *ec2bk.InMemoryBackend
}

func newManagedFixture(t *testing.T) (*fixture, *managedWorld) {
	t.Helper()

	f := newFixture(t)
	w := &managedWorld{
		asg: asg.NewInMemoryBackendWithConfig(testAccount, testRegion),
		eks: eksbk.NewInMemoryBackend(t.Context(), testAccount, testRegion),
		ec2: ec2bk.NewInMemoryBackend(testAccount, testRegion),
	}

	f.backend.SetAppConfig(&extSiblings{
		siblings: &siblings{elb: elbv2.NewHandler(f.elb), cw: cw.NewHandler(f.cw)},
		asg:      asg.NewHandler(w.asg),
		eks:      eksbk.NewHandler(w.eks),
		ec2:      ec2bk.NewHandler(w.ec2),
	})

	return f, w
}

func (w *managedWorld) group(t *testing.T, name string, enabled bool) string {
	t.Helper()

	g, err := w.asg.CreateAutoScalingGroup(asg.CreateAutoScalingGroupInput{
		AutoScalingGroupName: name, MinSize: 0, MaxSize: 2,
		AvailabilityZones:                []string{"us-east-1a", "us-east-1b"},
		AvailabilityZoneImpairmentPolicy: &asg.AvailabilityZoneImpairmentPolicy{ZonalShiftEnabled: enabled},
	})
	require.NoError(t, err)

	return g.AutoScalingGroupARN
}

func (w *managedWorld) cluster(t *testing.T, name string, zonalShift json.RawMessage) string {
	t.Helper()

	vpc, err := w.ec2.CreateVpc("10.0.0.0/16", "")
	require.NoError(t, err)

	subnets := make([]string, 0, 2)

	for i, az := range []string{"us-east-1a", "us-east-1c"} {
		s, subErr := w.ec2.CreateSubnet(vpc.ID, "10.0."+string(rune('0'+i))+".0/24", az)
		require.NoError(t, subErr)

		subnets = append(subnets, s.ID)
	}

	opts := eksbk.ClusterOptionalConfig{}
	if zonalShift != nil {
		opts.ConfigBlocks = map[string]json.RawMessage{"zonalShiftConfig": zonalShift}
	}

	c, err := w.eks.CreateCluster(name, "1.31", "arn:aws:iam::"+testAccount+":role/eks",
		&eksbk.VpcConfig{SubnetIDs: subnets}, nil, nil, opts)
	require.NoError(t, err)

	return c.ARN
}

func TestManagedResourcesAutoScalingAndEKS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create    func(t *testing.T, w *managedWorld) string
		name      string
		wantName  string
		wantZones []string
		managed   bool
	}{
		{
			name: "asg with zonal shift enabled", managed: true, wantName: "web",
			wantZones: []string{"use1-az1", "use1-az2"},
			create: func(t *testing.T, w *managedWorld) string {
				t.Helper()

				return w.group(t, "web", true)
			},
		},
		{
			name: "asg without zonal shift", managed: false,
			create: func(t *testing.T, w *managedWorld) string {
				t.Helper()

				return w.group(t, "web", false)
			},
		},
		{
			name: "eks with zonal shift enabled", managed: true, wantName: "k8s",
			wantZones: []string{"use1-az1", "use1-az3"},
			create: func(t *testing.T, w *managedWorld) string {
				t.Helper()

				return w.cluster(t, "k8s", json.RawMessage(`{"enabled":true}`))
			},
		},
		{
			name: "eks with zonal shift disabled", managed: false,
			create: func(t *testing.T, w *managedWorld) string {
				t.Helper()

				return w.cluster(t, "k8s", json.RawMessage(`{"enabled":false}`))
			},
		},
		{
			name: "eks without zonal shift config", managed: false,
			create: func(t *testing.T, w *managedWorld) string {
				t.Helper()

				return w.cluster(t, "k8s", nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, w := newManagedFixture(t)
			arn := tt.create(t, w)
			ctx := t.Context()

			list, err := f.client.ListManagedResources(ctx, &sdk.ListManagedResourcesInput{})
			require.NoError(t, err)

			_, startErr := f.client.StartZonalShift(ctx, &sdk.StartZonalShiftInput{
				ResourceIdentifier: aws.String(arn), AwayFrom: aws.String("use1-az1"),
				Comment: aws.String("drill"), ExpiresIn: aws.String("30m"),
			})

			got, getErr := f.client.GetManagedResource(
				ctx,
				&sdk.GetManagedResourceInput{ResourceIdentifier: aws.String(arn)},
			)

			if !tt.managed {
				assert.Empty(t, list.Items)
				assert.Equal(t, "ResourceNotFoundException", apiCode(getErr))
				assert.Equal(t, "ResourceNotFoundException", apiCode(startErr))

				return
			}

			require.NoError(t, getErr)
			require.NoError(t, startErr)
			require.Len(t, list.Items, 1)
			assert.Equal(t, arn, aws.ToString(list.Items[0].Arn))
			assert.Equal(t, tt.wantName, aws.ToString(list.Items[0].Name))
			assert.ElementsMatch(t, tt.wantZones, list.Items[0].AvailabilityZones)
			assert.Equal(t, tt.wantName, aws.ToString(got.Name))
			assert.InDelta(t, float32(0), got.AppliedWeights["use1-az1"], 0.0001)
			require.Len(t, got.ZonalShifts, 1)
			assert.Equal(t, types.AppliedStatusApplied, got.ZonalShifts[0].AppliedStatus)

			require.NoError(t, f.cw.PutMetricAlarm(&cw.MetricAlarm{AlarmName: "outcome", StateValue: "OK"}))

			practice, err := f.client.CreatePracticeRunConfiguration(ctx, &sdk.CreatePracticeRunConfigurationInput{
				ResourceIdentifier: aws.String(arn),
				OutcomeAlarms: []types.ControlCondition{{
					AlarmIdentifier: aws.String("arn:aws:cloudwatch:us-east-1:123456789012:alarm:outcome"),
					Type:            types.ControlConditionTypeCloudwatch,
				}},
			})
			require.NoError(t, err)
			assert.Equal(t, arn, aws.ToString(practice.Arn))

			_, err = f.client.UpdateZonalAutoshiftConfiguration(ctx, &sdk.UpdateZonalAutoshiftConfigurationInput{
				ResourceIdentifier: aws.String(arn), ZonalAutoshiftStatus: types.ZonalAutoshiftStatusEnabled,
			})
			require.NoError(t, err)

			again, err := f.client.GetManagedResource(
				ctx,
				&sdk.GetManagedResourceInput{ResourceIdentifier: aws.String(arn)},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ZonalAutoshiftStatusEnabled, again.ZonalAutoshiftStatus)
		})
	}
}

func TestManagedResourceStopsWhenZonalShiftDisabled(t *testing.T) {
	t.Parallel()

	f, w := newManagedFixture(t)
	arn := w.group(t, "web", true)

	_, err := f.client.GetManagedResource(
		t.Context(),
		&sdk.GetManagedResourceInput{ResourceIdentifier: aws.String(arn)},
	)
	require.NoError(t, err)

	_, err = w.asg.UpdateAutoScalingGroup(asg.UpdateAutoScalingGroupInput{
		AutoScalingGroupName:             "web",
		AvailabilityZoneImpairmentPolicy: &asg.AvailabilityZoneImpairmentPolicy{ZonalShiftEnabled: false},
	})
	require.NoError(t, err)

	_, err = f.client.GetManagedResource(
		context.Background(),
		&sdk.GetManagedResourceInput{ResourceIdentifier: aws.String(arn)},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
