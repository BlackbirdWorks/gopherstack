package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndpointConfig_VariantScalingAndRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mis     *smtypes.ProductionVariantManagedInstanceScaling
		route   *smtypes.ProductionVariantRoutingConfig
		cap     *smtypes.ProductionVariantCapacityReservationConfig
		name    string
		pools   []smtypes.InstancePool
		wantErr bool
	}{
		{
			name: "valid",
			mis: &smtypes.ProductionVariantManagedInstanceScaling{
				Status:           smtypes.ManagedInstanceScalingStatusEnabled,
				MinInstanceCount: aws.Int32(1),
				MaxInstanceCount: aws.Int32(4),
				ScaleInPolicy: &smtypes.ProductionVariantManagedInstanceScalingScaleInPolicy{
					Strategy:          smtypes.ManagedInstanceScalingScaleInStrategyConsolidation,
					CooldownInMinutes: aws.Int32(10),
					MaximumStepSize:   aws.Int32(2),
				},
			},
			route: &smtypes.ProductionVariantRoutingConfig{RoutingStrategy: smtypes.RoutingStrategyRandom},
			cap: &smtypes.ProductionVariantCapacityReservationConfig{
				CapacityReservationPreference: smtypes.CapacityReservationPreferenceCapacityReservationsOnly,
				MlReservationArn:              aws.String("arn:aws:sagemaker:us-east-1:123456789012:ml-reservation/r"),
			},
			pools: []smtypes.InstancePool{{InstanceType: "ml.g5.xlarge", Priority: aws.Int32(1)}},
		},
		{
			name:    "bad_capacity_preference",
			cap:     &smtypes.ProductionVariantCapacityReservationConfig{CapacityReservationPreference: "whatever"},
			wantErr: true,
		},
		{
			name: "bad_status",
			mis:  &smtypes.ProductionVariantManagedInstanceScaling{Status: "MAYBE"}, wantErr: true,
		},
		{
			name: "bad_strategy",
			mis: &smtypes.ProductionVariantManagedInstanceScaling{
				ScaleInPolicy: &smtypes.ProductionVariantManagedInstanceScalingScaleInPolicy{Strategy: "NOPE"},
			},
			wantErr: true,
		},
		{
			name:    "bad_routing",
			route:   &smtypes.ProductionVariantRoutingConfig{RoutingStrategy: "ROUND_ROBIN"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSageMakerClient(t, newTestHandler(t))

			_, err := client.CreateEndpointConfig(t.Context(), &sagemakersdk.CreateEndpointConfigInput{
				EndpointConfigName: aws.String("cfg"),
				ProductionVariants: []smtypes.ProductionVariant{{
					VariantName:               aws.String("v1"),
					ModelName:                 aws.String("m"),
					ManagedInstanceScaling:    tt.mis,
					RoutingConfig:             tt.route,
					CapacityReservationConfig: tt.cap,
					InstancePools:             tt.pools,
				}},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			desc, err := client.DescribeEndpointConfig(t.Context(), &sagemakersdk.DescribeEndpointConfigInput{
				EndpointConfigName: aws.String("cfg"),
			})
			require.NoError(t, err)
			pv := desc.ProductionVariants[0]
			assert.Equal(t, int32(4), aws.ToInt32(pv.ManagedInstanceScaling.MaxInstanceCount))
			assert.Equal(t, smtypes.ManagedInstanceScalingScaleInStrategyConsolidation,
				pv.ManagedInstanceScaling.ScaleInPolicy.Strategy)
			assert.Equal(t, smtypes.RoutingStrategyRandom, pv.RoutingConfig.RoutingStrategy)
			assert.Equal(t, smtypes.CapacityReservationPreferenceCapacityReservationsOnly,
				pv.CapacityReservationConfig.CapacityReservationPreference)
			require.Len(t, pv.InstancePools, 1)

			_, err = client.CreateEndpoint(t.Context(), &sagemakersdk.CreateEndpointInput{
				EndpointName: aws.String("ep"), EndpointConfigName: aws.String("cfg"),
			})
			require.NoError(t, err)

			ep, err := client.DescribeEndpoint(t.Context(), &sagemakersdk.DescribeEndpointInput{
				EndpointName: aws.String("ep"),
			})
			require.NoError(t, err)
			epv := ep.ProductionVariants[0]
			assert.Equal(t, int32(1), aws.ToInt32(epv.ManagedInstanceScaling.MinInstanceCount))
			assert.Equal(t, smtypes.RoutingStrategyRandom, epv.RoutingConfig.RoutingStrategy)
			assert.Equal(t, "arn:aws:sagemaker:us-east-1:123456789012:ml-reservation/r",
				aws.ToString(epv.CapacityReservationConfig.MlReservationArn))
			require.Len(t, epv.InstancePools, 1)
			assert.Equal(t, smtypes.ProductionVariantInstanceType("ml.g5.xlarge"), epv.InstancePools[0].InstanceType)
		})
	}
}
