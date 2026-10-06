package lightsail_test

import (
	"context"
	"errors"
	"testing"

	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lstypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func enumStrings[T ~string](vs []T) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = string(v)
	}

	return out
}

func TestSDK_EnumInputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call   func(ctx context.Context, c *lightsailsdk.Client, v string) error
		name   string
		field  string
		values []string
	}{
		{
			name:   "CreateDistribution.IpAddressType",
			field:  "ipAddressType",
			values: enumStrings(lstypes.IpAddressType("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.CreateDistributionInput{
					IpAddressType: lstypes.IpAddressType(v),
				}
				_, err := c.CreateDistribution(ctx, in)

				return err
			},
		},
		{
			name:   "CreateDistribution.ViewerMinimumTlsProtocolVersion",
			field:  "viewerMinimumTlsProtocolVersion",
			values: enumStrings(lstypes.ViewerMinimumTlsProtocolVersionEnum("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.CreateDistributionInput{
					ViewerMinimumTlsProtocolVersion: lstypes.ViewerMinimumTlsProtocolVersionEnum(v),
				}
				_, err := c.CreateDistribution(ctx, in)

				return err
			},
		},
		{
			name:   "CreateInstances.IpAddressType",
			field:  "ipAddressType",
			values: enumStrings(lstypes.IpAddressType("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.CreateInstancesInput{
					IpAddressType: lstypes.IpAddressType(v),
				}
				_, err := c.CreateInstances(ctx, in)

				return err
			},
		},
		{
			name:   "CreateInstancesFromSnapshot.IpAddressType",
			field:  "ipAddressType",
			values: enumStrings(lstypes.IpAddressType("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.CreateInstancesFromSnapshotInput{
					IpAddressType: lstypes.IpAddressType(v),
				}
				_, err := c.CreateInstancesFromSnapshot(ctx, in)

				return err
			},
		},
		{
			name:   "CreateLoadBalancer.IpAddressType",
			field:  "ipAddressType",
			values: enumStrings(lstypes.IpAddressType("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.CreateLoadBalancerInput{
					IpAddressType: lstypes.IpAddressType(v),
				}
				_, err := c.CreateLoadBalancer(ctx, in)

				return err
			},
		},
		{
			name:   "GetBlueprints.AppCategory",
			field:  "appCategory",
			values: enumStrings(lstypes.AppCategory("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.GetBlueprintsInput{
					AppCategory: lstypes.AppCategory(v),
				}
				_, err := c.GetBlueprints(ctx, in)

				return err
			},
		},
		{
			name:   "GetBucketMetricData.MetricName",
			field:  "metricName",
			values: enumStrings(lstypes.BucketMetricName("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.GetBucketMetricDataInput{
					MetricName: lstypes.BucketMetricName(v),
				}
				_, err := c.GetBucketMetricData(ctx, in)

				return err
			},
		},
		{
			name:   "GetBundles.AppCategory",
			field:  "appCategory",
			values: enumStrings(lstypes.AppCategory("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.GetBundlesInput{
					AppCategory: lstypes.AppCategory(v),
				}
				_, err := c.GetBundles(ctx, in)

				return err
			},
		},
		{
			name:   "GetContainerServiceMetricData.MetricName",
			field:  "metricName",
			values: enumStrings(lstypes.ContainerServiceMetricName("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.GetContainerServiceMetricDataInput{
					MetricName: lstypes.ContainerServiceMetricName(v),
				}
				_, err := c.GetContainerServiceMetricData(ctx, in)

				return err
			},
		},
		{
			name:   "GetDistributionMetricData.MetricName",
			field:  "metricName",
			values: enumStrings(lstypes.DistributionMetricName("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.GetDistributionMetricDataInput{
					MetricName: lstypes.DistributionMetricName(v),
				}
				_, err := c.GetDistributionMetricData(ctx, in)

				return err
			},
		},
		{
			name:   "GetInstanceMetricData.MetricName",
			field:  "metricName",
			values: enumStrings(lstypes.InstanceMetricName("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.GetInstanceMetricDataInput{
					MetricName: lstypes.InstanceMetricName(v),
				}
				_, err := c.GetInstanceMetricData(ctx, in)

				return err
			},
		},
		{
			name:   "GetLoadBalancerMetricData.MetricName",
			field:  "metricName",
			values: enumStrings(lstypes.LoadBalancerMetricName("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.GetLoadBalancerMetricDataInput{
					MetricName: lstypes.LoadBalancerMetricName(v),
				}
				_, err := c.GetLoadBalancerMetricData(ctx, in)

				return err
			},
		},
		{
			name:   "GetRelationalDatabaseMetricData.MetricName",
			field:  "metricName",
			values: enumStrings(lstypes.RelationalDatabaseMetricName("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.GetRelationalDatabaseMetricDataInput{
					MetricName: lstypes.RelationalDatabaseMetricName(v),
				}
				_, err := c.GetRelationalDatabaseMetricData(ctx, in)

				return err
			},
		},
		{
			name:   "PutAlarm.ComparisonOperator",
			field:  "comparisonOperator",
			values: enumStrings(lstypes.ComparisonOperator("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.PutAlarmInput{
					ComparisonOperator: lstypes.ComparisonOperator(v),
				}
				_, err := c.PutAlarm(ctx, in)

				return err
			},
		},
		{
			name:   "PutAlarm.MetricName",
			field:  "metricName",
			values: enumStrings(lstypes.MetricName("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.PutAlarmInput{
					MetricName: lstypes.MetricName(v),
				}
				_, err := c.PutAlarm(ctx, in)

				return err
			},
		},
		{
			name:   "PutAlarm.TreatMissingData",
			field:  "treatMissingData",
			values: enumStrings(lstypes.TreatMissingData("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.PutAlarmInput{
					TreatMissingData: lstypes.TreatMissingData(v),
				}
				_, err := c.PutAlarm(ctx, in)

				return err
			},
		},
		{
			name:   "SetIpAddressType.IpAddressType",
			field:  "ipAddressType",
			values: enumStrings(lstypes.IpAddressType("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.SetIpAddressTypeInput{
					IpAddressType: lstypes.IpAddressType(v),
				}
				_, err := c.SetIpAddressType(ctx, in)

				return err
			},
		},
		{
			name:   "SetResourceAccessForBucket.Access",
			field:  "access",
			values: enumStrings(lstypes.ResourceBucketAccess("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.SetResourceAccessForBucketInput{
					Access: lstypes.ResourceBucketAccess(v),
				}
				_, err := c.SetResourceAccessForBucket(ctx, in)

				return err
			},
		},
		{
			name:   "SetupInstanceHttps.CertificateProvider",
			field:  "certificateProvider",
			values: enumStrings(lstypes.CertificateProvider("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.SetupInstanceHttpsInput{
					CertificateProvider: lstypes.CertificateProvider(v),
				}
				_, err := c.SetupInstanceHttps(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateDistribution.ViewerMinimumTlsProtocolVersion",
			field:  "viewerMinimumTlsProtocolVersion",
			values: enumStrings(lstypes.ViewerMinimumTlsProtocolVersionEnum("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.UpdateDistributionInput{
					ViewerMinimumTlsProtocolVersion: lstypes.ViewerMinimumTlsProtocolVersionEnum(v),
				}
				_, err := c.UpdateDistribution(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateInstanceMetadataOptions.HttpEndpoint",
			field:  "httpEndpoint",
			values: enumStrings(lstypes.HttpEndpoint("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.UpdateInstanceMetadataOptionsInput{
					HttpEndpoint: lstypes.HttpEndpoint(v),
				}
				_, err := c.UpdateInstanceMetadataOptions(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateInstanceMetadataOptions.HttpProtocolIpv6",
			field:  "httpProtocolIpv6",
			values: enumStrings(lstypes.HttpProtocolIpv6("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.UpdateInstanceMetadataOptionsInput{
					HttpProtocolIpv6: lstypes.HttpProtocolIpv6(v),
				}
				_, err := c.UpdateInstanceMetadataOptions(ctx, in)

				return err
			},
		},
		{
			name:   "UpdateInstanceMetadataOptions.HttpTokens",
			field:  "httpTokens",
			values: enumStrings(lstypes.HttpTokens("").Values()),
			call: func(ctx context.Context, c *lightsailsdk.Client, v string) error {
				in := &lightsailsdk.UpdateInstanceMetadataOptionsInput{
					HttpTokens: lstypes.HttpTokens(v),
				}
				_, err := c.UpdateInstanceMetadataOptions(ctx, in)

				return err
			},
		},
	}

	base := newTestClient(t)
	client := lightsailsdk.New(base.Options(), func(o *lightsailsdk.Options) {
		o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
			_, _ = s.Initialize.Remove("OperationInputValidation")

			return nil
		})
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, tt.values)

			t.Run("invalid", func(t *testing.T) {
				t.Parallel()

				err := tt.call(t.Context(), client, "NOT_A_REAL_VALUE")

				var invalid *lstypes.InvalidInputException

				require.ErrorAs(t, err, &invalid)
				assert.Contains(t, invalid.ErrorMessage(), "invalid "+tt.field)
			})

			t.Run("every sdk value accepted", func(t *testing.T) {
				t.Parallel()

				for _, v := range tt.values {
					err := tt.call(t.Context(), client, v)

					if invalid, ok := errors.AsType[*lstypes.InvalidInputException](err); ok {
						assert.NotContains(t, invalid.ErrorMessage(), "invalid "+tt.field,
							"%s rejected sdk value %q: %v", tt.field, v, err)
					}
				}
			})
		})
	}
}
