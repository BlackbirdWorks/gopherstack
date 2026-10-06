package kinesisanalyticsv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisanalyticsv2sdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	kav2types "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func TestUpdateApplication_CheckpointAndRunConfigSemantics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update       kinesisanalyticsv2sdk.UpdateApplicationInput
		name         string
		wantType     kav2types.ConfigurationType
		wantInterval int64
		wantPause    int64
	}{
		{
			name: "custom to default resets checkpoint values",
			update: kinesisanalyticsv2sdk.UpdateApplicationInput{
				ApplicationConfigurationUpdate: &kav2types.ApplicationConfigurationUpdate{
					FlinkApplicationConfigurationUpdate: &kav2types.FlinkApplicationConfigurationUpdate{
						CheckpointConfigurationUpdate: &kav2types.CheckpointConfigurationUpdate{
							ConfigurationTypeUpdate: kav2types.ConfigurationTypeDefault,
						},
					},
				},
			},
			wantType: kav2types.ConfigurationTypeDefault, wantInterval: 60000, wantPause: 5000,
		},
		{
			name: "custom interval only keeps pause",
			update: kinesisanalyticsv2sdk.UpdateApplicationInput{
				ApplicationConfigurationUpdate: &kav2types.ApplicationConfigurationUpdate{
					FlinkApplicationConfigurationUpdate: &kav2types.FlinkApplicationConfigurationUpdate{
						CheckpointConfigurationUpdate: &kav2types.CheckpointConfigurationUpdate{
							CheckpointIntervalUpdate: aws.Int64(2000),
						},
					},
				},
			},
			wantType: kav2types.ConfigurationTypeCustom, wantInterval: 2000, wantPause: 100,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := kinesisanalyticsv2.NewInMemoryBackend(kav2RTAccountID, kav2RTRegion)
			client := newTestKAV2SDKClient(t, kinesisanalyticsv2.NewHandler(backend))

			_, err := client.CreateApplication(ctx, &kinesisanalyticsv2sdk.CreateApplicationInput{
				ApplicationName:      aws.String("app"),
				RuntimeEnvironment:   kav2types.RuntimeEnvironmentFlink118,
				ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/r"),
				ApplicationConfiguration: &kav2types.ApplicationConfiguration{
					FlinkApplicationConfiguration: &kav2types.FlinkApplicationConfiguration{
						CheckpointConfiguration: &kav2types.CheckpointConfiguration{
							ConfigurationType:          kav2types.ConfigurationTypeCustom,
							CheckpointInterval:         aws.Int64(1000),
							MinPauseBetweenCheckpoints: aws.Int64(100),
							CheckpointingEnabled:       aws.Bool(true),
						},
					},
				},
			})
			require.NoError(t, err)

			tc.update.ApplicationName = aws.String("app")
			tc.update.CurrentApplicationVersionId = aws.Int64(1)
			_, err = client.UpdateApplication(ctx, &tc.update)
			require.NoError(t, err)

			got, err := client.DescribeApplication(ctx, &kinesisanalyticsv2sdk.DescribeApplicationInput{
				ApplicationName: aws.String("app"),
			})
			require.NoError(t, err)

			cp := got.ApplicationDetail.ApplicationConfigurationDescription.
				FlinkApplicationConfigurationDescription.CheckpointConfigurationDescription
			require.NotNil(t, cp)
			assert.Equal(t, tc.wantType, cp.ConfigurationType)
			assert.Equal(t, tc.wantInterval, aws.ToInt64(cp.CheckpointInterval))
			assert.Equal(t, tc.wantPause, aws.ToInt64(cp.MinPauseBetweenCheckpoints))
		})
	}
}

func TestUpdateApplication_AllowNonRestoredStateResets(t *testing.T) {
	t.Parallel()

	cases := []struct {
		second *kav2types.RunConfigurationUpdate
		name   string
		want   bool
	}{
		{
			name: "restore only update resets to false",
			second: &kav2types.RunConfigurationUpdate{
				ApplicationRestoreConfiguration: &kav2types.ApplicationRestoreConfiguration{
					ApplicationRestoreType: kav2types.ApplicationRestoreTypeSkipRestoreFromSnapshot,
				},
			},
			want: false,
		},
		{
			name: "explicit true is kept",
			second: &kav2types.RunConfigurationUpdate{FlinkRunConfiguration: &kav2types.FlinkRunConfiguration{
				AllowNonRestoredState: aws.Bool(true),
			}},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := kinesisanalyticsv2.NewInMemoryBackend(kav2RTAccountID, kav2RTRegion)
			client := newTestKAV2SDKClient(t, kinesisanalyticsv2.NewHandler(backend))

			_, err := client.CreateApplication(ctx, &kinesisanalyticsv2sdk.CreateApplicationInput{
				ApplicationName:      aws.String("app"),
				RuntimeEnvironment:   kav2types.RuntimeEnvironmentFlink118,
				ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/r"),
			})
			require.NoError(t, err)

			_, err = client.UpdateApplication(ctx, &kinesisanalyticsv2sdk.UpdateApplicationInput{
				ApplicationName:             aws.String("app"),
				CurrentApplicationVersionId: aws.Int64(1),
				RunConfigurationUpdate: &kav2types.RunConfigurationUpdate{
					FlinkRunConfiguration: &kav2types.FlinkRunConfiguration{AllowNonRestoredState: aws.Bool(true)},
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateApplication(ctx, &kinesisanalyticsv2sdk.UpdateApplicationInput{
				ApplicationName:             aws.String("app"),
				CurrentApplicationVersionId: aws.Int64(2),
				RunConfigurationUpdate:      tc.second,
			})
			require.NoError(t, err)

			got, err := client.DescribeApplication(ctx, &kinesisanalyticsv2sdk.DescribeApplicationInput{
				ApplicationName: aws.String("app"),
			})
			require.NoError(t, err)

			run := got.ApplicationDetail.ApplicationConfigurationDescription.RunConfigurationDescription
			require.NotNil(t, run)
			require.NotNil(t, run.FlinkRunConfigurationDescription)
			assert.Equal(t, tc.want, aws.ToBool(run.FlinkRunConfigurationDescription.AllowNonRestoredState))
		})
	}
}
