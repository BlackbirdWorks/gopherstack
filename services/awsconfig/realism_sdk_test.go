package awsconfig_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func apiErrorOf(t *testing.T, err error) smithy.APIError {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)

	return apiErr
}

func TestSDK_PutConfigRuleValidationAndRoundTrip(t *testing.T) {
	t.Parallel()

	lambdaSource := func(msgType string) *types.Source {
		return &types.Source{
			Owner:            types.OwnerCustomLambda,
			SourceIdentifier: aws.String("arn:aws:lambda:us-east-1:000000000000:function:f"),
			SourceDetails: []types.SourceDetail{{
				EventSource:               types.EventSourceAwsConfig,
				MessageType:               types.MessageType(msgType),
				MaximumExecutionFrequency: types.MaximumExecutionFrequencyTwentyFourHours,
			}},
		}
	}

	tests := []struct {
		rule     types.ConfigRule
		wantCode string
		name     string
	}{
		{
			name: "lambda_source_details",
			rule: types.ConfigRule{ConfigRuleName: aws.String("r"), Source: lambdaSource("ScheduledNotification")},
		},
		{
			name:     "bad_message_type",
			rule:     types.ConfigRule{ConfigRuleName: aws.String("r"), Source: lambdaSource("Bogus")},
			wantCode: "InvalidParameterValueException",
		},
		{
			name: "bad_owner",
			rule: types.ConfigRule{
				ConfigRuleName: aws.String("r"),
				Source:         &types.Source{Owner: types.Owner("NOPE"), SourceIdentifier: aws.String("X")},
			},
			wantCode: "InvalidParameterValueException",
		},
		{
			name: "bad_frequency",
			rule: types.ConfigRule{
				ConfigRuleName:            aws.String("r"),
				MaximumExecutionFrequency: types.MaximumExecutionFrequency("Weekly"),
				Source:                    &types.Source{Owner: types.OwnerAws, SourceIdentifier: aws.String("X")},
			},
			wantCode: "InvalidParameterValueException",
		},
		{
			name: "policy_needs_details",
			rule: types.ConfigRule{
				ConfigRuleName: aws.String("r"),
				Source:         &types.Source{Owner: types.OwnerCustomPolicy},
			},
			wantCode: "InvalidParameterValueException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newOpenItemsClient(t)

			_, err := c.PutConfigRule(t.Context(), &configservicesdk.PutConfigRuleInput{ConfigRule: &tt.rule})
			if tt.wantCode != "" {
				require.Error(t, err)
				apiErr := apiErrorOf(t, err)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
				assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode)

				return
			}

			require.NoError(t, err)

			out, err := c.DescribeConfigRules(t.Context(), &configservicesdk.DescribeConfigRulesInput{})
			require.NoError(t, err)
			require.Len(t, out.ConfigRules, 1)
			require.Len(t, out.ConfigRules[0].Source.SourceDetails, 1)
			assert.Equal(
				t,
				types.MessageTypeScheduledNotification,
				out.ConfigRules[0].Source.SourceDetails[0].MessageType,
			)
		})
	}
}

func TestSDK_ConfigRuleEvaluationStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		names    []string
		wantLen  int
	}{
		{name: "all_rules", wantLen: 1},
		{name: "named_rule", names: []string{"r1"}, wantLen: 1},
		{name: "unknown_rule", names: []string{"nope"}, wantCode: "NoSuchConfigRuleException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newOpenItemsClient(t)

			_, err := c.PutConfigRule(t.Context(), &configservicesdk.PutConfigRuleInput{ConfigRule: &types.ConfigRule{
				ConfigRuleName: aws.String("r1"),
				Source: &types.Source{
					Owner: types.OwnerAws, SourceIdentifier: aws.String("S3_BUCKET_VERSIONING_ENABLED"),
				},
			}})
			require.NoError(t, err)

			out, err := c.DescribeConfigRuleEvaluationStatus(
				t.Context(), &configservicesdk.DescribeConfigRuleEvaluationStatusInput{ConfigRuleNames: tt.names},
			)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiErrorOf(t, err).ErrorCode())

				return
			}

			require.NoError(t, err)
			require.Len(t, out.ConfigRulesEvaluationStatus, tt.wantLen)

			st := out.ConfigRulesEvaluationStatus[0]
			assert.Equal(t, "r1", aws.ToString(st.ConfigRuleName))
			assert.NotEmpty(t, aws.ToString(st.ConfigRuleArn))
			assert.NotNil(t, st.FirstActivatedTime)
			assert.Nil(t, st.LastSuccessfulInvocationTime)
		})
	}
}

func TestSDK_RecorderStatusTimestamps(t *testing.T) {
	t.Parallel()

	b, c := newOpenItemsClient(t)
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	b.SetClock(func() time.Time { return now })

	_, err := c.PutConfigurationRecorder(t.Context(), &configservicesdk.PutConfigurationRecorderInput{
		ConfigurationRecorder: &types.ConfigurationRecorder{
			Name: aws.String("default"), RoleARN: aws.String("arn:aws:iam::000000000000:role/r"),
		},
	})
	require.NoError(t, err)
	_, err = c.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
		DeliveryChannel: &types.DeliveryChannel{Name: aws.String("default"), S3BucketName: aws.String("b")},
	})
	require.NoError(t, err)

	status := func() types.ConfigurationRecorderStatus {
		out, serr := c.DescribeConfigurationRecorderStatus(
			t.Context(), &configservicesdk.DescribeConfigurationRecorderStatusInput{},
		)
		require.NoError(t, serr)
		require.Len(t, out.ConfigurationRecordersStatus, 1)

		return out.ConfigurationRecordersStatus[0]
	}

	st := status()
	assert.False(t, st.Recording)
	assert.Equal(t, types.RecorderStatusPending, st.LastStatus)
	assert.Nil(t, st.LastStartTime)

	_, err = c.StartConfigurationRecorder(t.Context(), &configservicesdk.StartConfigurationRecorderInput{
		ConfigurationRecorderName: aws.String("default"),
	})
	require.NoError(t, err)

	st = status()
	assert.True(t, st.Recording)
	assert.Equal(t, types.RecorderStatusSuccess, st.LastStatus)
	require.NotNil(t, st.LastStartTime)
	assert.True(t, st.LastStartTime.Equal(now))

	now = now.Add(time.Hour)

	_, err = c.StopConfigurationRecorder(t.Context(), &configservicesdk.StopConfigurationRecorderInput{
		ConfigurationRecorderName: aws.String("default"),
	})
	require.NoError(t, err)

	st = status()
	assert.False(t, st.Recording)
	assert.Equal(t, types.RecorderStatusSuccess, st.LastStatus)
	require.NotNil(t, st.LastStopTime)
	assert.True(t, st.LastStopTime.Equal(now))
}

func TestSDK_ConformancePackDeploymentLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		packName  string
		wantCode  string
		wantState types.ConformancePackState
		delay     time.Duration
		elapsed   time.Duration
	}{
		{name: "instant", packName: "cp", wantState: types.ConformancePackStateCreateComplete},
		{
			name: "in_progress", packName: "cp", delay: time.Minute, elapsed: time.Second,
			wantState: types.ConformancePackStateCreateInProgress,
		},
		{
			name: "complete_after_delay", packName: "cp", delay: time.Minute, elapsed: 2 * time.Minute,
			wantState: types.ConformancePackStateCreateComplete,
		},
		{name: "bad_name", packName: "bad name!", wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, c := newOpenItemsClient(t)
			now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
			b.SetClock(func() time.Time { return now })
			b.SetLifecycleDelay(tt.delay)

			put, err := c.PutConformancePack(t.Context(), &configservicesdk.PutConformancePackInput{
				ConformancePackName: aws.String(tt.packName), TemplateBody: aws.String("Resources: {}"),
			})
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiErrorOf(t, err).ErrorCode())

				return
			}

			require.NoError(t, err)

			now = now.Add(tt.elapsed)

			out, err := c.DescribeConformancePackStatus(
				t.Context(), &configservicesdk.DescribeConformancePackStatusInput{},
			)
			require.NoError(t, err)
			require.Len(t, out.ConformancePackStatusDetails, 1)

			st := out.ConformancePackStatusDetails[0]
			assert.Equal(t, tt.wantState, st.ConformancePackState)
			assert.Equal(t, aws.ToString(put.ConformancePackArn), aws.ToString(st.ConformancePackArn))
			assert.NotNil(t, st.LastUpdateRequestedTime)

			_, err = c.DescribeConformancePackStatus(
				t.Context(),
				&configservicesdk.DescribeConformancePackStatusInput{ConformancePackNames: []string{"missing"}},
			)
			require.Error(t, err)
			assert.Equal(t, "NoSuchConformancePackException", apiErrorOf(t, err).ErrorCode())
		})
	}
}

func TestSDK_DeliveryChannelLimitsAndLookups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run      func(t *testing.T, c *configservicesdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "second_channel",
			run: func(t *testing.T, c *configservicesdk.Client) error {
				t.Helper()

				_, err := c.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
					DeliveryChannel: &types.DeliveryChannel{Name: aws.String("other"), S3BucketName: aws.String("b")},
				})

				return err
			},
			wantCode: "MaxNumberOfDeliveryChannelsExceededException",
		},
		{
			name: "update_same_channel",
			run: func(t *testing.T, c *configservicesdk.Client) error {
				t.Helper()

				_, err := c.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
					DeliveryChannel: &types.DeliveryChannel{Name: aws.String("first"), S3BucketName: aws.String("b2")},
				})

				return err
			},
		},
		{
			name: "bad_frequency",
			run: func(t *testing.T, c *configservicesdk.Client) error {
				t.Helper()

				_, err := c.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
					DeliveryChannel: &types.DeliveryChannel{
						Name: aws.String("first"), S3BucketName: aws.String("b"),
						ConfigSnapshotDeliveryProperties: &types.ConfigSnapshotDeliveryProperties{
							DeliveryFrequency: types.MaximumExecutionFrequency("Weekly"),
						},
					},
				})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "describe_unknown",
			run: func(t *testing.T, c *configservicesdk.Client) error {
				t.Helper()

				_, err := c.DescribeDeliveryChannels(t.Context(), &configservicesdk.DescribeDeliveryChannelsInput{
					DeliveryChannelNames: []string{"nope"},
				})

				return err
			},
			wantCode: "NoSuchDeliveryChannelException",
		},
		{
			name: "describe_recorder_unknown",
			run: func(t *testing.T, c *configservicesdk.Client) error {
				t.Helper()

				_, err := c.DescribeConfigurationRecorders(
					t.Context(), &configservicesdk.DescribeConfigurationRecordersInput{
						ConfigurationRecorderNames: []string{"nope"},
					})

				return err
			},
			wantCode: "NoSuchConfigurationRecorderException",
		},
		{
			name: "aggregator_bad_account",
			run: func(t *testing.T, c *configservicesdk.Client) error {
				t.Helper()

				_, err := c.PutConfigurationAggregator(t.Context(), &configservicesdk.PutConfigurationAggregatorInput{
					ConfigurationAggregatorName: aws.String("agg"),
					AccountAggregationSources: []types.AccountAggregationSource{
						{AccountIds: []string{"abc"}, AllAwsRegions: true},
					},
				})

				return err
			},
			wantCode: "InvalidParameterValueException",
		},
		{
			name: "authorization_bad_account",
			run: func(t *testing.T, c *configservicesdk.Client) error {
				t.Helper()

				_, err := c.PutAggregationAuthorization(t.Context(), &configservicesdk.PutAggregationAuthorizationInput{
					AuthorizedAccountId: aws.String("123"), AuthorizedAwsRegion: aws.String("us-east-1"),
				})

				return err
			},
			wantCode: "InvalidParameterValueException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newOpenItemsClient(t)

			_, err := c.PutDeliveryChannel(t.Context(), &configservicesdk.PutDeliveryChannelInput{
				DeliveryChannel: &types.DeliveryChannel{Name: aws.String("first"), S3BucketName: aws.String("b")},
			})
			require.NoError(t, err)

			err = tt.run(t, c)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			apiErr := apiErrorOf(t, err)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode)
		})
	}
}
