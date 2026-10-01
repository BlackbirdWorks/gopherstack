package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func TestTopicRule_UnmodeledActionsAndErrorActionRoundTrip(t *testing.T) {
	t.Parallel()

	s3 := types.Action{S3: &types.S3Action{
		BucketName: aws.String("b"), Key: aws.String("k"), RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
	}}
	ddb := types.Action{DynamoDB: &types.DynamoDBAction{
		HashKeyField: aws.String("h"), HashKeyValue: aws.String("v"),
		RoleArn: aws.String("arn:aws:iam::000000000000:role/r"), TableName: aws.String("tbl"),
	}}

	tests := []struct {
		name    string
		replace bool
	}{
		{name: "create", replace: false},
		{name: "replace", replace: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))
			payload := &types.TopicRulePayload{
				Sql:         aws.String("SELECT * FROM 't'"),
				Actions:     []types.Action{s3, ddb},
				ErrorAction: &types.Action{S3: s3.S3},
			}
			if tc.replace {
				_, err := client.CreateTopicRule(t.Context(), &iotsdk.CreateTopicRuleInput{
					RuleName: aws.String("r1"),
					TopicRulePayload: &types.TopicRulePayload{
						Sql:     aws.String("SELECT * FROM 'x'"),
						Actions: []types.Action{},
					},
				})
				require.NoError(t, err)
				_, err = client.ReplaceTopicRule(t.Context(), &iotsdk.ReplaceTopicRuleInput{
					RuleName: aws.String("r1"), TopicRulePayload: payload,
				})
				require.NoError(t, err)
			} else {
				_, err := client.CreateTopicRule(t.Context(), &iotsdk.CreateTopicRuleInput{
					RuleName: aws.String("r1"), TopicRulePayload: payload,
				})
				require.NoError(t, err)
			}

			got, err := client.GetTopicRule(t.Context(), &iotsdk.GetTopicRuleInput{RuleName: aws.String("r1")})
			require.NoError(t, err)
			require.Len(t, got.Rule.Actions, 2)
			require.NotNil(t, got.Rule.Actions[0].S3)
			assert.Equal(t, "b", aws.ToString(got.Rule.Actions[0].S3.BucketName))
			require.NotNil(t, got.Rule.Actions[1].DynamoDB)
			assert.Equal(t, "tbl", aws.ToString(got.Rule.Actions[1].DynamoDB.TableName))
			require.NotNil(t, got.Rule.ErrorAction)
			assert.Equal(t, "k", aws.ToString(got.Rule.ErrorAction.S3.Key))
		})
	}
}

func TestTopicRule_UnmodeledActionsSurvivePersistence(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	client := newTestIoTClient(t, iot.NewHandler(b, nil))
	_, err := client.CreateTopicRule(t.Context(), &iotsdk.CreateTopicRuleInput{
		RuleName: aws.String("r1"),
		TopicRulePayload: &types.TopicRulePayload{
			Sql: aws.String("SELECT * FROM 't'"),
			Actions: []types.Action{{S3: &types.S3Action{
				BucketName: aws.String(
					"b",
				),
				Key:     aws.String("k"),
				RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
			}}},
			ErrorAction: &types.Action{Republish: &types.RepublishAction{
				RoleArn: aws.String("arn:aws:iam::000000000000:role/r"), Topic: aws.String("err"),
			}},
		},
	})
	require.NoError(t, err)

	b2 := iot.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), b.Snapshot(t.Context())))
	got, err := newTestIoTClient(t, iot.NewHandler(b2, nil)).
		GetTopicRule(t.Context(), &iotsdk.GetTopicRuleInput{RuleName: aws.String("r1")})
	require.NoError(t, err)
	require.Len(t, got.Rule.Actions, 1)
	assert.Equal(t, "b", aws.ToString(got.Rule.Actions[0].S3.BucketName))
	assert.Equal(t, "err", aws.ToString(got.Rule.ErrorAction.Republish.Topic))
}

func TestTopicRuleDestination_InfluxDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg     *types.InfluxDBDestinationConfiguration
		name    string
		errCode string
	}{
		{
			name: "v2_with_secret_key",
			cfg: &types.InfluxDBDestinationConfiguration{
				Endpoint: aws.String("https://influx.example.com:8086"), InfluxDBVersion: types.InfluxDBVersionV2,
				SecretId:  aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:tok"),
				SecretKey: aws.String("token"), SecretType: types.InfluxDBSecretTypeSecretString,
			},
		},
		{
			name: "v3_minimal",
			cfg: &types.InfluxDBDestinationConfiguration{
				Endpoint: aws.String("https://i3.example.com"), InfluxDBVersion: types.InfluxDBVersionV3,
				SecretId: aws.String("tok"),
			},
		},
		{
			name: "bad_version",
			cfg: &types.InfluxDBDestinationConfiguration{
				Endpoint: aws.String("https://i3.example.com"), InfluxDBVersion: "V9", SecretId: aws.String("tok"),
			},
			errCode: "InvalidRequestException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			client := newTestIoTClient(t, iot.NewHandler(b, nil))
			created, err := client.CreateTopicRuleDestination(t.Context(), &iotsdk.CreateTopicRuleDestinationInput{
				DestinationConfiguration: &types.TopicRuleDestinationConfiguration{InfluxDBConfiguration: tc.cfg},
			})
			if tc.errCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tc.errCode, apiErr.ErrorCode())

				return
			}
			require.NoError(t, err)
			arn := created.TopicRuleDestination.Arn
			assert.Contains(t, aws.ToString(arn), "ruledestination/influxdb/")
			assert.Equal(t, types.TopicRuleDestinationStatusEnabled, created.TopicRuleDestination.Status)

			assertInflux := func(p *types.InfluxDBDestinationProperties) {
				require.NotNil(t, p)
				assert.Equal(t, tc.cfg.Endpoint, p.Endpoint)
				assert.Equal(t, tc.cfg.InfluxDBVersion, p.InfluxDBVersion)
				assert.Equal(t, tc.cfg.SecretId, p.SecretId)
				assert.Equal(t, tc.cfg.SecretKey, p.SecretKey)
				assert.Equal(t, tc.cfg.SecretType, p.SecretType)
			}
			assertInflux(created.TopicRuleDestination.InfluxDBProperties)

			got, err := client.GetTopicRuleDestination(t.Context(), &iotsdk.GetTopicRuleDestinationInput{Arn: arn})
			require.NoError(t, err)
			assertInflux(got.TopicRuleDestination.InfluxDBProperties)

			list, err := client.ListTopicRuleDestinations(t.Context(), &iotsdk.ListTopicRuleDestinationsInput{})
			require.NoError(t, err)
			require.Len(t, list.DestinationSummaries, 1)
			s := list.DestinationSummaries[0].InfluxDBSummary
			require.NotNil(t, s)
			assert.Equal(t, tc.cfg.Endpoint, s.Endpoint)
			assert.Equal(t, tc.cfg.InfluxDBVersion, s.InfluxDBVersion)

			b2 := iot.NewInMemoryBackend()
			require.NoError(t, b2.Restore(t.Context(), b.Snapshot(t.Context())))
			got2, err := newTestIoTClient(t, iot.NewHandler(b2, nil)).
				GetTopicRuleDestination(t.Context(), &iotsdk.GetTopicRuleDestinationInput{Arn: arn})
			require.NoError(t, err)
			assertInflux(got2.TopicRuleDestination.InfluxDBProperties)
		})
	}
}

func TestTopicRuleDestination_VPCSurvivesPersistence(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	client := newTestIoTClient(t, iot.NewHandler(b, nil))
	created, err := client.CreateTopicRuleDestination(t.Context(), &iotsdk.CreateTopicRuleDestinationInput{
		DestinationConfiguration: &types.TopicRuleDestinationConfiguration{
			VpcConfiguration: &types.VpcDestinationConfiguration{
				RoleArn: aws.String("arn:aws:iam::000000000000:role/vpc"), SubnetIds: []string{"subnet-1"},
				SecurityGroups: []string{"sg-1"}, VpcId: aws.String("vpc-1"),
			},
		},
	})
	require.NoError(t, err)

	b2 := iot.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), b.Snapshot(t.Context())))
	got, err := newTestIoTClient(t, iot.NewHandler(b2, nil)).GetTopicRuleDestination(
		t.Context(), &iotsdk.GetTopicRuleDestinationInput{Arn: created.TopicRuleDestination.Arn})
	require.NoError(t, err)
	require.NotNil(t, got.TopicRuleDestination.VpcProperties)
	assert.Equal(t, "vpc-1", aws.ToString(got.TopicRuleDestination.VpcProperties.VpcId))
	assert.Equal(t, []string{"subnet-1"}, got.TopicRuleDestination.VpcProperties.SubnetIds)
}
