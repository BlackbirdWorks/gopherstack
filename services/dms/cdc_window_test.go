package dms_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedReplicationConfig(t *testing.T, c *dmssdk.Client, prefix string) string {
	t.Helper()

	src, err := c.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String(prefix + "-src"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	tgt, err := c.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String(prefix + "-tgt"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	rc, err := c.CreateReplicationConfig(t.Context(), &dmssdk.CreateReplicationConfigInput{
		ReplicationConfigIdentifier: aws.String(prefix + "-rc"),
		ReplicationType:             types.MigrationTypeValueFullLoadAndCdc,
		SourceEndpointArn:           src.Endpoint.EndpointArn,
		TargetEndpointArn:           tgt.Endpoint.EndpointArn,
		TableMappings:               aws.String(`{"rules":[]}`),
		ComputeConfig:               &types.ComputeConfig{},
	})
	require.NoError(t, err)

	return aws.ToString(rc.ReplicationConfig.ReplicationConfigArn)
}

func TestStartReplication_CDCWindowAndAssessmentSettings(t *testing.T) {
	t.Parallel()

	start := time.Unix(1700000000, 0)

	cases := []struct {
		name     string
		input    dmssdk.StartReplicationInput
		wantPos  string
		wantStop string
		wantTime bool
		wantErr  bool
	}{
		{
			name: "position",
			input: dmssdk.StartReplicationInput{
				CdcStartPosition: aws.String("mysql-bin.000001:4"),
				CdcStopPosition:  aws.String("server_time:2030-01-01T00:00:00"),
			},
			wantPos:  "mysql-bin.000001:4",
			wantStop: "server_time:2030-01-01T00:00:00",
		},
		{name: "time", input: dmssdk.StartReplicationInput{CdcStartTime: &start}, wantTime: true},
		{name: "settings ok", input: dmssdk.StartReplicationInput{
			PremigrationAssessmentSettings: aws.String(
				`{"ResultEncryptionMode":"SSE_S3","FailOnAssessmentFailure":false}`,
			),
		}},
		{name: "bad encryption mode", wantErr: true, input: dmssdk.StartReplicationInput{
			PremigrationAssessmentSettings: aws.String(`{"ResultEncryptionMode":"ROT13"}`),
		}},
		{name: "include and exclude", wantErr: true, input: dmssdk.StartReplicationInput{
			PremigrationAssessmentSettings: aws.String(`{"IncludeOnly":"a","Exclude":"b"}`),
		}},
		{name: "not json", wantErr: true, input: dmssdk.StartReplicationInput{
			PremigrationAssessmentSettings: aws.String(`not-json`),
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestDMSClient(t, newTestDMSHandler())
			in := tc.input
			in.ReplicationConfigArn = aws.String(seedReplicationConfig(t, c, "cdc"))
			in.StartReplicationType = aws.String("start-replication")

			out, err := c.StartReplication(t.Context(), &in)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantPos, aws.ToString(out.Replication.CdcStartPosition))
			assert.Equal(t, tc.wantStop, aws.ToString(out.Replication.CdcStopPosition))

			if tc.wantTime {
				require.NotNil(t, out.Replication.CdcStartTime)
				assert.True(t, out.Replication.CdcStartTime.Equal(start))
			}
		})
	}
}

func TestStartReplicationTask_CDCPositionsApplied(t *testing.T) {
	t.Parallel()

	c := newTestDMSClient(t, newTestDMSHandler())
	taskArn := seedReplicationTask(t, c, "cdcpos")

	out, err := c.StartReplicationTask(t.Context(), &dmssdk.StartReplicationTaskInput{
		ReplicationTaskArn:       aws.String(taskArn),
		StartReplicationTaskType: types.StartReplicationTaskTypeValueStartReplication,
		CdcStartPosition:         aws.String("mysql-bin.000009:4"),
		CdcStopPosition:          aws.String("commit_time:2030-01-01T00:00:00"),
	})
	require.NoError(t, err)
	assert.Equal(t, "mysql-bin.000009:4", aws.ToString(out.ReplicationTask.CdcStartPosition))
	assert.Equal(t, "commit_time:2030-01-01T00:00:00", aws.ToString(out.ReplicationTask.CdcStopPosition))
}
