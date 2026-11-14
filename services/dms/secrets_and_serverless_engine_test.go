package dms_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
	"github.com/blackbirdworks/gopherstack/services/dms"
)

type fakeSecrets struct {
	values map[string]string
	asked  chan string
}

var errNoSuchSecret = errors.New("secret not found")

func (f *fakeSecrets) SecretString(_ context.Context, region, id string) (string, error) {
	if f.asked != nil {
		f.asked <- region + "/" + id
	}

	v, ok := f.values[id]
	if !ok {
		return "", errNoSuchSecret
	}

	return v, nil
}

const secretRole = "arn:aws:iam::123456789012:role/dms-secrets"

func secretEndpoint(secretID, role string) dms.EndpointConnectionSettings {
	settings := `{"SecretsManagerSecretId":"` + secretID + `"`
	if role != "" {
		settings += `,"SecretsManagerAccessRoleArn":"` + role + `"`
	}

	settings += `,"DatabaseName":"appdb"}`

	return dms.EndpointConnectionSettings{EngineSettings: map[string]string{"MySQLSettings": settings}}
}

func TestEndpointCredentialsFromSecretsManager(t *testing.T) {
	t.Parallel()

	secrets := &fakeSecrets{values: map[string]string{
		"db/creds": `{"username":"app","password":"from-secret","host":"db.internal","port":"3307"}`,
		"db/junk":  `not json`,
	}}

	tests := []struct {
		secrets  dms.SecretResolver
		name     string
		secretID string
		role     string
		wantMsg  string
		wantOK   bool
	}{
		{name: "resolved", secretID: "db/creds", role: secretRole, secrets: secrets, wantOK: true},
		{name: "role is required", secretID: "db/creds", secrets: secrets, wantMsg: "SecretsManagerAccessRoleArn"},
		{name: "unknown secret", secretID: "db/none", role: secretRole, secrets: secrets, wantMsg: "db/none"},
		{
			name:     "malformed secret",
			secretID: "db/junk",
			role:     secretRole,
			secrets:  secrets,
			wantMsg:  "not a JSON object",
		},
		{name: "no secrets manager", secretID: "db/creds", role: secretRole, wantMsg: "no Secrets Manager"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEngineEnv(t, "mysql", "full-load")
			if tt.secrets != nil {
				e.b.SetSecrets(tt.secrets)
			}

			ep, err := e.b.CreateEndpoint(t.Context(), "secret-src", "source", "mysql", "", "", "", "", 0, nil,
				secretEndpoint(tt.secretID, tt.role))
			require.NoError(t, err)

			conn, err := e.b.TestConnection(t.Context(), e.ri, ep.EndpointArn)
			require.NoError(t, err)

			if !tt.wantOK {
				assert.Equal(t, "failed", conn.Status)
				assert.Contains(t, conn.LastFailureMessage, tt.wantMsg)
				assert.NotContains(t, conn.LastFailureMessage, "from-secret")

				return
			}

			assert.Equal(t, "successful", conn.Status)

			e.op.mu.Lock()
			defer e.op.mu.Unlock()

			assert.Equal(t, "app", e.op.seen.User)
			assert.Equal(t, "from-secret", e.op.seen.Password)
			assert.Equal(t, "db.internal", e.op.seen.Server)
			assert.Equal(t, 3307, e.op.seen.Port)
			assert.Equal(t, "appdb", e.op.seen.Database)
		})
	}
}

func TestSecretFailureFailsTheTaskWithoutLeakingIt(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, "mysql", "full-load")
	e.b.SetSecrets(&fakeSecrets{values: map[string]string{"db/creds": `{"username":"u"}`}})

	ep, err := e.b.CreateEndpoint(t.Context(), "secret-src", "source", "mysql", "", "", "", "", 0, nil,
		secretEndpoint("db/creds", secretRole))
	require.NoError(t, err)

	task, err := e.b.CreateReplicationTask(t.Context(), "secret-task", ep.EndpointArn, e.tgt, e.ri, "full-load",
		selectApp, "", nil, dms.ReplicationTaskCDCSettings{})
	require.NoError(t, err)

	_, err = e.b.StartReplicationTask(t.Context(), task.ReplicationTaskArn, dms.ReplicationTaskCDCSettings{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		tasks, derr := e.b.DescribeReplicationTasks(t.Context(),
			dms.NewIdentifierFilter("replication-task-arn", task.ReplicationTaskArn))

		return derr == nil && len(tasks) == 1 && tasks[0].Status == "failed" &&
			assert.Contains(t, tasks[0].LastFailureMessage, "source endpoint: invalid endpoint secret")
	}, 10*time.Second, 5*time.Millisecond)
}

type serverlessEnv struct {
	*engineEnv
	config string
}

func newServerlessEnv(t *testing.T, replicationType string) *serverlessEnv {
	t.Helper()

	e := newEngineEnv(t, "mysql", "full-load")

	minUnits, maxUnits := int32(2), int32(8)
	rc, err := e.b.CreateReplicationConfig(t.Context(), dms.CreateReplicationConfigParams{
		Identifier: "serverless", ReplicationType: replicationType, SourceEndpointArn: e.src,
		TargetEndpointArn: e.tgt, TableMappings: selectApp,
		ComputeConfig: &dms.ComputeConfig{MinCapacityUnits: &minUnits, MaxCapacityUnits: &maxUnits},
	}, nil)
	require.NoError(t, err)

	return &serverlessEnv{engineEnv: e, config: rc.ReplicationConfigArn}
}

func (s *serverlessEnv) replication(t *testing.T) *dms.ReplicationConfig {
	t.Helper()

	list, err := s.b.DescribeReplications(t.Context(), dms.NewIdentifierFilter("replication-config-arn", s.config))
	require.NoError(t, err)
	require.Len(t, list, 1)

	return list[0]
}

func (s *serverlessEnv) awaitStatus(t *testing.T, status string) *dms.ReplicationConfig {
	t.Helper()

	var got *dms.ReplicationConfig

	require.Eventually(t, func() bool {
		got = s.replication(t)

		return got.Status == status
	}, 10*time.Second, 5*time.Millisecond, "status %s", status)

	return got
}

func TestServerlessReplicationMovesData(t *testing.T) {
	t.Parallel()

	s := newServerlessEnv(t, "full-load")

	started, err := s.b.StartReplication(t.Context(), s.config, "start-replication", dms.StartReplicationCDC{})
	require.NoError(t, err)
	assert.Equal(t, "starting", started.Status)

	_, err = s.b.StartReplication(t.Context(), s.config, "start-replication", dms.StartReplicationCDC{})
	require.ErrorIs(t, err, dms.ErrInvalidState)

	err = s.b.DeleteReplicationConfig(t.Context(), s.config)
	require.ErrorIs(t, err, dms.ErrInvalidState, "an active replication cannot be deleted")

	s.awaitStatus(t, "running")
	assert.Zero(t, s.replication(t).ElapsedTimeMillis)

	s.clock.Advance(45 * time.Second)
	close(s.op.src.hold)

	done := s.awaitStatus(t, "stopped")
	assert.Equal(t, dmsengine.StopFullLoadOnly, done.StopReason)
	assert.EqualValues(t, 45_000, done.ElapsedTimeMillis)
	assert.False(t, done.FullLoadFinishDate.IsZero())
	require.Len(t, done.TableStats, 1)
	assert.Equal(t, "Table completed", done.TableStats[0].TableState)
	assert.EqualValues(t, 3, done.TableStats[0].FullLoadRows)

	require.NoError(t, s.b.DeleteReplicationConfig(t.Context(), s.config))
}

func TestServerlessReplicationCDCStopAndResume(t *testing.T) {
	t.Parallel()

	s := newServerlessEnv(t, "cdc")

	_, err := s.b.StopReplication(t.Context(), s.config)
	require.ErrorIs(t, err, dms.ErrInvalidState)

	_, err = s.b.StartReplication(t.Context(), s.config, "start-replication",
		dms.StartReplicationCDC{StartPosition: "bin.1:100"})
	require.NoError(t, err)
	s.awaitStatus(t, "running")
	assert.Equal(t, dmsengine.StreamStart{Position: "bin.1:100"}, <-s.op.src.startAts)

	s.op.src.changes <- dmsengine.Change{Commit: true, Position: "bin.1:777"}
	require.Eventually(t, func() bool { return s.replication(t).RecoveryCheckpoint == "bin.1:777" },
		10*time.Second, 5*time.Millisecond)

	stopping, err := s.b.StopReplication(t.Context(), s.config)
	require.NoError(t, err)
	assert.Equal(t, "stopping", stopping.Status)

	stopped := s.awaitStatus(t, "stopped")
	assert.Equal(t, dmsengine.StopNormal, stopped.StopReason)

	_, err = s.b.StartReplication(t.Context(), s.config, "resume-processing", dms.StartReplicationCDC{})
	require.NoError(t, err)
	s.awaitStatus(t, "running")
	assert.Equal(t, dmsengine.StreamStart{Position: "bin.1:777"}, <-s.op.src.startAts)
}

func TestServerlessReplicationFailureIsReported(t *testing.T) {
	t.Parallel()

	s := newServerlessEnv(t, "full-load")
	s.op.src.pingErr = errUnreachable

	_, err := s.b.StartReplication(t.Context(), s.config, "start-replication", dms.StartReplicationCDC{})
	require.NoError(t, err)

	failed := s.awaitStatus(t, "failed")
	assert.Equal(t, dmsengine.StopFatalError, failed.StopReason)
	assert.Contains(t, failed.FailureMessage, "connection refused")
	assert.NotContains(t, failed.FailureMessage, "secret-pass")
}

func TestServerlessReplicationWireShapes(t *testing.T) {
	t.Parallel()

	s := newServerlessEnv(t, "full-load")
	client := newTestDMSClient(t, dms.NewHandler(s.b))

	started, err := client.StartReplication(t.Context(), &dmssdk.StartReplicationInput{
		ReplicationConfigArn: aws.String(s.config), StartReplicationType: aws.String("start-replication"),
	})
	require.NoError(t, err)
	assert.Equal(t, "starting", aws.ToString(started.Replication.Status))
	assert.NotNil(t, started.Replication.ReplicationCreateTime)

	s.awaitStatus(t, "running")
	s.clock.Advance(30 * time.Second)
	close(s.op.src.hold)
	s.awaitStatus(t, "stopped")

	desc, err := client.DescribeReplications(t.Context(), &dmssdk.DescribeReplicationsInput{})
	require.NoError(t, err)
	require.Len(t, desc.Replications, 1)

	r := desc.Replications[0]
	assert.Equal(t, "stopped", aws.ToString(r.Status))
	assert.Equal(t, "Stop Reason FULL_LOAD_ONLY_FINISHED", aws.ToString(r.StopReason))
	require.NotNil(t, r.ReplicationStats)
	assert.EqualValues(t, 30_000, r.ReplicationStats.ElapsedTimeMillis)
	assert.EqualValues(t, 100, r.ReplicationStats.FullLoadProgressPercent)
	assert.EqualValues(t, 1, r.ReplicationStats.TablesLoaded)
	assert.NotNil(t, r.ReplicationStats.FullLoadFinishDate)
	assert.NotNil(t, r.ReplicationLastStopTime)
	assert.Empty(t, r.FailureMessages)

	stats, err := client.DescribeReplicationTableStatistics(
		t.Context(),
		&dmssdk.DescribeReplicationTableStatisticsInput{
			ReplicationConfigArn: aws.String(s.config),
		},
	)
	require.NoError(t, err)
	require.Len(t, stats.ReplicationTableStatistics, 1)
	assert.Equal(t, "items", aws.ToString(stats.ReplicationTableStatistics[0].TableName))
	assert.EqualValues(t, 3, stats.ReplicationTableStatistics[0].FullLoadRows)
}

func TestServerlessReplicationProvisionDataFromCapacity(t *testing.T) {
	t.Parallel()

	s := newServerlessEnv(t, "cdc")
	client := newTestDMSClient(t, dms.NewHandler(s.b))

	_, err := client.StartReplication(t.Context(), &dmssdk.StartReplicationInput{
		ReplicationConfigArn: aws.String(s.config), StartReplicationType: aws.String("start-replication"),
	})
	require.NoError(t, err)
	s.awaitStatus(t, "running")

	desc, err := client.DescribeReplications(t.Context(), &dmssdk.DescribeReplicationsInput{})
	require.NoError(t, err)
	require.NotNil(t, desc.Replications[0].ProvisionData)
	assert.EqualValues(t, 2, desc.Replications[0].ProvisionData.ProvisionedCapacityUnits)
}

func TestServerlessSnapshotSettlesRunningReplications(t *testing.T) {
	t.Parallel()

	s := newServerlessEnv(t, "cdc")

	_, err := s.b.StartReplication(t.Context(), s.config, "start-replication", dms.StartReplicationCDC{})
	require.NoError(t, err)
	s.awaitStatus(t, "running")

	restored := dms.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), s.b.Snapshot(t.Context())))

	list, err := restored.DescribeReplications(t.Context(), dms.DescribeFilters{})
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "stopped", list[0].Status)
	assert.NotEmpty(t, list[0].StopReason)
	assert.Equal(t, "serverless", list[0].ReplicationConfigIdentifier)
}
