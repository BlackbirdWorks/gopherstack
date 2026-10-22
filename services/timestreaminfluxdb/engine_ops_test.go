package timestreaminfluxdb_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	sdk "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreaminfluxdb"
)

var errNoBucket = errors.New("NoSuchBucket")

type fakeSecrets struct {
	values map[string]string
	mu     sync.Mutex
	next   int
}

func (s *fakeSecrets) CreateSecret(region, name, value string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.next++
	arn := "arn:aws:secretsmanager:" + region + ":123456789012:secret:" + name + "-" + string(rune('a'+s.next))
	s.values[arn] = value

	return arn, nil
}

func (s *fakeSecrets) DeleteSecret(_, arn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.values, arn)

	return nil
}

func (s *fakeSecrets) all() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return maps.Clone(s.values)
}

type fakeS3 struct {
	buckets map[string]bool
	objects map[string]string
	mu      sync.Mutex
}

func (s *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)] = string(body)

	return &s3.PutObjectOutput{}, nil
}

func (s *fakeS3) HeadBucket(_ context.Context, in *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.buckets[aws.ToString(in.Bucket)] {
		return nil, errNoBucket
	}

	return &s3.HeadBucketOutput{}, nil
}

func (s *fakeS3) snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return maps.Clone(s.objects)
}

type opsEnv struct {
	*fixture

	eng     *fakeEngine
	secrets *fakeSecrets
	s3      *fakeS3
	dataDir string
}

func newOpsEnv(t *testing.T) *opsEnv {
	t.Helper()

	return newOpsEnvWith(t, nil)
}

// newOpsEnvWith builds the backend with a real engine when given one, else the recording fake.
func newOpsEnvWith(t *testing.T, custom timestreaminfluxdb.Engine) *opsEnv {
	t.Helper()

	e := &opsEnv{
		eng:     &fakeEngine{},
		secrets: &fakeSecrets{values: map[string]string{}},
		s3:      &fakeS3{buckets: map[string]bool{"log-bucket": true}, objects: map[string]string{}},
		dataDir: t.TempDir(),
	}

	b := timestreaminfluxdb.NewInMemoryBackend(testAccount, "us-east-1")
	b.SetDataDir(e.dataDir)
	if custom != nil {
		b.SetEngine(custom)
	} else {
		b.SetEngine(e.eng)
	}

	b.SetSecretStore(e.secrets)
	b.SetLogSink(e.s3)
	t.Cleanup(b.Close)

	e.fixture = newFixtureFromBackend(t, b, &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)})

	return e
}

func (e *opsEnv) waitAvailable(t *testing.T, id *string) {
	t.Helper()

	require.Eventually(t, func() bool { return status(t, e.fixture, id) == types.StatusAvailable },
		5*time.Second, time.Millisecond)
}

func (e *opsEnv) available(t *testing.T, in *sdk.CreateDbInstanceInput) *string {
	t.Helper()

	out, err := e.client.CreateDbInstance(t.Context(), in)
	require.NoError(t, err)
	e.waitAvailable(t, out.Id)

	return out.Id
}

func (e *opsEnv) backupStatus(t *testing.T, id *string) types.DbBackupStatus {
	t.Helper()

	got, err := e.client.GetDbBackup(t.Context(), &sdk.GetDbBackupInput{Identifier: id})
	require.NoError(t, err)

	return got.Status
}

func TestRebootRestartsServerAndKeepsData(t *testing.T) {
	t.Parallel()

	e := newOpsEnv(t)
	id := e.available(t, instanceInput("reboot-db"))
	dir := filepath.Join(e.dataDir, "instances", aws.ToString(id))
	require.DirExists(t, dir)

	out, err := e.client.RebootDbInstance(t.Context(), &sdk.RebootDbInstanceInput{Identifier: id})
	require.NoError(t, err)
	assert.Equal(t, types.StatusRebooting, out.Status)

	e.waitAvailable(t, id)

	_, _, restarted, _ := e.eng.snapshot()
	assert.Equal(t, []string{"ctr-" + aws.ToString(id)}, restarted)
	assert.DirExists(t, dir, "a reboot keeps the data directory")

	_, err = e.client.DeleteDbInstance(t.Context(), &sdk.DeleteDbInstanceInput{Identifier: id})
	require.NoError(t, err)
	assert.NoDirExists(t, dir, "delete removes the data directory")
}

func TestUpdateAppliesParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update   func(id *string, group *string) *sdk.UpdateDbInstanceInput
		name     string
		wantEnv  []string
		recreate bool
	}{
		{
			name: "parameter group restarts with new settings",
			update: func(id, group *string) *sdk.UpdateDbInstanceInput {
				return &sdk.UpdateDbInstanceInput{Identifier: id, DbParameterGroupIdentifier: group}
			},
			recreate: true,
			wantEnv: []string{
				"INFLUXD_HTTP_IDLE_TIMEOUT=30s",
				"INFLUXD_LOG_LEVEL=debug",
				"INFLUXD_NO_TASKS=true",
				"INFLUXD_QUERY_CONCURRENCY=10",
			},
		},
		{
			name: "storage growth is metadata only",
			update: func(id, _ *string) *sdk.UpdateDbInstanceInput {
				return &sdk.UpdateDbInstanceInput{Identifier: id, AllocatedStorage: aws.Int32(500)}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newOpsEnv(t)
			group, err := e.client.CreateDbParameterGroup(t.Context(), &sdk.CreateDbParameterGroupInput{
				Name: aws.String("tuned"),
				Parameters: &types.ParametersMemberInfluxDBv2{Value: types.InfluxDBv2Parameters{
					LogLevel: types.LogLevelDebug, QueryConcurrency: aws.Int32(10), NoTasks: aws.Bool(true),
					HttpIdleTimeout: &types.Duration{DurationType: types.DurationTypeSeconds, Value: aws.Int64(30)},
				}},
			})
			require.NoError(t, err)

			id := e.available(t, instanceInput("update-db"))
			_, err = e.client.UpdateDbInstance(t.Context(), tt.update(id, group.Id))
			require.NoError(t, err)
			e.settle()
			e.waitAvailable(t, id)

			specs, recreated, _, _ := e.eng.snapshot()
			if !tt.recreate {
				assert.Empty(t, recreated)

				return
			}

			require.Len(t, recreated, 1)
			assert.Equal(t, tt.wantEnv, recreated[0].Env)
			assert.Equal(t, specs[0].DataDir, recreated[0].DataDir, "the server keeps its volume")
			assert.Equal(t, specs[0].Token, recreated[0].Token)
		})
	}
}

func TestAuthParametersSecret(t *testing.T) {
	t.Parallel()

	e := newOpsEnv(t)
	in := instanceInput("secret-db")
	in.Username, in.Organization, in.Bucket = aws.String("alice"), aws.String("acme"), aws.String("metrics")

	out, err := e.client.CreateDbInstance(t.Context(), in)
	require.NoError(t, err)

	arn := aws.ToString(out.InfluxAuthParametersSecretArn)
	assert.Regexp(t, `^arn:aws:secretsmanager:us-east-1:123456789012:secret:`, arn)

	e.waitAvailable(t, out.Id)

	got, err := e.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: out.Id})
	require.NoError(t, err)
	assert.Equal(t, arn, aws.ToString(got.InfluxAuthParametersSecretArn))

	var secret map[string]string
	require.NoError(t, json.Unmarshal([]byte(e.secrets.all()[arn]), &secret))

	specs, _, _, _ := e.eng.snapshot()
	assert.Equal(t, map[string]string{
		"username": "alice", "password": "Passw0rdPassw0rd", "organization": "acme", "bucket": "metrics",
		"token": specs[0].Token,
	}, secret)
	assert.Len(t, specs[0].Token, 64)

	_, err = e.client.DeleteDbInstance(t.Context(), &sdk.DeleteDbInstanceInput{Identifier: out.Id})
	require.NoError(t, err)
	assert.Empty(t, e.secrets.all())
}

func TestClusterMembersRunServers(t *testing.T) {
	t.Parallel()

	e := newOpsEnv(t)
	out, err := e.client.CreateDbCluster(t.Context(), clusterInput("cluster-db"))
	require.NoError(t, err)

	getStatus := func() types.ClusterStatus {
		got, gerr := e.client.GetDbCluster(t.Context(), &sdk.GetDbClusterInput{DbClusterId: out.DbClusterId})
		require.NoError(t, gerr)

		return got.Status
	}

	require.Eventually(t, settled(e, getStatus),
		5*time.Second, time.Millisecond)

	specs, _, _, _ := e.eng.snapshot()
	assert.Len(t, specs, 3, "every member runs a server")

	got, err := e.client.GetDbCluster(t.Context(), &sdk.GetDbClusterInput{DbClusterId: out.DbClusterId})
	require.NoError(t, err)
	require.NotNil(t, got.InfluxAuthParametersSecretArn)
	assert.Len(t, e.secrets.all(), 1)

	_, err = e.client.RebootDbCluster(t.Context(), &sdk.RebootDbClusterInput{DbClusterId: out.DbClusterId})
	require.NoError(t, err)
	require.Eventually(t, settled(e, getStatus),
		5*time.Second, time.Millisecond)

	_, _, restarted, _ := e.eng.snapshot()
	assert.Len(t, restarted, 3)

	_, err = e.client.DeleteDbCluster(t.Context(), &sdk.DeleteDbClusterInput{DbClusterId: out.DbClusterId})
	require.NoError(t, err)
	assert.Len(t, e.eng.stoppedIDs(), 3)
	assert.Empty(t, e.secrets.all())
}

func backupInstance(name string) *sdk.CreateDbInstanceInput {
	in := instanceInput(name)
	in.DbBackupConfigurations = []types.DbBackupConfiguration{
		{Enabled: aws.Bool(true), RetentionDays: aws.Int32(2), Type: types.AutomatedDbBackupTypeDaily},
	}

	return in
}

func TestAutomatedBackupsRunOnTheClock(t *testing.T) {
	t.Parallel()

	e := newOpsEnv(t)
	id := e.available(t, backupInstance("backup-db"))

	e.backend.RunScheduled(t.Context())

	list, err := e.client.ListDbBackups(t.Context(), &sdk.ListDbBackupsInput{DbResourceId: id})
	require.NoError(t, err)
	assert.Empty(t, list.Items, "nothing is due before the first midnight")

	e.clock.advance(24 * time.Hour)
	e.backend.RunScheduled(t.Context())

	list, err = e.client.ListDbBackups(t.Context(), &sdk.ListDbBackupsInput{DbResourceId: id})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "DAILY", string(list.Items[0].Type))
	assert.True(t, strings.HasPrefix(aws.ToString(list.Items[0].Name), "automated-backup-db-"))

	first := list.Items[0].Id
	require.Eventually(t, func() bool { return e.backupStatus(t, first) == types.DbBackupStatusCompleted },
		5*time.Second, time.Millisecond)

	specs, _, _, backups := e.eng.snapshot()
	assert.Equal(t, []string{"ctr-" + aws.ToString(id) + "|" + specs[0].Token + "|" + aws.ToString(first)}, backups)

	e.backend.RunScheduled(t.Context())

	again, err := e.client.ListDbBackups(t.Context(), &sdk.ListDbBackupsInput{DbResourceId: id})
	require.NoError(t, err)
	assert.Len(t, again.Items, 1, "the same day does not back up twice")

	e.clock.advance(72 * time.Hour)
	e.backend.RunScheduled(t.Context())
	e.settle()

	_, err = e.client.GetDbBackup(t.Context(), &sdk.GetDbBackupInput{Identifier: first})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "a backup past its retention is deleted")
}

func TestBackupFailureAndRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		backupErr  error
		wantStatus types.DbBackupStatus
		wantSource bool
	}{
		{name: "completed backup restores its data", wantStatus: types.DbBackupStatusCompleted, wantSource: true},
		{name: "failed backup is reported", backupErr: errBoom, wantStatus: types.DbBackupStatusFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newOpsEnv(t)
			e.eng.backupErr = tt.backupErr
			id := e.available(t, instanceInput("source-db"))

			bk, err := e.client.CreateDbBackup(t.Context(), &sdk.CreateDbBackupInput{
				DbResourceId: id, Name: aws.String("manual-one"),
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool { return e.backupStatus(t, bk.Id) == tt.wantStatus },
				5*time.Second, time.Millisecond)

			if !tt.wantSource {
				return
			}

			restored, err := e.client.RestoreFromDbBackup(t.Context(), &sdk.RestoreFromDbBackupInput{
				DbBackupId: bk.Id,
				Name:       aws.String("restored-db"),
			})
			require.NoError(t, err)
			e.waitAvailable(t, restored.RestoredDbResourceId)

			specs, _, _, _ := e.eng.snapshot()
			require.Len(t, specs, 2)
			assert.Equal(t, aws.ToString(bk.Id), specs[1].RestoreFrom, "the restored server replays the backup")
			assert.Equal(t, specs[0].Token, specs[1].Token, "a full restore brings back the original token")
		})
	}
}

func TestMaintenanceRecordsLastRun(t *testing.T) {
	t.Parallel()

	e := newOpsEnv(t)
	id := e.available(t, instanceInput("maint-db"))

	lastRun := func() *time.Time {
		got, err := e.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: id})
		require.NoError(t, err)

		return got.LastMaintenanceTime
	}

	e.clock.advance(24 * time.Hour)
	e.backend.RunScheduled(t.Context())
	assert.Nil(t, lastRun(), "the Sunday window has not passed")

	e.clock.advance(48 * time.Hour)
	e.backend.RunScheduled(t.Context())

	got := lastRun()
	require.NotNil(t, got)
	assert.True(t, got.Equal(e.clock.now()))
}

func TestLogDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucket     string
		wantObject bool
	}{
		{name: "existing bucket receives logs", bucket: "log-bucket", wantObject: true},
		{name: "missing bucket keeps the logs buffered", bucket: "absent-bucket"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newOpsEnv(t)
			e.eng.logs = []byte("ts=1 lvl=info msg=hello")

			in := instanceInput("log-db")
			in.LogDeliveryConfiguration = &types.LogDeliveryConfiguration{S3Configuration: &types.S3Configuration{
				BucketName: aws.String(tt.bucket), Enabled: aws.Bool(true),
			}}
			id := e.available(t, in)

			e.backend.RunScheduled(t.Context())
			assert.Empty(t, e.s3.snapshot(), "the delivery interval has not elapsed")

			e.clock.advance(2 * time.Minute)
			e.backend.RunScheduled(t.Context())

			objs := e.s3.snapshot()
			if !tt.wantObject {
				assert.Empty(t, objs)
				assert.NotEmpty(t, e.eng.logs)

				return
			}

			require.Len(t, objs, 1)

			for k, v := range objs {
				assert.True(t, strings.HasPrefix(k, "log-bucket/timestream-influxdb/"+aws.ToString(id)+"/"), k)
				assert.Equal(t, "ts=1 lvl=info msg=hello", v)
			}
		})
	}
}

func TestDataDirectoryIsRemovedOnClose(t *testing.T) {
	t.Parallel()

	eng := &fakeEngine{}
	f := newEngineFixture(t, eng)

	out, err := f.client.CreateDbInstance(t.Context(), instanceInput("temp-db"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return status(t, f, out.Id) == types.StatusAvailable },
		5*time.Second, time.Millisecond)

	specs, _, _, _ := eng.snapshot()
	require.NotEmpty(t, specs[0].DataDir)
	require.DirExists(t, specs[0].DataDir)

	f.backend.Close()
	assert.NoDirExists(t, specs[0].DataDir)

	_, err = os.Stat(filepath.Dir(specs[0].DataDir))
	assert.True(t, os.IsNotExist(err))
}

func settled(e *opsEnv, getStatus func() types.ClusterStatus) func() bool {
	return func() bool {
		e.settle()

		return getStatus() == types.ClusterStatusAvailable
	}
}
