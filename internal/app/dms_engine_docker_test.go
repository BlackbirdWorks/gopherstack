package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	dmstypes "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	dmsDockerWait = 5 * time.Minute
	dmsPoll       = 500 * time.Millisecond
	dmsMasterPass = "firstpass1"
	dmsRoleARN    = "arn:aws:iam::000000000000:role/dms"
)

func requireDMSDocker(t *testing.T, images ...string) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}

	for _, img := range images {
		if err := exec.CommandContext(t.Context(), "docker", "image", "inspect", img).Run(); err != nil {
			t.Skipf("%s not present", img)
		}
	}
}

type dmsHarness struct {
	cfg aws.Config
	rds *rds.Client
	dms *databasemigrationservice.Client
}

func newDMSHarness(t *testing.T) *dmsHarness {
	t.Helper()

	cli := CLI{RDSEngine: "docker", DMSEngine: "docker"}
	log := buildLogger("")
	cli.AccountID, cli.Region = "000000000000", "us-east-1"
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(srv.URL),
	)
	require.NoError(t, err)

	return &dmsHarness{cfg: cfg, rds: rds.NewFromConfig(cfg), dms: databasemigrationservice.NewFromConfig(cfg)}
}

type dbEndpoint struct {
	host string
	port int32
}

func (h *dmsHarness) createDB(t *testing.T, id, engine, version string) dbEndpoint {
	t.Helper()

	ctx := t.Context()
	_, err := h.rds.CreateDBInstance(ctx, &rds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(id), Engine: aws.String(engine), EngineVersion: aws.String(version),
		DBInstanceClass: aws.String("db.t3.micro"), AllocatedStorage: aws.Int32(20), DBName: aws.String("appdb"),
		MasterUsername: aws.String("master"), MasterUserPassword: aws.String(dmsMasterPass),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		bg := context.WithoutCancel(ctx)
		_, _ = h.rds.DeleteDBInstance(bg, &rds.DeleteDBInstanceInput{
			DBInstanceIdentifier: aws.String(id), SkipFinalSnapshot: aws.Bool(true),
		})
		require.Eventually(t, func() bool {
			_, derr := h.rds.DescribeDBInstances(bg, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(id)})

			return derr != nil
		}, time.Minute, dmsPoll)
	})

	var ep dbEndpoint

	require.Eventually(t, func() bool {
		out, derr := h.rds.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(id)})
		if derr != nil || len(out.DBInstances) == 0 {
			return false
		}

		inst := out.DBInstances[0]
		if aws.ToString(inst.DBInstanceStatus) == "failed" {
			require.FailNow(t, "database instance failed")
		}

		if aws.ToString(inst.DBInstanceStatus) != "available" || inst.Endpoint == nil {
			return false
		}

		ep = dbEndpoint{host: aws.ToString(inst.Endpoint.Address), port: aws.ToInt32(inst.Endpoint.Port)}

		return true
	}, dmsDockerWait, dmsPoll)

	return ep
}

func openDB(t *testing.T, kind string, ep dbEndpoint) *sql.DB {
	t.Helper()

	addr := net.JoinHostPort(ep.host, strconv.Itoa(int(ep.port)))

	var (
		db  *sql.DB
		err error
	)

	if kind == "postgres" {
		db, err = sql.Open("pgx", "postgres://master:"+dmsMasterPass+"@"+addr+"/appdb?sslmode=disable")
	} else {
		c := mysql.NewConfig()
		c.User, c.Passwd, c.Net, c.Addr, c.DBName = "master", dmsMasterPass, "tcp", addr, "appdb"

		connector, cerr := mysql.NewConnector(c)
		require.NoError(t, cerr)

		db = sql.OpenDB(connector)
	}

	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func mustExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()

	for _, s := range stmts {
		_, err := db.ExecContext(t.Context(), s)
		require.NoError(t, err, s)
	}
}

func (h *dmsHarness) endpoint(
	t *testing.T, id string, kind dmstypes.ReplicationEndpointTypeValue, engine string, db *dbEndpoint,
	mut func(*databasemigrationservice.CreateEndpointInput),
) string {
	t.Helper()

	in := &databasemigrationservice.CreateEndpointInput{
		EndpointIdentifier: aws.String(id), EndpointType: kind, EngineName: aws.String(engine),
	}

	if db != nil {
		in.ServerName, in.Port = aws.String(db.host), aws.Int32(db.port)
		in.Username, in.Password, in.DatabaseName = aws.String("master"), aws.String(dmsMasterPass), aws.String("appdb")
	}

	if mut != nil {
		mut(in)
	}

	out, err := h.dms.CreateEndpoint(t.Context(), in)
	require.NoError(t, err)

	return aws.ToString(out.Endpoint.EndpointArn)
}

func (h *dmsHarness) instance(t *testing.T) string {
	t.Helper()

	out, err := h.dms.CreateReplicationInstance(t.Context(), &databasemigrationservice.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("ri"), ReplicationInstanceClass: aws.String("dms.t3.micro"),
	})
	require.NoError(t, err)

	return aws.ToString(out.ReplicationInstance.ReplicationInstanceArn)
}

func (h *dmsHarness) task(t *testing.T, id, src, tgt, ri, migration, mappings string) string {
	t.Helper()

	out, err := h.dms.CreateReplicationTask(t.Context(), &databasemigrationservice.CreateReplicationTaskInput{
		ReplicationTaskIdentifier: aws.String(id), SourceEndpointArn: aws.String(src), TargetEndpointArn: aws.String(tgt),
		ReplicationInstanceArn: aws.String(ri), MigrationType: dmstypes.MigrationTypeValue(migration),
		TableMappings: aws.String(mappings),
	})
	require.NoError(t, err)

	arn := aws.ToString(out.ReplicationTask.ReplicationTaskArn)
	_, err = h.dms.StartReplicationTask(t.Context(), &databasemigrationservice.StartReplicationTaskInput{
		ReplicationTaskArn: aws.String(arn), StartReplicationTaskType: dmstypes.StartReplicationTaskTypeValueStartReplication,
	})
	require.NoError(t, err)

	return arn
}

func (h *dmsHarness) describe(t *testing.T, arn string) dmstypes.ReplicationTask {
	t.Helper()

	out, err := h.dms.DescribeReplicationTasks(t.Context(), &databasemigrationservice.DescribeReplicationTasksInput{
		Filters: []dmstypes.Filter{{Name: aws.String("replication-task-arn"), Values: []string{arn}}},
	})
	require.NoError(t, err)
	require.Len(t, out.ReplicationTasks, 1)

	return out.ReplicationTasks[0]
}

func (h *dmsHarness) awaitStatus(t *testing.T, arn, want string) dmstypes.ReplicationTask {
	t.Helper()

	var got dmstypes.ReplicationTask

	require.Eventually(t, func() bool {
		got = h.describe(t, arn)
		if aws.ToString(got.Status) == "failed" {
			require.FailNow(t, "task failed: "+aws.ToString(got.LastFailureMessage))
		}

		return aws.ToString(got.Status) == want
	}, dmsDockerWait, dmsPoll)

	return got
}

func (h *dmsHarness) tableStats(t *testing.T, arn string) map[string]dmstypes.TableStatistics {
	t.Helper()

	out, err := h.dms.DescribeTableStatistics(t.Context(), &databasemigrationservice.DescribeTableStatisticsInput{
		ReplicationTaskArn: aws.String(arn),
	})
	require.NoError(t, err)

	m := map[string]dmstypes.TableStatistics{}
	for _, s := range out.TableStatistics {
		m[aws.ToString(s.SchemaName)+"."+aws.ToString(s.TableName)] = s
	}

	return m
}

func queryRows(t *testing.T, db *sql.DB, q string) [][]string {
	t.Helper()

	rows, err := db.QueryContext(t.Context(), q)
	if err != nil {
		return nil
	}
	defer rows.Close()

	cols, _ := rows.Columns()

	var out [][]string

	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if rows.Scan(ptrs...) != nil {
			return nil
		}

		r := make([]string, len(cols))
		for i, v := range vals {
			r[i] = v.String
		}

		out = append(out, r)
	}

	return out
}

func awaitRows(t *testing.T, db *sql.DB, q string, want [][]string) {
	t.Helper()

	var got [][]string

	require.Eventually(t, func() bool {
		got = queryRows(t, db, q)

		return assert.ObjectsAreEqual(want, got)
	}, dmsDockerWait, dmsPoll, "query %q want %v last %v", q, want, got)
}

const itemsMapping = `{"rules":[
 {"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"appdb","table-name":"items"},"rule-action":"include"}]}`

const renameMapping = `{"rules":[
 {"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"appdb","table-name":"items"},"rule-action":"include"},
 {"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-target":"schema","rule-action":"rename","object-locator":{"schema-name":"appdb"},"value":"dst"},
 {"rule-type":"transformation","rule-id":"3","rule-name":"3","rule-target":"table","rule-action":"rename","object-locator":{"schema-name":"appdb","table-name":"items"},"value":"items_copy"}]}`

const pgRenameMapping = `{"rules":[
 {"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"public","table-name":"items"},"rule-action":"include"},
 {"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-target":"schema","rule-action":"rename","object-locator":{"schema-name":"public"},"value":"appdb"},
 {"rule-type":"transformation","rule-id":"3","rule-name":"3","rule-target":"column","rule-action":"remove-column","object-locator":{"schema-name":"public","table-name":"items","column-name":"note"}}]}`

func TestDMSDockerMySQLToPostgres(t *testing.T) {
	t.Parallel()
	requireDMSDocker(t, "mysql:8.4", "postgres:16")

	h := newDMSHarness(t)
	srcEP, tgtEP := h.createDB(t, "src", "mysql", "8.4"), h.createDB(t, "tgt", "postgres", "16")
	src, tgt := openDB(t, "mysql", srcEP), openDB(t, "postgres", tgtEP)

	mustExec(t, src,
		"CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(40), qty INT UNSIGNED)",
		"CREATE TABLE ignored (id INT PRIMARY KEY)",
		"INSERT INTO items VALUES (1,'alpha',10),(2,'beta',20)")

	ri := h.instance(t)
	srcARN := h.endpoint(t, "src-ep", dmstypes.ReplicationEndpointTypeValueSource, "mysql", &srcEP, nil)
	tgtARN := h.endpoint(t, "tgt-ep", dmstypes.ReplicationEndpointTypeValueTarget, "postgres", &tgtEP, nil)

	for _, ep := range []string{srcARN, tgtARN} {
		out, err := h.dms.TestConnection(t.Context(), &databasemigrationservice.TestConnectionInput{
			ReplicationInstanceArn: aws.String(ri), EndpointArn: aws.String(ep),
		})
		require.NoError(t, err)
		assert.Equal(t, "successful", aws.ToString(out.Connection.Status))
	}

	bad := srcEP
	bad.port = 1
	badARN := h.endpoint(t, "bad-ep", dmstypes.ReplicationEndpointTypeValueSource, "mysql", &bad, nil)
	conn, err := h.dms.TestConnection(t.Context(), &databasemigrationservice.TestConnectionInput{
		ReplicationInstanceArn: aws.String(ri), EndpointArn: aws.String(badARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "failed", aws.ToString(conn.Connection.Status))
	assert.Contains(t, aws.ToString(conn.Connection.LastFailureMessage), "connect")
	assert.NotContains(t, aws.ToString(conn.Connection.LastFailureMessage), dmsMasterPass)

	taskARN := h.task(t, "mysql-to-pg", srcARN, tgtARN, ri, "full-load-and-cdc", renameMapping)

	running := h.awaitStatus(t, taskARN, "running")
	require.NotNil(t, running.ReplicationTaskStats)

	awaitRows(t, tgt, "SELECT id::text, name, qty::text FROM dst.items_copy ORDER BY id",
		[][]string{{"1", "alpha", "10"}, {"2", "beta", "20"}})

	require.Eventually(t, func() bool {
		return aws.ToString(h.tableStats(t, taskARN)["appdb.items"].TableState) == "Table completed"
	}, dmsDockerWait, dmsPoll)

	st := h.tableStats(t, taskARN)["appdb.items"]
	assert.EqualValues(t, 2, st.FullLoadRows)
	assert.NotContains(t, h.tableStats(t, taskARN), "appdb.ignored")

	stats := h.describe(t, taskARN).ReplicationTaskStats
	require.NotNil(t, stats)
	assert.EqualValues(t, 1, stats.TablesLoaded)
	assert.EqualValues(t, 100, stats.FullLoadProgressPercent)
	assert.Positive(t, stats.ElapsedTimeMillis)

	mustExec(t, src,
		"INSERT INTO items VALUES (3,'gamma',30)",
		"UPDATE items SET name='ALPHA', qty=11 WHERE id=1",
		"DELETE FROM items WHERE id=2")
	awaitRows(t, tgt, "SELECT id::text, name, qty::text FROM dst.items_copy ORDER BY id",
		[][]string{{"1", "ALPHA", "11"}, {"3", "gamma", "30"}})

	require.Eventually(t, func() bool {
		s := h.tableStats(t, taskARN)["appdb.items"]

		return s.Inserts == 1 && s.Updates == 1 && s.Deletes == 1
	}, dmsDockerWait, dmsPoll)

	_, err = h.dms.StopReplicationTask(t.Context(), &databasemigrationservice.StopReplicationTaskInput{
		ReplicationTaskArn: aws.String(taskARN),
	})
	require.NoError(t, err)

	stopped := h.awaitStatus(t, taskARN, "stopped")
	assert.Equal(t, "Stop Reason NORMAL", aws.ToString(stopped.StopReason))

	mustExec(t, src, "INSERT INTO items VALUES (4,'late',40)")
	assert.Equal(t, [][]string{{"1", "ALPHA", "11"}, {"3", "gamma", "30"}},
		queryRows(t, tgt, "SELECT id::text, name, qty::text FROM dst.items_copy ORDER BY id"))
}

func TestDMSDockerPostgresToMySQL(t *testing.T) {
	t.Parallel()
	requireDMSDocker(t, "mysql:8.4", "postgres:16")

	h := newDMSHarness(t)
	srcEP, tgtEP := h.createDB(t, "src", "postgres", "16"), h.createDB(t, "tgt", "mysql", "8.4")
	src, tgt := openDB(t, "postgres", srcEP), openDB(t, "mysql", tgtEP)

	mustExec(t, src,
		"CREATE TABLE items (id integer PRIMARY KEY, name text, note text, price numeric(8,2), seen timestamp)",
		"INSERT INTO items VALUES (1,'alpha','n1',1.50,'2024-01-02 03:04:05')")

	ri := h.instance(t)
	srcARN := h.endpoint(t, "src-ep", dmstypes.ReplicationEndpointTypeValueSource, "postgres", &srcEP, nil)
	tgtARN := h.endpoint(t, "tgt-ep", dmstypes.ReplicationEndpointTypeValueTarget, "mysql", &tgtEP, nil)
	taskARN := h.task(t, "pg-to-mysql", srcARN, tgtARN, ri, "full-load-and-cdc", pgRenameMapping)

	h.awaitStatus(t, taskARN, "running")
	q := "SELECT id, name, price, seen FROM appdb.items ORDER BY id"
	awaitRows(t, tgt, q, [][]string{{"1", "alpha", "1.50", "2024-01-02 03:04:05.000000"}})

	cols := queryRows(t, tgt, "SELECT column_name FROM information_schema.columns WHERE table_schema='appdb' AND table_name='items' ORDER BY ordinal_position")
	assert.Equal(t, [][]string{{"id"}, {"name"}, {"price"}, {"seen"}}, cols)

	mustExec(t, src,
		"INSERT INTO items VALUES (2,'beta','n2',2.25,'2024-02-03 04:05:06')",
		"UPDATE items SET name='ALPHA' WHERE id=1",
		"DELETE FROM items WHERE id=1")
	awaitRows(t, tgt, q, [][]string{{"2", "beta", "2.25", "2024-02-03 04:05:06.000000"}})

	require.Eventually(t, func() bool {
		s := h.tableStats(t, taskARN)["public.items"]

		return s.Inserts == 1 && s.Updates == 1 && s.Deletes == 1
	}, dmsDockerWait, dmsPoll)

	_, err := h.dms.StopReplicationTask(t.Context(), &databasemigrationservice.StopReplicationTaskInput{
		ReplicationTaskArn: aws.String(taskARN),
	})
	require.NoError(t, err)
	h.awaitStatus(t, taskARN, "stopped")

	_, err = h.dms.DeleteReplicationTask(t.Context(), &databasemigrationservice.DeleteReplicationTaskInput{
		ReplicationTaskArn: aws.String(taskARN),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(queryRows(t, src, "SELECT slot_name FROM pg_replication_slots")) == 0
	}, time.Minute, dmsPoll)
}

func TestDMSDockerMySQLToInProcessTargets(t *testing.T) {
	t.Parallel()
	requireDMSDocker(t, "mysql:8.4")

	h := newDMSHarness(t)
	srcEP := h.createDB(t, "src", "mysql", "8.4")
	src := openDB(t, "mysql", srcEP)
	mustExec(t, src,
		"CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(40))",
		"INSERT INTO items VALUES (1,'alpha'),(2,'be,ta')")

	ri := h.instance(t)
	srcARN := h.endpoint(t, "src-ep", dmstypes.ReplicationEndpointTypeValueSource, "mysql", &srcEP, nil)

	s3c := s3.NewFromConfig(h.cfg, func(o *s3.Options) { o.UsePathStyle = true })
	getObject := func(key string) (string, bool) {
		out, err := s3c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String("dms-out"), Key: aws.String(key)})
		if err != nil {
			return "", false
		}
		defer out.Body.Close()

		b, _ := io.ReadAll(out.Body)

		return string(b), true
	}

	t.Run("s3 csv", func(t *testing.T) {
		_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("dms-out")})
		require.NoError(t, err)

		tgt := h.endpoint(t, "s3-ep", dmstypes.ReplicationEndpointTypeValueTarget, "s3", nil,
			func(in *databasemigrationservice.CreateEndpointInput) {
				in.S3Settings = &dmstypes.S3Settings{
					BucketName: aws.String("dms-out"), BucketFolder: aws.String("full"),
					ServiceAccessRoleArn: aws.String(dmsRoleARN),
				}
			})
		arn := h.task(t, "to-s3", srcARN, tgt, ri, "full-load", itemsMapping)

		done := h.awaitStatus(t, arn, "stopped")
		assert.Equal(t, "Stop Reason FULL_LOAD_ONLY_FINISHED", aws.ToString(done.StopReason))

		body, ok := getObject("full/appdb/items/LOAD00000001.csv")
		require.True(t, ok)
		assert.Equal(t, "1,alpha\n2,\"be,ta\"\n", body)
		assert.EqualValues(t, 2, h.tableStats(t, arn)["appdb.items"].FullLoadRows)
	})

	t.Run("s3 cdc csv", func(t *testing.T) {
		tgt := h.endpoint(t, "s3-cdc-ep", dmstypes.ReplicationEndpointTypeValueTarget, "s3", nil,
			func(in *databasemigrationservice.CreateEndpointInput) {
				in.S3Settings = &dmstypes.S3Settings{
					BucketName: aws.String("dms-out"), BucketFolder: aws.String("cdc"),
					ServiceAccessRoleArn: aws.String(dmsRoleARN),
				}
			})
		arn := h.task(t, "to-s3-cdc", srcARN, tgt, ri, "full-load-and-cdc", itemsMapping)
		h.awaitStatus(t, arn, "running")
		require.Eventually(t, func() bool {
			_, ok := getObject("cdc/appdb/items/LOAD00000001.csv")

			return ok
		}, dmsDockerWait, dmsPoll)

		mustExec(t, src, "INSERT INTO items VALUES (3,'gamma')", "DELETE FROM items WHERE id=1")

		list := func() []string {
			out, err := s3c.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
				Bucket: aws.String("dms-out"), Prefix: aws.String("cdc/appdb/items/"),
			})
			require.NoError(t, err)

			var keys []string

			for _, o := range out.Contents {
				if !strings.Contains(aws.ToString(o.Key), "LOAD") {
					keys = append(keys, aws.ToString(o.Key))
				}
			}

			return keys
		}

		var all string

		require.Eventually(t, func() bool {
			all = ""
			for _, k := range list() {
				b, _ := getObject(k)
				all += b
			}

			return strings.Contains(all, "I,3,gamma\n") && strings.Contains(all, "D,1,alpha\n")
		}, dmsDockerWait, dmsPoll, "cdc files: %q", all)

		_, err := h.dms.StopReplicationTask(t.Context(), &databasemigrationservice.StopReplicationTaskInput{ReplicationTaskArn: aws.String(arn)})
		require.NoError(t, err)
		h.awaitStatus(t, arn, "stopped")
	})

	t.Run("kinesis json", func(t *testing.T) {
		kc := kinesis.NewFromConfig(h.cfg)
		_, err := kc.CreateStream(t.Context(), &kinesis.CreateStreamInput{StreamName: aws.String("dms-stream"), ShardCount: aws.Int32(1)})
		require.NoError(t, err)

		var desc *kinesis.DescribeStreamSummaryOutput

		require.Eventually(t, func() bool {
			var derr error

			desc, derr = kc.DescribeStreamSummary(t.Context(), &kinesis.DescribeStreamSummaryInput{
				StreamName: aws.String("dms-stream"),
			})

			return derr == nil && desc.StreamDescriptionSummary.StreamStatus == "ACTIVE"
		}, time.Minute, dmsPoll)

		tgt := h.endpoint(t, "kin-ep", dmstypes.ReplicationEndpointTypeValueTarget, "kinesis", nil,
			func(in *databasemigrationservice.CreateEndpointInput) {
				in.KinesisSettings = &dmstypes.KinesisSettings{
					StreamArn: desc.StreamDescriptionSummary.StreamARN, ServiceAccessRoleArn: aws.String(dmsRoleARN),
					MessageFormat: dmstypes.MessageFormatValueJsonUnformatted,
				}
			})
		arn := h.task(t, "to-kinesis", srcARN, tgt, ri, "full-load-and-cdc", itemsMapping)
		h.awaitStatus(t, arn, "running")
		mustExec(t, src, "INSERT INTO items VALUES (9,'omega')")

		it, err := kc.GetShardIterator(t.Context(), &kinesis.GetShardIteratorInput{
			StreamName: aws.String("dms-stream"), ShardId: aws.String("shardId-000000000000"),
			ShardIteratorType: "TRIM_HORIZON",
		})
		require.NoError(t, err)

		var got []dmsKinesisRec

		iter := it.ShardIterator

		require.Eventually(t, func() bool {
			out, gerr := kc.GetRecords(t.Context(), &kinesis.GetRecordsInput{ShardIterator: iter})
			require.NoError(t, gerr)

			iter = out.NextShardIterator

			for _, r := range out.Records {
				var x dmsKinesisRec
				require.NoError(t, json.Unmarshal(r.Data, &x))
				got = append(got, x)
			}

			return len(got) >= 3
		}, dmsDockerWait, dmsPoll)

		ops := map[string]int{}
		for _, r := range got {
			assert.Equal(t, "data", r.Metadata["record-type"])
			assert.Equal(t, "appdb", r.Metadata["schema-name"])
			assert.Equal(t, "items", r.Metadata["table-name"])
			ops[r.Metadata["operation"].(string)]++
		}

		assert.Equal(t, 2, ops["load"])
		assert.Equal(t, 1, ops["insert"])
		assert.EqualValues(t, 9, lastWithOp(got, "insert").Data["id"])

		_, err = h.dms.StopReplicationTask(t.Context(), &databasemigrationservice.StopReplicationTaskInput{ReplicationTaskArn: aws.String(arn)})
		require.NoError(t, err)
		h.awaitStatus(t, arn, "stopped")
	})

	t.Run("dynamodb", func(t *testing.T) {
		tgt := h.endpoint(t, "ddb-ep", dmstypes.ReplicationEndpointTypeValueTarget, "dynamodb", nil,
			func(in *databasemigrationservice.CreateEndpointInput) {
				in.DynamoDbSettings = &dmstypes.DynamoDbSettings{ServiceAccessRoleArn: aws.String(dmsRoleARN)}
			})
		arn := h.task(t, "to-ddb", srcARN, tgt, ri, "full-load", itemsMapping)
		h.awaitStatus(t, arn, "stopped")

		ddb := dynamodb.NewFromConfig(h.cfg)
		out, err := ddb.Scan(t.Context(), &dynamodb.ScanInput{TableName: aws.String("items")})
		require.NoError(t, err)
		assert.NotEmpty(t, out.Items)
		assert.Equal(t, int32(len(out.Items)), out.Count)
	})
}

type dmsKinesisRec struct {
	Data     map[string]any `json:"data"`
	Metadata map[string]any `json:"metadata"`
}

func lastWithOp(recs []dmsKinesisRec, op string) dmsKinesisRec {
	var out dmsKinesisRec

	for _, r := range recs {
		if r.Metadata["operation"] == op {
			out = r
		}
	}

	return out
}
