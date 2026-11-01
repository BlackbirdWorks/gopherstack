package app

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	dmstypes "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const filterItemsRule = `{"rule-type":"selection","rule-id":"1","rule-name":"1","rule-action":"include",` +
	`"object-locator":{"schema-name":"appdb","table-name":"items"},"filters":[` +
	`{"filter-type":"source","column-name":"id","filter-conditions":[{"filter-operator":"ste","value":"5"}]},` +
	`{"filter-type":"source","column-name":"qty","filter-conditions":[{"filter-operator":"gte","value":"10"}]}]}`

func columnRule(id, action, column, extra string) string {
	return fmt.Sprintf(`{"rule-type":"transformation","rule-id":"%s","rule-name":"%s","rule-target":"column",`+
		`"rule-action":"%s","object-locator":{"schema-name":"appdb","table-name":"items","column-name":"%s"}%s}`,
		id, id, action, column, extra)
}

func TestDMSDockerFiltersAndColumnRules(t *testing.T) {
	t.Parallel()
	requireDMSDocker(t, "mysql:8.4", "postgres:16")

	h := newDMSHarness(t)
	srcEP, tgtEP := h.createDB(t, "src", "mysql", "8.4"), h.createDB(t, "tgt", "postgres", "16")
	src, tgt := openDB(t, "mysql", srcEP), openDB(t, "postgres", tgtEP)

	mustExec(t, src,
		"CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(40), qty INT)",
		"CREATE TABLE logs (id INT PRIMARY KEY)",
		"INSERT INTO items VALUES (1,'alpha',10),(2,'beta',5),(3,'gamma',30),(6,'delta',60)",
		"INSERT INTO logs VALUES (1)")

	ri := h.instance(t)
	srcARN := h.endpoint(t, "src-ep", dmstypes.ReplicationEndpointTypeValueSource, "mysql", &srcEP, nil)
	tgtARN := h.endpoint(t, "tgt-ep", dmstypes.ReplicationEndpointTypeValueTarget, "postgres", &tgtEP, nil)

	mapping := mappingRules(
		filterItemsRule,
		`{"rule-type":"selection","rule-id":"2","rule-name":"2","rule-action":"include",`+
			`"object-locator":{"schema-name":"appdb","table-name":"log_"}}`,
		columnRule("3", "add-column", "label",
			`,"value":"label","expression":"upper($name)||'-'||$id","data-type":{"type":"string","length":30}`),
		columnRule("4", "change-data-type", "qty", `,"data-type":{"type":"string","length":"12"}`),
	)

	taskARN := h.task(t, "rules", srcARN, tgtARN, ri, "full-load-and-cdc", mapping)
	h.awaitStatus(t, taskARN, "running")

	const q = "SELECT id::text, name, qty, label FROM appdb.items ORDER BY id"

	awaitRows(t, tgt, q, [][]string{{"1", "alpha", "10", "ALPHA-1"}, {"3", "gamma", "30", "GAMMA-3"}})
	awaitRows(t, tgt, "SELECT id::text FROM appdb.logs", [][]string{{"1"}})

	types := queryRows(t, tgt, "SELECT column_name, data_type, COALESCE(character_maximum_length::text,'') "+
		"FROM information_schema.columns WHERE table_schema='appdb' AND table_name='items' ORDER BY ordinal_position")
	assert.Equal(t, [][]string{
		{"id", "integer", ""}, {"name", "character varying", "40"}, {"qty", "character varying", "12"},
		{"label", "character varying", "30"},
	}, types)

	mustExec(t, src,
		"INSERT INTO items VALUES (4,'epsilon',40),(5,'low',1),(9,'high-id',90)",
		"UPDATE items SET name='ALPHA2' WHERE id=1",
		"UPDATE items SET qty=1 WHERE id=3",
		"UPDATE items SET qty=70 WHERE id=2")
	awaitRows(t, tgt, q, [][]string{
		{"1", "ALPHA2", "10", "ALPHA2-1"}, {"2", "beta", "70", "BETA-2"}, {"4", "epsilon", "40", "EPSILON-4"},
	})

	h.stop(t, taskARN)
}

func TestDMSDockerS3SourceCSVAndCDC(t *testing.T) {
	t.Parallel()
	requireDMSDocker(t, "postgres:16")

	h := newDMSHarness(t)
	tgtEP := h.createDB(t, "tgt", "postgres", "16")
	tgt := openDB(t, "postgres", tgtEP)

	s3c := s3.NewFromConfig(h.cfg, func(o *s3.Options) { o.UsePathStyle = true })
	_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("dms-in")})
	require.NoError(t, err)

	put := func(key, body string) {
		_, perr := s3c.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String("dms-in"), Key: aws.String(key), Body: strings.NewReader(body),
		})
		require.NoError(t, perr)
	}

	put("src/hr/employee/LOAD001.csv", "101,Smith,2014-06-04 10:00:00.123,12.50\n102,Jones,2015-10-08 00:00:00.000,7\n")

	const definition = `{"TableCount":"1","Tables":[{"TableName":"employee","TablePath":"hr/employee/",` +
		`"TableOwner":"hr","TableColumns":[` +
		`{"ColumnName":"Id","ColumnType":"INT8","ColumnNullable":"false","ColumnIsPk":"true"},` +
		`{"ColumnName":"LastName","ColumnType":"STRING","ColumnLength":"20","ColumnNullable":"true"},` +
		`{"ColumnName":"HireDate","ColumnType":"DATETIME","ColumnScale":"3","ColumnNullable":"true"},` +
		`{"ColumnName":"Rate","ColumnType":"NUMERIC","ColumnPrecision":"5","ColumnScale":"2","ColumnNullable":"true"}],` +
		`"TableColumnsTotal":"4"}]}`

	ri := h.instance(t)
	srcARN := h.endpoint(t, "s3-src", dmstypes.ReplicationEndpointTypeValueSource, "s3", nil,
		func(in *databasemigrationservice.CreateEndpointInput) {
			in.ExternalTableDefinition = aws.String(definition)
			in.S3Settings = &dmstypes.S3Settings{
				BucketName: aws.String("dms-in"), BucketFolder: aws.String("src"), CdcPath: aws.String("changes"),
				ServiceAccessRoleArn: aws.String(dmsRoleARN),
			}
		})
	tgtARN := h.endpoint(t, "tgt-ep", dmstypes.ReplicationEndpointTypeValueTarget, "postgres", &tgtEP, nil)

	conn, err := h.dms.TestConnection(t.Context(), &databasemigrationservice.TestConnectionInput{
		ReplicationInstanceArn: aws.String(ri), EndpointArn: aws.String(srcARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "successful", aws.ToString(conn.Connection.Status))

	taskARN := h.task(t, "s3-to-pg", srcARN, tgtARN, ri, "full-load-and-cdc", `{"rules":[`+
		`{"rule-type":"selection","rule-id":"1","rule-name":"1","rule-action":"include",`+
		`"object-locator":{"schema-name":"hr","table-name":"employee"}}]}`)
	h.awaitStatus(t, taskARN, "running")

	const q = `SELECT "Id"::text, "LastName", to_char("HireDate",'YYYY-MM-DD HH24:MI:SS.MS'), "Rate"::text ` +
		`FROM hr.employee ORDER BY "Id"`

	awaitRows(t, tgt, q, [][]string{
		{"101", "Smith", "2014-06-04 10:00:00.123", "12.50"}, {"102", "Jones", "2015-10-08 00:00:00.000", "7.00"},
	})

	put("src/changes/CDC00001.csv",
		"INSERT,employee,hr,103,Brown,2017-03-13 00:00:00.000,9.99\n"+
			"U,employee,hr,101,Smith-Updated,2014-06-04 10:00:00.123,13.00\n"+
			"D,employee,hr,102,Jones,2015-10-08 00:00:00.000,7.00\n")
	awaitRows(t, tgt, q, [][]string{
		{
			"101",
			"Smith-Updated",
			"2014-06-04 10:00:00.123",
			"13.00",
		}, {"103", "Brown", "2017-03-13 00:00:00.000", "9.99"},
	})

	require.Eventually(t, func() bool {
		s := h.tableStats(t, taskARN)["hr.employee"]

		return s.FullLoadRows == 2 && s.Inserts == 1 && s.Updates == 1 && s.Deletes == 1
	}, dmsDockerWait, dmsPoll)

	h.stop(t, taskARN)
}

func TestDMSDockerEndpointFromSecretsManager(t *testing.T) {
	t.Parallel()
	requireDMSDocker(t, "mysql:8.4", "postgres:16")

	h := newDMSHarness(t)
	srcEP, tgtEP := h.createDB(t, "src", "mysql", "8.4"), h.createDB(t, "tgt", "postgres", "16")
	src, tgt := openDB(t, "mysql", srcEP), openDB(t, "postgres", tgtEP)

	mustExec(
		t,
		src,
		"CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(40))",
		"INSERT INTO items VALUES (1,'alpha')",
	)

	sm := secretsmanager.NewFromConfig(h.cfg)
	secret := func(name, password string) string {
		out, err := sm.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
			Name: aws.String(name), SecretString: aws.String(fmt.Sprintf(
				`{"username":"master","password":%q,"host":%q,"port":"%d"}`, password, srcEP.host, srcEP.port)),
		})
		require.NoError(t, err)

		return aws.ToString(out.ARN)
	}

	secretEndpoint := func(id, secretID string) string {
		return h.endpoint(t, id, dmstypes.ReplicationEndpointTypeValueSource, "mysql", nil,
			func(in *databasemigrationservice.CreateEndpointInput) {
				in.MySQLSettings = &dmstypes.MySQLSettings{
					DatabaseName: aws.String("appdb"), SecretsManagerSecretId: aws.String(secretID),
					SecretsManagerAccessRoleArn: aws.String(dmsRoleARN),
				}
			})
	}

	ri := h.instance(t)
	goodARN := secretEndpoint("good-ep", secret("dms/good", dmsMasterPass))
	badARN := secretEndpoint("bad-ep", secret("dms/bad", "wrong-password-xyz"))
	tgtARN := h.endpoint(t, "tgt-ep", dmstypes.ReplicationEndpointTypeValueTarget, "postgres", &tgtEP, nil)

	test := func(arn string) *dmstypes.Connection {
		out, err := h.dms.TestConnection(t.Context(), &databasemigrationservice.TestConnectionInput{
			ReplicationInstanceArn: aws.String(ri), EndpointArn: aws.String(arn),
		})
		require.NoError(t, err)

		return out.Connection
	}

	assert.Equal(t, "successful", aws.ToString(test(goodARN).Status))

	bad := test(badARN)
	assert.Equal(t, "failed", aws.ToString(bad.Status))
	assert.NotContains(t, aws.ToString(bad.LastFailureMessage), "wrong-password-xyz")
	assert.NotContains(t, aws.ToString(bad.LastFailureMessage), dmsMasterPass)

	taskARN := h.task(t, "secret-task", goodARN, tgtARN, ri, "full-load", tableMapping("items"))
	h.awaitStatus(t, taskARN, "stopped")
	assert.Equal(t, [][]string{{"1", "alpha"}}, queryRows(t, tgt, "SELECT id::text, name FROM appdb.items"))
}

func TestDMSDockerServerlessReplication(t *testing.T) {
	t.Parallel()
	requireDMSDocker(t, "mysql:8.4", "postgres:16")

	h := newDMSHarness(t)
	srcEP, tgtEP := h.createDB(t, "src", "mysql", "8.4"), h.createDB(t, "tgt", "postgres", "16")
	src, tgt := openDB(t, "mysql", srcEP), openDB(t, "postgres", tgtEP)

	mustExec(t, src, "CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(40))",
		"INSERT INTO items VALUES (1,'alpha'),(2,'beta')")

	srcARN := h.endpoint(t, "src-ep", dmstypes.ReplicationEndpointTypeValueSource, "mysql", &srcEP, nil)
	tgtARN := h.endpoint(t, "tgt-ep", dmstypes.ReplicationEndpointTypeValueTarget, "postgres", &tgtEP, nil)

	cfg, err := h.dms.CreateReplicationConfig(t.Context(), &databasemigrationservice.CreateReplicationConfigInput{
		ReplicationConfigIdentifier: aws.String(
			"serverless",
		), ReplicationType: dmstypes.MigrationTypeValueFullLoadAndCdc,
		SourceEndpointArn: aws.String(srcARN), TargetEndpointArn: aws.String(tgtARN),
		TableMappings: aws.String(tableMapping("items")),
		ComputeConfig: &dmstypes.ComputeConfig{MinCapacityUnits: aws.Int32(2), MaxCapacityUnits: aws.Int32(8)},
	})
	require.NoError(t, err)

	cfgARN := aws.ToString(cfg.ReplicationConfig.ReplicationConfigArn)
	replication := func() dmstypes.Replication {
		out, derr := h.dms.DescribeReplications(t.Context(), &databasemigrationservice.DescribeReplicationsInput{
			Filters: []dmstypes.Filter{{Name: aws.String("replication-config-arn"), Values: []string{cfgARN}}},
		})
		require.NoError(t, derr)
		require.Len(t, out.Replications, 1)

		return out.Replications[0]
	}
	await := func(status string) dmstypes.Replication {
		var r dmstypes.Replication

		require.Eventually(t, func() bool {
			r = replication()
			if aws.ToString(r.Status) == "failed" {
				require.FailNow(t, "replication failed: "+strings.Join(r.FailureMessages, "; "))
			}

			return aws.ToString(r.Status) == status
		}, dmsDockerWait, dmsPoll)

		return r
	}
	start := func(kind string) {
		_, serr := h.dms.StartReplication(t.Context(), &databasemigrationservice.StartReplicationInput{
			ReplicationConfigArn: aws.String(cfgARN), StartReplicationType: aws.String(kind),
		})
		require.NoError(t, serr)
	}

	start("start-replication")
	await("running")

	const q = "SELECT id::text, name FROM appdb.items ORDER BY id"

	awaitRows(t, tgt, q, [][]string{{"1", "alpha"}, {"2", "beta"}})

	mustExec(t, src, "INSERT INTO items VALUES (3,'gamma')", "UPDATE items SET name='BETA' WHERE id=2")
	awaitRows(t, tgt, q, [][]string{{"1", "alpha"}, {"2", "BETA"}, {"3", "gamma"}})

	require.Eventually(t, func() bool {
		stats, serr := h.dms.DescribeReplicationTableStatistics(t.Context(),
			&databasemigrationservice.DescribeReplicationTableStatisticsInput{ReplicationConfigArn: aws.String(cfgARN)})
		if serr != nil || len(stats.ReplicationTableStatistics) != 1 {
			return false
		}

		s := stats.ReplicationTableStatistics[0]

		return s.FullLoadRows == 2 && s.Inserts == 1 && s.Updates == 1
	}, dmsDockerWait, dmsPoll)

	running := replication()
	require.NotNil(t, running.ReplicationStats)
	assert.EqualValues(t, 1, running.ReplicationStats.TablesLoaded)
	assert.EqualValues(t, 100, running.ReplicationStats.FullLoadProgressPercent)
	require.NotNil(t, running.ProvisionData)
	assert.EqualValues(t, 2, running.ProvisionData.ProvisionedCapacityUnits)

	_, err = h.dms.StopReplication(t.Context(), &databasemigrationservice.StopReplicationInput{
		ReplicationConfigArn: aws.String(cfgARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "Stop Reason NORMAL", aws.ToString(await("stopped").StopReason))

	mustExec(t, src, "INSERT INTO items VALUES (4,'delta')")
	start("resume-processing")
	await("running")
	awaitRows(t, tgt, q, [][]string{{"1", "alpha"}, {"2", "BETA"}, {"3", "gamma"}, {"4", "delta"}})

	_, err = h.dms.StopReplication(t.Context(), &databasemigrationservice.StopReplicationInput{
		ReplicationConfigArn: aws.String(cfgARN),
	})
	require.NoError(t, err)
	await("stopped")
}
