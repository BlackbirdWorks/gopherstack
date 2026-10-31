package glue_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/glue"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const crawlerName = "cr"

const crawlRole = "arn:aws:iam::000000000000:role/glue"

type crawlEnv struct {
	client *gluesdk.Client
	glue   *glue.InMemoryBackend
	s3     *s3backend.InMemoryBackend
	ddb    *ddbbackend.InMemoryDB
	t      *testing.T
}

func newCrawlEnv(t *testing.T) *crawlEnv {
	t.Helper()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	s3b := s3backend.NewInMemoryBackend(nil)
	ddb := ddbbackend.NewInMemoryDB()
	b.SetS3Backend(s3b)
	b.SetDynamoDBBackend(ddb)

	env := &crawlEnv{client: newTestGlueClient(t, glue.NewHandler(b)), glue: b, s3: s3b, ddb: ddb, t: t}

	_, err := s3b.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String("lake")})
	require.NoError(t, err)

	_, err = env.client.CreateDatabase(t.Context(), &gluesdk.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("db")},
	})
	require.NoError(t, err)

	return env
}

func (e *crawlEnv) put(objs map[string]string) {
	e.t.Helper()

	for k, v := range objs {
		_, err := e.s3.PutObject(e.t.Context(), &awss3.PutObjectInput{
			Bucket: aws.String("lake"), Key: aws.String(k), Body: bytes.NewReader([]byte(v)),
		})
		require.NoError(e.t, err)
	}
}

func (e *crawlEnv) create(targets *gluetypes.CrawlerTargets, mut func(*gluesdk.CreateCrawlerInput)) {
	e.t.Helper()

	in := &gluesdk.CreateCrawlerInput{
		Name: aws.String(crawlerName), Role: aws.String(crawlRole), DatabaseName: aws.String("db"), Targets: targets,
	}
	if mut != nil {
		mut(in)
	}

	_, err := e.client.CreateCrawler(e.t.Context(), in)
	require.NoError(e.t, err)
}

// crawl starts the crawler and returns once it is READY again.
func (e *crawlEnv) crawl() *gluetypes.Crawler {
	e.t.Helper()

	_, err := e.client.StartCrawler(e.t.Context(), &gluesdk.StartCrawlerInput{Name: aws.String(crawlerName)})
	require.NoError(e.t, err)

	var c *gluetypes.Crawler

	require.Eventually(e.t, func() bool {
		out, gerr := e.client.GetCrawler(e.t.Context(), &gluesdk.GetCrawlerInput{Name: aws.String(crawlerName)})
		require.NoError(e.t, gerr)
		c = out.Crawler

		return c.State == gluetypes.CrawlerStateReady
	}, 10*time.Second, 20*time.Millisecond)

	return c
}

func (e *crawlEnv) table(name string) *gluetypes.Table {
	e.t.Helper()

	out, err := e.client.GetTable(e.t.Context(), &gluesdk.GetTableInput{
		DatabaseName: aws.String("db"), Name: aws.String(name),
	})
	require.NoError(e.t, err)

	return out.Table
}

func (e *crawlEnv) hasTable(name string) bool {
	e.t.Helper()

	_, err := e.client.GetTable(e.t.Context(), &gluesdk.GetTableInput{
		DatabaseName: aws.String("db"), Name: aws.String(name),
	})

	return err == nil
}

func colSpec(cols []gluetypes.Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = aws.ToString(c.Name) + ":" + aws.ToString(c.Type)
	}

	return out
}

func (e *crawlEnv) partitionValues(table string) [][]string {
	e.t.Helper()

	out, err := e.client.GetPartitions(e.t.Context(), &gluesdk.GetPartitionsInput{
		DatabaseName: aws.String("db"), TableName: aws.String(table),
	})
	require.NoError(e.t, err)

	vals := make([][]string, len(out.Partitions))
	for i, p := range out.Partitions {
		vals[i] = p.Values
	}

	return vals
}

func (e *crawlEnv) lastCrawl() gluetypes.CrawlerHistory {
	e.t.Helper()

	out, err := e.client.ListCrawls(e.t.Context(), &gluesdk.ListCrawlsInput{CrawlerName: aws.String(crawlerName)})
	require.NoError(e.t, err)
	require.NotEmpty(e.t, out.Crawls)

	return out.Crawls[0]
}

func (e *crawlEnv) metrics() gluetypes.CrawlerMetrics {
	e.t.Helper()

	out, err := e.client.GetCrawlerMetrics(e.t.Context(), &gluesdk.GetCrawlerMetricsInput{
		CrawlerNameList: []string{crawlerName},
	})
	require.NoError(e.t, err)
	require.Len(e.t, out.CrawlerMetricsList, 1)

	return out.CrawlerMetricsList[0]
}

func ddbS(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: v} }
func ddbN(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberN{Value: v} }

func (e *crawlEnv) seedDynamoDB(ctx context.Context) {
	e.t.Helper()

	_, err := e.ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("orders"),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeN},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
	})
	require.NoError(e.t, err)

	for _, item := range []map[string]ddbtypes.AttributeValue{
		{
			"id": ddbN("1"), "name": ddbS("a"), "qty": ddbN("3"),
			"tags": &ddbtypes.AttributeValueMemberSS{Value: []string{"x"}},
			"meta": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{"w": ddbN("2")}},
		},
		{"id": ddbN("2"), "name": ddbS("b"), "qty": ddbN("2.5"), "ok": &ddbtypes.AttributeValueMemberBOOL{Value: true}},
	} {
		_, err = e.ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders"), Item: item})
		require.NoError(e.t, err)
	}
}

func TestCrawler_DynamoDBTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		prefix  string
		table   string
		wantErr bool
	}{
		{name: "infers schema", path: "orders", table: "orders"},
		{name: "table prefix", path: "orders", prefix: "ddb_", table: "ddb_orders"},
		{name: "missing table fails", path: "nope", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			env.seedDynamoDB(t.Context())
			env.create(&gluetypes.CrawlerTargets{
				DynamoDBTargets: []gluetypes.DynamoDBTarget{{Path: aws.String(tt.path), ScanAll: aws.Bool(true)}},
			}, func(in *gluesdk.CreateCrawlerInput) { in.TablePrefix = aws.String(tt.prefix) })

			c := env.crawl()

			if tt.wantErr {
				assert.Equal(t, gluetypes.LastCrawlStatusFailed, c.LastCrawl.Status)
				last := env.lastCrawl()
				assert.Equal(t, gluetypes.CrawlerHistoryStateFailed, last.State)
				assert.Contains(t, aws.ToString(last.ErrorMessage), "nope")

				return
			}

			tbl := env.table(tt.table)
			assert.Equal(t, []string{
				"id:bigint", "meta:struct<w:bigint>", "name:string", "ok:boolean", "qty:double", "tags:array<string>",
			}, colSpec(tbl.StorageDescriptor.Columns))
			assert.Equal(t, "dynamodb", tbl.Parameters["classification"])
			assert.Equal(t, "table", tbl.Parameters["typeOfData"])
			assert.Equal(t, "orders", aws.ToString(tbl.StorageDescriptor.Location))
			assert.Equal(t, "cr", tbl.Parameters["UPDATED_BY_CRAWLER"])

			m := env.metrics()
			assert.Equal(t, int32(1), m.TablesCreated)

			last := env.lastCrawl()
			assert.Equal(t, gluetypes.CrawlerHistoryStateCompleted, last.State)
			require.NotNil(t, last.StartTime, "ListCrawls carries StartTime/EndTime on the wire")
			require.NotNil(t, last.EndTime)
			assert.JSONEq(
				t,
				`{"TABLES_ADDED":1,"TABLES_UPDATED":0,"TABLES_DELETED":0,"PARTITIONS_ADDED":0}`,
				aws.ToString(last.Summary),
			)
		})
	}
}

const (
	deltaSchema = `{"type":"struct","fields":[` +
		`{"name":"id","type":"long","nullable":true,"metadata":{}},` +
		`{"name":"name","type":"string","nullable":true,"metadata":{}},` +
		`{"name":"year","type":"integer","nullable":true,"metadata":{}},` +
		`{"name":"tags","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}}]}`
)

func deltaCommit0() string {
	meta := `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` +
		jsonString(deltaSchema) + `,"partitionColumns":["year"]}}`

	return `{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}` + "\n" + meta + "\n" +
		`{"add":{"path":"year=2023/a.parquet","partitionValues":{"year":"2023"},"size":1,"dataChange":true}}` + "\n" +
		`{"add":{"path":"year=2024/b.parquet","partitionValues":{"year":"2024"},"size":1,"dataChange":true}}` + "\n"
}

func deltaCommit1() string {
	return `{"remove":{"path":"year=2023/a.parquet","dataChange":true}}` + "\n" +
		`{"add":{"path":"year=2025/c.parquet","partitionValues":{"year":"2025"},"size":1,"dataChange":true}}` + "\n"
}

func jsonString(s string) string {
	var sb strings.Builder

	sb.WriteByte('"')

	for _, r := range s {
		if r == '"' || r == '\\' {
			sb.WriteByte('\\')
		}

		sb.WriteRune(r)
	}

	sb.WriteByte('"')

	return sb.String()
}

func TestCrawler_DeltaTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check  func(t *testing.T, env *crawlEnv)
		name   string
		target gluetypes.DeltaTarget
	}{
		{
			name: "symlink table with manifest",
			target: gluetypes.DeltaTarget{
				DeltaTables: []string{"s3://lake/events"}, WriteManifest: aws.Bool(true),
			},
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()

				tbl := env.table("events")
				assert.Equal(
					t,
					[]string{"id:bigint", "name:string", "tags:array<string>"},
					colSpec(tbl.StorageDescriptor.Columns),
				)
				assert.Equal(t, []string{"year:int"}, colSpec(tbl.PartitionKeys))
				assert.Equal(
					t,
					"s3://lake/events/_symlink_format_manifest/",
					aws.ToString(tbl.StorageDescriptor.Location),
				)
				assert.Contains(t, aws.ToString(tbl.StorageDescriptor.InputFormat), "SymlinkTextInputFormat")
				assert.Equal(t, [][]string{{"2024"}, {"2025"}}, env.partitionValues("events"))

				out, err := env.s3.GetObject(t.Context(), &awss3.GetObjectInput{
					Bucket: aws.String("lake"), Key: aws.String("events/_symlink_format_manifest/year=2025/manifest"),
				})
				require.NoError(t, err)

				var buf bytes.Buffer

				_, err = buf.ReadFrom(out.Body)
				require.NoError(t, err)
				assert.Equal(t, "s3://lake/events/year=2025/c.parquet\n", buf.String())

				_, err = env.s3.GetObject(t.Context(), &awss3.GetObjectInput{
					Bucket: aws.String("lake"), Key: aws.String("events/_symlink_format_manifest/year=2023/manifest"),
				})
				require.Error(t, err, "removed file's partition has no manifest")
			},
		},
		{
			name:   "no manifest unless asked",
			target: gluetypes.DeltaTarget{DeltaTables: []string{"s3://lake/events"}},
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()

				tbl := env.table("events")
				assert.Equal(
					t,
					"s3://lake/events/_symlink_format_manifest/",
					aws.ToString(tbl.StorageDescriptor.Location),
				)

				_, err := env.s3.HeadObject(t.Context(), &awss3.HeadObjectInput{
					Bucket: aws.String("lake"), Key: aws.String("events/_symlink_format_manifest/year=2025/manifest"),
				})
				require.Error(t, err)
				assert.Empty(t, env.partitionValues("events"))
			},
		},
		{
			name: "native delta table",
			target: gluetypes.DeltaTarget{
				DeltaTables: []string{"s3://lake/events"}, CreateNativeDeltaTable: aws.Bool(true),
			},
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()

				tbl := env.table("events")
				assert.Equal(t, "DELTA", tbl.Parameters["table_type"])
				assert.Equal(t, "s3://lake/events/", aws.ToString(tbl.StorageDescriptor.Location))
				assert.Equal(t, []string{"year:int"}, colSpec(tbl.PartitionKeys))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			env.put(map[string]string{
				"events/_delta_log/00000000000000000000.json": deltaCommit0(),
				"events/_delta_log/00000000000000000001.json": deltaCommit1(),
				"events/year=2024/b.parquet":                  "x",
			})
			env.create(&gluetypes.CrawlerTargets{DeltaTargets: []gluetypes.DeltaTarget{tt.target}}, nil)
			env.crawl()
			tt.check(t, env)
		})
	}
}

const icebergMeta = `{"format-version":2,"location":"s3://lake/wh/sales","current-schema-id":1,` +
	`"schemas":[{"schema-id":0,"fields":[{"id":1,"name":"old","required":true,"type":"int"}]},` +
	`{"schema-id":1,"fields":[{"id":1,"name":"id","required":true,"type":"long"},` +
	`{"id":2,"name":"ts","required":false,"type":"timestamptz"},` +
	`{"id":3,"name":"attrs","required":false,` +
	`"type":{"type":"map","key-id":4,"key":"string","value-id":5,"value":"double"}},` +
	`{"id":6,"name":"price","required":false,"type":"decimal(10, 2)"}]}]}`

func TestCrawler_IcebergTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		objs      map[string]string
		name      string
		path      string
		wantTable string
		wantMeta  string
		maxDepth  int32
		wantNone  bool
	}{
		{
			name: "picks highest version", path: "s3://lake/wh/", wantTable: "sales",
			wantMeta: "s3://lake/wh/sales/metadata/00002-abc123.metadata.json",
			objs: map[string]string{
				"wh/sales/metadata/00001-def456.metadata.json": `{"schemas":[{"schema-id":0,"fields":[]}],"current-schema-id":0}`,
				"wh/sales/metadata/00002-abc123.metadata.json": icebergMeta,
				"wh/sales/data/part-0.parquet":                 "x",
			},
		},
		{
			name: "version hint wins", path: "s3://lake/wh/", wantTable: "sales",
			wantMeta: "s3://lake/wh/sales/metadata/v1.metadata.json",
			objs: map[string]string{
				"wh/sales/metadata/v1.metadata.json":  icebergMeta,
				"wh/sales/metadata/v2.metadata.json":  `{"schemas":[{"schema-id":0,"fields":[]}],"current-schema-id":0}`,
				"wh/sales/metadata/version-hint.text": "1",
			},
		},
		{
			name: "depth limit", path: "s3://lake/wh/", maxDepth: 1, wantNone: true, wantTable: "sales",
			objs: map[string]string{"wh/a/b/sales/metadata/v1.metadata.json": icebergMeta},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			env.put(tt.objs)
			env.create(&gluetypes.CrawlerTargets{IcebergTargets: []gluetypes.IcebergTarget{{
				Paths: []string{tt.path}, MaximumTraversalDepth: aws.Int32(tt.maxDepth),
			}}}, nil)
			env.crawl()

			if tt.wantNone {
				assert.False(t, env.hasTable(tt.wantTable))

				return
			}

			tbl := env.table(tt.wantTable)
			assert.Equal(t, "ICEBERG", tbl.Parameters["table_type"])
			assert.Equal(t, tt.wantMeta, tbl.Parameters["metadata_location"])
			assert.Equal(t, "s3://lake/wh/sales", aws.ToString(tbl.StorageDescriptor.Location))
			assert.Equal(t,
				[]string{"id:bigint", "ts:timestamp", "attrs:map<string,double>", "price:decimal(10, 2)"},
				colSpec(tbl.StorageDescriptor.Columns))
			assert.Equal(t, "1", tbl.StorageDescriptor.Columns[0].Parameters["iceberg.field.id"])
		})
	}
}

const hudiSchema = `{"type":"record","name":"trips","fields":[` +
	`{"name":"_hoodie_commit_time","type":["null","string"],"default":null},` +
	`{"name":"_hoodie_commit_seqno","type":["null","string"],"default":null},` +
	`{"name":"_hoodie_record_key","type":["null","string"],"default":null},` +
	`{"name":"_hoodie_partition_path","type":["null","string"],"default":null},` +
	`{"name":"_hoodie_file_name","type":["null","string"],"default":null},` +
	`{"name":"uuid","type":"string"},{"name":"fare","type":"double"},` +
	`{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"city","type":"string"}]}`

func TestCrawler_HudiTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tableType  string
		schemaFrom string
		wantTables []string
		wantInputs []string
	}{
		{
			name: "copy on write schema from properties", tableType: "COPY_ON_WRITE", schemaFrom: "props",
			wantTables: []string{"trips"}, wantInputs: []string{"org.apache.hudi.hadoop.HoodieParquetInputFormat"},
		},
		{
			name: "merge on read schema from commit", tableType: "MERGE_ON_READ", schemaFrom: "commit",
			wantTables: []string{"trips_ro", "trips_rt"},
			wantInputs: []string{
				"org.apache.hudi.hadoop.HoodieParquetInputFormat",
				"org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			props := "#Properties\nhoodie.table.name=trips\nhoodie.table.type=" + tt.tableType +
				"\nhoodie.table.partition.fields=city\n"
			objs := map[string]string{
				"hudi/trips/city=sf/f1.parquet": "x",
				"hudi/trips/city=la/f2.parquet": "x",
			}

			if tt.schemaFrom == "props" {
				props += "hoodie.table.create.schema=" + hudiSchema + "\n"
			} else {
				objs["hudi/trips/.hoodie/20240101000000.commit"] =
					`{"extraMetadata":{"schema":` + jsonString(hudiSchema) + `}}`
			}

			objs["hudi/trips/.hoodie/hoodie.properties"] = props
			env.put(objs)
			env.create(&gluetypes.CrawlerTargets{HudiTargets: []gluetypes.HudiTarget{{
				Paths: []string{"s3://lake/hudi/"},
			}}}, nil)
			env.crawl()

			for i, name := range tt.wantTables {
				tbl := env.table(name)
				assert.Equal(t, tt.wantInputs[i], aws.ToString(tbl.StorageDescriptor.InputFormat))
				assert.Equal(t, []string{"city:string"}, colSpec(tbl.PartitionKeys))
				assert.Equal(t, []string{
					"_hoodie_commit_time:string", "_hoodie_commit_seqno:string", "_hoodie_record_key:string",
					"_hoodie_partition_path:string", "_hoodie_file_name:string",
					"uuid:string", "fare:double", "ts:timestamp",
				}, colSpec(tbl.StorageDescriptor.Columns))
				assert.Equal(t, [][]string{{"la"}, {"sf"}}, env.partitionValues(name))
			}
		})
	}
}

func TestCrawler_SchemaChangeAndRecrawlPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check  func(t *testing.T, env *crawlEnv)
		mutate func(env *crawlEnv)
		policy *gluetypes.SchemaChangePolicy
		recr   gluetypes.RecrawlBehavior
		name   string
	}{
		{
			name:   "update in database rewrites schema",
			policy: &gluetypes.SchemaChangePolicy{UpdateBehavior: gluetypes.UpdateBehaviorUpdateInDatabase},
			mutate: func(env *crawlEnv) { env.put(map[string]string{"d/a.csv": "id,name,extra\n1,a,z\n"}) },
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()
				assert.Equal(t, []string{"id:bigint", "name:string", "extra:string"},
					colSpec(env.table("d").StorageDescriptor.Columns))
				assert.Equal(t, int32(1), env.metrics().TablesUpdated)
			},
		},
		{
			name:   "log leaves the table alone",
			policy: &gluetypes.SchemaChangePolicy{UpdateBehavior: gluetypes.UpdateBehaviorLog},
			mutate: func(env *crawlEnv) { env.put(map[string]string{"d/a.csv": "id,name,extra\n1,a,z\n"}) },
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()
				assert.Equal(t, []string{"id:bigint", "name:string"}, colSpec(env.table("d").StorageDescriptor.Columns))
				assert.Equal(t, int32(0), env.metrics().TablesUpdated)
			},
		},
		{
			name: "delete from database drops vanished tables",
			policy: &gluetypes.SchemaChangePolicy{
				UpdateBehavior: gluetypes.UpdateBehaviorUpdateInDatabase,
				DeleteBehavior: gluetypes.DeleteBehaviorDeleteFromDatabase,
			},
			mutate: func(env *crawlEnv) {
				_, err := env.s3.DeleteObject(env.t.Context(), &awss3.DeleteObjectInput{
					Bucket: aws.String("lake"), Key: aws.String("d/a.csv"),
				})
				require.NoError(env.t, err)
			},
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()
				assert.False(t, env.hasTable("d"))
				assert.Equal(t, int32(1), env.metrics().TablesDeleted)
			},
		},
		{
			name:   "deprecate marks vanished tables",
			policy: &gluetypes.SchemaChangePolicy{DeleteBehavior: gluetypes.DeleteBehaviorDeprecateInDatabase},
			mutate: func(env *crawlEnv) {
				_, err := env.s3.DeleteObject(env.t.Context(), &awss3.DeleteObjectInput{
					Bucket: aws.String("lake"), Key: aws.String("d/a.csv"),
				})
				require.NoError(env.t, err)
			},
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()
				assert.Contains(t, env.table("d").Parameters, "DEPRECATED_BY_CRAWLER")
			},
		},
		{
			name: "new folders only keeps schema but adds partitions",
			policy: &gluetypes.SchemaChangePolicy{
				UpdateBehavior: gluetypes.UpdateBehaviorLog,
				DeleteBehavior: gluetypes.DeleteBehaviorLog,
			},
			recr: gluetypes.RecrawlBehaviorCrawlNewFoldersOnly,
			mutate: func(env *crawlEnv) {
				env.put(
					map[string]string{"p/y=2/a.csv": "id,name,extra\n1,a,z\n", "p/y=1/a.csv": "id,name,extra\n1,a,z\n"},
				)
			},
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()
				assert.Equal(t, []string{"id:bigint", "name:string"}, colSpec(env.table("p").StorageDescriptor.Columns))
				assert.Equal(t, [][]string{{"1"}, {"2"}}, env.partitionValues("p"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			env.put(map[string]string{"d/a.csv": "id,name\n1,a\n", "p/y=1/a.csv": "id,name\n1,a\n"})

			path := "s3://lake/d/"
			if tt.recr != "" {
				path = "s3://lake/p/"
			}

			env.create(&gluetypes.CrawlerTargets{S3Targets: []gluetypes.S3Target{{Path: aws.String(path)}}},
				func(in *gluesdk.CreateCrawlerInput) {
					in.SchemaChangePolicy = tt.policy
					if tt.recr != "" {
						in.RecrawlPolicy = &gluetypes.RecrawlPolicy{RecrawlBehavior: tt.recr}
					}
				})
			env.crawl()

			tt.mutate(env)
			env.crawl()
			tt.check(t, env)
		})
	}
}

func TestCrawler_FailedCrawlReportsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		targets *gluetypes.CrawlerTargets
		name    string
		want    string
	}{
		{
			name: "missing bucket",
			targets: &gluetypes.CrawlerTargets{
				S3Targets: []gluetypes.S3Target{{Path: aws.String("s3://nobucket/x/")}},
			},
			want: "nobucket",
		},
		{
			name: "delta table without log",
			targets: &gluetypes.CrawlerTargets{
				DeltaTargets: []gluetypes.DeltaTarget{{DeltaTables: []string{"s3://lake/none"}}},
			},
			want: "no Delta commits",
		},
		{
			name: "jdbc connection missing",
			targets: &gluetypes.CrawlerTargets{
				JdbcTargets: []gluetypes.JdbcTarget{{ConnectionName: aws.String("c"), Path: aws.String("db/%")}},
			},
			want: `connection not found: "c"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			env.create(tt.targets, nil)

			c := env.crawl()
			assert.Equal(t, gluetypes.LastCrawlStatusFailed, c.LastCrawl.Status)
			assert.Contains(t, aws.ToString(c.LastCrawl.ErrorMessage), tt.want)

			last := env.lastCrawl()
			assert.Equal(t, gluetypes.CrawlerHistoryStateFailed, last.State)
			assert.Contains(t, aws.ToString(last.ErrorMessage), tt.want)
		})
	}
}
