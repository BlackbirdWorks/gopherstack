package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *crawlEnv) createTable(name, location string, params map[string]string, cols []gluetypes.Column,
	pkeys []gluetypes.Column,
) {
	e.t.Helper()

	_, err := e.client.CreateTable(e.t.Context(), &gluesdk.CreateTableInput{
		DatabaseName: aws.String("db"),
		TableInput: &gluetypes.TableInput{
			Name: aws.String(name), Parameters: params, PartitionKeys: pkeys,
			StorageDescriptor: &gluetypes.StorageDescriptor{Location: aws.String(location), Columns: cols},
		},
	})
	require.NoError(e.t, err)
}

func strCol(n string) gluetypes.Column {
	return gluetypes.Column{Name: aws.String(n), Type: aws.String("string")}
}

func catalogTargets(tables ...string) *gluetypes.CrawlerTargets {
	return &gluetypes.CrawlerTargets{CatalogTargets: []gluetypes.CatalogTarget{{
		DatabaseName: aws.String("db"), Tables: tables,
	}}}
}

func TestCrawler_CatalogTarget(t *testing.T) {
	t.Parallel()

	salesObjs := map[string]string{
		"sales/year=2024/a.csv": "id,name\n1,a\n",
		"sales/year=2025/a.csv": "id,name\n2,b\n",
	}

	tests := []struct {
		check  func(t *testing.T, env *crawlEnv)
		setup  func(env *crawlEnv)
		mut    func(in *gluesdk.CreateCrawlerInput)
		objs   map[string]string
		name   string
		tables []string
	}{
		{
			name: "s3 table schema and partitions", objs: salesObjs, tables: []string{"sales"},
			setup: func(env *crawlEnv) {
				env.createTable(
					"sales",
					"s3://lake/sales/",
					nil,
					[]gluetypes.Column{strCol("old")},
					[]gluetypes.Column{strCol("year")},
				)
			},
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()

				tbl := env.table("sales")
				assert.Equal(t, []string{"id:bigint", "name:string"}, colSpec(tbl.StorageDescriptor.Columns))
				assert.Equal(t, []string{"year:string"}, colSpec(tbl.PartitionKeys))
				assert.Equal(t, "cr", tbl.Parameters["UPDATED_BY_CRAWLER"])
				assert.Equal(t, [][]string{{"2024"}, {"2025"}}, env.partitionValues("sales"))
				assert.Equal(t, int32(1), env.metrics().TablesUpdated)
				assert.Equal(t, int32(0), env.metrics().TablesCreated)
			},
		},
		{
			name: "update behavior log keeps schema but adds partitions", objs: salesObjs, tables: []string{"sales"},
			mut: func(in *gluesdk.CreateCrawlerInput) {
				in.SchemaChangePolicy = &gluetypes.SchemaChangePolicy{
					UpdateBehavior: gluetypes.UpdateBehaviorLog, DeleteBehavior: gluetypes.DeleteBehaviorLog,
				}
			},
			setup: func(env *crawlEnv) {
				env.createTable(
					"sales",
					"s3://lake/sales/",
					nil,
					[]gluetypes.Column{strCol("old")},
					[]gluetypes.Column{strCol("year")},
				)
			},
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()

				assert.Equal(t, []string{"old:string"}, colSpec(env.table("sales").StorageDescriptor.Columns))
				assert.Equal(t, [][]string{{"2024"}, {"2025"}}, env.partitionValues("sales"))
			},
		},
		{
			name: "table without data is kept despite delete behavior", tables: []string{"gone"},
			mut: func(in *gluesdk.CreateCrawlerInput) {
				in.SchemaChangePolicy = &gluetypes.SchemaChangePolicy{
					UpdateBehavior: gluetypes.UpdateBehaviorUpdateInDatabase,
					DeleteBehavior: gluetypes.DeleteBehaviorDeleteFromDatabase,
				}
			},
			setup: func(env *crawlEnv) {
				env.createTable("gone", "s3://lake/gone/", map[string]string{"UPDATED_BY_CRAWLER": "cr"},
					[]gluetypes.Column{strCol("c")}, nil)
			},
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()

				assert.True(t, env.hasTable("gone"))
				assert.Equal(t, int32(0), env.metrics().TablesDeleted)
			},
		},
		{
			name: "delta table", tables: []string{"events"},
			objs: map[string]string{
				"events/_delta_log/00000000000000000000.json": deltaCommit0(),
				"events/_delta_log/00000000000000000001.json": deltaCommit1(),
			},
			setup: func(env *crawlEnv) {
				env.createTable("events", "s3://lake/events/", map[string]string{"table_type": "DELTA"},
					[]gluetypes.Column{strCol("old")}, nil)
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
			},
		},
		{
			name: "dynamodb table", tables: []string{"orders"},
			setup: func(env *crawlEnv) {
				env.seedDynamoDB(env.t.Context())
				env.createTable("orders", "orders", map[string]string{"classification": "dynamodb"},
					[]gluetypes.Column{strCol("old")}, nil)
			},
			check: func(t *testing.T, env *crawlEnv) {
				t.Helper()

				cols := colSpec(env.table("orders").StorageDescriptor.Columns)
				assert.Equal(t, "id:bigint", cols[0])
				assert.Contains(t, cols, "name:string")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			env.put(tt.objs)
			tt.setup(env)
			env.create(catalogTargets(tt.tables...), tt.mut)

			c := env.crawl()
			require.Equal(
				t,
				gluetypes.LastCrawlStatusSucceeded,
				c.LastCrawl.Status,
				aws.ToString(c.LastCrawl.ErrorMessage),
			)
			tt.check(t, env)
		})
	}
}

func TestCrawler_CatalogTargetFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(env *crawlEnv)
		targets *gluetypes.CrawlerTargets
		name    string
		want    string
	}{
		{name: "missing table", targets: catalogTargets("nope"), want: "catalog table not found: db.nope"},
		{
			name: "unsupported store", targets: catalogTargets("pg"), want: "not backed by Amazon S3",
			setup: func(env *crawlEnv) {
				env.createTable("pg", "appdb.public.t", nil, []gluetypes.Column{strCol("c")}, nil)
			},
		},
		{
			name: "missing connection", want: "connection not found",
			targets: &gluetypes.CrawlerTargets{CatalogTargets: []gluetypes.CatalogTarget{{
				DatabaseName: aws.String("db"), Tables: []string{"t"}, ConnectionName: aws.String("net"),
			}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			if tt.setup != nil {
				tt.setup(env)
			}

			env.create(tt.targets, nil)

			c := env.crawl()
			assert.Equal(t, gluetypes.LastCrawlStatusFailed, c.LastCrawl.Status)
			assert.Contains(t, aws.ToString(c.LastCrawl.ErrorMessage), tt.want)
		})
	}
}

func TestCrawler_CatalogTargetValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		targets *gluetypes.CrawlerTargets
		name    string
	}{
		{
			name: "mixed with s3",
			targets: &gluetypes.CrawlerTargets{
				CatalogTargets: []gluetypes.CatalogTarget{{DatabaseName: aws.String("db"), Tables: []string{"t"}}},
				S3Targets:      []gluetypes.S3Target{{Path: aws.String("s3://lake/x")}},
			},
		},
		{
			name: "bad queue arn",
			targets: &gluetypes.CrawlerTargets{CatalogTargets: []gluetypes.CatalogTarget{{
				DatabaseName: aws.String("db"), Tables: []string{"t"}, EventQueueArn: aws.String("not-an-arn"),
			}}},
		},
		{
			name: "dlq without queue",
			targets: &gluetypes.CrawlerTargets{CatalogTargets: []gluetypes.CatalogTarget{{
				DatabaseName: aws.String("db"), Tables: []string{"t"},
				DlqEventQueueArn: aws.String("arn:aws:sqs:us-east-1:000000000000:dlq"),
			}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			_, err := env.client.CreateCrawler(t.Context(), &gluesdk.CreateCrawlerInput{
				Name: aws.String(
					crawlerName,
				),
				Role:         aws.String(crawlRole),
				DatabaseName: aws.String("db"),
				Targets:      tt.targets,
			})

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())
		})
	}
}

func TestCrawler_CatalogTargetRoundTrip(t *testing.T) {
	t.Parallel()

	env := newCrawlEnv(t)
	env.create(&gluetypes.CrawlerTargets{CatalogTargets: []gluetypes.CatalogTarget{{
		DatabaseName: aws.String("db"), Tables: []string{"t"}, ConnectionName: aws.String("net"),
		EventQueueArn:    aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
		DlqEventQueueArn: aws.String("arn:aws:sqs:us-east-1:000000000000:dlq"),
	}}}, nil)

	out, err := env.client.GetCrawler(t.Context(), &gluesdk.GetCrawlerInput{Name: aws.String(crawlerName)})
	require.NoError(t, err)
	require.Len(t, out.Crawler.Targets.CatalogTargets, 1)

	ct := out.Crawler.Targets.CatalogTargets[0]
	assert.Equal(t, "net", aws.ToString(ct.ConnectionName))
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:q", aws.ToString(ct.EventQueueArn))
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:dlq", aws.ToString(ct.DlqEventQueueArn))
}
