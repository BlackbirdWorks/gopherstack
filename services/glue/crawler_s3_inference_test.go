package glue_test

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

func putCrawlObjects(ctx context.Context, t *testing.T, objs map[string]string) *s3backend.InMemoryBackend {
	t.Helper()

	s3b := s3backend.NewInMemoryBackend(nil)
	_, err := s3b.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("lake")})
	require.NoError(t, err)

	for k, v := range objs {
		_, err = s3b.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("lake"), Key: aws.String(k), Body: bytes.NewReader([]byte(v)),
		})
		require.NoError(t, err)
	}

	return s3b
}

func runCrawl(t *testing.T, b *glue.InMemoryBackend, name string) {
	t.Helper()

	require.NoError(t, b.StartCrawler(name))
	time.Sleep(300 * time.Millisecond)

	c, err := b.GetCrawler(name)
	require.NoError(t, err)
	require.Equal(t, "READY", c.State)
}

func TestCrawler_InfersTablesFromS3(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(t *testing.T, b *glue.InMemoryBackend)
		objs    map[string]string
		name    string
		path    string
		prefix  string
		wantCnt int
	}{
		{
			name: "csv header hive partitions",
			path: "s3://lake/sales/", prefix: "p_",
			objs: map[string]string{
				"sales/year=2023/a.csv": "id,Name,price\n1,a,1.5\n2,b,2\n",
				"sales/year=2024/a.csv": "id,Name,price\n3,c,3\n",
			},
			wantCnt: 1,
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()

				tbl, err := b.GetTable("db", "p_sales")
				require.NoError(t, err)
				assert.Equal(
					t,
					[]glue.Column{
						{Name: "id", Type: "bigint"},
						{Name: "name", Type: "string"},
						{Name: "price", Type: "double"},
					},
					tbl.StorageDescriptor.Columns,
				)
				assert.Equal(t, []glue.Column{{Name: "year", Type: "bigint"}}, tbl.PartitionKeys)
				assert.Equal(t, "s3://lake/sales/", tbl.StorageDescriptor.Location)
				assert.Equal(t, "csv", tbl.Parameters["classification"])
				assert.Equal(t, "1", tbl.Parameters["skip.header.line.count"])

				parts, err := b.GetPartitions("db", "p_sales")
				require.NoError(t, err)
				require.Len(t, parts, 2)
				assert.Equal(t, []string{"2023"}, parts[0].Values)
				assert.Equal(t, "s3://lake/sales/year=2023/", parts[0].StorageDescriptor.Location)
			},
		},
		{
			name:    "csv all strings no header",
			path:    "s3://lake/raw",
			objs:    map[string]string{"raw/x.csv": "a,b\nc,d\n"},
			wantCnt: 1,
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()

				tbl, err := b.GetTable("db", "raw")
				require.NoError(t, err)
				assert.Equal(
					t,
					[]glue.Column{{Name: "col0", Type: "string"}, {Name: "col1", Type: "string"}},
					tbl.StorageDescriptor.Columns,
				)
				assert.Empty(t, tbl.PartitionKeys)
			},
		},
		{
			name: "json lines",
			path: "s3://lake/ev/",
			objs: map[string]string{
				"ev/e.json": "{\"id\":1,\"ok\":true,\"tags\":[\"a\"],\"u\":{\"n\":1}}\n{\"id\":2,\"ok\":false,\"extra\":1.5}\n",
			},
			wantCnt: 1,
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()

				tbl, err := b.GetTable("db", "ev")
				require.NoError(t, err)
				assert.Equal(t, []glue.Column{
					{Name: "id", Type: "bigint"}, {Name: "ok", Type: "boolean"}, {Name: "tags", Type: "array<string>"},
					{Name: "u", Type: "struct<n:bigint>"}, {Name: "extra", Type: "double"},
				}, tbl.StorageDescriptor.Columns)
				assert.Equal(t, "json", tbl.Parameters["classification"])
			},
		},
		{
			name:    "table per subfolder",
			path:    "s3://lake/",
			objs:    map[string]string{"orders/o.csv": "1,x\n", "users/u.csv": "2,y\n"},
			wantCnt: 2,
			check: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()

				for _, n := range []string{"orders", "users"} {
					tbl, err := b.GetTable("db", n)
					require.NoError(t, err)
					assert.Equal(t, "s3://lake/"+n+"/", tbl.StorageDescriptor.Location)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := glue.NewInMemoryBackend("000000000000", "us-east-1")
				t.Cleanup(b.Close)
				b.SetS3Backend(putCrawlObjects(t.Context(), t, tt.objs))

				_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
				require.NoError(t, err)
				_, err = b.CreateCrawlerWithOptions(
					"cr",
					"arn:aws:iam::000000000000:role/g",
					"db",
					glue.CrawlerTarget{
						S3Targets: []glue.S3Target{{Path: tt.path}},
					},
					nil,
					glue.CrawlerOptions{TablePrefix: tt.prefix},
				)
				require.NoError(t, err)

				runCrawl(t, b, "cr")
				tt.check(t, b)
				assert.Len(t, mustGetTables(t, b), tt.wantCnt)
			})
		})
	}
}

func mustGetTables(t *testing.T, b *glue.InMemoryBackend) []*glue.Table {
	t.Helper()

	tbls, err := b.GetTables("db")
	require.NoError(t, err)

	return tbls
}

func TestCrawler_RecrawlAndManualTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		manual       bool
		wantPartsAft int
		wantCols     int
	}{
		{name: "crawler managed updates", wantPartsAft: 2, wantCols: 2},
		{name: "manual table untouched", manual: true, wantPartsAft: 0, wantCols: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				s3b := putCrawlObjects(t.Context(), t, map[string]string{"d/y=1/a.csv": "id,v\n1,2\n"})
				b := glue.NewInMemoryBackend("000000000000", "us-east-1")
				t.Cleanup(b.Close)
				b.SetS3Backend(s3b)

				_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
				require.NoError(t, err)

				if tt.manual {
					_, err = b.CreateTable("db", glue.TableInput{
						Name: "d",
						StorageDescriptor: glue.StorageDescriptor{
							Columns: []glue.Column{{Name: "only", Type: "string"}},
						},
					})
					require.NoError(t, err)
				}

				_, err = b.CreateCrawler("cr", "arn:aws:iam::000000000000:role/g", "db",
					glue.CrawlerTarget{S3Targets: []glue.S3Target{{Path: "s3://lake/d"}}}, nil)
				require.NoError(t, err)
				runCrawl(t, b, "cr")

				_, err = s3b.PutObject(t.Context(), &awss3.PutObjectInput{
					Bucket: aws.String(
						"lake",
					),
					Key:  aws.String("d/y=2/a.csv"),
					Body: bytes.NewReader([]byte("id,v\n3,4\n")),
				})
				require.NoError(t, err)
				runCrawl(t, b, "cr")

				tbl, err := b.GetTable("db", "d")
				require.NoError(t, err)
				assert.Len(t, tbl.StorageDescriptor.Columns, tt.wantCols)

				parts, err := b.GetPartitions("db", "d")
				require.NoError(t, err)
				assert.Len(t, parts, tt.wantPartsAft)
			})
		})
	}
}
