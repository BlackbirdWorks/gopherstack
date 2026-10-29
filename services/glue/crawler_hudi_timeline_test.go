package glue_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const hudiCommitAvroSchema = `{"type":"record","name":"HoodieCommitMetadata",` +
	`"namespace":"org.apache.hudi.avro.model",` +
	`"fields":[{"name":"extraMetadata","type":{"type":"map","values":"string"}},` +
	`{"name":"version","type":"int"}]}`

func hudiAvroCommit(t *testing.T, schema string) string {
	t.Helper()

	var buf bytes.Buffer

	enc, err := ocf.NewEncoder(hudiCommitAvroSchema, &buf)
	require.NoError(t, err)
	require.NoError(t, enc.Encode(map[string]any{
		"extraMetadata": map[string]any{"schema": schema}, "version": int32(1),
	}))
	require.NoError(t, enc.Close())

	return buf.String()
}

func TestCrawler_HudiTimelineLayouts(t *testing.T) {
	t.Parallel()

	const staleSchema = `{"type":"record","name":"trips","fields":[{"name":"old","type":"string"}]}`

	tests := []struct {
		objs     func(t *testing.T) map[string]string
		name     string
		wantCols []string
	}{
		{
			name: "v8 avro commit in timeline dir picks latest",
			objs: func(t *testing.T) map[string]string {
				t.Helper()

				return map[string]string{
					"hudi/trips/.hoodie/timeline/20240101000000000_20240101000500000.commit": hudiAvroCommit(
						t,
						staleSchema,
					),
					"hudi/trips/.hoodie/timeline/20240102000000000_20240102000500000.commit": hudiAvroCommit(
						t,
						hudiSchema,
					),
					"hudi/trips/.hoodie/timeline/20240103000000000.commit.requested": "",
				}
			},
			wantCols: []string{"uuid:string", "fare:double", "ts:timestamp"},
		},
		{
			name: "v8 json commit in timeline dir",
			objs: func(t *testing.T) map[string]string {
				t.Helper()

				return map[string]string{
					"hudi/trips/.hoodie/timeline/20240102000000000_20240102000500000.commit": `{"extraMetadata":{"schema":` +
						jsonString(
							hudiSchema,
						) + `}}`,
				}
			},
			wantCols: []string{"uuid:string", "fare:double", "ts:timestamp"},
		},
		{
			name: "v8 deltacommit",
			objs: func(t *testing.T) map[string]string {
				t.Helper()

				return map[string]string{
					"hudi/trips/.hoodie/timeline/20240102000000000_20240102000500000.deltacommit": hudiAvroCommit(
						t,
						hudiSchema,
					),
				}
			},
			wantCols: []string{"uuid:string", "fare:double", "ts:timestamp"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			objs := tt.objs(t)
			objs["hudi/trips/.hoodie/hoodie.properties"] = "hoodie.table.name=trips\nhoodie.table.version=8\n" +
				"hoodie.table.type=COPY_ON_WRITE\nhoodie.table.partition.fields=city\n"
			objs["hudi/trips/city=sf/f1.parquet"] = "x"
			env.put(objs)
			env.create(&gluetypes.CrawlerTargets{HudiTargets: []gluetypes.HudiTarget{{
				Paths: []string{"s3://lake/hudi/"},
			}}}, nil)

			c := env.crawl()
			require.Equal(
				t,
				gluetypes.LastCrawlStatusSucceeded,
				c.LastCrawl.Status,
				aws.ToString(c.LastCrawl.ErrorMessage),
			)

			tbl := env.table("trips")
			assert.Equal(t, tt.wantCols, colSpec(tbl.StorageDescriptor.Columns)[5:])
			assert.Equal(t, [][]string{{"sf"}}, env.partitionValues("trips"))
		})
	}
}
