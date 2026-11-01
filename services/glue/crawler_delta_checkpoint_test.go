package glue_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cpAdd struct {
	PartitionValues map[string]string `parquet:"partitionValues"`
	Path            string            `parquet:"path"`
	Size            int64             `parquet:"size"`
}

type cpMeta struct {
	Configuration    map[string]string `parquet:"configuration"`
	SchemaString     string            `parquet:"schemaString"`
	PartitionColumns []string          `parquet:"partitionColumns"`
}

type cpProto struct {
	MinReaderVersion int32 `parquet:"minReaderVersion"`
}

type cpRow struct {
	Add      *cpAdd   `parquet:"add,optional"`
	MetaData *cpMeta  `parquet:"metaData,optional"`
	Protocol *cpProto `parquet:"protocol,optional"`
}

func deltaCheckpoint(t *testing.T) string {
	t.Helper()

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[cpRow](&buf)
	_, err := w.Write([]cpRow{
		{Protocol: &cpProto{MinReaderVersion: 1}},
		{MetaData: &cpMeta{
			SchemaString: deltaSchema, PartitionColumns: []string{"year"}, Configuration: map[string]string{},
		}},
		{Add: &cpAdd{Path: "year=2023/a.parquet", Size: 1, PartitionValues: map[string]string{"year": "2023"}}},
		{Add: &cpAdd{Path: "year=2024/b.parquet", Size: 1, PartitionValues: map[string]string{"year": "2024"}}},
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.String()
}

func TestCrawler_DeltaCheckpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		objs     func(t *testing.T) map[string]string
		name     string
		wantPart [][]string
	}{
		{
			name: "checkpoint pointer then later commit",
			objs: func(t *testing.T) map[string]string {
				t.Helper()

				return map[string]string{
					"events/_delta_log/00000000000000000010.checkpoint.parquet": deltaCheckpoint(t),
					"events/_delta_log/_last_checkpoint":                        `{"version":10,"size":4}`,
					"events/_delta_log/00000000000000000011.json":               deltaCommit1(),
				}
			},
			wantPart: [][]string{{"2024"}, {"2025"}},
		},
		{
			name: "checkpoint without pointer",
			objs: func(t *testing.T) map[string]string {
				t.Helper()

				return map[string]string{
					"events/_delta_log/00000000000000000010.checkpoint.parquet": deltaCheckpoint(t),
				}
			},
			wantPart: [][]string{{"2023"}, {"2024"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCrawlEnv(t)
			env.put(tt.objs(t))
			env.create(&gluetypes.CrawlerTargets{DeltaTargets: []gluetypes.DeltaTarget{{
				DeltaTables: []string{"s3://lake/events"}, WriteManifest: aws.Bool(true),
			}}}, nil)

			c := env.crawl()
			require.Equal(
				t,
				gluetypes.LastCrawlStatusSucceeded,
				c.LastCrawl.Status,
				aws.ToString(c.LastCrawl.ErrorMessage),
			)

			tbl := env.table("events")
			assert.Equal(
				t,
				[]string{"id:bigint", "name:string", "tags:array<string>"},
				colSpec(tbl.StorageDescriptor.Columns),
			)
			assert.Equal(t, []string{"year:int"}, colSpec(tbl.PartitionKeys))
			assert.Equal(t, tt.wantPart, env.partitionValues("events"))
		})
	}
}
