package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlueCrawlerInfersSchemaFromS3(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
	gc := glue.NewFromConfig(fx.cfg)
	ctx := t.Context()

	_, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("crawl-lake")})
	require.NoError(t, err)

	objs := map[string]string{
		"sales/region=us/a.csv": "id,amount\n1,10\n2,20\n",
		"sales/region=eu/b.csv": "id,amount\n3,30\n",
		"events/e.json":         `{"user":"u1","clicks":3}` + "\n" + `{"user":"u2","clicks":5}` + "\n",
	}

	for k, v := range objs {
		_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("crawl-lake"), Key: aws.String(k), Body: bytes.NewReader([]byte(v)),
		})
		require.NoError(t, err)
	}

	_, err = gc.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("crawl_db")},
	})
	require.NoError(t, err)

	_, err = gc.CreateCrawler(ctx, &glue.CreateCrawlerInput{
		Name:         aws.String("lake-crawler"),
		Role:         aws.String("arn:aws:iam::000000000000:role/glue"),
		DatabaseName: aws.String("crawl_db"),
		Targets: &gluetypes.CrawlerTargets{
			S3Targets: []gluetypes.S3Target{{Path: aws.String("s3://crawl-lake/")}},
		},
	})
	require.NoError(t, err)

	_, err = gc.StartCrawler(ctx, &glue.StartCrawlerInput{Name: aws.String("lake-crawler")})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, getErr := gc.GetCrawler(ctx, &glue.GetCrawlerInput{Name: aws.String("lake-crawler")})

		return getErr == nil && out.Crawler.State == gluetypes.CrawlerStateReady && out.Crawler.LastCrawl != nil
	}, 10*time.Second, 50*time.Millisecond)

	tables, err := gc.GetTables(ctx, &glue.GetTablesInput{DatabaseName: aws.String("crawl_db")})
	require.NoError(t, err)

	byName := map[string]gluetypes.Table{}
	for _, tbl := range tables.TableList {
		byName[aws.ToString(tbl.Name)] = tbl
	}

	tests := []struct {
		name     string
		wantCols []string
		wantPart []string
	}{
		{name: "sales", wantCols: []string{"id", "amount"}, wantPart: []string{"region"}},
		{name: "events", wantCols: []string{"user", "clicks"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tbl, ok := byName[tt.name]
			require.True(t, ok, "crawler must create table %q", tt.name)

			var cols, parts []string
			for _, c := range tbl.StorageDescriptor.Columns {
				cols = append(cols, aws.ToString(c.Name))
			}

			for _, p := range tbl.PartitionKeys {
				parts = append(parts, aws.ToString(p.Name))
			}

			assert.ElementsMatch(t, tt.wantCols, cols)
			assert.ElementsMatch(t, tt.wantPart, parts)
		})
	}
}
