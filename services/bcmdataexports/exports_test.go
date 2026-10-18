package bcmdataexports_test

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bcmsdk "github.com/aws/aws-sdk-go-v2/service/bcmdataexports"
	bcmtypes "github.com/aws/aws-sdk-go-v2/service/bcmdataexports/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const curQuery = "SELECT line_item_product_code, line_item_unblended_cost FROM COST_AND_USAGE_REPORT"

func TestCreateExport_Validation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		mutate func(e *bcmtypes.Export)
		name   string
		code   string
	}{
		{func(*bcmtypes.Export) {}, "valid", ""},
		{func(e *bcmtypes.Export) { e.Name = aws.String("bad name!") }, "bad_name", "ValidationException"},
		{
			func(e *bcmtypes.Export) { e.DataQuery.QueryStatement = aws.String("DELETE FROM x") },
			"not_select",
			"ValidationException",
		},
		{func(e *bcmtypes.Export) {
			e.DataQuery.QueryStatement = aws.String("SELECT nope FROM COST_AND_USAGE_REPORT")
		}, "bad_column", "ValidationException"},
		{func(e *bcmtypes.Export) {
			e.DataQuery.QueryStatement = aws.String("SELECT * FROM NO_SUCH_TABLE")
		}, "bad_table", "ValidationException"},
		{func(e *bcmtypes.Export) {
			e.DataQuery.QueryStatement = aws.String("SELECT * FROM COST_AND_USAGE_REPORT WHERE nope = 'x'")
		}, "bad_where_column", "ValidationException"},
		{func(e *bcmtypes.Export) {
			e.DataQuery.TableConfigurations = map[string]map[string]string{"COST_AND_USAGE_REPORT": {"BOGUS": "1"}}
		}, "bad_property", "ValidationException"},
		{func(e *bcmtypes.Export) {
			e.DataQuery.TableConfigurations = map[string]map[string]string{
				"COST_AND_USAGE_REPORT": {"TIME_GRANULARITY": "WEEKLY"},
			}
		}, "bad_property_value", "ValidationException"},
		{func(e *bcmtypes.Export) {
			e.DestinationConfigurations.S3Destination.S3OutputConfigurations.Format = bcmtypes.FormatOptionParquet
		}, "parquet_gzip", "ValidationException"},
		{func(e *bcmtypes.Export) {
			c := e.DestinationConfigurations.S3Destination.S3OutputConfigurations
			c.Format, c.Compression = bcmtypes.FormatOptionParquet, bcmtypes.CompressionOptionParquet
		}, "parquet_ok", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			e := export("e1", curQuery)
			tc.mutate(e)

			_, err := f.client.CreateExport(t.Context(), &bcmsdk.CreateExportInput{Export: e})
			assert.Equal(t, tc.code, apiCode(err))
		})
	}
}

func TestExportLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	out, err := f.client.CreateExport(ctx, &bcmsdk.CreateExportInput{
		Export:       export("e1", curQuery),
		ResourceTags: []bcmtypes.ResourceTag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	require.NoError(t, err)

	exportArn := aws.ToString(out.ExportArn)
	assert.Contains(t, exportArn, ":bcm-data-exports:us-east-1:"+testAccount+":export/e1-")

	_, err = f.client.CreateExport(ctx, &bcmsdk.CreateExportInput{Export: export("e1", curQuery)})
	assert.Equal(t, "ValidationException", apiCode(err))

	got, err := f.client.GetExport(ctx, &bcmsdk.GetExportInput{ExportArn: out.ExportArn})
	require.NoError(t, err)
	assert.Equal(t, "e1", aws.ToString(got.Export.Name))
	assert.Equal(t, exportArn, aws.ToString(got.Export.ExportArn))
	assert.Equal(t, bcmtypes.ExportStatusCodeHealthy, got.ExportStatus.StatusCode)
	assert.NotNil(t, got.ExportStatus.CreatedAt)

	upd := export("e1", "SELECT * FROM COST_AND_USAGE_REPORT")
	_, err = f.client.UpdateExport(ctx, &bcmsdk.UpdateExportInput{ExportArn: out.ExportArn, Export: upd})
	require.NoError(t, err)

	_, err = f.client.UpdateExport(ctx, &bcmsdk.UpdateExportInput{ExportArn: aws.String("arn:nope"), Export: upd})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	execs, err := f.client.ListExecutions(ctx, &bcmsdk.ListExecutionsInput{ExportArn: out.ExportArn})
	require.NoError(t, err)
	assert.Len(t, execs.Executions, 2)

	_, err = f.client.TagResource(ctx, &bcmsdk.TagResourceInput{
		ResourceArn:  out.ExportArn,
		ResourceTags: []bcmtypes.ResourceTag{{Key: aws.String("a"), Value: aws.String("b")}},
	})
	require.NoError(t, err)
	_, err = f.client.UntagResource(
		ctx,
		&bcmsdk.UntagResourceInput{ResourceArn: out.ExportArn, ResourceTagKeys: []string{"env"}},
	)
	require.NoError(t, err)

	tags, err := f.client.ListTagsForResource(ctx, &bcmsdk.ListTagsForResourceInput{ResourceArn: out.ExportArn})
	require.NoError(t, err)
	require.Len(t, tags.ResourceTags, 1)
	assert.Equal(t, "a", aws.ToString(tags.ResourceTags[0].Key))

	del, err := f.client.DeleteExport(ctx, &bcmsdk.DeleteExportInput{ExportArn: out.ExportArn})
	require.NoError(t, err)
	assert.Equal(t, exportArn, aws.ToString(del.ExportArn))

	_, err = f.client.GetExport(ctx, &bcmsdk.GetExportInput{ExportArn: out.ExportArn})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.ListExecutions(ctx, &bcmsdk.ListExecutionsInput{ExportArn: out.ExportArn})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestExportPagination(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	for i := range 5 {
		f.create(t, export(fmt.Sprintf("e%d", i), curQuery))
	}

	p1, err := f.client.ListExports(t.Context(), &bcmsdk.ListExportsInput{MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, p1.Exports, 2)
	require.NotNil(t, p1.NextToken)
	assert.Equal(t, "e0", aws.ToString(p1.Exports[0].ExportName))

	seen := len(p1.Exports)
	tok := p1.NextToken

	for tok != nil {
		p, perr := f.client.ListExports(t.Context(), &bcmsdk.ListExportsInput{MaxResults: aws.Int32(2), NextToken: tok})
		require.NoError(t, perr)

		seen += len(p.Exports)
		tok = p.NextToken
	}

	assert.Equal(t, 5, seen)

	_, err = f.client.ListExports(t.Context(), &bcmsdk.ListExportsInput{MaxResults: aws.Int32(0)})
	assert.Equal(t, "ValidationException", apiCode(err))
}

func TestExecutionLifecycleAndDelivery(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	e := export("e1", curQuery+" WHERE line_item_product_code = 'AWSLambda'")
	e.DataQuery.TableConfigurations = map[string]map[string]string{
		"COST_AND_USAGE_REPORT": {"TIME_GRANULARITY": "DAILY"},
	}
	arn := f.create(t, e)

	execs, err := f.client.ListExecutions(ctx, &bcmsdk.ListExecutionsInput{ExportArn: aws.String(arn)})
	require.NoError(t, err)
	require.Len(t, execs.Executions, 1)

	id := execs.Executions[0].ExecutionId
	want := []bcmtypes.ExecutionStatusCode{
		bcmtypes.ExecutionStatusCodeInitiationInProcess,
		bcmtypes.ExecutionStatusCodeQueryQueued,
		bcmtypes.ExecutionStatusCodeQueryInProcess,
		bcmtypes.ExecutionStatusCodeDeliveryInProcess,
		bcmtypes.ExecutionStatusCodeDeliverySuccess,
	}

	for i, code := range want {
		got, gerr := f.client.GetExecution(ctx, &bcmsdk.GetExecutionInput{ExportArn: aws.String(arn), ExecutionId: id})
		require.NoError(t, gerr)
		assert.Equal(t, code, got.ExecutionStatus.StatusCode, "step %d", i)

		f.backend.ProcessExecutions(ctx)
	}

	got, err := f.client.GetExecution(ctx, &bcmsdk.GetExecutionInput{ExportArn: aws.String(arn), ExecutionId: id})
	require.NoError(t, err)
	assert.NotNil(t, got.ExecutionStatus.CompletedAt)
	assert.Equal(t, "e1", aws.ToString(got.Export.Name))

	var dataKey string

	for _, k := range f.keys(t) {
		if strings.HasSuffix(k, ".csv.gz") {
			dataKey = k
		}
	}

	require.Equal(t, "out/e1/data/BILLING_PERIOD=2026-03/e1-00001.csv.gz", dataKey)

	zr, err := gzip.NewReader(bytes.NewReader(f.object(t, dataKey)))
	require.NoError(t, err)

	rows, err := csv.NewReader(zr).ReadAll()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"line_item_product_code", "line_item_unblended_cost"},
		{"AWSLambda", "12"},
	}, rows)

	_, err = f.client.GetExecution(
		ctx,
		&bcmsdk.GetExecutionInput{ExportArn: aws.String(arn), ExecutionId: aws.String("nope")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestDelivery_QueryVariants(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		query string
		table string
		gran  string
		rows  int
	}{
		{"star_daily", "SELECT * FROM COST_AND_USAGE_REPORT", "COST_AND_USAGE_REPORT", "DAILY", 2},
		{"hourly", "SELECT line_item_usage_start_date FROM COST_AND_USAGE_REPORT", "", "", 48},
		{"monthly", "SELECT line_item_product_code FROM COST_AND_USAGE_REPORT", "COST_AND_USAGE_REPORT", "MONTHLY", 2},
		{"limit", "SELECT line_item_product_code FROM COST_AND_USAGE_REPORT LIMIT 3", "", "", 3},
		{
			"or_where",
			"SELECT line_item_product_code FROM COST_AND_USAGE_REPORT " +
				"WHERE line_item_product_code = 'AWSLambda' OR line_item_product_code = 'AmazonS3'",
			"COST_AND_USAGE_REPORT",
			"DAILY",
			2,
		},
		{
			"numeric_where",
			"SELECT line_item_unblended_cost FROM COST_AND_USAGE_REPORT WHERE line_item_unblended_cost > 20",
			"COST_AND_USAGE_REPORT",
			"DAILY",
			1,
		},
		{
			"like",
			"SELECT line_item_product_code FROM COST_AND_USAGE_REPORT WHERE line_item_product_code LIKE 'Amazon%'",
			"COST_AND_USAGE_REPORT",
			"DAILY",
			1,
		},
		{"focus", "SELECT ServiceName, BilledCost FROM FOCUS_1_0_AWS", "FOCUS_1_0_AWS", "DAILY", 2},
		{"carbon_empty", "SELECT product_code FROM CARBON_EMISSIONS", "", "", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			e := export("e1", tc.query)
			if tc.gran != "" {
				e.DataQuery.TableConfigurations = map[string]map[string]string{tc.table: {"TIME_GRANULARITY": tc.gran}}
			}
			f.create(t, e)
			f.settle(t)

			var dataKey string

			for _, k := range f.keys(t) {
				if strings.HasSuffix(k, ".csv.gz") {
					dataKey = k
				}
			}

			require.NotEmpty(t, dataKey, "keys: %v", f.keys(t))

			zr, err := gzip.NewReader(bytes.NewReader(f.object(t, dataKey)))
			require.NoError(t, err)

			rows, err := csv.NewReader(zr).ReadAll()
			require.NoError(t, err)
			assert.Len(t, rows, tc.rows+1)
		})
	}
}

func TestDelivery_ParquetAndNewReport(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	e := export("pq", curQuery)
	c := e.DestinationConfigurations.S3Destination.S3OutputConfigurations
	c.Format, c.Compression = bcmtypes.FormatOptionParquet, bcmtypes.CompressionOptionParquet
	c.Overwrite = bcmtypes.OverwriteOptionCreateNewReport
	f.create(t, e)
	f.settle(t)

	found := false

	for _, k := range f.keys(t) {
		if strings.HasSuffix(k, ".snappy.parquet") {
			found = true

			assert.Contains(t, k, "out/pq/data/BILLING_PERIOD=2026-03/20260315T103000Z-")
			assert.True(t, bytes.HasPrefix(f.object(t, k), []byte("PAR1")))
		}
	}

	assert.True(t, found, "keys: %v", f.keys(t))
}

func TestDelivery_Failures(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	e := export("bad", curQuery)
	e.DestinationConfigurations.S3Destination.S3Bucket = aws.String("missing-bucket")
	arn := f.create(t, e)
	f.settle(t)

	execs, err := f.client.ListExecutions(t.Context(), &bcmsdk.ListExecutionsInput{ExportArn: aws.String(arn)})
	require.NoError(t, err)
	require.Len(t, execs.Executions, 1)
	assert.Equal(t, bcmtypes.ExecutionStatusCodeDeliveryFailure, execs.Executions[0].ExecutionStatus.StatusCode)

	got, err := f.client.GetExport(t.Context(), &bcmsdk.GetExportInput{ExportArn: aws.String(arn)})
	require.NoError(t, err)
	assert.Equal(t, bcmtypes.ExportStatusCodeUnhealthy, got.ExportStatus.StatusCode)
}
