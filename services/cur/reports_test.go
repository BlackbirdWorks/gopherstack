package cur_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cursdk "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice"
	curtypes "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutReportDefinition_Validation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		mutate func(d *curtypes.ReportDefinition)
		name   string
		code   string
	}{
		{func(*curtypes.ReportDefinition) {}, "valid", ""},
		{
			func(d *curtypes.ReportDefinition) { d.ReportName = aws.String("bad name") },
			"bad_name",
			"ValidationException",
		},
		{func(d *curtypes.ReportDefinition) { d.S3Region = "mars-1" }, "bad_region", "ValidationException"},
		{func(d *curtypes.ReportDefinition) { d.TimeUnit = "WEEKLY" }, "bad_time_unit", "ValidationException"},
		{func(d *curtypes.ReportDefinition) { d.S3Bucket = aws.String("missing") }, "no_bucket", "ValidationException"},
		{
			func(d *curtypes.ReportDefinition) { d.Compression = curtypes.CompressionFormatParquet },
			"csv_parquet",
			"ValidationException",
		},
		{
			func(d *curtypes.ReportDefinition) { d.Format = curtypes.ReportFormatParquet },
			"parquet_gzip",
			"ValidationException",
		},
		{func(d *curtypes.ReportDefinition) {
			d.AdditionalArtifacts = []curtypes.AdditionalArtifact{curtypes.AdditionalArtifactAthena}
		}, "athena_csv", "ValidationException"},
		{func(d *curtypes.ReportDefinition) {
			d.Format, d.Compression = curtypes.ReportFormatParquet, curtypes.CompressionFormatParquet
			d.AdditionalArtifacts = []curtypes.AdditionalArtifact{curtypes.AdditionalArtifactAthena}
		}, "athena_needs_overwrite", "ValidationException"},
		{func(d *curtypes.ReportDefinition) {
			d.Format, d.Compression = curtypes.ReportFormatParquet, curtypes.CompressionFormatParquet
			d.ReportVersioning = curtypes.ReportVersioningOverwriteReport
			d.AdditionalArtifacts = []curtypes.AdditionalArtifact{curtypes.AdditionalArtifactAthena}
		}, "athena_ok", ""},
		{func(d *curtypes.ReportDefinition) {
			d.Compression = curtypes.CompressionFormatZip
			d.AdditionalArtifacts = []curtypes.AdditionalArtifact{curtypes.AdditionalArtifactRedshift}
		}, "redshift_zip", "ValidationException"},
		{func(d *curtypes.ReportDefinition) {
			d.AdditionalArtifacts = []curtypes.AdditionalArtifact{
				curtypes.AdditionalArtifactRedshift, curtypes.AdditionalArtifactQuicksight,
			}
		}, "redshift_quicksight", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.bucket(t)

			d := def("r1")
			tc.mutate(d)

			_, err := f.client.PutReportDefinition(t.Context(), &cursdk.PutReportDefinitionInput{ReportDefinition: d})
			assert.Equal(t, tc.code, apiCode(err))
		})
	}
}

func TestReportLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t)
	ctx := t.Context()

	_, err := f.client.PutReportDefinition(ctx, &cursdk.PutReportDefinitionInput{
		ReportDefinition: def("r1"),
		Tags:             []curtypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	require.NoError(t, err)

	_, err = f.client.PutReportDefinition(ctx, &cursdk.PutReportDefinitionInput{ReportDefinition: def("r1")})
	assert.Equal(t, "DuplicateReportNameException", apiCode(err))

	out, err := f.client.DescribeReportDefinitions(ctx, &cursdk.DescribeReportDefinitionsInput{})
	require.NoError(t, err)
	require.Len(t, out.ReportDefinitions, 1)
	got := out.ReportDefinitions[0]
	assert.Equal(t, "r1", aws.ToString(got.ReportName))
	assert.Equal(t, curtypes.ReportVersioningCreateNewReport, got.ReportVersioning)
	assert.True(t, aws.ToBool(got.RefreshClosedReports))
	assert.Nil(t, got.ReportStatus)

	mod := def("r1")
	mod.TimeUnit = curtypes.TimeUnitMonthly
	_, err = f.client.ModifyReportDefinition(ctx, &cursdk.ModifyReportDefinitionInput{
		ReportName: aws.String("r1"), ReportDefinition: mod,
	})
	require.NoError(t, err)

	_, err = f.client.ModifyReportDefinition(ctx, &cursdk.ModifyReportDefinitionInput{
		ReportName: aws.String("nope"), ReportDefinition: def("nope"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	out, err = f.client.DescribeReportDefinitions(ctx, &cursdk.DescribeReportDefinitionsInput{})
	require.NoError(t, err)
	assert.Equal(t, curtypes.TimeUnitMonthly, out.ReportDefinitions[0].TimeUnit)

	_, err = f.client.TagResource(ctx, &cursdk.TagResourceInput{
		ReportName: aws.String("r1"), Tags: []curtypes.Tag{{Key: aws.String("a"), Value: aws.String("b")}},
	})
	require.NoError(t, err)
	_, err = f.client.UntagResource(
		ctx,
		&cursdk.UntagResourceInput{ReportName: aws.String("r1"), TagKeys: []string{"env"}},
	)
	require.NoError(t, err)

	tags, err := f.client.ListTagsForResource(ctx, &cursdk.ListTagsForResourceInput{ReportName: aws.String("r1")})
	require.NoError(t, err)
	require.Len(t, tags.Tags, 1)
	assert.Equal(t, "a", aws.ToString(tags.Tags[0].Key))

	_, err = f.client.ListTagsForResource(ctx, &cursdk.ListTagsForResourceInput{ReportName: aws.String("nope")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	del, err := f.client.DeleteReportDefinition(ctx, &cursdk.DeleteReportDefinitionInput{ReportName: aws.String("r1")})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(del.ResponseMessage))

	_, err = f.client.DeleteReportDefinition(ctx, &cursdk.DeleteReportDefinitionInput{ReportName: aws.String("r1")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestDescribeReportDefinitions_Pagination(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t)
	ctx := t.Context()

	for i := range 7 {
		_, err := f.client.PutReportDefinition(ctx, &cursdk.PutReportDefinitionInput{
			ReportDefinition: def(fmt.Sprintf("r%d", i)),
		})
		require.NoError(t, err)
	}

	p1, err := f.client.DescribeReportDefinitions(ctx, &cursdk.DescribeReportDefinitionsInput{MaxResults: aws.Int32(5)})
	require.NoError(t, err)
	require.Len(t, p1.ReportDefinitions, 5)
	require.NotNil(t, p1.NextToken)

	p2, err := f.client.DescribeReportDefinitions(ctx, &cursdk.DescribeReportDefinitionsInput{NextToken: p1.NextToken})
	require.NoError(t, err)
	assert.Len(t, p2.ReportDefinitions, 2)
	assert.Nil(t, p2.NextToken)

	_, err = f.client.DescribeReportDefinitions(ctx, &cursdk.DescribeReportDefinitionsInput{MaxResults: aws.Int32(6)})
	assert.Equal(t, "ValidationException", apiCode(err))
}

func TestReportLimit(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t)

	for i := range 10 {
		_, err := f.client.PutReportDefinition(t.Context(), &cursdk.PutReportDefinitionInput{
			ReportDefinition: def(fmt.Sprintf("r%d", i)),
		})
		require.NoError(t, err)
	}

	_, err := f.client.PutReportDefinition(
		t.Context(),
		&cursdk.PutReportDefinitionInput{ReportDefinition: def("extra")},
	)
	assert.Equal(t, "ReportLimitReachedException", apiCode(err))
}
