// Package dynamodb_test covers gopherstack-rrtz item 2: ImportTable,
// DescribeImport and ListImports all shared a wire struct
// (importTableDescriptionWire) that dropped ClientToken,
// CloudWatchLogGroupArn, EndTime, StartTime, TableId, and the
// InputFormatOptions/S3BucketSource echoes. StartTime and TableId were
// called out as the two most defensible to fix; the rest are display fields
// fixed alongside them since they were cheap once the struct was touched.
// Each test drives the real aws-sdk-go-v2 client over HTTP so the wire
// decode -> backend conversion is exercised end to end.
package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestImportTable_WireFields_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	t.Cleanup(backend.Close)
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	importOut, err := client.ImportTable(t.Context(), &sdk.ImportTableInput{
		ClientToken: aws.String("client-token-1"),
		S3BucketSource: &types.S3BucketSource{
			S3Bucket:      aws.String("my-import-bucket"),
			S3KeyPrefix:   aws.String("prefix/"),
			S3BucketOwner: aws.String("111122223333"),
		},
		InputFormat: types.InputFormatCsv,
		InputFormatOptions: &types.InputFormatOptions{
			Csv: &types.CsvOptions{
				Delimiter:  aws.String(";"),
				HeaderList: []string{"pk", "value"},
			},
		},
		TableCreationParameters: &types.TableCreationParameters{
			TableName: aws.String("ImportWireTable"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
			BillingMode: types.BillingModePayPerRequest,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, importOut.ImportTableDescription)

	desc := importOut.ImportTableDescription
	require.NotNil(t, desc.StartTime)
	assert.False(t, desc.StartTime.IsZero())
	assert.NotEmpty(t, aws.ToString(desc.TableId))
	assert.Equal(t, "client-token-1", aws.ToString(desc.ClientToken))
	assert.NotEmpty(t, aws.ToString(desc.CloudWatchLogGroupArn))
	require.NotNil(t, desc.S3BucketSource)
	assert.Equal(t, "my-import-bucket", aws.ToString(desc.S3BucketSource.S3Bucket))
	assert.Equal(t, "111122223333", aws.ToString(desc.S3BucketSource.S3BucketOwner))
	require.NotNil(t, desc.InputFormatOptions)
	require.NotNil(t, desc.InputFormatOptions.Csv)
	assert.Equal(t, ";", aws.ToString(desc.InputFormatOptions.Csv.Delimiter))
	assert.Equal(t, []string{"pk", "value"}, desc.InputFormatOptions.Csv.HeaderList)

	importArn := aws.ToString(desc.ImportArn)
	tableArn := aws.ToString(desc.TableArn)

	descOut, err := client.DescribeImport(t.Context(), &sdk.DescribeImportInput{
		ImportArn: aws.String(importArn),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.ImportTableDescription)
	assert.Equal(t, aws.ToString(desc.TableId), aws.ToString(descOut.ImportTableDescription.TableId))
	require.NotNil(t, descOut.ImportTableDescription.StartTime)
	assert.False(t, descOut.ImportTableDescription.StartTime.IsZero())
	assert.Equal(t, "client-token-1", aws.ToString(descOut.ImportTableDescription.ClientToken))

	listOut, err := client.ListImports(t.Context(), &sdk.ListImportsInput{
		TableArn: aws.String(tableArn),
	})
	require.NoError(t, err)
	require.Len(t, listOut.ImportSummaryList, 1)
	summary := listOut.ImportSummaryList[0]
	require.NotNil(t, summary.StartTime)
	assert.False(t, summary.StartTime.IsZero())
	assert.Equal(t, tableArn, aws.ToString(summary.TableArn))
	assert.Equal(t, types.InputFormatCsv, summary.InputFormat)
}
