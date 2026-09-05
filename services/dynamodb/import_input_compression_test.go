package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestDescribeImport_InputCompressionType proves the real
// ImportTableDescription.InputCompressionType (dynamodb@v1.63.1
// deserializers.go, awsAwsjson10_deserializeDocumentImportTableDescription,
// case "InputCompressionType") round-trips through the real typed SDK
// client. ImportTable already tracks the caller's InputCompressionType on
// storedImport.InputCompression, but the wire converter
// (importDescriptionFromRecord/importTableDescriptionWire) never surfaced
// it, so every DescribeImport/ImportTable response reported it as empty
// regardless of what was requested.
func TestDescribeImport_InputCompressionType(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	h := dynamodb.NewHandler(backend)
	client := newTestDynamoDBClient(t, h)

	importOut, err := client.ImportTable(t.Context(), &sdkdynamodb.ImportTableInput{
		S3BucketSource: &sdktypes.S3BucketSource{
			S3Bucket: aws.String("dict-bucket"),
		},
		InputFormat:          sdktypes.InputFormatDynamodbJson,
		InputCompressionType: sdktypes.InputCompressionTypeGzip,
		TableCreationParameters: &sdktypes.TableCreationParameters{
			TableName: aws.String("dict-table"),
			KeySchema: []sdktypes.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: sdktypes.KeyTypeHash},
			},
			AttributeDefinitions: []sdktypes.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: sdktypes.ScalarAttributeTypeS},
			},
			BillingMode: sdktypes.BillingModePayPerRequest,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, importOut.ImportTableDescription)
	require.Equal(t, sdktypes.InputCompressionTypeGzip, importOut.ImportTableDescription.InputCompressionType,
		"ImportTable response must echo the requested InputCompressionType")

	descOut, err := client.DescribeImport(t.Context(), &sdkdynamodb.DescribeImportInput{
		ImportArn: importOut.ImportTableDescription.ImportArn,
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.ImportTableDescription)
	require.Equal(t, sdktypes.InputCompressionTypeGzip, descOut.ImportTableDescription.InputCompressionType,
		"DescribeImport must report the InputCompressionType the import was created with")
}
