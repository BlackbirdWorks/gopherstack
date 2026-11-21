package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestDynamoDBAdapterKeyAttributeNames(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	str := ddbsdktypes.ScalarAttributeTypeS

	_, err := db.CreateTable(t.Context(), &sdkddb.CreateTableInput{
		TableName:   aws.String("composite"),
		BillingMode: ddbsdktypes.BillingModePayPerRequest,
		KeySchema: []ddbsdktypes.KeySchemaElement{
			{AttributeName: aws.String("sk"), KeyType: ddbsdktypes.KeyTypeRange},
			{AttributeName: aws.String("pk"), KeyType: ddbsdktypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbsdktypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: str},
			{AttributeName: aws.String("sk"), AttributeType: str},
		},
	})
	require.NoError(t, err)

	a := &dynamoDBAdapter{db: db}

	names, err := a.KeyAttributeNames(t.Context(), "composite")
	require.NoError(t, err)
	assert.Equal(t, []string{"pk", "sk"}, names)

	_, err = a.KeyAttributeNames(t.Context(), "missing")
	require.Error(t, err)
}
