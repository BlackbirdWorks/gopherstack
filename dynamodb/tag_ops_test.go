package dynamodb_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddb "github.com/blackbirdworks/gopherstack/dynamodb"
)

// tableARN returns a fake DynamoDB table ARN used in tag tests.
func tableARN(name string) string {
	return "arn:aws:dynamodb:us-east-1:000000000000:table/" + name
}

func TestTagResource_AddAndList(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()

	_, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("tag-table"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	_, err = db.TagResource(context.Background(), &dynamodb.TagResourceInput{
		ResourceArn: aws.String(tableARN("tag-table")),
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	out, err := db.ListTagsOfResource(context.Background(), &dynamodb.ListTagsOfResourceInput{
		ResourceArn: aws.String(tableARN("tag-table")),
	})
	require.NoError(t, err)
	require.Len(t, out.Tags, 2)

	tagMap := make(map[string]string)
	for _, t := range out.Tags {
		tagMap[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}

	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "platform", tagMap["team"])
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()

	_, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("untag-table"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	// Add two tags, then remove one.
	_, err = db.TagResource(context.Background(), &dynamodb.TagResourceInput{
		ResourceArn: aws.String(tableARN("untag-table")),
		Tags: []types.Tag{
			{Key: aws.String("keep"), Value: aws.String("yes")},
			{Key: aws.String("remove"), Value: aws.String("no")},
		},
	})
	require.NoError(t, err)

	_, err = db.UntagResource(context.Background(), &dynamodb.UntagResourceInput{
		ResourceArn: aws.String(tableARN("untag-table")),
		TagKeys:     []string{"remove"},
	})
	require.NoError(t, err)

	out, err := db.ListTagsOfResource(context.Background(), &dynamodb.ListTagsOfResourceInput{
		ResourceArn: aws.String(tableARN("untag-table")),
	})
	require.NoError(t, err)
	require.Len(t, out.Tags, 1)
	assert.Equal(t, "keep", aws.ToString(out.Tags[0].Key))
	assert.Equal(t, "yes", aws.ToString(out.Tags[0].Value))
}

func TestListTagsOfResource_Empty(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()

	_, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("empty-tag-table"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	out, err := db.ListTagsOfResource(context.Background(), &dynamodb.ListTagsOfResourceInput{
		ResourceArn: aws.String(tableARN("empty-tag-table")),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Tags)
}

func TestTagResource_NotFound(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()

	_, err := db.TagResource(context.Background(), &dynamodb.TagResourceInput{
		ResourceArn: aws.String(tableARN("nonexistent")),
		Tags:        []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "ResourceNotFoundException")
}

func TestTagResource_OverwriteValue(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()

	_, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("overwrite-tag-table"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	_, err = db.TagResource(context.Background(), &dynamodb.TagResourceInput{
		ResourceArn: aws.String(tableARN("overwrite-tag-table")),
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("staging")}},
	})
	require.NoError(t, err)

	_, err = db.TagResource(context.Background(), &dynamodb.TagResourceInput{
		ResourceArn: aws.String(tableARN("overwrite-tag-table")),
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	out, err := db.ListTagsOfResource(context.Background(), &dynamodb.ListTagsOfResourceInput{
		ResourceArn: aws.String(tableARN("overwrite-tag-table")),
	})
	require.NoError(t, err)
	require.Len(t, out.Tags, 1)
	assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
}
