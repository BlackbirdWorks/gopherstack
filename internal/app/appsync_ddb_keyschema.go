package app

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
)

var _ appsyncbackend.KeySchemaReader = (*dynamoDBAdapter)(nil)

// KeyAttributeNames returns the table's key attribute names, hash key first.
func (a *dynamoDBAdapter) KeyAttributeNames(ctx context.Context, tableName string) ([]string, error) {
	out, err := a.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		return nil, fmt.Errorf("appsync ddb adapter: describe table: %w", err)
	}

	var hash, rng []string

	for _, k := range out.Table.KeySchema {
		if k.KeyType == ddbsdktypes.KeyTypeHash {
			hash = append(hash, *k.AttributeName)
		} else {
			rng = append(rng, *k.AttributeName)
		}
	}

	return append(hash, rng...), nil
}
