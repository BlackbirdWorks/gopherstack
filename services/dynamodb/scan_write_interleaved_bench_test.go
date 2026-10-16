package dynamodb_test

import (
	"strconv"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func BenchmarkScanAfterWrite(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			db := dynamodb.NewInMemoryDB()
			ctx := b.Context()
			_, err := db.CreateTable(ctx, &sdk.CreateTableInput{
				TableName: aws.String("T"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
			})
			require.NoError(b, err)

			for i := range size {
				_, err = db.PutItem(ctx, &sdk.PutItemInput{
					TableName: aws.String("T"),
					Item:      scanCacheItem("k"+strconv.Itoa(i*7919%size), "v"),
				})
				require.NoError(b, err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; b.Loop(); i++ {
				_, err = db.PutItem(ctx, &sdk.PutItemInput{
					TableName: aws.String("T"),
					Item:      scanCacheItem("k"+strconv.Itoa(i%size), strconv.Itoa(i)),
				})
				require.NoError(b, err)

				_, err = db.Scan(ctx, &sdk.ScanInput{TableName: aws.String("T"), Limit: aws.Int32(10)})
				require.NoError(b, err)
			}
		})
	}
}
