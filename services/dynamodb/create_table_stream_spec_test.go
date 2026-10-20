package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestCreateTable_OutputReportsStreamSpecification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		spec       *types.StreamSpecification
		name       string
		wantStream bool
	}{
		{
			name: "enabled",
			spec: &types.StreamSpecification{
				StreamEnabled:  aws.Bool(true),
				StreamViewType: types.StreamViewTypeNewAndOldImages,
			},
			wantStream: true,
		},
		{name: "absent", spec: nil, wantStream: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			out, err := db.CreateTable(t.Context(), &dynamodb_sdk.CreateTableInput{
				TableName: aws.String("t"),
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
				},
				BillingMode:         types.BillingModePayPerRequest,
				StreamSpecification: tt.spec,
			})
			require.NoError(t, err)

			td := out.TableDescription
			if !tt.wantStream {
				assert.Nil(t, td.LatestStreamArn)
				assert.Nil(t, td.StreamSpecification)

				return
			}

			require.NotNil(t, td.LatestStreamArn)
			assert.Contains(t, *td.LatestStreamArn, ":table/t/stream/")
			assert.NotNil(t, td.LatestStreamLabel)
			require.NotNil(t, td.StreamSpecification)
			assert.Equal(t, types.StreamViewTypeNewAndOldImages, td.StreamSpecification.StreamViewType)

			desc, err := db.DescribeTable(t.Context(), &dynamodb_sdk.DescribeTableInput{TableName: aws.String("t")})
			require.NoError(t, err)
			assert.Equal(t, td.LatestStreamArn, desc.Table.LatestStreamArn)
		})
	}
}
