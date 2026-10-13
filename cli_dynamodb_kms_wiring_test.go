package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamoDBKMSKeyInaccessible(t *testing.T) {
	t.Parallel()

	tests := []struct {
		disrupt func(t *testing.T, c *kms.Client, keyID string)
		name    string
	}{
		{
			name: "disabled",
			disrupt: func(t *testing.T, c *kms.Client, keyID string) {
				t.Helper()

				_, err := c.DisableKey(t.Context(), &kms.DisableKeyInput{KeyId: aws.String(keyID)})
				require.NoError(t, err)
			},
		},
		{
			name: "pending_deletion",
			disrupt: func(t *testing.T, c *kms.Client, keyID string) {
				t.Helper()

				_, err := c.ScheduleKeyDeletion(t.Context(), &kms.ScheduleKeyDeletionInput{
					KeyId: aws.String(keyID), PendingWindowInDays: aws.Int32(7),
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			kc := kms.NewFromConfig(fx.cfg)
			dc := dynamodb.NewFromConfig(fx.cfg)

			key, err := kc.CreateKey(t.Context(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			_, err = dc.CreateTable(t.Context(), &dynamodb.CreateTableInput{
				TableName:   aws.String("enc"),
				BillingMode: ddbtypes.BillingModePayPerRequest,
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
				},
				AttributeDefinitions: []ddbtypes.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
				},
				SSESpecification: &ddbtypes.SSESpecification{
					Enabled: aws.Bool(true), SSEType: ddbtypes.SSETypeKms, KMSMasterKeyId: key.KeyMetadata.Arn,
				},
			})
			require.NoError(t, err)

			desc, err := dc.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: aws.String("enc")})
			require.NoError(t, err)
			assert.Equal(t, ddbtypes.TableStatusActive, desc.Table.TableStatus)
			assert.Nil(t, desc.Table.SSEDescription.InaccessibleEncryptionDateTime)

			tt.disrupt(t, kc, *key.KeyMetadata.KeyId)

			desc, err = dc.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: aws.String("enc")})
			require.NoError(t, err)
			assert.Equal(t, ddbtypes.TableStatusInaccessibleEncryptionCredentials, desc.Table.TableStatus)
			require.NotNil(t, desc.Table.SSEDescription.InaccessibleEncryptionDateTime)

			first := *desc.Table.SSEDescription.InaccessibleEncryptionDateTime

			desc, err = dc.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: aws.String("enc")})
			require.NoError(t, err)
			assert.True(t, first.Equal(*desc.Table.SSEDescription.InaccessibleEncryptionDateTime))
		})
	}
}
