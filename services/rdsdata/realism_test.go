package rdsdata_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsdatasdk "github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

func TestRealism_TransactionNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(c *rdsdatasdk.Client) error
		name string
	}{
		{
			name: "commit",
			call: func(c *rdsdatasdk.Client) error {
				_, err := c.CommitTransaction(t.Context(), &rdsdatasdk.CommitTransactionInput{
					ResourceArn: aws.String(execSQLResourceARN), SecretArn: aws.String(execSQLSecretARN),
					TransactionId: aws.String("bogus"),
				})

				return err
			},
		},
		{
			name: "execute",
			call: func(c *rdsdatasdk.Client) error {
				_, err := c.ExecuteStatement(t.Context(), &rdsdatasdk.ExecuteStatementInput{
					ResourceArn: aws.String(execSQLResourceARN), SecretArn: aws.String(execSQLSecretARN),
					Sql: aws.String("select 1"), TransactionId: aws.String("bogus"),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
			t.Cleanup(backend.Close)
			client := newRoundTripClient(t, rdsdata.NewHandler(backend))

			err := tt.call(client)
			require.Error(t, err)

			var nf *types.NotFoundException

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "TransactionNotFoundException", apiErr.ErrorCode())
			assert.Equal(t, "transaction bogus not found", apiErr.ErrorMessage())
			assert.NotErrorAs(t, err, &nf)

			var respErr *http.ResponseError

			require.ErrorAs(t, err, &respErr)
			assert.Equal(t, 404, respErr.HTTPStatusCode())
		})
	}
}
