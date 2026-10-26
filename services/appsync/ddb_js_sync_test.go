package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateCode_DynamoDBSyncHelper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "sync fields",
			body: `sync({ limit: 5, nextToken: 'tok', lastSync: 1700000000000 })`,
			want: `{"operation":"Sync","limit":5,"nextToken":"tok","lastSync":1700000000000}`,
		},
		{
			name: "sync with filter",
			body: `sync({ filter: { title: { eq: 'x' } }, basePartitionKey: 'pk', deltaIndexName: 'idx' })`,
			want: `{"operation":"Sync","basePartitionKey":"pk","deltaIndexName":"idx",` +
				`"filter":{"expression":"(#title = :title_eq)","expressionNames":{"#title":"title"},` +
				`"expressionValues":{":title_eq":{"S":"x"}}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			client := newTestAppsyncClient(t, h)

			out, err := client.EvaluateCode(t.Context(), &appsyncsdk.EvaluateCodeInput{
				Code: aws.String("import { sync } from '@aws-appsync/utils/dynamodb';\n" +
					"export function request(ctx) { return " + tt.body + "; }"),
				Context: aws.String(`{}`), Function: aws.String("request"),
				Runtime: &appsynctypes.AppSyncRuntime{
					Name: appsynctypes.RuntimeNameAppsyncJs, RuntimeVersion: aws.String("1.0.0"),
				},
			})
			require.NoError(t, err)
			require.Nil(t, out.Error)
			assert.JSONEq(t, tt.want, aws.ToString(out.EvaluationResult))
		})
	}
}
