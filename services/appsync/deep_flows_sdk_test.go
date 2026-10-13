package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartSchemaCreation_AppSyncDefinitions_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sdl     string
		wantErr bool
	}{
		{
			name: "aws scalars",
			sdl:  "type Query { at: AWSDateTime j: AWSJSON e: AWSEmail u: AWSURL ts: AWSTimestamp }",
		},
		{
			name: "subscribe directive",
			sdl: "type Todo { id: ID! }\ntype Query { t: Todo }\ntype Mutation { put: Todo }\n" +
				"type Subscription { onPut: Todo @aws_subscribe(mutations: [\"put\"]) }",
		},
		{
			name: "auth directives",
			sdl: "type Query { a: String @aws_iam @aws_api_key @aws_oidc @aws_lambda " +
				"@aws_cognito_user_pools(cognito_groups: [\"x\"]) @aws_auth(cognito_groups: [\"a\"]) }",
		},
		{name: "user declared scalar", sdl: "scalar AWSDate\ntype Query { d: AWSDate }"},
		{name: "unknown directive", sdl: "type Query { a: String @nope }", wantErr: true},
		{name: "unknown scalar", sdl: "type Query { a: AWSNope }", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			client := newTestAppsyncClient(t, h)

			api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
				Name:               aws.String("a"),
				AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
			})
			require.NoError(t, err)

			_, err = client.StartSchemaCreation(t.Context(), &appsyncsdk.StartSchemaCreationInput{
				ApiId: api.GraphqlApi.ApiId, Definition: []byte(tt.sdl),
			})
			if tt.wantErr {
				var bad *appsynctypes.BadRequestException
				require.ErrorAs(t, err, &bad)

				return
			}

			require.NoError(t, err)

			st, err := client.GetSchemaCreationStatus(t.Context(), &appsyncsdk.GetSchemaCreationStatusInput{
				ApiId: api.GraphqlApi.ApiId,
			})
			require.NoError(t, err)
			assert.Equal(t, appsynctypes.SchemaStatusActive, st.Status)

			intro, err := client.GetIntrospectionSchema(t.Context(), &appsyncsdk.GetIntrospectionSchemaInput{
				ApiId: api.GraphqlApi.ApiId, Format: appsynctypes.OutputTypeSdl,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.sdl, string(intro.Schema))
		})
	}
}
