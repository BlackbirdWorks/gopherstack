package apigateway_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateRestApi_CloneFrom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cloneFrom func(srcID string) string
		name      string
		wantErr   bool
	}{
		{name: "clones_contents", cloneFrom: func(id string) string { return id }},
		{name: "unknown_source", cloneFrom: func(string) string { return "nope" }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, srcID, rootID := setupSDKMethod(t, nil)
			ctx := t.Context()

			child, err := client.CreateResource(ctx, &apigwsdk.CreateResourceInput{
				RestApiId: aws.String(srcID), ParentId: aws.String(rootID), PathPart: aws.String("pets"),
			})
			require.NoError(t, err)

			auth, err := client.CreateAuthorizer(ctx, &apigwsdk.CreateAuthorizerInput{
				RestApiId: aws.String(srcID),
				Name:      aws.String("a"),
				Type:      apigwtypes.AuthorizerTypeToken,
				AuthorizerUri: aws.String(
					"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:1:function:f/invocations",
				),
				IdentitySource: aws.String("method.request.header.Auth"),
			})
			require.NoError(t, err)

			_, err = client.PutMethod(ctx, &apigwsdk.PutMethodInput{
				RestApiId: aws.String(srcID), ResourceId: child.Id, HttpMethod: aws.String("POST"),
				AuthorizationType: aws.String("CUSTOM"), AuthorizerId: auth.Id,
			})
			require.NoError(t, err)

			_, err = client.CreateModel(ctx, &apigwsdk.CreateModelInput{
				RestApiId: aws.String(srcID), Name: aws.String("Pet"), ContentType: aws.String("application/json"),
				Schema: aws.String(`{"type":"object"}`),
			})
			require.NoError(t, err)

			clone, err := client.CreateRestApi(ctx, &apigwsdk.CreateRestApiInput{
				Name: aws.String("clone"), CloneFrom: aws.String(tt.cloneFrom(srcID)),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEqual(t, srcID, aws.ToString(clone.Id))

			resources, err := client.GetResources(ctx, &apigwsdk.GetResourcesInput{
				RestApiId: clone.Id, Embed: []string{"methods"},
			})
			require.NoError(t, err)
			require.Len(t, resources.Items, 2)

			byPath := map[string]apigwtypes.Resource{}
			for _, r := range resources.Items {
				byPath[aws.ToString(r.Path)] = r
			}

			assert.Contains(t, byPath["/"].ResourceMethods, "GET", "root method cloned")
			assert.Equal(t, aws.ToString(clone.RootResourceId), aws.ToString(byPath["/"].Id))
			require.Contains(t, byPath["/pets"].ResourceMethods, "POST")
			assert.Equal(t, aws.ToString(byPath["/"].Id), aws.ToString(byPath["/pets"].ParentId))
			assert.NotEqual(
				t,
				aws.ToString(child.Id),
				aws.ToString(byPath["/pets"].Id),
				"clone gets fresh resource IDs",
			)

			auths, err := client.GetAuthorizers(ctx, &apigwsdk.GetAuthorizersInput{RestApiId: clone.Id})
			require.NoError(t, err)
			require.Len(t, auths.Items, 1)
			assert.Equal(
				t,
				aws.ToString(auths.Items[0].Id),
				aws.ToString(byPath["/pets"].ResourceMethods["POST"].AuthorizerId),
				"cloned method points at the cloned authorizer",
			)
			assert.NotEqual(t, aws.ToString(auth.Id), aws.ToString(auths.Items[0].Id))

			models, err := client.GetModels(ctx, &apigwsdk.GetModelsInput{RestApiId: clone.Id})
			require.NoError(t, err)
			assert.Len(t, models.Items, 1)
		})
	}
}
