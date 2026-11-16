package app

import (
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestCloudControlSecretGenerateSecretString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(t *testing.T, value string)
		name    string
		desired string
		wantErr bool
	}{
		{
			name: "password_only",
			desired: `{"Name":"cc-gen-1","GenerateSecretString":{"PasswordLength":40,"ExcludeNumbers":true,` +
				`"ExcludePunctuation":true}}`,
			check: func(t *testing.T, v string) {
				t.Helper()
				assert.Len(t, v, 40)
				assert.False(t, strings.ContainsAny(v, "0123456789"))
			},
		},
		{
			name: "template_with_key",
			desired: `{"Name":"cc-gen-2","GenerateSecretString":{"SecretStringTemplate":"{\"username\":\"admin\"}",` +
				`"GenerateStringKey":"password","PasswordLength":20}}`,
			check: func(t *testing.T, v string) {
				t.Helper()

				var doc map[string]string

				require.NoError(t, json.Unmarshal([]byte(v), &doc, jsonx.V1()))
				assert.Equal(t, "admin", doc["username"])
				assert.Len(t, doc["password"], 20)
			},
		},
		{
			name:    "both_secret_string_and_generate",
			desired: `{"Name":"cc-gen-3","SecretString":"x","GenerateSecretString":{"PasswordLength":20}}`,
			wantErr: true,
		},
		{
			name:    "template_without_key",
			desired: `{"Name":"cc-gen-4","GenerateSecretString":{"SecretStringTemplate":"{}"}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)

			cc := cloudcontrol.NewFromConfig(fx.cfg)

			created, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
				TypeName: aws.String("AWS::SecretsManager::Secret"), DesiredState: aws.String(tt.desired),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			sm := secretsmanager.NewFromConfig(fx.cfg)

			got, err := sm.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{
				SecretId: created.ProgressEvent.Identifier,
			})
			require.NoError(t, err)
			tt.check(t, aws.ToString(got.SecretString))
		})
	}
}

func TestCloudControlRestAPICloneAndBody(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	cc := cloudcontrol.NewFromConfig(fx.cfg)
	apigw := apigateway.NewFromConfig(fx.cfg)

	src, err := apigw.CreateRestApi(t.Context(), &apigateway.CreateRestApiInput{Name: aws.String("cc-src")})
	require.NoError(t, err)

	_, err = apigw.CreateResource(t.Context(), &apigateway.CreateResourceInput{
		RestApiId: src.Id, ParentId: src.RootResourceId, PathPart: aws.String("pets"),
	})
	require.NoError(t, err)

	created, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
		TypeName:     aws.String("AWS::ApiGateway::RestApi"),
		DesiredState: aws.String(`{"Name":"cc-clone","CloneFrom":"` + aws.ToString(src.Id) + `"}`),
	})
	require.NoError(t, err)

	id := created.ProgressEvent.Identifier

	res, err := apigw.GetResources(t.Context(), &apigateway.GetResourcesInput{RestApiId: id})
	require.NoError(t, err)

	paths := make([]string, 0, len(res.Items))
	for _, r := range res.Items {
		paths = append(paths, aws.ToString(r.Path))
	}

	assert.Contains(t, paths, "/pets")

	body := `{"swagger":"2.0","info":{"title":"cc-clone","version":"1"},"paths":{"/cats":{"get":` +
		`{"responses":{"200":{"description":"ok"}},"x-amazon-apigateway-integration":{"type":"MOCK",` +
		`"requestTemplates":{"application/json":"{\"statusCode\":200}"}}}}}}`

	_, err = cc.UpdateResource(t.Context(), &cloudcontrol.UpdateResourceInput{
		TypeName: aws.String("AWS::ApiGateway::RestApi"), Identifier: id,
		PatchDocument: aws.String(`[{"op":"add","path":"/Body","value":` + body + `}]`),
	})
	require.NoError(t, err)

	res, err = apigw.GetResources(t.Context(), &apigateway.GetResourcesInput{RestApiId: id})
	require.NoError(t, err)

	paths = paths[:0]
	for _, r := range res.Items {
		paths = append(paths, aws.ToString(r.Path))
	}

	assert.Contains(t, paths, "/cats")
}
