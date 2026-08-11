package apigateway_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// patchOp builds one apigwtypes.PatchOperation. value is omitted (nil) when empty,
// matching how the real SDK omits Value for a "remove" op with no payload.
func patchOp(op apigwtypes.Op, path, value string) apigwtypes.PatchOperation {
	p := apigwtypes.PatchOperation{Op: op, Path: aws.String(path)}
	if value != "" {
		p.Value = aws.String(value)
	}

	return p
}

// setupSDKMethod creates a REST API, its root resource, and a method on it
// through the real aws-sdk-go-v2 client, returning the client and the ids
// every PATCH test below needs.
func setupSDKMethod(t *testing.T, extra func(*apigwsdk.PutMethodInput)) (*apigwsdk.Client, string, string) {
	t.Helper()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	api, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("patch-ops-api")})
	require.NoError(t, err)

	resources, err := client.GetResources(t.Context(), &apigwsdk.GetResourcesInput{RestApiId: api.Id})
	require.NoError(t, err)
	require.NotEmpty(t, resources.Items)
	rootID := resources.Items[0].Id

	putInput := &apigwsdk.PutMethodInput{
		RestApiId:         api.Id,
		ResourceId:        rootID,
		HttpMethod:        aws.String("GET"),
		AuthorizationType: aws.String("NONE"),
	}
	if extra != nil {
		extra(putInput)
	}

	_, err = client.PutMethod(t.Context(), putInput)
	require.NoError(t, err)

	return client, aws.ToString(api.Id), aws.ToString(rootID)
}

// TestSDK_UpdateResource_ParentIDPatch drives UpdateResource through the real
// SDK client with a "/parentId" PatchOperation (patch-operations.html:
// UpdateResource documents "/parentId" and "/pathPart" as its only two
// replace-only paths). Before the fix UpdateResourceInput had no ParentID
// field, so a real client's PATCH silently dropped the move and the resource
// (and any descendants) kept its old Path.
func TestSDK_UpdateResource_ParentIDPatch(t *testing.T) {
	t.Parallel()

	client, apiID, rootID := setupSDKMethod(t, nil)

	parentA, err := client.CreateResource(t.Context(), &apigwsdk.CreateResourceInput{
		RestApiId: aws.String(apiID), ParentId: aws.String(rootID), PathPart: aws.String("a"),
	})
	require.NoError(t, err)

	parentB, err := client.CreateResource(t.Context(), &apigwsdk.CreateResourceInput{
		RestApiId: aws.String(apiID), ParentId: aws.String(rootID), PathPart: aws.String("b"),
	})
	require.NoError(t, err)

	child, err := client.CreateResource(t.Context(), &apigwsdk.CreateResourceInput{
		RestApiId: aws.String(apiID), ParentId: parentA.Id, PathPart: aws.String("child"),
	})
	require.NoError(t, err)
	require.Equal(t, "/a/child", aws.ToString(child.Path))

	t.Run("move recomputes descendant paths", func(t *testing.T) {
		t.Parallel()

		moved, moveErr := client.UpdateResource(t.Context(), &apigwsdk.UpdateResourceInput{
			RestApiId:  aws.String(apiID),
			ResourceId: parentA.Id,
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpReplace, "/parentId", aws.ToString(parentB.Id)),
			},
		})
		require.NoError(t, moveErr)
		assert.Equal(t, aws.ToString(parentB.Id), aws.ToString(moved.ParentId))
		assert.Equal(t, "/b/a", aws.ToString(moved.Path))

		gotChild, getErr := client.GetResource(t.Context(), &apigwsdk.GetResourceInput{
			RestApiId: aws.String(apiID), ResourceId: child.Id,
		})
		require.NoError(t, getErr)
		assert.Equal(t, "/b/a/child", aws.ToString(gotChild.Path), "descendant path must follow the move")
	})

	t.Run("move under own descendant rejected", func(t *testing.T) {
		t.Parallel()

		_, cycleErr := client.UpdateResource(t.Context(), &apigwsdk.UpdateResourceInput{
			RestApiId:  aws.String(apiID),
			ResourceId: parentA.Id,
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpReplace, "/parentId", aws.ToString(child.Id)),
			},
		})
		require.Error(t, cycleErr)
	})

	t.Run("add op on parentid rejected", func(t *testing.T) {
		t.Parallel()

		_, addErr := client.UpdateResource(t.Context(), &apigwsdk.UpdateResourceInput{
			RestApiId:  aws.String(apiID),
			ResourceId: parentA.Id,
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpAdd, "/parentId", aws.ToString(parentB.Id)),
			},
		})
		require.Error(t, addErr)
	})
}

// TestSDK_UpdateMethod_PatchOperations drives UpdateMethod through the real
// SDK client. Before the fix, "/requestParameters/{name}" and
// "/requestModels/{content-type}" (per-key paths patch-operations.html
// documents for UpdateMethod) fell through the generic single-field flatten,
// which drops any path containing "/" -- so a real client's PATCH silently
// did nothing.
func TestSDK_UpdateMethod_PatchOperations(t *testing.T) {
	t.Parallel()

	client, apiID, rootID := setupSDKMethod(t, func(in *apigwsdk.PutMethodInput) {
		in.RequestParameters = map[string]bool{"method.request.header.X-Existing": true}
	})

	t.Run("requestparameters add merges with existing", func(t *testing.T) {
		t.Parallel()

		out, err := client.UpdateMethod(t.Context(), &apigwsdk.UpdateMethodInput{
			RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpAdd, "/requestParameters/method.request.querystring.id", "true"),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]bool{
			"method.request.header.X-Existing": true,
			"method.request.querystring.id":    true,
		}, out.RequestParameters)
	})

	t.Run("requestmodels add then remove", func(t *testing.T) {
		t.Parallel()

		out, err := client.UpdateMethod(t.Context(), &apigwsdk.UpdateMethodInput{
			RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpAdd, "/requestModels/application~1json", "Empty"),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"application/json": "Empty"}, out.RequestModels)

		out, err = client.UpdateMethod(t.Context(), &apigwsdk.UpdateMethodInput{
			RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpRemove, "/requestModels/application~1json", ""),
			},
		})
		require.NoError(t, err)
		assert.Empty(t, out.RequestModels)
	})

	t.Run("requestvalidatorid replace", func(t *testing.T) {
		t.Parallel()

		out, err := client.UpdateMethod(t.Context(), &apigwsdk.UpdateMethodInput{
			RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpReplace, "/requestValidatorId", "some-validator-id"),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "some-validator-id", aws.ToString(out.RequestValidatorId))
	})

	t.Run("authorizationscopes rejected as unmodeled", func(t *testing.T) {
		t.Parallel()

		_, err := client.UpdateMethod(t.Context(), &apigwsdk.UpdateMethodInput{
			RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpAdd, "/authorizationScopes", "admin"),
			},
		})
		require.Error(t, err)
	})
}

// setupSDKIntegration creates a method plus a HTTP integration on it, seeding
// one requestParameters/requestTemplates/cacheKeyParameters entry so the PATCH
// merge tests can prove siblings survive.
func setupSDKIntegration(t *testing.T) (*apigwsdk.Client, string, string) {
	t.Helper()

	client, apiID, rootID := setupSDKMethod(t, nil)

	_, err := client.PutIntegration(t.Context(), &apigwsdk.PutIntegrationInput{
		RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
		Type:                  apigwtypes.IntegrationTypeHttp,
		Uri:                   aws.String("https://example.com/existing"),
		IntegrationHttpMethod: aws.String("GET"),
		CacheKeyParameters:    []string{"method.request.path.existing"},
		RequestParameters:     map[string]string{"integration.request.header.Existing": "'x'"},
		RequestTemplates:      map[string]string{"text/plain": "existing"},
	})
	require.NoError(t, err)

	return client, apiID, rootID
}

// TestSDK_UpdateIntegration_PatchOperations drives UpdateIntegration through
// the real SDK client. Before the fix, "/cacheKeyParameters" (a single-segment
// list-membership path whose Value is a JSON string) unmarshaled into
// UpdateIntegrationInput's []string field and FAILED the whole PATCH outright
// -- this is the "cannot be called successfully at all" class of bug named in
// gopherstack-oius, not a silent no-op. "/requestParameters/{name}" and
// "/requestTemplates/{content-type}" fell through the generic flatten and were
// silently dropped. "/type" is documented "Not supported" for every op but
// previously succeeded silently since IntegrationType has a matching struct
// field.
func TestSDK_UpdateIntegration_PatchOperations(t *testing.T) {
	t.Parallel()

	t.Run("cachekeyparameters add merges with existing", func(t *testing.T) {
		t.Parallel()

		client, apiID, rootID := setupSDKIntegration(t)

		out, err := client.UpdateIntegration(t.Context(), &apigwsdk.UpdateIntegrationInput{
			RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpAdd, "/cacheKeyParameters", "method.request.path.id"),
			},
		})
		require.NoError(t, err)
		assert.ElementsMatch(
			t, []string{"method.request.path.existing", "method.request.path.id"}, out.CacheKeyParameters,
		)
	})

	t.Run("per-key merge for parameters and templates", func(t *testing.T) {
		t.Parallel()

		client, apiID, rootID := setupSDKIntegration(t)

		out, err := client.UpdateIntegration(t.Context(), &apigwsdk.UpdateIntegrationInput{
			RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpAdd, "/requestParameters/integration.request.header.New", "'y'"),
				patchOp(apigwtypes.OpAdd, "/requestTemplates/application~1json", "{}"),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			"integration.request.header.Existing": "'x'",
			"integration.request.header.New":      "'y'",
		}, out.RequestParameters)
		assert.Equal(t, map[string]string{
			"text/plain":       "existing",
			"application/json": "{}",
		}, out.RequestTemplates)
	})

	t.Run("timeoutinmillis string coerces to int", func(t *testing.T) {
		t.Parallel()

		client, apiID, rootID := setupSDKIntegration(t)

		out, err := client.UpdateIntegration(t.Context(), &apigwsdk.UpdateIntegrationInput{
			RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpReplace, "/timeoutInMillis", "15000"),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, int32(15000), out.TimeoutInMillis)
	})

	t.Run("type rejected for any op", func(t *testing.T) {
		t.Parallel()

		client, apiID, rootID := setupSDKIntegration(t)

		_, err := client.UpdateIntegration(t.Context(), &apigwsdk.UpdateIntegrationInput{
			RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpReplace, "/type", "MOCK"),
			},
		})
		require.Error(t, err)
	})
}

// TestSDK_UpdateIntegrationResponse_PatchOperations drives
// UpdateIntegrationResponse through the real SDK client with per-key
// "/responseParameters/{name}" and "/responseTemplates/{content-type}" PATCH
// ops, which previously fell through the generic flatten (path contains "/")
// and were silently dropped.
func TestSDK_UpdateIntegrationResponse_PatchOperations(t *testing.T) {
	t.Parallel()

	client, apiID, rootID := setupSDKIntegration(t)

	_, err := client.PutIntegrationResponse(t.Context(), &apigwsdk.PutIntegrationResponseInput{
		RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
		StatusCode:         aws.String("200"),
		ResponseParameters: map[string]string{"method.response.header.Existing": "'x'"},
		ResponseTemplates:  map[string]string{"application/json": "existing"},
	})
	require.NoError(t, err)

	out, err := client.UpdateIntegrationResponse(t.Context(), &apigwsdk.UpdateIntegrationResponseInput{
		RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
		StatusCode: aws.String("200"),
		PatchOperations: []apigwtypes.PatchOperation{
			patchOp(apigwtypes.OpAdd, "/responseParameters/method.response.header.New", "'y'"),
			patchOp(apigwtypes.OpReplace, "/responseTemplates/application~1json", "updated"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"method.response.header.Existing": "'x'",
		"method.response.header.New":      "'y'",
	}, out.ResponseParameters)
	assert.Equal(t, map[string]string{"application/json": "updated"}, out.ResponseTemplates)
}

// TestSDK_UpdateMethodResponse_PatchOperations drives UpdateMethodResponse
// through the real SDK client with per-key "/responseModels/{content-type}"
// and "/responseParameters/{name}" PATCH ops, which previously fell through
// the generic flatten and were silently dropped.
func TestSDK_UpdateMethodResponse_PatchOperations(t *testing.T) {
	t.Parallel()

	client, apiID, rootID := setupSDKMethod(t, nil)

	_, err := client.PutMethodResponse(t.Context(), &apigwsdk.PutMethodResponseInput{
		RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
		StatusCode:         aws.String("200"),
		ResponseModels:     map[string]string{"application/json": "Empty"},
		ResponseParameters: map[string]bool{"method.response.header.Existing": true},
	})
	require.NoError(t, err)

	out, err := client.UpdateMethodResponse(t.Context(), &apigwsdk.UpdateMethodResponseInput{
		RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
		StatusCode: aws.String("200"),
		PatchOperations: []apigwtypes.PatchOperation{
			patchOp(apigwtypes.OpAdd, "/responseModels/text~1plain", "Empty"),
			patchOp(apigwtypes.OpAdd, "/responseParameters/method.response.header.New", "true"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"application/json": "Empty", "text/plain": "Empty"}, out.ResponseModels)
	assert.Equal(t, map[string]bool{
		"method.response.header.Existing": true,
		"method.response.header.New":      true,
	}, out.ResponseParameters)
}

// TestSDK_UpdateBasePathMapping_PatchOperations drives UpdateBasePathMapping
// through the real SDK client with both PATCH path spellings AWS's own docs
// use: patch-operations.html documents "/basepath" and "/restapiId" (lowercase,
// gopherstack-6q5h), while the AWS CLI reference's own worked example uses
// "/basePath" (https://docs.aws.amazon.com/cli/latest/reference/apigateway/update-base-path-mapping.html).
// Before the fix, the lowercase spelling silently no-opped: it never matched
// UpdateBasePathMappingInput's camelCase json tags.
func TestSDK_UpdateBasePathMapping_PatchOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{"documented lowercase basepath", "/basepath"},
		{"cli-reference camelcase basepath", "/basePath"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

			api, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("bpm-api")})
			require.NoError(t, err)

			_, err = client.CreateDomainName(t.Context(), &apigwsdk.CreateDomainNameInput{
				DomainName: aws.String("api.example.com"),
			})
			require.NoError(t, err)

			_, err = client.CreateBasePathMapping(t.Context(), &apigwsdk.CreateBasePathMappingInput{
				DomainName: aws.String("api.example.com"), RestApiId: api.Id, BasePath: aws.String("v1"),
			})
			require.NoError(t, err)

			out, err := client.UpdateBasePathMapping(t.Context(), &apigwsdk.UpdateBasePathMappingInput{
				DomainName: aws.String("api.example.com"), BasePath: aws.String("v1"),
				PatchOperations: []apigwtypes.PatchOperation{patchOp(apigwtypes.OpReplace, tt.path, "v2")},
			})
			require.NoError(t, err)
			assert.Equal(t, "v2", aws.ToString(out.BasePath), "PATCH path %q must actually change basePath", tt.path)
		})
	}

	t.Run("documented restapiid casing", func(t *testing.T) {
		t.Parallel()

		client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

		api1, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("bpm-api-1")})
		require.NoError(t, err)
		api2, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("bpm-api-2")})
		require.NoError(t, err)

		_, err = client.CreateDomainName(t.Context(), &apigwsdk.CreateDomainNameInput{
			DomainName: aws.String("api2.example.com"),
		})
		require.NoError(t, err)

		_, err = client.CreateBasePathMapping(t.Context(), &apigwsdk.CreateBasePathMappingInput{
			DomainName: aws.String("api2.example.com"), RestApiId: api1.Id, BasePath: aws.String("v1"),
		})
		require.NoError(t, err)

		out, err := client.UpdateBasePathMapping(t.Context(), &apigwsdk.UpdateBasePathMappingInput{
			DomainName: aws.String("api2.example.com"), BasePath: aws.String("v1"),
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpReplace, "/restapiId", aws.ToString(api2.Id)),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(api2.Id), aws.ToString(out.RestApiId))
	})
}

// TestSDK_UpdateAuthorizer_ProviderARNsPatch drives UpdateAuthorizer through
// the real SDK client with "/providerARNs" add/remove (patch-operations.html:
// UpdateAuthorizer documents add/remove as supported, replace as not).
// Before the fix, applyResourcePatchOp had no case for UpdateAuthorizer, so
// the op fell through to the generic single-field flatten, which wrote the
// Value's raw JSON string straight into a field typed []string -- a hard
// json.Unmarshal 500, not a silent no-op.
func TestSDK_UpdateAuthorizer_ProviderARNsPatch(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	api, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("authz-api")})
	require.NoError(t, err)

	authz, err := client.CreateAuthorizer(t.Context(), &apigwsdk.CreateAuthorizerInput{
		RestApiId: api.Id, Name: aws.String("authz"), Type: apigwtypes.AuthorizerTypeToken,
		ProviderARNs: []string{"arn:aws:cognito-idp:us-east-1:000000000000:userpool/existing"},
	})
	require.NoError(t, err)

	t.Run("add merges with existing", func(t *testing.T) {
		t.Parallel()

		out, addErr := client.UpdateAuthorizer(t.Context(), &apigwsdk.UpdateAuthorizerInput{
			RestApiId: api.Id, AuthorizerId: authz.Id,
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpAdd, "/providerARNs", "arn:aws:cognito-idp:us-east-1:000000000000:userpool/new"),
			},
		})
		require.NoError(t, addErr)
		assert.ElementsMatch(t, []string{
			"arn:aws:cognito-idp:us-east-1:000000000000:userpool/existing",
			"arn:aws:cognito-idp:us-east-1:000000000000:userpool/new",
		}, out.ProviderARNs)
	})

	t.Run("remove clears the last entry", func(t *testing.T) {
		t.Parallel()

		solo, createErr := client.CreateAuthorizer(t.Context(), &apigwsdk.CreateAuthorizerInput{
			RestApiId: api.Id, Name: aws.String("authz-solo"), Type: apigwtypes.AuthorizerTypeToken,
			ProviderARNs: []string{"arn:aws:cognito-idp:us-east-1:000000000000:userpool/solo"},
		})
		require.NoError(t, createErr)

		out, removeErr := client.UpdateAuthorizer(t.Context(), &apigwsdk.UpdateAuthorizerInput{
			RestApiId: api.Id, AuthorizerId: solo.Id,
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(
					apigwtypes.OpRemove, "/providerARNs", "arn:aws:cognito-idp:us-east-1:000000000000:userpool/solo",
				),
			},
		})
		require.NoError(t, removeErr)
		assert.Empty(t, out.ProviderARNs)
	})

	t.Run("replace rejected", func(t *testing.T) {
		t.Parallel()

		_, replaceErr := client.UpdateAuthorizer(t.Context(), &apigwsdk.UpdateAuthorizerInput{
			RestApiId: api.Id, AuthorizerId: authz.Id,
			PatchOperations: []apigwtypes.PatchOperation{
				patchOp(apigwtypes.OpReplace, "/providerARNs", "arn:aws:cognito-idp:us-east-1:000000000000:userpool/x"),
			},
		})
		require.Error(t, replaceErr)
	})
}

// TestSDK_UpdateAccount_FeaturesPatch drives UpdateAccount through the real
// SDK client with "/features" add/remove (patch-operations.html: UpdateAccount
// documents add supported, remove supported except for the UsagePlans
// feature, replace not supported). Before the fix, Account.Features existed
// as a read-only field nothing ever wrote to, and UpdateAccountInput had no
// Features field at all, so the op silently no-opped via the generic
// single-field flatten.
func TestSDK_UpdateAccount_FeaturesPatch(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	t.Run("add a new feature", func(t *testing.T) {
		t.Parallel()

		out, err := client.UpdateAccount(t.Context(), &apigwsdk.UpdateAccountInput{
			PatchOperations: []apigwtypes.PatchOperation{patchOp(apigwtypes.OpAdd, "/features", "SNAPSTART")},
		})
		require.NoError(t, err)
		assert.Contains(t, out.Features, "SNAPSTART")
		assert.Contains(t, out.Features, "UsagePlans", "add must merge, not replace, the existing feature list")
	})

	t.Run("remove usageplans rejected", func(t *testing.T) {
		t.Parallel()

		_, err := client.UpdateAccount(t.Context(), &apigwsdk.UpdateAccountInput{
			PatchOperations: []apigwtypes.PatchOperation{patchOp(apigwtypes.OpRemove, "/features", "UsagePlans")},
		})
		require.Error(t, err)
	})

	t.Run("replace rejected", func(t *testing.T) {
		t.Parallel()

		_, err := client.UpdateAccount(t.Context(), &apigwsdk.UpdateAccountInput{
			PatchOperations: []apigwtypes.PatchOperation{patchOp(apigwtypes.OpReplace, "/features", "SNAPSTART")},
		})
		require.Error(t, err)
	})
}
