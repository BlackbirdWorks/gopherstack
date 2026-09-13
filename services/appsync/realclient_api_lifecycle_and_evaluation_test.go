package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// testEventConfig is the minimal valid EventConfig every v2 Api (Event API)
// create/update call requires.
func testEventConfig() *appsynctypes.EventConfig {
	return &appsynctypes.EventConfig{
		AuthProviders: []appsynctypes.AuthProvider{
			{AuthType: appsynctypes.AuthenticationTypeApiKey},
		},
		ConnectionAuthModes: []appsynctypes.AuthMode{
			{AuthType: appsynctypes.AuthenticationTypeApiKey},
		},
		DefaultPublishAuthModes: []appsynctypes.AuthMode{
			{AuthType: appsynctypes.AuthenticationTypeApiKey},
		},
		DefaultSubscribeAuthModes: []appsynctypes.AuthMode{
			{AuthType: appsynctypes.AuthenticationTypeApiKey},
		},
	}
}

// TestEventAPILifecycle_RealClient drives the v2 "Api" (Event API) resource family
// (ListApis, UpdateApi, DeleteApi) and its ChannelNamespace subresource
// (ListChannelNamespaces, UpdateChannelNamespace, DeleteChannelNamespace) through a
// real client, gopherstack-n3zi.
func TestEventAPILifecycle_RealClient(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "ListApis", run: func(t *testing.T) {
			t.Helper()

			backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
			client := newTestAppsyncClient(t, appsync.NewHandler(backend))

			created, err := client.CreateApi(t.Context(), &appsyncsdk.CreateApiInput{
				Name:        aws.String("slice34-event-api"),
				EventConfig: testEventConfig(),
			})
			require.NoError(t, err)
			apiID := aws.ToString(created.Api.ApiId)
			require.NotEmpty(t, apiID)

			out, listErr := client.ListApis(t.Context(), &appsyncsdk.ListApisInput{})
			require.NoError(t, listErr)

			var found bool
			for _, a := range out.Apis {
				if aws.ToString(a.ApiId) == apiID {
					found = true
					assert.Equal(t, "slice34-event-api", aws.ToString(a.Name))
				}
			}
			assert.True(t, found, "created api must appear in ListApis")
		}},
		{name: "UpdateApi", run: func(t *testing.T) {
			t.Helper()

			backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
			client := newTestAppsyncClient(t, appsync.NewHandler(backend))

			created, err := client.CreateApi(t.Context(), &appsyncsdk.CreateApiInput{
				Name:        aws.String("slice34-event-api"),
				EventConfig: testEventConfig(),
			})
			require.NoError(t, err)
			apiID := aws.ToString(created.Api.ApiId)

			updated, updateErr := client.UpdateApi(t.Context(), &appsyncsdk.UpdateApiInput{
				ApiId:        aws.String(apiID),
				Name:         aws.String("slice34-event-api-renamed"),
				EventConfig:  testEventConfig(),
				OwnerContact: aws.String("team@example.com"),
			})
			require.NoError(t, updateErr)
			assert.Equal(t, "slice34-event-api-renamed", aws.ToString(updated.Api.Name))
			assert.Equal(t, "team@example.com", aws.ToString(updated.Api.OwnerContact))
		}},
		{name: "ChannelNamespace lifecycle", run: func(t *testing.T) {
			t.Helper()

			nsBackend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
			nsClient := newTestAppsyncClient(t, appsync.NewHandler(nsBackend))

			nsAPI, nsErr := nsClient.CreateApi(t.Context(), &appsyncsdk.CreateApiInput{
				Name:        aws.String("slice34-ns-host-api"),
				EventConfig: testEventConfig(),
			})
			require.NoError(t, nsErr)
			nsAPIID := aws.ToString(nsAPI.Api.ApiId)

			_, createNSErr := nsClient.CreateChannelNamespace(t.Context(), &appsyncsdk.CreateChannelNamespaceInput{
				ApiId: aws.String(nsAPIID),
				Name:  aws.String("slice34-ns"),
			})
			require.NoError(t, createNSErr)

			listed, listErr := nsClient.ListChannelNamespaces(
				t.Context(),
				&appsyncsdk.ListChannelNamespacesInput{ApiId: aws.String(nsAPIID)},
			)
			require.NoError(t, listErr)
			require.Len(t, listed.ChannelNamespaces, 1)
			assert.Equal(t, "slice34-ns", aws.ToString(listed.ChannelNamespaces[0].Name))

			updated, updateErr := nsClient.UpdateChannelNamespace(t.Context(), &appsyncsdk.UpdateChannelNamespaceInput{
				ApiId:        aws.String(nsAPIID),
				Name:         aws.String("slice34-ns"),
				CodeHandlers: aws.String("export function onPublish(ctx) { return ctx.events; }"),
			})
			require.NoError(t, updateErr)
			assert.Equal(
				t,
				"export function onPublish(ctx) { return ctx.events; }",
				aws.ToString(updated.ChannelNamespace.CodeHandlers),
			)

			_, delErr := nsClient.DeleteChannelNamespace(t.Context(), &appsyncsdk.DeleteChannelNamespaceInput{
				ApiId: aws.String(nsAPIID),
				Name:  aws.String("slice34-ns"),
			})
			require.NoError(t, delErr)

			_, getErr := nsClient.GetChannelNamespace(t.Context(), &appsyncsdk.GetChannelNamespaceInput{
				ApiId: aws.String(nsAPIID),
				Name:  aws.String("slice34-ns"),
			})
			require.Error(t, getErr, "channel namespace must be gone after delete")
		}},
		{name: "DeleteApi", run: func(t *testing.T) {
			t.Helper()

			delBackend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
			delClient := newTestAppsyncClient(t, appsync.NewHandler(delBackend))

			delAPI, delAPIErr := delClient.CreateApi(t.Context(), &appsyncsdk.CreateApiInput{
				Name:        aws.String("slice34-delete-me"),
				EventConfig: testEventConfig(),
			})
			require.NoError(t, delAPIErr)

			_, delErr := delClient.DeleteApi(
				t.Context(),
				&appsyncsdk.DeleteApiInput{ApiId: delAPI.Api.ApiId},
			)
			require.NoError(t, delErr)

			_, getErr := delClient.GetApi(t.Context(), &appsyncsdk.GetApiInput{ApiId: delAPI.Api.ApiId})
			require.Error(t, getErr, "api must be gone after delete")
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestApiKeyLifecycle_RealClient drives ListApiKeys, UpdateApiKey and DeleteApiKey
// through a real client.
func TestApiKeyLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
	client := newTestAppsyncClient(t, appsync.NewHandler(backend))

	api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("slice34-apikey-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)
	apiID := aws.ToString(api.GraphqlApi.ApiId)

	key, keyErr := client.CreateApiKey(t.Context(), &appsyncsdk.CreateApiKeyInput{
		ApiId:       aws.String(apiID),
		Description: aws.String("slice34 key"),
	})
	require.NoError(t, keyErr)
	keyID := aws.ToString(key.ApiKey.Id)
	require.NotEmpty(t, keyID)

	listed, listErr := client.ListApiKeys(t.Context(), &appsyncsdk.ListApiKeysInput{ApiId: aws.String(apiID)})
	require.NoError(t, listErr)
	require.Len(t, listed.ApiKeys, 1)
	assert.Equal(t, keyID, aws.ToString(listed.ApiKeys[0].Id))
	assert.Equal(t, "slice34 key", aws.ToString(listed.ApiKeys[0].Description))

	updated, updateErr := client.UpdateApiKey(t.Context(), &appsyncsdk.UpdateApiKeyInput{
		ApiId:       aws.String(apiID),
		Id:          aws.String(keyID),
		Description: aws.String("slice34 key updated"),
	})
	require.NoError(t, updateErr)
	assert.Equal(t, "slice34 key updated", aws.ToString(updated.ApiKey.Description))

	_, delErr := client.DeleteApiKey(
		t.Context(),
		&appsyncsdk.DeleteApiKeyInput{ApiId: aws.String(apiID), Id: aws.String(keyID)},
	)
	require.NoError(t, delErr)

	after, afterErr := client.ListApiKeys(t.Context(), &appsyncsdk.ListApiKeysInput{ApiId: aws.String(apiID)})
	require.NoError(t, afterErr)
	assert.Empty(t, after.ApiKeys, "key must be gone after delete")
}

// TestApiCacheLifecycle_RealClient drives CreateApiCache, GetApiCache, UpdateApiCache,
// FlushApiCache and DeleteApiCache through a real client.
//
// Also proves the fix for a real wire bug: models.go's APICache.HealthMetrics was a
// bool with JSON tag "healthMetricsConfig", but appsync@v1.60.0's
// types.ApiCache.HealthMetricsConfig (types/types.go:128) is the string enum
// CacheHealthMetricsConfig ("ENABLED"/"DISABLED", types/enums.go:176-181) -- a real
// client's CreateApiCache/UpdateApiCache call with HealthMetricsConfig set failed
// json.Unmarshal outright (string into a Go bool field) before the fix.
func TestApiCacheLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
	client := newTestAppsyncClient(t, appsync.NewHandler(backend))

	api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("slice34-cache-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)
	apiID := aws.ToString(api.GraphqlApi.ApiId)

	created, createErr := client.CreateApiCache(t.Context(), &appsyncsdk.CreateApiCacheInput{
		ApiId:               aws.String(apiID),
		ApiCachingBehavior:  appsynctypes.ApiCachingBehaviorFullRequestCaching,
		Ttl:                 300,
		Type:                appsynctypes.ApiCacheTypeSmall,
		HealthMetricsConfig: appsynctypes.CacheHealthMetricsConfigEnabled,
	})
	require.NoError(t, createErr)
	require.NotNil(t, created.ApiCache)
	assert.Equal(t, appsynctypes.CacheHealthMetricsConfigEnabled, created.ApiCache.HealthMetricsConfig)
	assert.Equal(t, int64(300), created.ApiCache.Ttl)

	got, getErr := client.GetApiCache(t.Context(), &appsyncsdk.GetApiCacheInput{ApiId: aws.String(apiID)})
	require.NoError(t, getErr)
	assert.Equal(t, appsynctypes.CacheHealthMetricsConfigEnabled, got.ApiCache.HealthMetricsConfig)

	updated, updateErr := client.UpdateApiCache(t.Context(), &appsyncsdk.UpdateApiCacheInput{
		ApiId:               aws.String(apiID),
		ApiCachingBehavior:  appsynctypes.ApiCachingBehaviorFullRequestCaching,
		Ttl:                 600,
		Type:                appsynctypes.ApiCacheTypeSmall,
		HealthMetricsConfig: appsynctypes.CacheHealthMetricsConfigDisabled,
	})
	require.NoError(t, updateErr)
	assert.Equal(t, int64(600), updated.ApiCache.Ttl)
	assert.Equal(t, appsynctypes.CacheHealthMetricsConfigDisabled, updated.ApiCache.HealthMetricsConfig)

	_, flushErr := client.FlushApiCache(t.Context(), &appsyncsdk.FlushApiCacheInput{ApiId: aws.String(apiID)})
	require.NoError(t, flushErr)

	_, delErr := client.DeleteApiCache(t.Context(), &appsyncsdk.DeleteApiCacheInput{ApiId: aws.String(apiID)})
	require.NoError(t, delErr)

	_, getAfterErr := client.GetApiCache(t.Context(), &appsyncsdk.GetApiCacheInput{ApiId: aws.String(apiID)})
	require.Error(t, getAfterErr, "cache must be gone after delete")
}

// TestFunctionLifecycle_RealClient drives GetFunction, ListFunctions, UpdateFunction
// and DeleteFunction through a real client.
func TestFunctionLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
	client := newTestAppsyncClient(t, appsync.NewHandler(backend))

	api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("slice34-fn-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)
	apiID := aws.ToString(api.GraphqlApi.ApiId)

	_, dsErr := client.CreateDataSource(t.Context(), &appsyncsdk.CreateDataSourceInput{
		ApiId: aws.String(apiID),
		Name:  aws.String("slice34ds"),
		Type:  appsynctypes.DataSourceTypeNone,
	})
	require.NoError(t, dsErr)

	created, createErr := client.CreateFunction(t.Context(), &appsyncsdk.CreateFunctionInput{
		ApiId:                   aws.String(apiID),
		Name:                    aws.String("slice34fn"),
		DataSourceName:          aws.String("slice34ds"),
		RequestMappingTemplate:  aws.String("{}"),
		ResponseMappingTemplate: aws.String("$util.toJson($ctx.result)"),
	})
	require.NoError(t, createErr)
	functionID := aws.ToString(created.FunctionConfiguration.FunctionId)
	require.NotEmpty(t, functionID)

	got, getErr := client.GetFunction(
		t.Context(),
		&appsyncsdk.GetFunctionInput{ApiId: aws.String(apiID), FunctionId: aws.String(functionID)},
	)
	require.NoError(t, getErr)
	assert.Equal(t, "slice34fn", aws.ToString(got.FunctionConfiguration.Name))

	listed, listErr := client.ListFunctions(t.Context(), &appsyncsdk.ListFunctionsInput{ApiId: aws.String(apiID)})
	require.NoError(t, listErr)
	require.Len(t, listed.Functions, 1)
	assert.Equal(t, functionID, aws.ToString(listed.Functions[0].FunctionId))

	updated, updateErr := client.UpdateFunction(t.Context(), &appsyncsdk.UpdateFunctionInput{
		ApiId:                   aws.String(apiID),
		FunctionId:              aws.String(functionID),
		Name:                    aws.String("slice34fn"),
		DataSourceName:          aws.String("slice34ds"),
		RequestMappingTemplate:  aws.String("{}"),
		ResponseMappingTemplate: aws.String("$util.toJson($ctx.result.updated)"),
	})
	require.NoError(t, updateErr)
	assert.Equal(
		t,
		"$util.toJson($ctx.result.updated)",
		aws.ToString(updated.FunctionConfiguration.ResponseMappingTemplate),
	)

	_, delErr := client.DeleteFunction(
		t.Context(),
		&appsyncsdk.DeleteFunctionInput{ApiId: aws.String(apiID), FunctionId: aws.String(functionID)},
	)
	require.NoError(t, delErr)

	_, getAfterErr := client.GetFunction(
		t.Context(),
		&appsyncsdk.GetFunctionInput{ApiId: aws.String(apiID), FunctionId: aws.String(functionID)},
	)
	require.Error(t, getAfterErr, "function must be gone after delete")
}

// TestTypeLifecycle_RealClient drives GetType, ListTypes, UpdateType and DeleteType
// through a real client.
func TestTypeLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
	client := newTestAppsyncClient(t, appsync.NewHandler(backend))

	api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("slice34-type-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)
	apiID := aws.ToString(api.GraphqlApi.ApiId)

	created, createErr := client.CreateType(t.Context(), &appsyncsdk.CreateTypeInput{
		ApiId:      aws.String(apiID),
		Format:     appsynctypes.TypeDefinitionFormatSdl,
		Definition: aws.String("type Widget { id: ID! }"),
	})
	require.NoError(t, createErr)
	typeName := aws.ToString(created.Type.Name)
	require.Equal(t, "Widget", typeName)

	got, getErr := client.GetType(t.Context(), &appsyncsdk.GetTypeInput{
		ApiId:    aws.String(apiID),
		TypeName: aws.String(typeName),
		Format:   appsynctypes.TypeDefinitionFormatSdl,
	})
	require.NoError(t, getErr)
	assert.Equal(t, "type Widget { id: ID! }", aws.ToString(got.Type.Definition))

	listed, listErr := client.ListTypes(t.Context(), &appsyncsdk.ListTypesInput{
		ApiId:  aws.String(apiID),
		Format: appsynctypes.TypeDefinitionFormatSdl,
	})
	require.NoError(t, listErr)
	require.Len(t, listed.Types, 1)
	assert.Equal(t, typeName, aws.ToString(listed.Types[0].Name))

	updated, updateErr := client.UpdateType(t.Context(), &appsyncsdk.UpdateTypeInput{
		ApiId:      aws.String(apiID),
		TypeName:   aws.String(typeName),
		Format:     appsynctypes.TypeDefinitionFormatSdl,
		Definition: aws.String("type Widget { id: ID! name: String }"),
	})
	require.NoError(t, updateErr)
	assert.Equal(t, "type Widget { id: ID! name: String }", aws.ToString(updated.Type.Definition))

	_, delErr := client.DeleteType(
		t.Context(),
		&appsyncsdk.DeleteTypeInput{ApiId: aws.String(apiID), TypeName: aws.String(typeName)},
	)
	require.NoError(t, delErr)

	_, getAfterErr := client.GetType(t.Context(), &appsyncsdk.GetTypeInput{
		ApiId:    aws.String(apiID),
		TypeName: aws.String(typeName),
		Format:   appsynctypes.TypeDefinitionFormatSdl,
	})
	require.Error(t, getAfterErr, "type must be gone after delete")
}

// TestDomainNameLifecycle_RealClient drives ListDomainNames, UpdateDomainName,
// AssociateApi, DisassociateApi and DeleteDomainName through a real client.
func TestDomainNameLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
	client := newTestAppsyncClient(t, appsync.NewHandler(backend))

	api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("slice34-domain-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)
	apiID := aws.ToString(api.GraphqlApi.ApiId)

	_, createErr := client.CreateDomainName(t.Context(), &appsyncsdk.CreateDomainNameInput{
		DomainName:     aws.String("slice34.example.com"),
		CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/slice34"),
	})
	require.NoError(t, createErr)

	listed, listErr := client.ListDomainNames(t.Context(), &appsyncsdk.ListDomainNamesInput{})
	require.NoError(t, listErr)

	var found bool
	for _, dn := range listed.DomainNameConfigs {
		if aws.ToString(dn.DomainName) == "slice34.example.com" {
			found = true
		}
	}
	assert.True(t, found, "created domain name must appear in ListDomainNames")

	updated, updateErr := client.UpdateDomainName(t.Context(), &appsyncsdk.UpdateDomainNameInput{
		DomainName:  aws.String("slice34.example.com"),
		Description: aws.String("slice34 updated description"),
	})
	require.NoError(t, updateErr)
	assert.Equal(t, "slice34 updated description", aws.ToString(updated.DomainNameConfig.Description))

	assoc, assocErr := client.AssociateApi(t.Context(), &appsyncsdk.AssociateApiInput{
		DomainName: aws.String("slice34.example.com"),
		ApiId:      aws.String(apiID),
	})
	require.NoError(t, assocErr)
	assert.Equal(t, apiID, aws.ToString(assoc.ApiAssociation.ApiId))

	_, disErr := client.DisassociateApi(
		t.Context(),
		&appsyncsdk.DisassociateApiInput{DomainName: aws.String("slice34.example.com")},
	)
	require.NoError(t, disErr)

	_, getAssocErr := client.GetApiAssociation(
		t.Context(),
		&appsyncsdk.GetApiAssociationInput{DomainName: aws.String("slice34.example.com")},
	)
	require.Error(t, getAssocErr, "api association must be gone after disassociate")

	_, delErr := client.DeleteDomainName(
		t.Context(),
		&appsyncsdk.DeleteDomainNameInput{DomainName: aws.String("slice34.example.com")},
	)
	require.NoError(t, delErr)

	_, getAfterErr := client.GetDomainName(
		t.Context(),
		&appsyncsdk.GetDomainNameInput{DomainName: aws.String("slice34.example.com")},
	)
	require.Error(t, getAfterErr, "domain name must be gone after delete")
}

// TestMergedAPIAssociationLifecycle_RealClient drives AssociateMergedGraphqlApi,
// DisassociateMergedGraphqlApi, AssociateSourceGraphqlApi,
// UpdateSourceApiAssociation, StartSchemaMerge and DisassociateSourceGraphqlApi
// through a real client.
func TestMergedAPIAssociationLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
	client := newTestAppsyncClient(t, appsync.NewHandler(backend))

	source, sErr := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("slice34-source-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, sErr)
	sourceID := aws.ToString(source.GraphqlApi.ApiId)

	merged, mErr := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("slice34-merged-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, mErr)
	mergedID := aws.ToString(merged.GraphqlApi.ApiId)
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "AssociateMergedGraphqlApi + DisassociateMergedGraphqlApi", run: func(t *testing.T) {
			t.Helper()

			assoc, assocErr := client.AssociateMergedGraphqlApi(t.Context(), &appsyncsdk.AssociateMergedGraphqlApiInput{
				SourceApiIdentifier: aws.String(sourceID),
				MergedApiIdentifier: aws.String(mergedID),
				Description:         aws.String("slice34 merge"),
			})
			require.NoError(t, assocErr)
			require.NotNil(t, assoc.SourceApiAssociation)
			assocID := aws.ToString(assoc.SourceApiAssociation.AssociationId)
			require.NotEmpty(t, assocID)
			assert.Equal(t, "slice34 merge", aws.ToString(assoc.SourceApiAssociation.Description))

			_, disErr := client.DisassociateMergedGraphqlApi(
				t.Context(),
				&appsyncsdk.DisassociateMergedGraphqlApiInput{
					SourceApiIdentifier: aws.String(sourceID),
					AssociationId:       aws.String(assocID),
				},
			)
			require.NoError(t, disErr)
		}},
		{
			name: "AssociateSourceGraphqlApi + UpdateSourceApiAssociation + StartSchemaMerge + DisassociateSourceGraphqlApi",
			run: func(t *testing.T) {
				t.Helper()

				assoc, assocErr := client.AssociateSourceGraphqlApi(
					t.Context(),
					&appsyncsdk.AssociateSourceGraphqlApiInput{
						MergedApiIdentifier: aws.String(mergedID),
						SourceApiIdentifier: aws.String(sourceID),
						Description:         aws.String("slice34 reverse merge"),
					},
				)
				require.NoError(t, assocErr)
				assocID := aws.ToString(assoc.SourceApiAssociation.AssociationId)
				require.NotEmpty(t, assocID)

				updated, updateErr := client.UpdateSourceApiAssociation(
					t.Context(),
					&appsyncsdk.UpdateSourceApiAssociationInput{
						MergedApiIdentifier: aws.String(mergedID),
						AssociationId:       aws.String(assocID),
						Description:         aws.String("slice34 reverse merge updated"),
					},
				)
				require.NoError(t, updateErr)
				assert.Equal(t, "slice34 reverse merge updated", aws.ToString(updated.SourceApiAssociation.Description))

				merge, mergeErr := client.StartSchemaMerge(t.Context(), &appsyncsdk.StartSchemaMergeInput{
					MergedApiIdentifier: aws.String(mergedID),
					AssociationId:       aws.String(assocID),
				})
				require.NoError(t, mergeErr)
				assert.Equal(t, appsynctypes.SourceApiAssociationStatusMergeSuccess, merge.SourceApiAssociationStatus)

				_, disErr := client.DisassociateSourceGraphqlApi(
					t.Context(),
					&appsyncsdk.DisassociateSourceGraphqlApiInput{
						MergedApiIdentifier: aws.String(mergedID),
						AssociationId:       aws.String(assocID),
					},
				)
				require.NoError(t, disErr)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestEvaluationOps_RealClient drives EvaluateCode and EvaluateMappingTemplate
// through a real client.
func TestEvaluationOps_RealClient(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
	client := newTestAppsyncClient(t, appsync.NewHandler(backend))
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "EvaluateMappingTemplate", run: func(t *testing.T) {
			t.Helper()

			out, err := client.EvaluateMappingTemplate(t.Context(), &appsyncsdk.EvaluateMappingTemplateInput{
				Template: aws.String(`{"version":"2018-05-29","payload":$util.toJson($context.arguments)}`),
				Context:  aws.String(`{"arguments":{"id":"42"}}`),
			})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.EvaluationResult), `"id":"42"`)
		}},
		{name: "EvaluateCode", run: func(t *testing.T) {
			t.Helper()

			code := `
export function request(ctx) {
  return { payload: ctx.arguments };
}
export function response(ctx) {
  return ctx.result;
}
`
			out, err := client.EvaluateCode(t.Context(), &appsyncsdk.EvaluateCodeInput{
				Code:     aws.String(code),
				Context:  aws.String(`{"arguments":{"id":"42"}}`),
				Function: aws.String("request"),
				Runtime: &appsynctypes.AppSyncRuntime{
					Name:           appsynctypes.RuntimeNameAppsyncJs,
					RuntimeVersion: aws.String("1.0.0"),
				},
			})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.EvaluationResult), `"id":"42"`)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestDataSourceIntrospectionLifecycle_RealClient drives StartDataSourceIntrospection
// and GetDataSourceIntrospection through a real client.
func TestDataSourceIntrospectionLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
	client := newTestAppsyncClient(t, appsync.NewHandler(backend))

	started, err := client.StartDataSourceIntrospection(t.Context(), &appsyncsdk.StartDataSourceIntrospectionInput{
		RdsDataApiConfig: &appsynctypes.RdsDataApiConfig{
			ResourceArn:  aws.String("arn:aws:rds:us-east-1:000000000000:cluster:slice34"),
			SecretArn:    aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:slice34"),
			DatabaseName: aws.String("slice34db"),
		},
	})
	require.NoError(t, err)
	introspectionID := aws.ToString(started.IntrospectionId)
	require.NotEmpty(t, introspectionID)

	got, getErr := client.GetDataSourceIntrospection(t.Context(), &appsyncsdk.GetDataSourceIntrospectionInput{
		IntrospectionId: aws.String(introspectionID),
	})
	require.NoError(t, getErr)
	assert.Equal(t, introspectionID, aws.ToString(got.IntrospectionId))
	assert.Equal(t, appsynctypes.DataSourceIntrospectionStatusSuccess, got.IntrospectionStatus)
}

// TestGraphqlApiEnvironmentVariables_RealClient drives
// PutGraphqlApiEnvironmentVariables and GetGraphqlApiEnvironmentVariables through a
// real client.
func TestGraphqlApiEnvironmentVariables_RealClient(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
	client := newTestAppsyncClient(t, appsync.NewHandler(backend))

	api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("slice34-envvar-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)
	apiID := aws.ToString(api.GraphqlApi.ApiId)

	put, putErr := client.PutGraphqlApiEnvironmentVariables(
		t.Context(),
		&appsyncsdk.PutGraphqlApiEnvironmentVariablesInput{
			ApiId:                aws.String(apiID),
			EnvironmentVariables: map[string]string{"STAGE": "prod"},
		},
	)
	require.NoError(t, putErr)
	assert.Equal(t, map[string]string{"STAGE": "prod"}, put.EnvironmentVariables)

	got, getErr := client.GetGraphqlApiEnvironmentVariables(
		t.Context(),
		&appsyncsdk.GetGraphqlApiEnvironmentVariablesInput{ApiId: aws.String(apiID)},
	)
	require.NoError(t, getErr)
	assert.Equal(t, map[string]string{"STAGE": "prod"}, got.EnvironmentVariables)
}

// TestGetIntrospectionSchema_RealClient drives GetIntrospectionSchema through a real
// client.
func TestGetIntrospectionSchema_RealClient(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
	client := newTestAppsyncClient(t, appsync.NewHandler(backend))

	api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("slice34-introspect-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)
	apiID := aws.ToString(api.GraphqlApi.ApiId)

	_, schemaErr := client.StartSchemaCreation(t.Context(), &appsyncsdk.StartSchemaCreationInput{
		ApiId:      aws.String(apiID),
		Definition: []byte("type Query { hello: String }"),
	})
	require.NoError(t, schemaErr)

	out, getErr := client.GetIntrospectionSchema(t.Context(), &appsyncsdk.GetIntrospectionSchemaInput{
		ApiId:  aws.String(apiID),
		Format: appsynctypes.OutputTypeSdl,
	})
	require.NoError(t, getErr)
	assert.Contains(t, string(out.Schema), "type Query")
}
