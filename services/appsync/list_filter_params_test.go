package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/require"
)

// TestListGraphqlApis_OwnerFilter proves the owner query parameter is
// honored. gopherstack simulates a single AWS account, so every API is
// CURRENT_ACCOUNT; filtering for OTHER_ACCOUNTS must return none, but
// listGraphqlAPIs (handler_graphql_apis.go) never read the owner query
// parameter at all.
func TestListGraphqlApis_OwnerFilter(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)
	ctx := t.Context()

	_, err := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("api-a"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	out, err := client.ListGraphqlApis(ctx, &appsyncsdk.ListGraphqlApisInput{
		Owner: appsynctypes.OwnershipOtherAccounts,
	})
	require.NoError(t, err)
	require.Empty(t, out.GraphqlApis)
}

// TestListResolversByFunction_Pagination proves MaxResults/NextToken are
// honored. listResolversByFunction (handler_resolvers.go) called the
// backend and returned every resolver in one page, ignoring both query
// parameters entirely.
func TestListResolversByFunction_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)
	ctx := t.Context()

	api, err := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("api-a"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	apiID := api.GraphqlApi.ApiId

	ds, err := client.CreateDataSource(ctx, &appsyncsdk.CreateDataSourceInput{
		ApiId: apiID,
		Name:  aws.String("ds-a"),
		Type:  appsynctypes.DataSourceTypeNone,
	})
	require.NoError(t, err)

	fn, err := client.CreateFunction(ctx, &appsyncsdk.CreateFunctionInput{
		ApiId:          apiID,
		Name:           aws.String("fn-a"),
		DataSourceName: ds.DataSource.Name,
	})
	require.NoError(t, err)

	for _, field := range []string{"fieldA", "fieldB", "fieldC"} {
		_, resolverErr := client.CreateResolver(ctx, &appsyncsdk.CreateResolverInput{
			ApiId:     apiID,
			TypeName:  aws.String("Query"),
			FieldName: aws.String(field),
			Kind:      appsynctypes.ResolverKindPipeline,
			PipelineConfig: &appsynctypes.PipelineConfig{
				Functions: []string{aws.ToString(fn.FunctionConfiguration.FunctionId)},
			},
		})
		require.NoError(t, resolverErr)
	}

	out, err := client.ListResolversByFunction(ctx, &appsyncsdk.ListResolversByFunctionInput{
		ApiId:      apiID,
		FunctionId: fn.FunctionConfiguration.FunctionId,
		MaxResults: 2,
	})
	require.NoError(t, err)
	require.Len(t, out.Resolvers, 2)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListSourceApiAssociations_Pagination proves MaxResults/NextToken are
// honored. listSourceAPIAssociations (handler_source_api_associations.go)
// never read either query parameter.
func TestListSourceApiAssociations_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)
	ctx := t.Context()

	merged, err := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("merged-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
		ApiType:            appsynctypes.GraphQLApiTypeMerged,
	})
	require.NoError(t, err)

	for _, name := range []string{"source-a", "source-b", "source-c"} {
		src, srcErr := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
			Name:               aws.String(name),
			AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
		})
		require.NoError(t, srcErr)

		_, assocErr := client.AssociateSourceGraphqlApi(ctx, &appsyncsdk.AssociateSourceGraphqlApiInput{
			MergedApiIdentifier: merged.GraphqlApi.ApiId,
			SourceApiIdentifier: src.GraphqlApi.ApiId,
		})
		require.NoError(t, assocErr)
	}

	out, err := client.ListSourceApiAssociations(ctx, &appsyncsdk.ListSourceApiAssociationsInput{
		ApiId:      merged.GraphqlApi.ApiId,
		MaxResults: 2,
	})
	require.NoError(t, err)
	require.Len(t, out.SourceApiAssociationSummaries, 2)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestListTypesByAssociation_Pagination proves MaxResults/NextToken are
// honored. listTypesByAssociation (handler_schema_types.go) called the
// backend and returned every type on one page, ignoring both query
// parameters entirely.
func TestListTypesByAssociation_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)
	ctx := t.Context()

	merged, err := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("merged-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
		ApiType:            appsynctypes.GraphQLApiTypeMerged,
	})
	require.NoError(t, err)

	src, err := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("source-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	assoc, err := client.AssociateSourceGraphqlApi(ctx, &appsyncsdk.AssociateSourceGraphqlApiInput{
		MergedApiIdentifier: merged.GraphqlApi.ApiId,
		SourceApiIdentifier: src.GraphqlApi.ApiId,
	})
	require.NoError(t, err)

	for _, name := range []string{"TypeA", "TypeB", "TypeC"} {
		_, typeErr := client.CreateType(ctx, &appsyncsdk.CreateTypeInput{
			ApiId:      merged.GraphqlApi.ApiId,
			Definition: aws.String("type " + name + " { id: ID }"),
			Format:     appsynctypes.TypeDefinitionFormatSdl,
		})
		require.NoError(t, typeErr)
	}

	out, err := client.ListTypesByAssociation(ctx, &appsyncsdk.ListTypesByAssociationInput{
		MergedApiIdentifier: merged.GraphqlApi.ApiId,
		AssociationId:       assoc.SourceApiAssociation.AssociationId,
		Format:              appsynctypes.TypeDefinitionFormatSdl,
		MaxResults:          2,
	})
	require.NoError(t, err)
	require.Len(t, out.Types, 2)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}
