package appsync_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// TestGetApiAssociation_NoAssociation_NotFound proves GetApiAssociation
// returns NotFoundException, matching real AWS, when a domain name exists
// but has no API association -- not a 200 body carrying a synthetic
// AssociationStatus. Real ApiAssociation.AssociationStatus is
// types.AssociationStatus (PROCESSING/FAILED/SUCCESS only, appsync@v1.56.4
// types/enums.go:96); pre-fix, gopherstack fabricated "NOT_FOUND", a value
// no member of that enum names.
func TestGetApiAssociation_NoAssociation_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)

	_, err := client.CreateDomainName(t.Context(), &appsyncsdk.CreateDomainNameInput{
		DomainName:     aws.String("no-assoc.example.com"),
		CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/abc"),
	})
	require.NoError(t, err)

	_, err = client.GetApiAssociation(t.Context(), &appsyncsdk.GetApiAssociationInput{
		DomainName: aws.String("no-assoc.example.com"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
}

// TestSourceApiAssociation_StatusWireKey proves SourceApiAssociation's status
// field round-trips through the real SDK client. Before the fix, the wire key
// was "associationStatus" (copied from the similarly-named but genuinely-
// different ApiAssociation type) instead of the real "sourceApiAssociationStatus"
// -- a real client's typed SourceApiAssociationStatus field was always empty.
func TestSourceApiAssociation_StatusWireKey(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)

	src, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("source-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	merged, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("merged-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
		ApiType:            appsynctypes.GraphQLApiTypeMerged,
	})
	require.NoError(t, err)

	assocOut, err := client.AssociateSourceGraphqlApi(t.Context(), &appsyncsdk.AssociateSourceGraphqlApiInput{
		MergedApiIdentifier: merged.GraphqlApi.ApiId,
		SourceApiIdentifier: src.GraphqlApi.ApiId,
		Description:         aws.String("test association"),
	})
	require.NoError(t, err)
	require.NotNil(t, assocOut.SourceApiAssociation)
	assert.Equal(t, appsynctypes.SourceApiAssociationStatusMergeScheduled,
		assocOut.SourceApiAssociation.SourceApiAssociationStatus)

	getOut, err := client.GetSourceApiAssociation(t.Context(), &appsyncsdk.GetSourceApiAssociationInput{
		MergedApiIdentifier: merged.GraphqlApi.ApiId,
		AssociationId:       assocOut.SourceApiAssociation.AssociationId,
	})
	require.NoError(t, err)
	assert.Equal(t, appsynctypes.SourceApiAssociationStatusMergeScheduled,
		getOut.SourceApiAssociation.SourceApiAssociationStatus)

	// NOTE: the real SourceApiAssociationSummary item type (used by
	// ListSourceApiAssociations) has no status field at all (verified against
	// deserializers.go's awsRestjson1_deserializeDocumentSourceApiAssociationSummary
	// case list: associationArn/associationId/description/mergedApiArn/
	// mergedApiId/sourceApiArn/sourceApiId only) -- so status is only checked
	// via Associate/Get above, matching what a real client can actually see.
	listOut, err := client.ListSourceApiAssociations(t.Context(), &appsyncsdk.ListSourceApiAssociationsInput{
		ApiId: merged.GraphqlApi.ApiId,
	})
	require.NoError(t, err)
	require.Len(t, listOut.SourceApiAssociationSummaries, 1)
	assert.Equal(t, assocOut.SourceApiAssociation.AssociationId, listOut.SourceApiAssociationSummaries[0].AssociationId)
}

// TestGraphqlApi_EnvironmentVariablesNotLeaked proves that setting environment
// variables via PutGraphqlApiEnvironmentVariables does not leak their values
// into GetGraphqlApi/ListGraphqlApis/UpdateGraphqlApi -- the real GraphqlApi
// type has no "environmentVariables" member at all (verified against
// deserializers.go's awsRestjson1_deserializeDocumentGraphqlApi case list).
// A typed SDK client can never observe the leak directly (unknown JSON keys
// are silently dropped on decode), so this checks the raw wire body via
// doRequest, the only way to prove the key's absence.
func TestGraphqlApi_EnvironmentVariablesNotLeaked(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()

	api, err := b.CreateGraphqlAPI(
		"envvar-api", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil,
	)
	require.NoError(t, err)

	_, err = b.PutGraphqlAPIEnvironmentVariables(api.APIID, map[string]string{
		"DB_PASSWORD": "super-secret-value",
	})
	require.NoError(t, err)

	rec := doRequest(t, h, "GET", "/v1/apis/"+api.APIID, nil)
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	graphqlAPI, ok := resp["graphqlApi"].(map[string]any)
	require.True(t, ok)

	_, leaked := graphqlAPI["environmentVariables"]
	assert.False(t, leaked, "environmentVariables must not appear on the GraphqlApi wire object")

	// The dedicated op is still the correct, real way to read them back.
	envRec := doRequest(t, h, "GET", "/v1/apis/"+api.APIID+"/environmentVariables", nil)
	require.Equal(t, 200, envRec.Code)

	var envResp map[string]any
	require.NoError(t, json.NewDecoder(envRec.Body).Decode(&envResp))
	envVars, ok := envResp["environmentVariables"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "super-secret-value", envVars["DB_PASSWORD"])
}

// TestGraphqlApi_Owner proves the real "owner" member (account owner of the
// GraphQL API) is populated -- previously entirely unmodeled.
func TestGraphqlApi_Owner(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)

	out, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("owner-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)
	assert.Equal(t, "000000000000", aws.ToString(out.GraphqlApi.Owner))

	getOut, err := client.GetGraphqlApi(t.Context(), &appsyncsdk.GetGraphqlApiInput{
		ApiId: out.GraphqlApi.ApiId,
	})
	require.NoError(t, err)
	assert.Equal(t, "000000000000", aws.ToString(getOut.GraphqlApi.Owner))
}

// TestEventApi_LogConfigRoundTrip proves EventConfig.LogConfig round-trips
// through CreateApi/GetApi. Previously entirely unmodeled: gopherstack's
// EventConfig struct had no field for it at all, so a real client's
// CreateApi/UpdateApi LogConfig value was silently dropped by json.Unmarshal.
func TestEventApi_LogConfigRoundTrip(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)

	authMode := []appsynctypes.AuthMode{{AuthType: appsynctypes.AuthenticationTypeApiKey}}

	out, err := client.CreateApi(t.Context(), &appsyncsdk.CreateApiInput{
		Name: aws.String("log-config-api"),
		EventConfig: &appsynctypes.EventConfig{
			AuthProviders:             []appsynctypes.AuthProvider{{AuthType: appsynctypes.AuthenticationTypeApiKey}},
			ConnectionAuthModes:       authMode,
			DefaultPublishAuthModes:   authMode,
			DefaultSubscribeAuthModes: authMode,
			LogConfig: &appsynctypes.EventLogConfig{
				CloudWatchLogsRoleArn: aws.String("arn:aws:iam::000000000000:role/appsync-event-logs"),
				LogLevel:              appsynctypes.EventLogLevelInfo,
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.Api.EventConfig.LogConfig)
	assert.Equal(t, "arn:aws:iam::000000000000:role/appsync-event-logs",
		aws.ToString(out.Api.EventConfig.LogConfig.CloudWatchLogsRoleArn))
	assert.Equal(t, appsynctypes.EventLogLevelInfo, out.Api.EventConfig.LogConfig.LogLevel)

	getOut, err := client.GetApi(t.Context(), &appsyncsdk.GetApiInput{ApiId: out.Api.ApiId})
	require.NoError(t, err)
	require.NotNil(t, getOut.Api.EventConfig.LogConfig)
	assert.Equal(t, "arn:aws:iam::000000000000:role/appsync-event-logs",
		aws.ToString(getOut.Api.EventConfig.LogConfig.CloudWatchLogsRoleArn))
	assert.Equal(t, appsynctypes.EventLogLevelInfo, getOut.Api.EventConfig.LogConfig.LogLevel)
}

// TestDataSource_MetricsConfigRoundTrip proves DataSource.MetricsConfig
// round-trips through Create/Update/Get -- previously entirely unmodeled, a
// real client's value was silently dropped on both ops.
func TestDataSource_MetricsConfigRoundTrip(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)

	api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("metrics-ds-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	createOut, err := client.CreateDataSource(t.Context(), &appsyncsdk.CreateDataSourceInput{
		ApiId:         api.GraphqlApi.ApiId,
		Name:          aws.String("ds1"),
		Type:          appsynctypes.DataSourceTypeNone,
		MetricsConfig: appsynctypes.DataSourceLevelMetricsConfigEnabled,
	})
	require.NoError(t, err)
	assert.Equal(t, appsynctypes.DataSourceLevelMetricsConfigEnabled, createOut.DataSource.MetricsConfig)

	getOut, err := client.GetDataSource(t.Context(), &appsyncsdk.GetDataSourceInput{
		ApiId: api.GraphqlApi.ApiId,
		Name:  aws.String("ds1"),
	})
	require.NoError(t, err)
	assert.Equal(t, appsynctypes.DataSourceLevelMetricsConfigEnabled, getOut.DataSource.MetricsConfig)

	updateOut, err := client.UpdateDataSource(t.Context(), &appsyncsdk.UpdateDataSourceInput{
		ApiId:         api.GraphqlApi.ApiId,
		Name:          aws.String("ds1"),
		Type:          appsynctypes.DataSourceTypeNone,
		MetricsConfig: appsynctypes.DataSourceLevelMetricsConfigDisabled,
	})
	require.NoError(t, err)
	assert.Equal(t, appsynctypes.DataSourceLevelMetricsConfigDisabled, updateOut.DataSource.MetricsConfig)
}

// TestResolver_MetricsConfigRoundTrip proves Resolver.MetricsConfig
// round-trips through Create/Update/Get -- previously entirely unmodeled.
func TestResolver_MetricsConfigRoundTrip(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)

	api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
		Name:               aws.String("metrics-resolver-api"),
		AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	_, err = client.CreateDataSource(t.Context(), &appsyncsdk.CreateDataSourceInput{
		ApiId: api.GraphqlApi.ApiId,
		Name:  aws.String("ds1"),
		Type:  appsynctypes.DataSourceTypeNone,
	})
	require.NoError(t, err)

	createOut, err := client.CreateResolver(t.Context(), &appsyncsdk.CreateResolverInput{
		ApiId:          api.GraphqlApi.ApiId,
		TypeName:       aws.String("Query"),
		FieldName:      aws.String("getThing"),
		DataSourceName: aws.String("ds1"),
		MetricsConfig:  appsynctypes.ResolverLevelMetricsConfigEnabled,
	})
	require.NoError(t, err)
	assert.Equal(t, appsynctypes.ResolverLevelMetricsConfigEnabled, createOut.Resolver.MetricsConfig)

	getOut, err := client.GetResolver(t.Context(), &appsyncsdk.GetResolverInput{
		ApiId:     api.GraphqlApi.ApiId,
		TypeName:  aws.String("Query"),
		FieldName: aws.String("getThing"),
	})
	require.NoError(t, err)
	assert.Equal(t, appsynctypes.ResolverLevelMetricsConfigEnabled, getOut.Resolver.MetricsConfig)

	updateOut, err := client.UpdateResolver(t.Context(), &appsyncsdk.UpdateResolverInput{
		ApiId:          api.GraphqlApi.ApiId,
		TypeName:       aws.String("Query"),
		FieldName:      aws.String("getThing"),
		DataSourceName: aws.String("ds1"),
		MetricsConfig:  appsynctypes.ResolverLevelMetricsConfigDisabled,
	})
	require.NoError(t, err)
	assert.Equal(t, appsynctypes.ResolverLevelMetricsConfigDisabled, updateOut.Resolver.MetricsConfig)
}
