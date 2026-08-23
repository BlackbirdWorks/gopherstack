package opensearch_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/opensearchserverless"
	aosstypes "github.com/aws/aws-sdk-go-v2/service/opensearchserverless/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// newTestServerlessClient stands up the real aws-sdk-go-v2
// opensearchserverless client against an httptest server running this
// package's Handler, wired through the same pkgs/service registry/router
// used in production. opensearchserverless is a separate, JSON-RPC 1.0 SDK
// client from classic OpenSearch's REST-JSON one: it always POSTs to "/"
// with an X-Amz-Target header (see handler_serverless_jsonrpc.go's
// openSearchServerlessTargetPrefix comment for the serializers.go
// citation). Routing this through RouteMatcher, rather than calling
// h.Handle(c) directly, is the point -- RouteMatcher is what a real
// client's request has to pass before dispatch is even reached
// (gopherstack-92ft).
func newTestServerlessClient(t *testing.T, h *opensearch.Handler) *opensearchserverless.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return opensearchserverless.NewFromConfig(cfg, func(o *opensearchserverless.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func testServerlessHandler(t *testing.T) *opensearch.Handler {
	t.Helper()

	return opensearch.NewHandler(opensearch.NewInMemoryBackend("000000000000", "us-east-1"))
}

// decodeDocument unmarshals a response Policy field (a smithy document.Interface,
// AOSS's open-content type for arbitrary policy JSON -- distinct from the
// *string the CreateSecurityPolicy/CreateAccessPolicy *request* types use)
// back to its JSON text so tests can assert on policy content.
func decodeDocument(t *testing.T, doc interface{ UnmarshalSmithyDocument(v any) error }) string {
	t.Helper()

	var v any
	require.NoError(t, doc.UnmarshalSmithyDocument(&v))
	b, err := json.Marshal(v)
	require.NoError(t, err)

	return string(b)
}

// TestServerless_RealSDKClient_Collections drives collection ops through
// the real opensearchserverless client. Before this fix, RouteMatcher
// required the request path to start with one of openSearchPathPrefixes
// (the fabricated "/2021-11-01/opensearch/serverless/..." REST path); the
// real client always POSTs to "/", so no real request ever matched.
func TestServerless_RealSDKClient_Collections(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	created, err := client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
		Name: aws.String("sdk-collection"),
		Type: aosstypes.CollectionTypeVectorsearch,
		Tags: []aosstypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)
	require.NotNil(t, created.CreateCollectionDetail)
	assert.Equal(t, "sdk-collection", aws.ToString(created.CreateCollectionDetail.Name))
	id := aws.ToString(created.CreateCollectionDetail.Id)
	require.NotEmpty(t, id)

	batch, err := client.BatchGetCollection(t.Context(), &opensearchserverless.BatchGetCollectionInput{
		Ids: []string{id},
	})
	require.NoError(t, err)
	require.Len(t, batch.CollectionDetails, 1)
	assert.Equal(t, "sdk-collection", aws.ToString(batch.CollectionDetails[0].Name))

	listed, err := client.ListCollections(t.Context(), &opensearchserverless.ListCollectionsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, listed.CollectionSummaries)

	_, err = client.DeleteCollection(t.Context(), &opensearchserverless.DeleteCollectionInput{
		Id: aws.String(id),
	})
	require.NoError(t, err)
}

// TestServerless_RealSDKClient_DeleteCollection_NotFound confirms
// DeleteCollection on an unknown id maps to the real AOSS 404
// ResourceNotFoundException. Before this fix, DeleteServerlessCollection's
// ErrDomainNotFound sentinel was absent from serverlessErrorTable, so
// awserr.Classify fell through to the 500 InternalServerException fallback.
func TestServerless_RealSDKClient_DeleteCollection_NotFound(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	_, err := client.DeleteCollection(t.Context(), &opensearchserverless.DeleteCollectionInput{
		Id: aws.String("nonexistent-id"),
	})
	require.Error(t, err)

	var nf *aosstypes.ResourceNotFoundException
	require.ErrorAsf(t, err, &nf, "got error: %v", err)
}

// TestServerless_RealSDKClient_AccessPolicy drives the full AccessPolicy
// CRUD family through the real client.
func TestServerless_RealSDKClient_AccessPolicy(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	created, err := client.CreateAccessPolicy(t.Context(), &opensearchserverless.CreateAccessPolicyInput{
		Name:   aws.String("sdk-access-policy"),
		Type:   aosstypes.AccessPolicyTypeData,
		Policy: aws.String(`[{"Rules":[]}]`),
	})
	require.NoError(t, err)
	assert.Equal(t, "sdk-access-policy", aws.ToString(created.AccessPolicyDetail.Name))

	got, err := client.GetAccessPolicy(t.Context(), &opensearchserverless.GetAccessPolicyInput{
		Name: aws.String("sdk-access-policy"),
		Type: aosstypes.AccessPolicyTypeData,
	})
	require.NoError(t, err)
	assert.JSONEq(t, `[{"Rules":[]}]`, decodeDocument(t, got.AccessPolicyDetail.Policy))

	listed, err := client.ListAccessPolicies(t.Context(), &opensearchserverless.ListAccessPoliciesInput{
		Type: aosstypes.AccessPolicyTypeData,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, listed.AccessPolicySummaries)

	updated, err := client.UpdateAccessPolicy(t.Context(), &opensearchserverless.UpdateAccessPolicyInput{
		Name:          aws.String("sdk-access-policy"),
		Type:          aosstypes.AccessPolicyTypeData,
		Policy:        aws.String(`[{"Rules":[],"Description":"updated"}]`),
		PolicyVersion: got.AccessPolicyDetail.PolicyVersion,
	})
	require.NoError(t, err)
	assert.Contains(t, decodeDocument(t, updated.AccessPolicyDetail.Policy), "updated")

	_, err = client.DeleteAccessPolicy(t.Context(), &opensearchserverless.DeleteAccessPolicyInput{
		Name: aws.String("sdk-access-policy"),
		Type: aosstypes.AccessPolicyTypeData,
	})
	require.NoError(t, err)

	_, err = client.GetAccessPolicy(t.Context(), &opensearchserverless.GetAccessPolicyInput{
		Name: aws.String("sdk-access-policy"),
		Type: aosstypes.AccessPolicyTypeData,
	})
	require.Error(t, err)

	var nf *aosstypes.ResourceNotFoundException
	assert.ErrorAs(t, err, &nf)
}

// TestServerless_RealSDKClient_SecurityConfig drives the full
// SecurityConfig CRUD family through the real client.
func TestServerless_RealSDKClient_SecurityConfig(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	created, err := client.CreateSecurityConfig(t.Context(), &opensearchserverless.CreateSecurityConfigInput{
		Name: aws.String("sdk-security-config"),
		Type: aosstypes.SecurityConfigTypeSaml,
		SamlOptions: &aosstypes.SamlConfigOptions{
			Metadata:       aws.String("<EntityDescriptor/>"),
			UserAttribute:  aws.String("email"),
			GroupAttribute: aws.String("group"),
		},
	})
	require.NoError(t, err)
	id := aws.ToString(created.SecurityConfigDetail.Id)
	require.NotEmpty(t, id)
	assert.Equal(t, "email", aws.ToString(created.SecurityConfigDetail.SamlOptions.UserAttribute))

	got, err := client.GetSecurityConfig(t.Context(), &opensearchserverless.GetSecurityConfigInput{
		Id: aws.String(id),
	})
	require.NoError(t, err)
	assert.Equal(t, id, aws.ToString(got.SecurityConfigDetail.Id))

	listed, err := client.ListSecurityConfigs(t.Context(), &opensearchserverless.ListSecurityConfigsInput{
		Type: aosstypes.SecurityConfigTypeSaml,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, listed.SecurityConfigSummaries)

	updated, err := client.UpdateSecurityConfig(t.Context(), &opensearchserverless.UpdateSecurityConfigInput{
		Id:            aws.String(id),
		Description:   aws.String("updated description"),
		ConfigVersion: got.SecurityConfigDetail.ConfigVersion,
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", aws.ToString(updated.SecurityConfigDetail.Description))

	_, err = client.DeleteSecurityConfig(t.Context(), &opensearchserverless.DeleteSecurityConfigInput{
		Id: aws.String(id),
	})
	require.NoError(t, err)
}

// TestServerless_RealSDKClient_SecurityPolicy drives the SecurityPolicy
// family through the real client for both real discriminator values,
// "encryption" and "network" -- a single real op family, unlike the
// fabricated REST path's separate encryptionpolicies/networksecuritypolicies
// routes (see handler_serverless_jsonrpc.go's serverlessSecurityPolicyCRUD
// doc comment). GetSecurityPolicy/UpdateSecurityPolicy have no backend
// support for type=="network" (a pre-existing gap, not introduced by this
// fix -- the fabricated REST path has the identical gap), so those two
// assert the honest ValidationException rather than silent success.
func TestServerless_RealSDKClient_SecurityPolicy(t *testing.T) {
	t.Parallel()

	t.Run("encryption", func(t *testing.T) {
		t.Parallel()

		h := testServerlessHandler(t)
		client := newTestServerlessClient(t, h)

		created, err := client.CreateSecurityPolicy(t.Context(), &opensearchserverless.CreateSecurityPolicyInput{
			Name:   aws.String("sdk-encryption-policy"),
			Type:   aosstypes.SecurityPolicyTypeEncryption,
			Policy: aws.String(`{"Rules":[],"AWSOwnedKey":true}`),
		})
		require.NoError(t, err)
		assert.Equal(t, "sdk-encryption-policy", aws.ToString(created.SecurityPolicyDetail.Name))

		got, err := client.GetSecurityPolicy(t.Context(), &opensearchserverless.GetSecurityPolicyInput{
			Name: aws.String("sdk-encryption-policy"),
			Type: aosstypes.SecurityPolicyTypeEncryption,
		})
		require.NoError(t, err)

		listed, err := client.ListSecurityPolicies(t.Context(), &opensearchserverless.ListSecurityPoliciesInput{
			Type: aosstypes.SecurityPolicyTypeEncryption,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, listed.SecurityPolicySummaries)

		updated, err := client.UpdateSecurityPolicy(t.Context(), &opensearchserverless.UpdateSecurityPolicyInput{
			Name:          aws.String("sdk-encryption-policy"),
			Type:          aosstypes.SecurityPolicyTypeEncryption,
			Policy:        aws.String(`{"Rules":[],"AWSOwnedKey":false}`),
			PolicyVersion: got.SecurityPolicyDetail.PolicyVersion,
		})
		require.NoError(t, err)
		assert.Contains(t, decodeDocument(t, updated.SecurityPolicyDetail.Policy), "AWSOwnedKey")

		_, err = client.DeleteSecurityPolicy(t.Context(), &opensearchserverless.DeleteSecurityPolicyInput{
			Name: aws.String("sdk-encryption-policy"),
			Type: aosstypes.SecurityPolicyTypeEncryption,
		})
		require.NoError(t, err)
	})

	t.Run("network", func(t *testing.T) {
		t.Parallel()

		h := testServerlessHandler(t)
		client := newTestServerlessClient(t, h)

		created, err := client.CreateSecurityPolicy(t.Context(), &opensearchserverless.CreateSecurityPolicyInput{
			Name:   aws.String("sdk-network-policy"),
			Type:   aosstypes.SecurityPolicyTypeNetwork,
			Policy: aws.String(`[{"Rules":[]}]`),
		})
		require.NoError(t, err)
		assert.Equal(t, "sdk-network-policy", aws.ToString(created.SecurityPolicyDetail.Name))

		listed, err := client.ListSecurityPolicies(t.Context(), &opensearchserverless.ListSecurityPoliciesInput{
			Type: aosstypes.SecurityPolicyTypeNetwork,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, listed.SecurityPolicySummaries)

		_, err = client.GetSecurityPolicy(t.Context(), &opensearchserverless.GetSecurityPolicyInput{
			Name: aws.String("sdk-network-policy"),
			Type: aosstypes.SecurityPolicyTypeNetwork,
		})
		require.Error(t, err)
		var valErr *aosstypes.ValidationException
		require.ErrorAs(t, err, &valErr)

		_, err = client.DeleteSecurityPolicy(t.Context(), &opensearchserverless.DeleteSecurityPolicyInput{
			Name: aws.String("sdk-network-policy"),
			Type: aosstypes.SecurityPolicyTypeNetwork,
		})
		require.NoError(t, err)
	})
}
