package apigateway_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// newDeploymentInvokeServer stands up the real aws-sdk-go-v2 apigateway client
// (for control-plane calls) alongside the raw invoke base URL (for data-plane
// requests against a deployed stage -- there is no typed SDK for invoking an
// arbitrary deployed REST API, only plain HTTP).
func newDeploymentInvokeServer(t *testing.T) (*apigwsdk.Client, string) {
	t.Helper()

	h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := apigwsdk.NewFromConfig(cfg, func(o *apigwsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return client, srv.URL
}

// invokeStage issues a real HTTP GET against the "prod" stage's invoke URL
// and returns its status code and body.
func invokeStage(t *testing.T, baseURL, apiID, path string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodGet, baseURL+"/restapis/"+apiID+"/prod/_user_request_"+path, nil,
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(body)
}

// putMockResponse (re)configures resourceID's GET MOCK integration to return
// body verbatim, so tests can tell "which deployment served this request"
// apart by the body they got back.
func putMockResponse(t *testing.T, client *apigwsdk.Client, apiID, resourceID, body string) {
	t.Helper()
	ctx := t.Context()

	_, err := client.PutIntegration(ctx, &apigwsdk.PutIntegrationInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(resourceID),
		HttpMethod: aws.String(http.MethodGet),
		Type:       apigwtypes.IntegrationTypeMock,
		RequestTemplates: map[string]string{
			"application/json": `{"statusCode": 200}`,
		},
	})
	require.NoError(t, err)

	_, err = client.PutIntegrationResponse(ctx, &apigwsdk.PutIntegrationResponseInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(resourceID),
		HttpMethod: aws.String(http.MethodGet),
		StatusCode: aws.String("200"),
		ResponseTemplates: map[string]string{
			"application/json": body,
		},
	})
	require.NoError(t, err)
}

// setupMockAPI creates a REST API with a single GET /widgets resource wired
// to a MOCK integration returning initialBody, returning the API and
// resource IDs.
func setupMockAPI(t *testing.T, client *apigwsdk.Client, initialBody string) (string, string) {
	t.Helper()
	ctx := t.Context()

	api, err := client.CreateRestApi(ctx, &apigwsdk.CreateRestApiInput{Name: aws.String("deploy-snapshot-api")})
	require.NoError(t, err)
	apiID := aws.ToString(api.Id)

	resources, err := client.GetResources(ctx, &apigwsdk.GetResourcesInput{RestApiId: api.Id})
	require.NoError(t, err)
	rootID := resources.Items[0].Id

	resource, err := client.CreateResource(ctx, &apigwsdk.CreateResourceInput{
		RestApiId: api.Id,
		ParentId:  rootID,
		PathPart:  aws.String("widgets"),
	})
	require.NoError(t, err)
	resourceID := aws.ToString(resource.Id)

	_, err = client.PutMethod(ctx, &apigwsdk.PutMethodInput{
		RestApiId:         api.Id,
		ResourceId:        resource.Id,
		HttpMethod:        aws.String(http.MethodGet),
		AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	putMockResponse(t, client, apiID, resourceID, initialBody)

	return apiID, resourceID
}

// TestDeploymentSnapshot_ServesCapturedConfig covers the core gap: the invoke
// path must read the resource/method/integration snapshot CreateDeployment
// captured, not the live configuration, and must pick up a new snapshot only
// after a fresh CreateDeployment.
func TestDeploymentSnapshot_ServesCapturedConfig(t *testing.T) {
	t.Parallel()

	client, baseURL := newDeploymentInvokeServer(t)
	ctx := t.Context()

	apiID, resourceID := setupMockAPI(t, client, "v1")

	depl1, err := client.CreateDeployment(ctx, &apigwsdk.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	status, body := invokeStage(t, baseURL, apiID, "/widgets")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "v1", body)

	// Change the integration response live, without redeploying: the stage
	// must keep serving the deployed snapshot's old behaviour.
	putMockResponse(t, client, apiID, resourceID, "v2")

	status, body = invokeStage(t, baseURL, apiID, "/widgets")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "v1", body, "live edits must not be visible until redeployed")

	// Redeploy: the new behaviour is now served.
	depl2, err := client.CreateDeployment(ctx, &apigwsdk.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	status, body = invokeStage(t, baseURL, apiID, "/widgets")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "v2", body, "redeploying must pick up the live edit")

	// Roll back to the first deployment via UpdateStage: old behaviour again.
	_, err = client.UpdateStage(ctx, &apigwsdk.UpdateStageInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpReplace, Path: aws.String("/deploymentId"), Value: depl1.Id},
		},
	})
	require.NoError(t, err)

	status, body = invokeStage(t, baseURL, apiID, "/widgets")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "v1", body, "UpdateStage deploymentId must roll back to the old snapshot")

	// Rolling forward again reaches the second deployment's snapshot.
	_, err = client.UpdateStage(ctx, &apigwsdk.UpdateStageInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpReplace, Path: aws.String("/deploymentId"), Value: depl2.Id},
		},
	})
	require.NoError(t, err)

	status, body = invokeStage(t, baseURL, apiID, "/widgets")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "v2", body)
}

// TestDeploymentSnapshot_StageWithNoDeployment covers CreateStage without a
// deployment, which the real CreateStage API also always requires -- there is
// no client-reachable way to leave deploymentId off, so this exercises the
// same 403 documented for an undeployed API/stage
// ("Why did I receive a 403 Missing Authentication Token error from an API
// Gateway API endpoint?" lists "you didn't deploy the API" as a cause) by
// asserting a never-deployed stage name simply isn't invocable.
func TestDeploymentSnapshot_StageWithNoDeployment(t *testing.T) {
	t.Parallel()

	client, baseURL := newDeploymentInvokeServer(t)

	apiID, _ := setupMockAPI(t, client, "v1")

	status, _ := invokeStage(t, baseURL, apiID, "/widgets")
	assert.Equal(t, http.StatusForbidden, status)
}

// TestDeploymentSnapshot_StageVariables covers stage-variable interpolation
// in an integration URI, which is resolved from the stage's live Variables
// (not the deployment snapshot -- stage variables are stage state, not part
// of what CreateDeployment captures) at invoke time.
func TestDeploymentSnapshot_StageVariables(t *testing.T) {
	t.Parallel()

	client, baseURL := newDeploymentInvokeServer(t)
	ctx := t.Context()

	upstreamA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("upstream-a"))
	}))
	t.Cleanup(upstreamA.Close)

	upstreamB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("upstream-b"))
	}))
	t.Cleanup(upstreamB.Close)

	api, err := client.CreateRestApi(ctx, &apigwsdk.CreateRestApiInput{Name: aws.String("stagevar-api")})
	require.NoError(t, err)
	apiID := aws.ToString(api.Id)

	resources, err := client.GetResources(ctx, &apigwsdk.GetResourcesInput{RestApiId: api.Id})
	require.NoError(t, err)
	rootID := resources.Items[0].Id

	resource, err := client.CreateResource(ctx, &apigwsdk.CreateResourceInput{
		RestApiId: api.Id,
		ParentId:  rootID,
		PathPart:  aws.String("proxy"),
	})
	require.NoError(t, err)

	_, err = client.PutMethod(ctx, &apigwsdk.PutMethodInput{
		RestApiId:         api.Id,
		ResourceId:        resource.Id,
		HttpMethod:        aws.String(http.MethodGet),
		AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	_, err = client.PutIntegration(ctx, &apigwsdk.PutIntegrationInput{
		RestApiId:             api.Id,
		ResourceId:            resource.Id,
		HttpMethod:            aws.String(http.MethodGet),
		Type:                  apigwtypes.IntegrationTypeHttpProxy,
		IntegrationHttpMethod: aws.String(http.MethodGet),
		Uri:                   aws.String("${stageVariables.upstream}"),
	})
	require.NoError(t, err)

	_, err = client.CreateDeployment(ctx, &apigwsdk.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
		Variables: map[string]string{"upstream": upstreamA.URL},
	})
	require.NoError(t, err)

	status, body := invokeStage(t, baseURL, apiID, "/proxy")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "upstream-a", body)

	// Changing the stage variable takes effect immediately -- no redeploy
	// needed, since stage variables are stage state, not deployment state.
	_, err = client.UpdateStage(ctx, &apigwsdk.UpdateStageInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpReplace, Path: aws.String("/variables/upstream"), Value: aws.String(upstreamB.URL)},
		},
	})
	require.NoError(t, err)

	status, body = invokeStage(t, baseURL, apiID, "/proxy")
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "upstream-b", body)
}
