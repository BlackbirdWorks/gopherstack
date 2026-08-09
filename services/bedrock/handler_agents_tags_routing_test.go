package bedrock_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// newTestBedrockRegistryServer wires up core Bedrock's Handler and its
// AgentsHandler exactly as production does (provider.go: two separate
// InMemoryBackend instances, both registered into the same service.Registry
// and dispatched through service.NewServiceRouter). Unlike doAgentRequest,
// requests here are routed by RouteMatcher/MatchPriority, not delivered
// straight to AgentsHandler.Handler().
func newTestBedrockRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	coreHandler := bedrock.NewHandler(bedrock.NewInMemoryBackend("000000000000", "us-east-1"))
	agentsHandler := bedrock.NewAgentsHandler(bedrock.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, registry.Register(coreHandler))
	require.NoError(t, registry.Register(agentsHandler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func newTestBedrockAgentSDKClient(t *testing.T, baseURL string) *bedrockagentsdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return bedrockagentsdk.NewFromConfig(cfg, func(o *bedrockagentsdk.Options) {
		o.BaseEndpoint = aws.String(baseURL)
	})
}

// TestFlowTags_RouteThroughRouteMatcher proves /tags/{flowArn} reaches
// AgentsHandler through the real routing path (service.NewServiceRouter,
// same as production's registry wiring), not just when the handler is
// invoked directly. isBedrockAgentArn (handler_agents_dispatch.go) used to
// claim a /tags/ path only when the ARN literally contained ":bedrock-agent:"
// -- true for flow/prompt ARNs only because they were wrongly built that way.
// Once FlowArn correctly uses the "bedrock" service segment
// (gopherstack-esia), that check no longer matches and TagResource for a
// real flow ARN 404s. This test creates the flow through the router (not
// bedrock.NewAgentsHandler(...).Handler() directly), tags it via the real
// aws-sdk-go-v2 bedrockagent client, and asserts the tag round-trips.
func TestFlowTags_RouteThroughRouteMatcher(t *testing.T) {
	t.Parallel()

	srv := newTestBedrockRegistryServer(t)

	createBody, err := json.Marshal(map[string]any{
		"name":             "route-flow",
		"executionRoleArn": "arn:aws:iam::000000000000:role/role",
	})
	require.NoError(t, err)

	createReq, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, srv.URL+"/flows", strings.NewReader(string(createBody)),
	)
	require.NoError(t, err)
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := http.DefaultClient.Do(createReq)
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	var created struct {
		Flow struct {
			FlowArn string `json:"flowArn"`
		} `json:"flow"`
	}
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&created))
	require.NotEmpty(t, created.Flow.FlowArn)

	client := newTestBedrockAgentSDKClient(t, srv.URL)

	_, err = client.TagResource(t.Context(), &bedrockagentsdk.TagResourceInput{
		ResourceArn: aws.String(created.Flow.FlowArn),
		Tags:        map[string]string{"env": "prod"},
	})
	require.NoError(t, err, "TagResource should route to AgentsHandler for a real flow ARN")

	listOut, err := client.ListTagsForResource(t.Context(), &bedrockagentsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(created.Flow.FlowArn),
	})
	require.NoError(t, err)
	assert.Equal(t, "prod", listOut.Tags["env"])
}
