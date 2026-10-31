package bedrockagentcorecontrol_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestMalformedBodiesNeverPanic(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	tests := []struct {
		name, method, path, body string
	}{
		{"not json", http.MethodPut, "/runtimes/", `{`},
		{
			"runtime artifact wrong type",
			http.MethodPut,
			"/runtimes/",
			`{"agentRuntimeName":"a","roleArn":"x","agentRuntimeArtifact":5}`,
		},
		{
			"memory strategies wrong type",
			http.MethodPost,
			"/memories/create",
			`{"name":"m1","eventExpiryDuration":30,"memoryStrategies":[1,"x",null]}`,
		},
		{
			"memory update entries wrong type",
			http.MethodPut,
			"/memories/nope/update",
			`{"memoryStrategies":{"addMemoryStrategies":[7],"modifyMemoryStrategies":[7],"deleteMemoryStrategies":[7]}}`,
		},
		{
			"online evaluators wrong type",
			http.MethodPost,
			"/online-evaluation-configs/create",
			`{"onlineEvaluationConfigName":"c","evaluators":[1,"x"],"rule":{},"dataSourceConfig":{},` +
				`"evaluationExecutionRoleArn":"arn:aws:iam::000000000000:role/r","enableOnCreate":true}`,
		},
		{
			"secret config wrong type",
			http.MethodPost,
			"/identities/CreateApiKeyCredentialProvider",
			`{"name":"k","apiKeySecretConfig":"nope"}`,
		},
		{
			"payment config wrong type",
			http.MethodPost,
			"/identities/CreatePaymentCredentialProvider",
			`{"name":"p","credentialProviderVendor":"CoinbaseCDP","providerConfigurationInput":{"coinbaseCdpConfiguration":5}}`,
		},
		{
			"dataset examples wrong type",
			http.MethodPost,
			"/datasets/nope/examples/add",
			`{"source":{"inlineExamples":{"examples":[1,"a"]}}}`,
		},
		{
			"gateway target config wrong type",
			http.MethodPost,
			"/gateways/nope/targets/",
			`{"name":"t","targetConfiguration":{"mcp":7}}`,
		},
		{"tags wrong type", http.MethodPost, "/tags/arn%3Aaws%3Ax", `{"tags":{"a":1}}`},
		{"rate limits wrong type", http.MethodPut, "/gateways/nope/rate-limits/batch", `{"rateLimits":[1]}`},
		{
			"consent sources wrong type",
			http.MethodPost,
			"/identities/CreateConsentPortal",
			`{"name":"c","executionRoleArn":"arn:aws:iam::000000000000:role/r","idpConfig":{},"sources":[1]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(t.Context(), tt.method, f.url+tt.path, strings.NewReader(tt.body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("User-Agent", "aws-sdk-go-v2/1.41.0 api/bedrockagentcorecontrol#1.0.0")

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Less(t, resp.StatusCode, 500, "malformed input is a client error, not a crash")
			assert.GreaterOrEqual(t, resp.StatusCode, 400)
		})
	}
}
