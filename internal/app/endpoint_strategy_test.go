package app

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdkappsync "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	sdkecr "github.com/aws/aws-sdk-go-v2/service/ecr"
	sdkopensearch "github.com/aws/aws-sdk-go-v2/service/opensearch"
	sdksqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/endpointstrategy"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const strategyGateway = "gopherstack.test:4566"

// strategyServer composes every service behind the real router with the given strategies.
func strategyServer(t *testing.T, mutate func(*CLI)) *echo.Echo {
	t.Helper()

	log := buildLogger("")
	ctx := context.Background()

	cli := CLI{AccountID: "000000000000", Region: "us-east-1", Port: "4566"}
	mutate(&cli)
	cli.portAlloc = setupPortAllocatorWithReservations(ctx, log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: ctx, PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(ctx, log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	return e
}

type handlerTransport struct{ e *echo.Echo }

func (h handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body == nil {
		req.Body = http.NoBody
	}

	rec := httptest.NewRecorder()
	h.e.ServeHTTP(rec, req)

	return rec.Result(), nil
}

func strategyAWSConfig(t *testing.T, e *echo.Echo, region string) aws.Config {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithHTTPClient(&http.Client{Transport: handlerTransport{e: e}}),
	)
	require.NoError(t, err)

	return cfg
}

func rawStrategyRequest(
	t *testing.T, e *echo.Echo, method, rawURL, contentType, target, body string,
) (int, string) {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), method, rawURL, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	if target != "" {
		req.Header.Set("X-Amz-Target", target)
	}

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	out, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)

	return rec.Code, string(out)
}

func TestSQSEndpointStrategyThroughRouter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
		region   string
		wantURL  string
	}{
		{"default", "", "us-east-1", "http://localhost:4566/000000000000/q1"},
		{"off", "off", "us-east-1", "http://localhost:4566/000000000000/q1"},
		{"standard", "standard", "us-east-1", "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/q1"},
		{
			"standard region", "standard", "eu-west-1",
			"http://sqs.eu-west-1.localhost.localstack.cloud:4566/000000000000/q1",
		},
		{"domain", "domain", "eu-west-1", "http://eu-west-1.queue.localhost.localstack.cloud:4566/000000000000/q1"},
		{"domain us-east-1", "domain", "us-east-1", "http://queue.localhost.localstack.cloud:4566/000000000000/q1"},
		{"path", "path", "eu-west-1", "http://localhost:4566/queue/eu-west-1/000000000000/q1"},
		{"dynamic plain host", "dynamic", "us-east-1", "http://localhost:4566/000000000000/q1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := strategyServer(t, func(c *CLI) { c.Endpoints.SQS = tc.strategy })
			client := sdksqs.NewFromConfig(strategyAWSConfig(t, e, tc.region), func(o *sdksqs.Options) {
				o.BaseEndpoint = aws.String("http://" + strategyGateway)
			})

			created, err := client.CreateQueue(t.Context(), &sdksqs.CreateQueueInput{QueueName: aws.String("q1")})
			require.NoError(t, err)
			assert.Equal(t, tc.wantURL, aws.ToString(created.QueueUrl))

			got, err := client.GetQueueUrl(t.Context(), &sdksqs.GetQueueUrlInput{QueueName: aws.String("q1")})
			require.NoError(t, err)
			assert.Equal(t, tc.wantURL, aws.ToString(got.QueueUrl))

			listed, err := client.ListQueues(t.Context(), &sdksqs.ListQueuesInput{})
			require.NoError(t, err)
			assert.Equal(t, []string{tc.wantURL}, listed.QueueUrls)

			_, err = client.SendMessage(t.Context(), &sdksqs.SendMessageInput{
				QueueUrl: created.QueueUrl, MessageBody: aws.String("via-sdk"),
			})
			require.NoError(t, err)

			recv, err := client.ReceiveMessage(t.Context(), &sdksqs.ReceiveMessageInput{QueueUrl: created.QueueUrl})
			require.NoError(t, err)
			require.Len(t, recv.Messages, 1)
			assert.Equal(t, "via-sdk", aws.ToString(recv.Messages[0].Body))

			// raw, unsigned requests sent to the returned URL (host and path as given)
			form := "Action=SendMessage&MessageBody=" + url.QueryEscape("via-url")
			code, body := rawStrategyRequest(t, e, http.MethodPost, tc.wantURL,
				"application/x-www-form-urlencoded", "", form)
			require.Equal(t, http.StatusOK, code, body)

			code, body = rawStrategyRequest(t, e, http.MethodPost, tc.wantURL,
				"application/x-www-form-urlencoded", "", "Action=ReceiveMessage")
			require.Equal(t, http.StatusOK, code, body)
			assert.Contains(t, body, "via-url")

			code, body = rawStrategyRequest(t, e, http.MethodPost, tc.wantURL,
				"application/x-amz-json-1.0", "AmazonSQS.GetQueueAttributes",
				`{"QueueUrl":"`+tc.wantURL+`","AttributeNames":["QueueArn"]}`)
			require.Equal(t, http.StatusOK, code, body)
			assert.Contains(t, body, ":q1")
		})
	}
}

func TestOpenSearchEndpointStrategyThroughRouter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
		region   string
		engine   string
		wantHost string
		wantPath string
	}{
		{"default", "", "us-east-1", "OpenSearch_2.11", "search-dom1-000000000000.us-east-1.es.amazonaws.com", ""},
		{
			"domain",
			"domain",
			"eu-west-1",
			"OpenSearch_2.11",
			"dom1.eu-west-1.opensearch.localhost.localstack.cloud:4566",
			"",
		},
		{
			"domain elasticsearch", "domain", "us-east-1", "Elasticsearch_7.10",
			"dom1.us-east-1.es.localhost.localstack.cloud:4566", "",
		},
		{"path", "path", "eu-west-1", "OpenSearch_2.11", "localhost:4566", "/opensearch/eu-west-1/dom1"},
		{"path elasticsearch", "path", "us-east-1", "Elasticsearch_7.10", "localhost:4566", "/es/us-east-1/dom1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := strategyServer(t, func(c *CLI) { c.Endpoints.OpenSearch = tc.strategy })
			client := sdkopensearch.NewFromConfig(strategyAWSConfig(t, e, tc.region), func(o *sdkopensearch.Options) {
				o.BaseEndpoint = aws.String("http://" + strategyGateway)
			})

			out, err := client.CreateDomain(t.Context(), &sdkopensearch.CreateDomainInput{
				DomainName: aws.String("dom1"), EngineVersion: aws.String(tc.engine),
			})
			require.NoError(t, err)

			wantEndpoint := tc.wantHost + tc.wantPath
			assert.Equal(t, wantEndpoint, aws.ToString(out.DomainStatus.Endpoint))

			desc, err := client.DescribeDomain(
				t.Context(),
				&sdkopensearch.DescribeDomainInput{DomainName: aws.String("dom1")},
			)
			require.NoError(t, err)
			assert.Equal(t, wantEndpoint, aws.ToString(desc.DomainStatus.Endpoint))

			if tc.strategy == "" {
				return
			}

			base := "http://" + wantEndpoint

			code, body := rawStrategyRequest(t, e, http.MethodGet, base+"/", "", "", "")
			require.Equal(t, http.StatusOK, code, body)
			assert.Contains(t, body, `"DomainName":"dom1"`)

			code, body = rawStrategyRequest(t, e, http.MethodPost, base+"/index/idx1", "application/json", "", `{}`)
			require.Equal(t, http.StatusOK, code, body)

			code, body = rawStrategyRequest(
				t,
				e,
				http.MethodPost,
				base+"/idx1/_doc",
				"application/json",
				"",
				`{"colour":"teal"}`,
			)
			require.Equal(t, http.StatusOK, code, body)

			code, body = rawStrategyRequest(t, e, http.MethodPost, base+"/idx1/_search", "application/json", "",
				`{"query":{"match_all":{}}}`)
			require.Equal(t, http.StatusOK, code, body)
			assert.Contains(t, body, "teal")
		})
	}
}

func TestEndpointStrategyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		set     func(*endpointstrategy.Config)
		name    string
		wantErr bool
	}{
		{func(c *endpointstrategy.Config) { c.OpenSearch = "port" }, "opensearch port", true},
		{func(c *endpointstrategy.Config) { c.SQS = "bogus" }, "sqs bogus", true},
		{func(c *endpointstrategy.Config) { c.GraphQL = "standard" }, "graphql standard", true},
		{func(c *endpointstrategy.Config) { c.ECR = "path" }, "ecr path", true},
		{func(c *endpointstrategy.Config) {
			c.SQS, c.OpenSearch, c.GraphQL, c.ECR = "standard", "path", "legacy", "off"
		}, "valid", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var cfg endpointstrategy.Config
			tc.set(&cfg)

			err := cfg.Validate()
			if tc.wantErr {
				require.ErrorIs(t, err, endpointstrategy.ErrInvalidStrategy)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestGraphQLEndpointStrategyThroughRouter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
		wantURI  string
		callPath string
		host     string
	}{
		{"default", "", "http://localhost:4566/v1/apis/%s/graphql", "", ""},
		{"legacy", "legacy", "http://localhost:4566/graphql/%s", "", ""},
		{"path", "path", "http://localhost:4566/appsync-api/%s/graphql", "", ""},
		{"domain", "domain", "http://%s.appsync-api.localhost.localstack.cloud:4566/graphql", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := strategyServer(t, func(c *CLI) { c.Endpoints.GraphQL = tc.strategy })
			client := sdkappsync.NewFromConfig(strategyAWSConfig(t, e, "us-east-1"), func(o *sdkappsync.Options) {
				o.BaseEndpoint = aws.String("http://" + strategyGateway)
			})

			api, err := client.CreateGraphqlApi(t.Context(), &sdkappsync.CreateGraphqlApiInput{
				Name: aws.String("api1"), AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
			})
			require.NoError(t, err)

			id := aws.ToString(api.GraphqlApi.ApiId)
			wantURI := strings.ReplaceAll(tc.wantURI, "%s", id)
			assert.Equal(t, wantURI, api.GraphqlApi.Uris["GRAPHQL"])

			got, err := client.GetGraphqlApi(t.Context(), &sdkappsync.GetGraphqlApiInput{ApiId: aws.String(id)})
			require.NoError(t, err)
			assert.Equal(t, wantURI, got.GraphqlApi.Uris["GRAPHQL"])

			key, err := client.CreateApiKey(t.Context(), &sdkappsync.CreateApiKeyInput{ApiId: aws.String(id)})
			require.NoError(t, err)

			// the advertised URI must reach the same handler as the native path
			nativeCode, nativeBody := apiKeyPost(
				t,
				e,
				"http://localhost:4566/v1/apis/"+id+"/graphql",
				aws.ToString(key.ApiKey.Id),
			)
			uriCode, uriBody := apiKeyPost(t, e, wantURI, aws.ToString(key.ApiKey.Id))
			assert.Equal(t, nativeCode, uriCode)
			assert.Equal(t, nativeBody, uriBody)
			assert.NotContains(t, uriBody, "NoSuchBucket")
		})
	}
}

func apiKeyPost(t *testing.T, e *echo.Echo, rawURL, apiKey string) (int, string) {
	t.Helper()

	req := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		rawURL,
		strings.NewReader(`{"query":"{ __typename }"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", apiKey)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	out, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)

	return rec.Code, string(out)
}

func TestECREndpointStrategyThroughRouter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
		region   string
		wantURI  string
	}{
		{"default", "", "us-east-1", "000000000000.dkr.ecr.us-east-1.amazonaws.com/repo1"},
		{"domain", "domain", "eu-west-1", "000000000000.dkr.ecr.eu-west-1.localhost.localstack.cloud:4566/repo1"},
		{"off", "off", "us-east-1", "localhost.localstack.cloud:4566/repo1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := strategyServer(t, func(c *CLI) { c.Endpoints.ECR = tc.strategy })
			client := sdkecr.NewFromConfig(strategyAWSConfig(t, e, tc.region), func(o *sdkecr.Options) {
				o.BaseEndpoint = aws.String("http://" + strategyGateway)
			})

			created, err := client.CreateRepository(
				t.Context(),
				&sdkecr.CreateRepositoryInput{RepositoryName: aws.String("repo1")},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantURI, aws.ToString(created.Repository.RepositoryUri))

			desc, err := client.DescribeRepositories(t.Context(), &sdkecr.DescribeRepositoriesInput{})
			require.NoError(t, err)
			require.Len(t, desc.Repositories, 1)
			assert.Equal(t, tc.wantURI, aws.ToString(desc.Repositories[0].RepositoryUri))
		})
	}
}
