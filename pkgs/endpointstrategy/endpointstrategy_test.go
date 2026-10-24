package endpointstrategy_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/endpointstrategy"
)

func TestSQSQueueURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, strategy, host, region, want string
	}{
		{"default", "", "localhost:4566", "us-east-1", "http://localhost:4566/1/q"},
		{"off", "off", "localhost:4566", "eu-west-1", "http://localhost:4566/1/q"},
		{
			"standard", "standard", "localhost:4566", "eu-west-1",
			"http://sqs.eu-west-1.localhost.localstack.cloud:4566/1/q",
		},
		{
			"domain",
			"domain",
			"localhost:4566",
			"eu-west-1",
			"http://eu-west-1.queue.localhost.localstack.cloud:4566/1/q",
		},
		{
			"domain default region", "domain", "localhost:4566", "us-east-1",
			"http://queue.localhost.localstack.cloud:4566/1/q",
		},
		{"path", "path", "localhost:4566", "eu-west-1", "http://localhost:4566/queue/eu-west-1/1/q"},
		{
			"dynamic standard host", "dynamic", "sqs.eu-west-1.localhost.localstack.cloud:4566", "eu-west-1",
			"http://sqs.eu-west-1.localhost.localstack.cloud:4566/1/q",
		},
		{
			"dynamic domain host", "dynamic", "eu-west-1.queue.localhost.localstack.cloud:4566", "eu-west-1",
			"http://eu-west-1.queue.localhost.localstack.cloud:4566/1/q",
		},
		{"dynamic other host", "dynamic", "localhost:4566", "eu-west-1", "http://localhost:4566/1/q"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, endpointstrategy.SQSQueueURL(tc.strategy, "http", tc.host, tc.region, "1", "q"))
		})
	}
}

func TestSQSRegion(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, host, path, want string }{
		{"path", "localhost:4566", "/queue/eu-west-1/1/q", "eu-west-1"},
		{"standard", "sqs.eu-west-1.localhost.localstack.cloud:4566", "/1/q", "eu-west-1"},
		{"domain", "eu-west-1.queue.localhost.localstack.cloud", "/1/q", "eu-west-1"},
		{"domain default", "queue.localhost.localstack.cloud", "/1/q", "us-east-1"},
		{"none", "localhost:4566", "/1/q", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, endpointstrategy.SQSRegion(tc.host, tc.path))
		})
	}
}

func TestOpenSearchEndpointAndParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, strategy, endpoint string
		host, path, rest         string
		ok                       bool
	}{
		{
			"domain", "domain", "d1.eu-west-1.opensearch.localhost.localstack.cloud:4566",
			"d1.eu-west-1.opensearch.localhost.localstack.cloud:4566", "/idx/_search", "/idx/_search", true,
		},
		{
			"path",
			"path",
			"localhost:4566/es/eu-west-1/d1",
			"localhost:4566",
			"/es/eu-west-1/d1/idx/_search",
			"/idx/_search",
			true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := "opensearch"
			if tc.name == "path" {
				engine = "es"
			}

			assert.Equal(t, tc.endpoint,
				endpointstrategy.OpenSearchEndpoint(tc.strategy, "localhost:4566", engine, "eu-west-1", "1", "d1"))

			got, ok := endpointstrategy.ParseOpenSearchRequest(tc.host, tc.path)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, "d1", got.Domain)
			assert.Equal(t, "eu-west-1", got.Region)
			assert.Equal(t, tc.rest, got.Rest)
		})
	}

	_, ok := endpointstrategy.ParseOpenSearchRequest("localhost:4566", "/es/not-a-region/d1")
	assert.False(t, ok)
}

func TestGraphQLParse(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, host, path, id string }{
		{"legacy", "localhost:4566", "/graphql/abc", "abc"},
		{"path", "localhost:4566", "/appsync-api/abc/graphql", "abc"},
		{"domain", "abc.appsync-api.localhost.localstack.cloud:4566", "/graphql", "abc"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			id, rest, ok := endpointstrategy.ParseGraphQLRequest(tc.host, tc.path)
			assert.True(t, ok)
			assert.Equal(t, tc.id, id)
			assert.Empty(t, rest)
		})
	}

	_, _, ok := endpointstrategy.ParseGraphQLRequest("localhost:4566", "/v1/apis/abc/graphql")
	assert.False(t, ok)
}
