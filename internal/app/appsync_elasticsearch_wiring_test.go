package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	astypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *asDSFixture) addESDataSource(t *testing.T, endpoint string) {
	t.Helper()

	f.addDS(t, &appsync.CreateDataSourceInput{
		Name: aws.String("ES"), Type: astypes.DataSourceTypeAmazonElasticsearch,
		ElasticsearchConfig: &astypes.ElasticsearchDataSourceConfig{
			Endpoint: aws.String(endpoint), AwsRegion: aws.String("us-east-1"),
		},
	}, "os", asDSOSReq, `$util.toJson($ctx.result)`)
}

func (f *asDSFixture) esHits(t *testing.T) []any {
	t.Helper()

	res, _ := asJSONValue(t, f.query(t, `os(op: $op, path: $path, body: $body)`, map[string]any{
		"op": "POST", "path": "/idx/_search", "body": `{"query":{"match_all":{}}}`,
	})).(map[string]any)

	hits, _ := res["hits"].(map[string]any)
	list, _ := hits["hits"].([]any)

	return list
}

func (f *asDSFixture) esPut(t *testing.T, path, body string) {
	t.Helper()

	f.query(t, `os(op: $op, path: $path, body: $body)`, map[string]any{"op": "PUT", "path": path, "body": body})
}

func TestAppSyncElasticsearchDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, f *asDSFixture, es *elasticsearchservice.Client, endpoint string)
		name  string
	}{
		{
			name: "domain created through the elasticsearch api serves documents",
			check: func(t *testing.T, f *asDSFixture, _ *elasticsearchservice.Client, _ string) {
				t.Helper()

				f.esPut(t, "/idx", "")
				f.esPut(t, "/idx/_doc/1", `{"title":"gopher"}`)

				hits := f.esHits(t)
				require.Len(t, hits, 1)

				first, _ := hits[0].(map[string]any)
				assert.Equal(t, map[string]any{"title": "gopher"}, first["_source"])
			},
		},
		{
			name: "recreated domain does not inherit documents",
			check: func(t *testing.T, f *asDSFixture, es *elasticsearchservice.Client, endpoint string) {
				t.Helper()

				f.esPut(t, "/idx", "")
				f.esPut(t, "/idx/_doc/1", `{"title":"old"}`)
				require.Len(t, f.esHits(t), 1)

				_, err := es.DeleteElasticsearchDomain(
					t.Context(),
					&elasticsearchservice.DeleteElasticsearchDomainInput{
						DomainName: aws.String("books"),
					},
				)
				require.NoError(t, err)

				_, err = es.CreateElasticsearchDomain(t.Context(), &elasticsearchservice.CreateElasticsearchDomainInput{
					DomainName: aws.String("books"),
				})
				require.NoError(t, err)

				dom, err := es.DescribeElasticsearchDomain(
					t.Context(),
					&elasticsearchservice.DescribeElasticsearchDomainInput{
						DomainName: aws.String("books"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, endpoint, aws.ToString(dom.DomainStatus.Endpoint))

				f.esPut(t, "/idx", "")
				assert.Empty(t, f.esHits(t))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newASDSFixture(t)
			es := elasticsearchservice.NewFromConfig(f.fx.cfg)

			_, err := es.CreateElasticsearchDomain(t.Context(), &elasticsearchservice.CreateElasticsearchDomainInput{
				DomainName: aws.String("books"),
			})
			require.NoError(t, err)

			dom, err := es.DescribeElasticsearchDomain(
				t.Context(),
				&elasticsearchservice.DescribeElasticsearchDomainInput{
					DomainName: aws.String("books"),
				},
			)
			require.NoError(t, err)

			endpoint := aws.ToString(dom.DomainStatus.Endpoint)
			f.addESDataSource(t, "https://"+endpoint)

			tt.check(t, f, es, endpoint)
		})
	}
}
