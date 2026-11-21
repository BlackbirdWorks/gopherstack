package app

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	astypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	elasticsearchbackend "github.com/blackbirdworks/gopherstack/services/elasticsearch"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
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

func TestAppSyncElasticsearchMirrorConcurrentFirstUse(t *testing.T) {
	t.Parallel()

	f := newASDSFixture(t)
	es := elasticsearchservice.NewFromConfig(f.fx.cfg)

	_, err := es.CreateElasticsearchDomain(t.Context(), &elasticsearchservice.CreateElasticsearchDomainInput{
		DomainName: aws.String("books"),
	})
	require.NoError(t, err)

	dom, err := es.DescribeElasticsearchDomain(t.Context(), &elasticsearchservice.DescribeElasticsearchDomainInput{
		DomainName: aws.String("books"),
	})
	require.NoError(t, err)

	osH, ok := serviceByName(f.fx.services)["OpenSearch"].(*opensearchbackend.Handler)
	require.True(t, ok)

	esH, ok := serviceByName(f.fx.services)["Elasticsearch"].(*elasticsearchbackend.Handler)
	require.True(t, ok)

	mirror := appsyncOpenSearch{handler: osH, es: esH}
	endpoint := "https://" + aws.ToString(dom.DomainStatus.Endpoint)

	const workers = 16

	errs := make([]error, workers)

	var wg sync.WaitGroup

	for i := range workers {
		wg.Go(func() { _, errs[i] = mirror.domainFor(t.Context(), endpoint) })
	}

	wg.Wait()

	for _, e := range errs {
		assert.NoError(t, e)
	}
}

type esMirrorEnv struct {
	f      *asDSFixture
	es     *elasticsearchservice.Client
	mirror appsyncOpenSearch
}

func newESMirrorEnv(t *testing.T) *esMirrorEnv {
	t.Helper()

	f := newASDSFixture(t)

	osH, ok := serviceByName(f.fx.services)["OpenSearch"].(*opensearchbackend.Handler)
	require.True(t, ok)

	esH, ok := serviceByName(f.fx.services)["Elasticsearch"].(*elasticsearchbackend.Handler)
	require.True(t, ok)

	return &esMirrorEnv{
		f: f, es: elasticsearchservice.NewFromConfig(f.fx.cfg), mirror: appsyncOpenSearch{handler: osH, es: esH},
	}
}

func (e *esMirrorEnv) createES(t *testing.T, name string) string {
	t.Helper()

	_, err := e.es.CreateElasticsearchDomain(t.Context(), &elasticsearchservice.CreateElasticsearchDomainInput{
		DomainName: aws.String(name),
	})
	require.NoError(t, err)

	dom, err := e.es.DescribeElasticsearchDomain(t.Context(), &elasticsearchservice.DescribeElasticsearchDomainInput{
		DomainName: aws.String(name),
	})
	require.NoError(t, err)

	return "https://" + aws.ToString(dom.DomainStatus.Endpoint)
}

func (e *esMirrorEnv) deleteES(t *testing.T, name string) {
	t.Helper()

	_, err := e.es.DeleteElasticsearchDomain(t.Context(), &elasticsearchservice.DeleteElasticsearchDomainInput{
		DomainName: aws.String(name),
	})
	require.NoError(t, err)
}

func (e *esMirrorEnv) engineHasBooks() bool {
	_, err := e.mirror.handler.Backend.DescribeDomain("books")

	return err == nil
}

func TestAppSyncElasticsearchMirrorLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, e *esMirrorEnv)
		name  string
	}{
		{
			name: "recreate settles at once despite an engine delete delay",
			check: func(t *testing.T, e *esMirrorEnv) {
				t.Helper()

				bk, ok := e.mirror.handler.Backend.(*opensearchbackend.InMemoryBackend)
				require.True(t, ok)
				bk.SetProcessingDelay(time.Hour)

				endpoint := e.createES(t, "books")
				_, err := e.mirror.domainFor(t.Context(), endpoint)
				require.NoError(t, err)

				e.deleteES(t, "books")

				endpoint = e.createES(t, "books")
				_, err = e.mirror.domainFor(t.Context(), endpoint)
				require.NoError(t, err)
			},
		},
		{
			name: "mirror goes with the elasticsearch domain",
			check: func(t *testing.T, e *esMirrorEnv) {
				t.Helper()

				endpoint := e.createES(t, "books")
				_, err := e.mirror.domainFor(t.Context(), endpoint)
				require.NoError(t, err)
				require.True(t, e.engineHasBooks())

				e.deleteES(t, "books")
				e.createES(t, "other")

				_, err = e.mirror.domainFor(t.Context(), "https://unknown.example")
				require.Error(t, err)
				assert.False(t, e.engineHasBooks(), "the stale mirror is removed")
			},
		},
		{
			name: "release removes the mirror",
			check: func(t *testing.T, e *esMirrorEnv) {
				t.Helper()

				endpoint := e.createES(t, "books")
				_, err := e.mirror.domainFor(t.Context(), endpoint)
				require.NoError(t, err)

				e.mirror.ReleaseOpenSearch(t.Context(), endpoint)
				assert.False(t, e.engineHasBooks())
			},
		},
		{
			name: "a user opensearch domain without the stamp is never touched",
			check: func(t *testing.T, e *esMirrorEnv) {
				t.Helper()

				_, err := e.mirror.handler.Backend.CreateDomain(opensearchbackend.CreateDomainInput{Name: "books"})
				require.NoError(t, err)

				endpoint := e.createES(t, "books")
				name, err := e.mirror.domainFor(t.Context(), endpoint)
				require.NoError(t, err)
				assert.Equal(t, "books", name)

				e.mirror.ReleaseOpenSearch(t.Context(), endpoint)
				e.deleteES(t, "books")
				endpoint = e.createES(t, "books")

				_, err = e.mirror.domainFor(t.Context(), endpoint)
				require.NoError(t, err)
				e.mirror.ReleaseOpenSearch(t.Context(), endpoint)

				assert.True(t, e.engineHasBooks(), "the user domain survives")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.check(t, newESMirrorEnv(t))
		})
	}
}
