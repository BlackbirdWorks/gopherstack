package glue_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// pageLister invokes one glue List/Get operation with the given page size and
// continuation token, returning the item count of the page and the next
// token (nil/"" when exhausted).
type pageLister func(
	t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string,
) (count int, next *string)

// totalPaginationCases is the number of ops covered across every
// paginationCases* helper below -- used only to preallocate the combined
// slice in TestSDKRoundTrip_ListPagination.
const totalPaginationCases = 33

// paginationCase is one op fixed under gopherstack-awzv: seed populates a
// fresh backend with more than pageSize items, and list drives the real SDK
// client for that op.
type paginationCase struct {
	seed func(t *testing.T, b *glue.InMemoryBackend)
	list pageLister
	name string
	want int
}

// TestSDKRoundTrip_ListPagination drives every op fixed under gopherstack-awzv
// (glue's 30 real empty-struct-input MaxResults/NextToken/Filter/Tags
// candidates, services/glue/PARITY.md's "gopherstack-a250" 2026-08-13 entry)
// through the real aws-sdk-go-v2 client. Before the fix every one of these
// *Input types was a literal `struct{}`: MaxResults/NextToken were silently
// discarded on the wire and the handler always returned every stored item in
// one unbounded response. Each case here seeds more items than MaxResults,
// proves the first page truncates to MaxResults with a non-empty NextToken,
// and proves the second call using that token resumes past the first page
// rather than repeating it -- a case a literal struct{} input could never
// pass, since MaxResults could not reach the handler at all.
func TestSDKRoundTrip_ListPagination(t *testing.T) {
	t.Parallel()

	builtInConnectionTypeCount := len(glue.NewInMemoryBackend(testAccountID, testRegion).ListConnectionTypes())

	cases := make([]paginationCase, 0, totalPaginationCases)
	cases = append(cases, paginationCasesCatalogAndCrawl()...)
	cases = append(cases, paginationCasesConnectionsAndEntityTypes(builtInConnectionTypeCount)...)
	cases = append(cases, paginationCasesDataQuality()...)
	cases = append(cases, paginationCasesComputeAndCode()...)
	cases = append(cases, paginationCasesOpsAndMisc()...)
	cases = append(cases, paginationCasesSchemaRegistry()...)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runPaginationCase(t, tc)
		})
	}
}

// runPaginationCase seeds a fresh backend, then proves tc.list truncates to
// pageSize on the first call and resumes past it on the second.
func runPaginationCase(t *testing.T, tc paginationCase) {
	t.Helper()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	tc.seed(t, backend)
	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	pageSize := int32(tc.want - 1)

	firstCount, next := tc.list(t, ctx, client, pageSize, nil)
	assert.Equal(t, int(pageSize), firstCount, "first page must truncate to MaxResults")
	require.NotNil(t, next, "NextToken must be set when more items remain")
	require.NotEmpty(t, *next)

	secondCount, next2 := tc.list(t, ctx, client, pageSize, next)
	assert.Equal(t, tc.want-int(pageSize), secondCount, "second page must return the remaining items")
	assert.True(t, next2 == nil || *next2 == "", "NextToken must be empty once every item has been returned")
}

func paginationCasesCatalogAndCrawl() []paginationCase {
	return []paginationCase{
		{
			name: "list blueprints",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"bp1", "bp2", "bp3"} {
					_, err := b.CreateBlueprint(n, "s3://loc/"+n, "", nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListBlueprints(
					ctx,
					&gluesdk.ListBlueprintsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Blueprints), out.NextToken
			},
		},
		{
			name: "get crawlers",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, dbErr := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
				require.NoError(t, dbErr)

				for _, n := range []string{"cr1", "cr2", "cr3"} {
					_, crawlerErr := b.CreateCrawler(n, "role", "db", glue.CrawlerTarget{
						S3Targets: []glue.S3Target{{Path: "s3://bucket/" + n}},
					}, nil)
					require.NoError(t, crawlerErr)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetCrawlers(
					ctx,
					&gluesdk.GetCrawlersInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Crawlers), out.NextToken
			},
		},
		{
			name: "list crawlers",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, dbErr := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
				require.NoError(t, dbErr)

				for _, n := range []string{"lcr1", "lcr2", "lcr3"} {
					_, crawlerErr := b.CreateCrawler(n, "role", "db", glue.CrawlerTarget{
						S3Targets: []glue.S3Target{{Path: "s3://bucket/" + n}},
					}, nil)
					require.NoError(t, crawlerErr)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListCrawlers(
					ctx,
					&gluesdk.ListCrawlersInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.CrawlerNames), out.NextToken
			},
		},
		{
			name: "get catalogs",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"cat1", "cat2", "cat3"} {
					require.NoError(t, b.CreateCatalog(n, n, "", nil))
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetCatalogs(
					ctx,
					&gluesdk.GetCatalogsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.CatalogList), out.NextToken
			},
		},
		{
			name: "get classifiers",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"gc1", "gc2", "gc3"} {
					require.NoError(t, b.CreateClassifier(glue.Classifier{
						GrokClassifier: &glue.GrokClassifier{Name: n, GrokPattern: "%{NUMBER}"},
					}))
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetClassifiers(
					ctx,
					&gluesdk.GetClassifiersInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Classifiers), out.NextToken
			},
		},
		{
			name: "get column statistics task runs",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for range 3 {
					_, err := b.StartColumnStatisticsTaskRun("db1", "tbl1", "role")
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetColumnStatisticsTaskRuns(ctx, &gluesdk.GetColumnStatisticsTaskRunsInput{
					DatabaseName: aws.String("db1"),
					TableName:    aws.String("tbl1"),
					MaxResults:   aws.Int32(pageSize),
					NextToken:    token,
				})
				require.NoError(t, err)

				return len(out.ColumnStatisticsTaskRuns), out.NextToken
			},
		},
		{
			name: "list column statistics task runs",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for range 3 {
					_, err := b.StartColumnStatisticsTaskRun("db2", "tbl2", "role")
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListColumnStatisticsTaskRuns(
					ctx, &gluesdk.ListColumnStatisticsTaskRunsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.ColumnStatisticsTaskRunIds), out.NextToken
			},
		},
	}
}

func paginationCasesConnectionsAndEntityTypes(builtInConnectionTypeCount int) []paginationCase {
	return []paginationCase{
		{
			name: "get connections",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"conn1", "conn2", "conn3"} {
					_, err := b.CreateConnectionWithOptions(
						n,
						"JDBC",
						map[string]string{},
						nil,
						glue.ConnectionOptions{},
					)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetConnections(
					ctx,
					&gluesdk.GetConnectionsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.ConnectionList), out.NextToken
			},
		},
		{
			name: "list connection types",
			want: builtInConnectionTypeCount,
			seed: func(t *testing.T, _ *glue.InMemoryBackend) {
				t.Helper()
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListConnectionTypes(
					ctx, &gluesdk.ListConnectionTypesInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.ConnectionTypes), out.NextToken
			},
		},
		{
			name: "list custom entity types",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"cet1", "cet2", "cet3"} {
					_, err := b.CreateCustomEntityType(n, `\d+`, nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListCustomEntityTypes(
					ctx, &gluesdk.ListCustomEntityTypesInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.CustomEntityTypes), out.NextToken
			},
		},
		{
			name: "get dev endpoints",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"dep1", "dep2", "dep3"} {
					_, err := b.CreateDevEndpoint(n, glue.DevEndpointInput{}, "role", nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetDevEndpoints(
					ctx,
					&gluesdk.GetDevEndpointsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.DevEndpoints), out.NextToken
			},
		},
		{
			name: "list dev endpoints",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"ldep1", "ldep2", "ldep3"} {
					_, err := b.CreateDevEndpoint(n, glue.DevEndpointInput{}, "role", nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListDevEndpoints(
					ctx,
					&gluesdk.ListDevEndpointsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.DevEndpointNames), out.NextToken
			},
		},
	}
}

func paginationCasesDataQuality() []paginationCase {
	return []paginationCase{
		{
			name: "list data quality rulesets",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"dqr1", "dqr2", "dqr3"} {
					_, err := b.CreateDataQualityRuleset(n, "Rules = [ IsComplete \"id\" ]", nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListDataQualityRulesets(
					ctx, &gluesdk.ListDataQualityRulesetsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Rulesets), out.NextToken
			},
		},
		{
			name: "list data quality rule recommendation runs",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for range 3 {
					_, err := b.StartDataQualityRuleRecommendationRun("s3://bucket/data")
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListDataQualityRuleRecommendationRuns(
					ctx,
					&gluesdk.ListDataQualityRuleRecommendationRunsInput{
						MaxResults: aws.Int32(pageSize),
						NextToken:  token,
					},
				)
				require.NoError(t, err)

				return len(out.Runs), out.NextToken
			},
		},
		{
			name: "list data quality ruleset evaluation runs",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, rulesetErr := b.CreateDataQualityRuleset("dqre-ruleset", "Rules = [ IsComplete \"id\" ]", nil)
				require.NoError(t, rulesetErr)

				for range 3 {
					_, runErr := b.StartDataQualityRulesetEvaluationRun([]string{"dqre-ruleset"})
					require.NoError(t, runErr)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListDataQualityRulesetEvaluationRuns(
					ctx,
					&gluesdk.ListDataQualityRulesetEvaluationRunsInput{
						MaxResults: aws.Int32(pageSize),
						NextToken:  token,
					},
				)
				require.NoError(t, err)

				return len(out.Runs), out.NextToken
			},
		},
		{
			name: "list data quality results",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, id := range []string{"dqres1", "dqres2", "dqres3"} {
					b.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: id, Score: 0.9})
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListDataQualityResults(
					ctx, &gluesdk.ListDataQualityResultsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Results), out.NextToken
			},
		},
	}
}

func paginationCasesComputeAndCode() []paginationCase {
	return []paginationCase{
		{
			name: "describe integrations",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"ig1", "ig2", "ig3"} {
					_, err := b.CreateIntegration(n, "arn:aws:rds:us-east-1:000000000000:db/"+n,
						"arn:aws:redshift:us-east-1:000000000000:cluster/"+n, nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.DescribeIntegrations(
					ctx, &gluesdk.DescribeIntegrationsInput{MaxRecords: aws.Int32(pageSize), Marker: token},
				)
				require.NoError(t, err)

				return len(out.Integrations), out.Marker
			},
		},
		{
			name: "list jobs",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"job1", "job2", "job3"} {
					_, err := b.CreateJob(glue.Job{
						Name: n, Role: "role", Command: glue.JobCommand{Name: "glueetl"},
					})
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListJobs(ctx, &gluesdk.ListJobsInput{MaxResults: aws.Int32(pageSize), NextToken: token})
				require.NoError(t, err)

				return len(out.JobNames), out.NextToken
			},
		},
		{
			name: "get jobs",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"gjob1", "gjob2", "gjob3"} {
					_, err := b.CreateJob(glue.Job{
						Name: n, Role: "role", Command: glue.JobCommand{Name: "glueetl"},
					})
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetJobs(ctx, &gluesdk.GetJobsInput{MaxResults: aws.Int32(pageSize), NextToken: token})
				require.NoError(t, err)

				return len(out.Jobs), out.NextToken
			},
		},
		{
			name: "list materialized view refresh task runs",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for range 3 {
					_, err := b.StartMaterializedViewRefreshTaskRun("mvdb", "mvtbl")
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListMaterializedViewRefreshTaskRuns(
					ctx,
					&gluesdk.ListMaterializedViewRefreshTaskRunsInput{
						CatalogId: aws.String("000000000000"), MaxResults: aws.Int32(pageSize), NextToken: token,
					},
				)
				require.NoError(t, err)

				return len(out.MaterializedViewRefreshTaskRuns), out.NextToken
			},
		},
		{
			name: "get ml transforms",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"mlt1", "mlt2", "mlt3"} {
					_, err := b.CreateMLTransform(n, "", "role", nil, glue.MLTransformParameter{}, nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetMLTransforms(
					ctx,
					&gluesdk.GetMLTransformsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Transforms), out.NextToken
			},
		},
		{
			name: "list ml transforms",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"lmlt1", "lmlt2", "lmlt3"} {
					_, err := b.CreateMLTransform(n, "", "role", nil, glue.MLTransformParameter{}, nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListMLTransforms(
					ctx,
					&gluesdk.ListMLTransformsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.TransformIds), out.NextToken
			},
		},
	}
}

func paginationCasesOpsAndMisc() []paginationCase {
	return []paginationCase{
		{
			name: "list registries",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"reg1", "reg2", "reg3"} {
					_, err := b.CreateRegistry(n, "", nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListRegistries(
					ctx,
					&gluesdk.ListRegistriesInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Registries), out.NextToken
			},
		},
		{
			name: "list integration resource properties",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"r1", "r2", "r3"} {
					_, err := b.CreateIntegrationResourceProperty(
						"arn:aws:glue:us-east-1:000000000000:connection/"+n, nil, nil,
					)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListIntegrationResourceProperties(
					ctx,
					&gluesdk.ListIntegrationResourcePropertiesInput{MaxRecords: aws.Int32(pageSize), Marker: token},
				)
				require.NoError(t, err)

				return len(out.IntegrationResourcePropertyList), out.Marker
			},
		},
		{
			name: "get security configurations",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"sc1", "sc2", "sc3"} {
					_, err := b.CreateSecurityConfiguration(n, glue.EncryptionConfiguration{})
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetSecurityConfigurations(
					ctx, &gluesdk.GetSecurityConfigurationsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.SecurityConfigurations), out.NextToken
			},
		},
		{
			name: "list sessions",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"sess1", "sess2", "sess3"} {
					_, err := b.CreateSession(n, "role", glue.SessionCommand{Name: "glueetl"}, glue.Session{})
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListSessions(
					ctx,
					&gluesdk.ListSessionsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Ids), out.NextToken
			},
		},
		{
			name: "get triggers",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"trig1", "trig2", "trig3"} {
					_, err := b.CreateTrigger(glue.Trigger{
						Name: n, Type: "ON_DEMAND", Actions: []glue.TriggerAction{{JobName: "somejob"}},
					}, nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetTriggers(
					ctx,
					&gluesdk.GetTriggersInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Triggers), out.NextToken
			},
		},
		{
			name: "list triggers",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"ltrig1", "ltrig2", "ltrig3"} {
					_, err := b.CreateTrigger(glue.Trigger{
						Name: n, Type: "ON_DEMAND", Actions: []glue.TriggerAction{{JobName: "somejob"}},
					}, nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListTriggers(
					ctx,
					&gluesdk.ListTriggersInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.TriggerNames), out.NextToken
			},
		},
		{
			name: "list usage profiles",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"up1", "up2", "up3"} {
					_, err := b.CreateUsageProfile(n, "", nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListUsageProfiles(
					ctx, &gluesdk.ListUsageProfilesInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Profiles), out.NextToken
			},
		},
		{
			name: "list workflows",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"wf1", "wf2", "wf3"} {
					_, err := b.CreateWorkflow(glue.Workflow{Name: n}, nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListWorkflows(
					ctx,
					&gluesdk.ListWorkflowsInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Workflows), out.NextToken
			},
		},
		{
			name: "get resource policies",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy(`{"Version":"2012-10-17"}`, "", "", "", "")
				require.NoError(t, err)

				for _, arn := range []string{
					"arn:aws:glue:us-east-1:123456789012:catalog/rp1",
					"arn:aws:glue:us-east-1:123456789012:catalog/rp2",
				} {
					_, arnErr := b.PutResourcePolicy(`{"Version":"2012-10-17"}`, arn, "", "", "")
					require.NoError(t, arnErr)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.GetResourcePolicies(
					ctx, &gluesdk.GetResourcePoliciesInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.GetResourcePoliciesResponseList), out.NextToken
			},
		},
	}
}

// paginationCasesSchemaRegistry covers ListSchemas/ListSchemaVersions, fixed
// under gopherstack-q4qt: both real ops declare MaxResults/NextToken
// (glue@v1.152.0 api_op_ListSchemas.go / api_op_ListSchemaVersions.go) but,
// unlike the gopherstack-awzv sweep's 29 empty-struct-input ops, these two
// already took a real RegistryId/SchemaId and so were never swept -- every
// call returned every stored item in one unbounded response.
func paginationCasesSchemaRegistry() []paginationCase {
	return []paginationCase{
		{
			name: "list schemas",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				for _, n := range []string{"sch1", "sch2", "sch3"} {
					_, _, err := b.CreateSchema("", n, "JSON", "", "", "", nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListSchemas(
					ctx, &gluesdk.ListSchemasInput{MaxResults: aws.Int32(pageSize), NextToken: token},
				)
				require.NoError(t, err)

				return len(out.Schemas), out.NextToken
			},
		},
		{
			name: "list schema versions",
			want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, _, err := b.CreateSchema("", "versioned-schema", "JSON", "NONE", "", "", nil)
				require.NoError(t, err)

				for range 3 {
					_, verErr := b.RegisterSchemaVersion("", "versioned-schema", `{"type":"object"}`)
					require.NoError(t, verErr)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, pageSize int32, token *string) (int, *string) {
				t.Helper()
				out, err := c.ListSchemaVersions(
					ctx,
					&gluesdk.ListSchemaVersionsInput{
						SchemaId:   &types.SchemaId{SchemaName: aws.String("versioned-schema")},
						MaxResults: aws.Int32(pageSize),
						NextToken:  token,
					},
				)
				require.NoError(t, err)

				return len(out.Schemas), out.NextToken
			},
		},
	}
}
