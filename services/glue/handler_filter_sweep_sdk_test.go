package glue_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// farFutureTime returns a time well after any run created during t, used to
// prove StartedAfter genuinely excludes runs rather than being ignored.
func farFutureTime(t *testing.T) time.Time {
	t.Helper()

	return time.Now().Add(24 * time.Hour)
}

// inertDataSource satisfies the real SDK client's local "required" validation
// for DataQualityRuleRecommendationRunFilter.DataSource /
// DataQualityRulesetEvaluationRunFilter.DataSource. Neither field has honest
// backing in this backend (see handler_data_quality_rulesets.go) so its
// content is irrelevant to these tests -- it only needs to be present so the
// client will send the request at all.
func inertDataSource() *types.DataSource {
	return &types.DataSource{GlueTable: &types.GlueTable{DatabaseName: aws.String("db"), TableName: aws.String("t")}}
}

// TestSDKRoundTrip_TagsFilter drives every List op fixed under gopherstack-awzv
// whose Tags member has a real backing field (the entity carries Tags via
// tags.go's tagResource dispatch) through the real client, proving Tags
// actually excludes an untagged sibling rather than being silently discarded.
func TestSDKRoundTrip_TagsFilter(t *testing.T) {
	t.Parallel()

	type tagsCase struct {
		seed  func(t *testing.T, b *glue.InMemoryBackend)
		call  func(t *testing.T, c *gluesdk.Client) []string
		name  string
		match string
	}

	cases := []tagsCase{
		{
			name:  "list blueprints",
			match: "bp-tagged",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateBlueprint("bp-tagged", "s3://loc/a", "", map[string]string{"env": "prod"})
				require.NoError(t, err)
				_, err = b.CreateBlueprint("bp-untagged", "s3://loc/b", "", nil)
				require.NoError(t, err)
			},
			call: func(t *testing.T, c *gluesdk.Client) []string {
				t.Helper()
				out, err := c.ListBlueprints(
					t.Context(),
					&gluesdk.ListBlueprintsInput{Tags: map[string]string{"env": "prod"}},
				)
				require.NoError(t, err)

				return out.Blueprints
			},
		},
		{
			name:  "list crawlers",
			match: "cr-tagged",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				target := glue.CrawlerTarget{S3Targets: []glue.S3Target{{Path: "s3://bucket/x"}}}
				_, err := b.CreateCrawler("cr-tagged", "role", "", target, map[string]string{"env": "prod"})
				require.NoError(t, err)
				_, err = b.CreateCrawler("cr-untagged", "role", "", target, nil)
				require.NoError(t, err)
			},
			call: func(t *testing.T, c *gluesdk.Client) []string {
				t.Helper()
				out, err := c.ListCrawlers(
					t.Context(),
					&gluesdk.ListCrawlersInput{Tags: map[string]string{"env": "prod"}},
				)
				require.NoError(t, err)

				return out.CrawlerNames
			},
		},
		{
			name:  "list dev endpoints",
			match: "dep-tagged",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDevEndpoint(
					"dep-tagged",
					glue.DevEndpointInput{},
					"role",
					map[string]string{"env": "prod"},
				)
				require.NoError(t, err)
				_, err = b.CreateDevEndpoint("dep-untagged", glue.DevEndpointInput{}, "role", nil)
				require.NoError(t, err)
			},
			call: func(t *testing.T, c *gluesdk.Client) []string {
				t.Helper()
				out, err := c.ListDevEndpoints(
					t.Context(),
					&gluesdk.ListDevEndpointsInput{Tags: map[string]string{"env": "prod"}},
				)
				require.NoError(t, err)

				return out.DevEndpointNames
			},
		},
		{
			name:  "list jobs",
			match: "job-tagged",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				mk := func(name string, tags map[string]string) {
					_, err := b.CreateJob(glue.Job{
						Name: name, Role: "role", Command: glue.JobCommand{Name: "glueetl"}, Tags: tags,
					})
					require.NoError(t, err)
				}
				mk("job-tagged", map[string]string{"env": "prod"})
				mk("job-untagged", nil)
			},
			call: func(t *testing.T, c *gluesdk.Client) []string {
				t.Helper()
				out, err := c.ListJobs(t.Context(), &gluesdk.ListJobsInput{Tags: map[string]string{"env": "prod"}})
				require.NoError(t, err)

				return out.JobNames
			},
		},
		{
			name:  "list triggers",
			match: "trig-tagged",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				mk := func(name string, tags map[string]string) {
					_, err := b.CreateTrigger(glue.Trigger{
						Name: name, Type: "ON_DEMAND", Actions: []glue.TriggerAction{{JobName: "somejob"}},
					}, tags)
					require.NoError(t, err)
				}
				mk("trig-tagged", map[string]string{"env": "prod"})
				mk("trig-untagged", nil)
			},
			call: func(t *testing.T, c *gluesdk.Client) []string {
				t.Helper()
				out, err := c.ListTriggers(
					t.Context(),
					&gluesdk.ListTriggersInput{Tags: map[string]string{"env": "prod"}},
				)
				require.NoError(t, err)

				return out.TriggerNames
			},
		},
		{
			name:  "list data quality rulesets",
			match: "dqr-tagged",
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDataQualityRuleset(
					"dqr-tagged",
					"Rules = [ IsComplete \"id\" ]",
					map[string]string{"env": "prod"},
				)
				require.NoError(t, err)
				_, err = b.CreateDataQualityRuleset("dqr-untagged", "Rules = [ IsComplete \"id\" ]", nil)
				require.NoError(t, err)
			},
			call: func(t *testing.T, c *gluesdk.Client) []string {
				t.Helper()
				out, err := c.ListDataQualityRulesets(
					t.Context(), &gluesdk.ListDataQualityRulesetsInput{Tags: map[string]string{"env": "prod"}},
				)
				require.NoError(t, err)

				names := make([]string, 0, len(out.Rulesets))
				for _, r := range out.Rulesets {
					names = append(names, aws.ToString(r.Name))
				}

				return names
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend(testAccountID, testRegion)
			tc.seed(t, backend)
			client := newTestGlueClient(t, glue.NewHandler(backend))

			names := tc.call(t, client)

			require.Len(t, names, 1, "Tags filter must exclude the untagged sibling, not just accept the field")
			assert.Contains(t, names[0], tc.match)
		})
	}
}

// TestSDKRoundTrip_GetColumnStatisticsTaskRuns_ScopesByTable proves
// GetColumnStatisticsTaskRuns actually scopes to the requested
// DatabaseName/TableName (both real, required GetColumnStatisticsTaskRunsInput
// members per glue@v1.152.0 api_op_GetColumnStatisticsTaskRuns.go) instead of
// returning every column-statistics task run in the account regardless of
// table -- the inverse bug found while fixing this op's MaxResults/NextToken:
// the pre-fix handler ignored DatabaseName/TableName entirely, not just
// pagination.
func TestSDKRoundTrip_GetColumnStatisticsTaskRuns_ScopesByTable(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	_, err := backend.StartColumnStatisticsTaskRun("db1", "tblA")
	require.NoError(t, err)
	_, err = backend.StartColumnStatisticsTaskRun("db1", "tblB")
	require.NoError(t, err)
	_, err = backend.StartColumnStatisticsTaskRun("db2", "tblA")
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.GetColumnStatisticsTaskRuns(t.Context(), &gluesdk.GetColumnStatisticsTaskRunsInput{
		DatabaseName: aws.String("db1"),
		TableName:    aws.String("tblA"),
	})
	require.NoError(t, err)
	require.Len(t, out.ColumnStatisticsTaskRuns, 1, "must scope to the requested db1.tblA run only")
	assert.Equal(t, "db1", aws.ToString(out.ColumnStatisticsTaskRuns[0].DatabaseName))
	assert.Equal(t, "tblA", aws.ToString(out.ColumnStatisticsTaskRuns[0].TableName))
}

// TestSDKRoundTrip_Triggers_DependentJobName proves GetTriggers/ListTriggers'
// DependentJobName real semantics (api_op_GetTriggers.go: "The trigger that
// can start this job is returned, and if there is no such trigger, all
// triggers are returned"): a match narrows the result, and a name with no
// matching trigger falls back to every trigger rather than an empty list.
func TestSDKRoundTrip_Triggers_DependentJobName(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	_, err := backend.CreateTrigger(glue.Trigger{
		Name: "trig-a", Type: "ON_DEMAND", Actions: []glue.TriggerAction{{JobName: "job-a"}},
	}, nil)
	require.NoError(t, err)
	_, err = backend.CreateTrigger(glue.Trigger{
		Name: "trig-b", Type: "ON_DEMAND", Actions: []glue.TriggerAction{{JobName: "job-b"}},
	}, nil)
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))

	t.Run("matching job name narrows to its trigger", func(t *testing.T) {
		t.Parallel()

		out, callErr := client.ListTriggers(
			t.Context(), &gluesdk.ListTriggersInput{DependentJobName: aws.String("job-a")},
		)
		require.NoError(t, callErr)
		assert.Equal(t, []string{"trig-a"}, out.TriggerNames)
	})

	t.Run("unmatched job name falls back to every trigger", func(t *testing.T) {
		t.Parallel()

		out, callErr := client.ListTriggers(
			t.Context(), &gluesdk.ListTriggersInput{DependentJobName: aws.String("no-such-job")},
		)
		require.NoError(t, callErr)
		assert.ElementsMatch(t, []string{"trig-a", "trig-b"}, out.TriggerNames)
	})
}

// TestSDKRoundTrip_GetConnections_FilterAndHidePassword proves GetConnections'
// Filter.ConnectionType actually excludes non-matching connections and
// HidePassword actually redacts the PASSWORD connection property, rather than
// both being accepted and discarded.
func TestSDKRoundTrip_GetConnections_FilterAndHidePassword(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	_, err := backend.CreateConnectionWithOptions(
		"jdbc-conn", "JDBC", map[string]string{"PASSWORD": "secret", "USERNAME": "u"}, nil, glue.ConnectionOptions{},
	)
	require.NoError(t, err)
	_, err = backend.CreateConnectionWithOptions(
		"kafka-conn",
		"KAFKA",
		map[string]string{},
		nil,
		glue.ConnectionOptions{},
	)
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))

	t.Run("filter by connection type excludes the other type", func(t *testing.T) {
		t.Parallel()

		out, callErr := client.GetConnections(t.Context(), &gluesdk.GetConnectionsInput{
			Filter: &types.GetConnectionsFilter{ConnectionType: types.ConnectionType("JDBC")},
		})
		require.NoError(t, callErr)
		require.Len(t, out.ConnectionList, 1)
		assert.Equal(t, "jdbc-conn", aws.ToString(out.ConnectionList[0].Name))
	})

	t.Run("hide password redacts PASSWORD but keeps other properties", func(t *testing.T) {
		t.Parallel()

		out, callErr := client.GetConnections(t.Context(), &gluesdk.GetConnectionsInput{HidePassword: true})
		require.NoError(t, callErr)

		var jdbc *types.Connection

		for i := range out.ConnectionList {
			if aws.ToString(out.ConnectionList[i].Name) == "jdbc-conn" {
				jdbc = &out.ConnectionList[i]
			}
		}

		require.NotNil(t, jdbc)
		assert.NotContains(t, jdbc.ConnectionProperties, "PASSWORD")
		assert.Equal(t, "u", jdbc.ConnectionProperties["USERNAME"])
	})
}

// TestSDKRoundTrip_GetCatalogs_HasDatabases proves GetCatalogsInput.HasDatabases
// actually filters catalogs by whether any Database.CatalogId points at them,
// rather than being discarded.
func TestSDKRoundTrip_GetCatalogs_HasDatabases(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	// CreateDatabase always assigns the account's default CatalogID
	// (databases.go: CatalogID: b.accountID), so the catalog whose
	// presence/absence proves the filter must itself carry that ID.
	require.NoError(t, backend.CreateCatalog(testAccountID, "default", "", nil))
	require.NoError(t, backend.CreateCatalog("cat-empty", "cat-empty", "", nil))
	_, err := backend.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.GetCatalogs(t.Context(), &gluesdk.GetCatalogsInput{HasDatabases: aws.Bool(true)})
	require.NoError(t, err)

	ids := make([]string, 0, len(out.CatalogList))
	for _, c := range out.CatalogList {
		ids = append(ids, aws.ToString(c.CatalogId))
	}

	assert.Contains(t, ids, testAccountID, "the catalog a database actually points at must be included")
	assert.NotContains(t, ids, "cat-empty", "cat-empty has no Database pointing at it")
}

// TestSDKRoundTrip_MLTransforms_FilterAndSort proves GetMLTransforms/
// ListMLTransforms' Filter.Status actually excludes non-matching transforms
// and Sort.Column=NAME with SortDirection=DESCENDING actually reorders the
// result, rather than both being discarded.
func TestSDKRoundTrip_MLTransforms_FilterAndSort(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	for _, n := range []string{"aaa", "bbb", "ccc"} {
		_, err := backend.CreateMLTransform(n, "", "role", nil, glue.MLTransformParameter{}, nil)
		require.NoError(t, err)
	}

	client := newTestGlueClient(t, glue.NewHandler(backend))

	t.Run("sort by name descending reorders the result", func(t *testing.T) {
		t.Parallel()

		// CreateMLTransform assigns an opaque generated TransformId (ml.go),
		// so GetMLTransforms (which returns the full Transform, Name
		// included) proves the ordering, not ListMLTransforms' bare IDs.
		out, err := client.GetMLTransforms(t.Context(), &gluesdk.GetMLTransformsInput{
			Sort: &types.TransformSortCriteria{
				Column: types.TransformSortColumnTypeName, SortDirection: types.SortDirectionTypeDescending,
			},
		})
		require.NoError(t, err)
		require.Len(t, out.Transforms, 3)

		names := make([]string, len(out.Transforms))
		for i, tr := range out.Transforms {
			names[i] = aws.ToString(tr.Name)
		}
		assert.Equal(t, []string{"ccc", "bbb", "aaa"}, names)
	})

	t.Run("filter by status excludes non-matching transforms", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetMLTransforms(t.Context(), &gluesdk.GetMLTransformsInput{
			Filter: &types.TransformFilterCriteria{Status: types.TransformStatusTypeDeleting},
		})
		require.NoError(t, err)
		assert.Empty(t, out.Transforms, "no transform has DELETING status, so the filter must exclude all three")
	})
}

// TestSDKRoundTrip_ListMLTransforms_TagsFilter proves ListMLTransformsInput.Tags
// actually excludes an untagged sibling transform. Kept separate from
// TestSDKRoundTrip_TagsFilter's generic table because ListMLTransforms
// returns opaque generated TransformIds (ml.go), not names, so the assertion
// needs the actual created ID rather than a name substring match.
func TestSDKRoundTrip_ListMLTransforms_TagsFilter(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	tagged, err := backend.CreateMLTransform(
		"mlt-tagged", "", "role", nil, glue.MLTransformParameter{}, map[string]string{"env": "prod"},
	)
	require.NoError(t, err)
	_, err = backend.CreateMLTransform("mlt-untagged", "", "role", nil, glue.MLTransformParameter{}, nil)
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.ListMLTransforms(
		t.Context(), &gluesdk.ListMLTransformsInput{Tags: map[string]string{"env": "prod"}},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{tagged.TransformID}, out.TransformIds)
}

// TestSDKRoundTrip_DescribeIntegrations_FilterByStatus proves
// DescribeIntegrationsInput.Filters actually excludes non-matching
// integrations by the documented "Status" key rather than being discarded.
func TestSDKRoundTrip_DescribeIntegrations_FilterByStatus(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	_, err := backend.CreateIntegration(
		"ig-a", "arn:aws:rds:us-east-1:000000000000:db/a", "arn:aws:redshift:us-east-1:000000000000:cluster/a", nil,
	)
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.DescribeIntegrations(t.Context(), &gluesdk.DescribeIntegrationsInput{
		Filters: []types.IntegrationFilter{{Name: aws.String("Status"), Values: []string{"NO_SUCH_STATUS"}}},
	})
	require.NoError(t, err)
	assert.Empty(t, out.Integrations, "the created integration's real status must not match NO_SUCH_STATUS")
}

// TestSDKRoundTrip_ListMaterializedViewRefreshTaskRuns_ScopesByTable proves
// DatabaseName/TableName actually narrow ListMaterializedViewRefreshTaskRuns'
// result to the matching table rather than being discarded.
func TestSDKRoundTrip_ListMaterializedViewRefreshTaskRuns_ScopesByTable(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	_, err := backend.StartMaterializedViewRefreshTaskRun("dbA", "tblA")
	require.NoError(t, err)
	_, err = backend.StartMaterializedViewRefreshTaskRun("dbB", "tblB")
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.ListMaterializedViewRefreshTaskRuns(
		t.Context(),
		&gluesdk.ListMaterializedViewRefreshTaskRunsInput{
			CatalogId: aws.String(testAccountID), DatabaseName: aws.String("dbA"), TableName: aws.String("tblA"),
		},
	)
	require.NoError(t, err)
	require.Len(t, out.MaterializedViewRefreshTaskRuns, 1)
}

// TestSDKRoundTrip_DataQualityRuns_StartedFilterAndRulesetName proves
// ListDataQualityRuleRecommendationRuns/ListDataQualityRulesetEvaluationRuns'
// StartedAfter and, for evaluation runs, RulesetName actually narrow the
// result rather than being discarded.
func TestSDKRoundTrip_DataQualityRuns_StartedFilterAndRulesetName(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	_, err := backend.CreateDataQualityRuleset("ruleset-a", "Rules = [ IsComplete \"id\" ]", nil)
	require.NoError(t, err)
	_, err = backend.CreateDataQualityRuleset("ruleset-b", "Rules = [ IsComplete \"id\" ]", nil)
	require.NoError(t, err)
	runA, err := backend.StartDataQualityRulesetEvaluationRun([]string{"ruleset-a"})
	require.NoError(t, err)
	_, err = backend.StartDataQualityRulesetEvaluationRun([]string{"ruleset-b"})
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))

	t.Run("ruleset name narrows to the matching run", func(t *testing.T) {
		t.Parallel()

		out, callErr := client.ListDataQualityRulesetEvaluationRuns(
			t.Context(),
			&gluesdk.ListDataQualityRulesetEvaluationRunsInput{
				Filter: &types.DataQualityRulesetEvaluationRunFilter{
					RulesetName: aws.String("ruleset-a"),
					DataSource:  inertDataSource(),
				},
			},
		)
		require.NoError(t, callErr)
		require.Len(t, out.Runs, 1)
		assert.Equal(t, runA.RunID, aws.ToString(out.Runs[0].RunId))
	})

	t.Run("started after a future time excludes every run", func(t *testing.T) {
		t.Parallel()

		out, callErr := client.ListDataQualityRulesetEvaluationRuns(
			t.Context(),
			&gluesdk.ListDataQualityRulesetEvaluationRunsInput{
				Filter: &types.DataQualityRulesetEvaluationRunFilter{
					StartedAfter: aws.Time(farFutureTime(t)),
					DataSource:   inertDataSource(),
				},
			},
		)
		require.NoError(t, callErr)
		assert.Empty(t, out.Runs)
	})
}
