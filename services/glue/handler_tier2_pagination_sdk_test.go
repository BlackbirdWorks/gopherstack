package glue_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func seedGlueTable(t *testing.T, b *glue.InMemoryBackend, names ...string) {
	t.Helper()

	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)

	for _, n := range names {
		_, err = b.CreateTable("db", glue.TableInput{Name: n})
		require.NoError(t, err)
	}
}

func tier2PaginationCases() []paginationCase {
	return []paginationCase{
		{
			name: "get job runs", want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateJob(glue.Job{Name: "j", Role: "r", Command: glue.JobCommand{Name: "glueetl"}})
				require.NoError(t, err)

				for range 3 {
					_, err = b.StartJobRun("j", nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, size int32, tok *string) (int, *string) {
				t.Helper()

				out, err := c.GetJobRuns(
					ctx,
					&gluesdk.GetJobRunsInput{JobName: aws.String("j"), MaxResults: aws.Int32(size), NextToken: tok},
				)
				require.NoError(t, err)

				return len(out.JobRuns), out.NextToken
			},
		},
		{
			name: "search tables", want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) { t.Helper(); seedGlueTable(t, b, "t1", "t2", "t3") },
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, size int32, tok *string) (int, *string) {
				t.Helper()

				out, err := c.SearchTables(ctx, &gluesdk.SearchTablesInput{MaxResults: aws.Int32(size), NextToken: tok})
				require.NoError(t, err)

				return len(out.TableList), out.NextToken
			},
		},
		{
			name: "get crawler metrics", want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
				require.NoError(t, err)

				for _, n := range []string{"c1", "c2", "c3"} {
					_, err = b.CreateCrawler(n, "role", "db", glue.CrawlerTarget{
						S3Targets: []glue.S3Target{{Path: "s3://b/" + n}},
					}, nil)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, size int32, tok *string) (int, *string) {
				t.Helper()

				out, err := c.GetCrawlerMetrics(
					ctx,
					&gluesdk.GetCrawlerMetricsInput{MaxResults: aws.Int32(size), NextToken: tok},
				)
				require.NoError(t, err)

				return len(out.CrawlerMetricsList), out.NextToken
			},
		},
		{
			name: "get unfiltered partitions metadata", want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				seedGlueTable(t, b, "t")

				_, errs := b.BatchCreatePartition("db", "t", []glue.PartitionInput{
					{Values: []string{"a"}}, {Values: []string{"b"}}, {Values: []string{"c"}},
				})
				require.Empty(t, errs)
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, size int32, tok *string) (int, *string) {
				t.Helper()

				out, err := c.GetUnfilteredPartitionsMetadata(ctx, &gluesdk.GetUnfilteredPartitionsMetadataInput{
					CatalogId: aws.String("000000000000"), DatabaseName: aws.String("db"), TableName: aws.String("t"),
					SupportedPermissionTypes: []types.PermissionType{types.PermissionTypeColumnPermission},
					MaxResults:               aws.Int32(size), NextToken: tok,
				})
				require.NoError(t, err)

				return len(out.UnfilteredPartitions), out.NextToken
			},
		},
		{
			name: "get user defined functions", want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()
				seedGlueTable(t, b)

				for _, n := range []string{"f1", "f2", "f3"} {
					_, err := b.CreateUserDefinedFunction(
						"db",
						glue.UserDefinedFunction{FunctionName: n, ClassName: "C"},
						nil,
					)
					require.NoError(t, err)
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, size int32, tok *string) (int, *string) {
				t.Helper()

				out, err := c.GetUserDefinedFunctions(ctx, &gluesdk.GetUserDefinedFunctionsInput{
					DatabaseName: aws.String(
						"db",
					), Pattern: aws.String("f.*"), MaxResults: aws.Int32(size), NextToken: tok,
				})
				require.NoError(t, err)

				return len(out.UserDefinedFunctions), out.NextToken
			},
		},
		{
			name: "query schema version metadata", want: 3,
			seed: func(t *testing.T, b *glue.InMemoryBackend) {
				t.Helper()

				for _, k := range []string{"k1", "k2", "k3"} {
					require.NoError(t, b.PutSchemaVersionMetadata("sv-1", k, "v"))
				}
			},
			list: func(t *testing.T, ctx context.Context, c *gluesdk.Client, size int32, tok *string) (int, *string) {
				t.Helper()

				out, err := c.QuerySchemaVersionMetadata(ctx, &gluesdk.QuerySchemaVersionMetadataInput{
					SchemaVersionId: aws.String("sv-1"), MaxResults: aws.Int32(size), NextToken: tok,
				})
				require.NoError(t, err)

				return len(out.MetadataInfoMap), out.NextToken
			},
		},
	}
}

// TestSDKRoundTrip_Tier2ListPagination pages ops whose MaxResults/NextToken were ignored.
func TestSDKRoundTrip_Tier2ListPagination(t *testing.T) {
	t.Parallel()

	for _, tc := range tier2PaginationCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runPaginationCase(t, tc)
		})
	}
}

func TestSDKRoundTrip_Tier2FiltersAndTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(ctx context.Context, c *gluesdk.Client) (int, error)
		name    string
		want    int
		wantErr bool
	}{
		{name: "search_tables_name_token", want: 1, call: func(ctx context.Context, c *gluesdk.Client) (int, error) {
			out, err := c.SearchTables(ctx, &gluesdk.SearchTablesInput{
				Filters: []types.PropertyPredicate{{Key: aws.String("Name"), Value: aws.String("link")}},
			})
			if err != nil {
				return 0, err
			}

			return len(out.TableList), nil
		}},
		{
			name:    "search_tables_bad_token",
			wantErr: true,
			call: func(ctx context.Context, c *gluesdk.Client) (int, error) {
				_, err := c.SearchTables(ctx, &gluesdk.SearchTablesInput{NextToken: aws.String("x")})

				return 0, err
			},
		},
		{
			name:    "get_job_runs_bad_token",
			wantErr: true,
			call: func(ctx context.Context, c *gluesdk.Client) (int, error) {
				_, err := c.GetJobRuns(
					ctx,
					&gluesdk.GetJobRunsInput{JobName: aws.String("j"), NextToken: aws.String("-1")},
				)

				return 0, err
			},
		},
		{
			name:    "list_dq_statistics_bad_token",
			wantErr: true,
			call: func(ctx context.Context, c *gluesdk.Client) (int, error) {
				_, err := c.ListDataQualityStatistics(
					ctx,
					&gluesdk.ListDataQualityStatisticsInput{NextToken: aws.String("x")},
				)

				return 0, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			seedGlueTable(t, b, "xx-link-yy", "xxlinkyy", "other")
			_, err := b.CreateJob(glue.Job{Name: "j", Role: "r", Command: glue.JobCommand{Name: "glueetl"}})
			require.NoError(t, err)

			n, err := tt.call(t.Context(), newTestGlueClient(t, glue.NewHandler(b)))
			if tt.wantErr {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				require.Equal(t, "InvalidInputException", apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, n)
		})
	}
}

func TestSDKRoundTrip_IntegrationDataFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		modify *string
		name   string
		create string
		want   string
	}{
		{name: "create_echoes", create: "include: db.*", want: "include: db.*"},
		{name: "modify_replaces", create: "include: a.*", modify: aws.String("include: b.*"), want: "include: b.*"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestGlueClient(t, glue.NewHandler(glue.NewInMemoryBackend(testAccountID, testRegion)))
			created, err := c.CreateIntegration(t.Context(), &gluesdk.CreateIntegrationInput{
				IntegrationName: aws.String("ig"), DataFilter: aws.String(tt.create),
				SourceArn: aws.String("arn:aws:rds:us-east-1:000000000000:db:src"),
				TargetArn: aws.String("arn:aws:redshift:us-east-1:000000000000:namespace:tgt"),
			})
			require.NoError(t, err)
			require.Equal(t, tt.create, aws.ToString(created.DataFilter))

			if tt.modify != nil {
				out, modErr := c.ModifyIntegration(t.Context(), &gluesdk.ModifyIntegrationInput{
					IntegrationIdentifier: aws.String("ig"), DataFilter: tt.modify,
				})
				require.NoError(t, modErr)
				require.Equal(t, tt.want, aws.ToString(out.DataFilter))
			}
		})
	}
}
