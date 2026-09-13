package personalize_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"
	"github.com/aws/aws-sdk-go-v2/service/personalize/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// newPersonalizeClient stands up a fresh backend/handler pair plus a real
// personalize SDK client bound to it. Prerequisite fixtures still use the
// existing personalizeCreate* helpers (raw X-Amz-Target protocol) since FK
// chains here run several levels deep; the op under test in each subtest is
// always driven through the returned real client.
func newPersonalizeClient(t *testing.T) (*personalize.Handler, *personalizesdk.Client) {
	t.Helper()

	b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
	h := personalize.NewHandler(b)

	return h, newTestPersonalizeClient(t, h)
}

// TestRealClient_ResourceLifecycle drives every op the census still
// listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage). Each case builds its own fresh backend/client.
func TestRealClient_ResourceLifecycle(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "schema_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newPersonalizeClient(t)

				created, err := client.CreateSchema(t.Context(), &personalizesdk.CreateSchemaInput{
					Name:   aws.String("s1"),
					Schema: aws.String(`{"type":"record","name":"i","fields":[]}`),
				})
				require.NoError(t, err)
				schemaArn := aws.ToString(created.SchemaArn)
				require.NotEmpty(t, schemaArn)

				described, err := client.DescribeSchema(t.Context(), &personalizesdk.DescribeSchemaInput{
					SchemaArn: aws.String(schemaArn),
				})
				require.NoError(t, err)
				assert.Equal(t, "s1", aws.ToString(described.Schema.Name))
				assert.JSONEq(
					t,
					`{"type":"record","name":"i","fields":[]}`,
					aws.ToString(described.Schema.Schema),
				)

				listed, err := client.ListSchemas(t.Context(), &personalizesdk.ListSchemasInput{})
				require.NoError(t, err)
				require.Len(t, listed.Schemas, 1)
				assert.Equal(t, schemaArn, aws.ToString(listed.Schemas[0].SchemaArn))

				_, err = client.DeleteSchema(t.Context(), &personalizesdk.DeleteSchemaInput{
					SchemaArn: aws.String(schemaArn),
				})
				require.NoError(t, err)

				_, err = client.DescribeSchema(t.Context(), &personalizesdk.DescribeSchemaInput{
					SchemaArn: aws.String(schemaArn),
				})
				require.Error(t, err)
			},
		},
		{
			name: "dataset_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				dgArn := personalizeCreateDatasetGroup(t, h, "ds-dg")
				schemaArn := personalizeCreateSchema(t, h, "ds-schema")
				schemaArn2 := personalizeCreateSchema(t, h, "ds-schema-2")

				created, err := client.CreateDataset(t.Context(), &personalizesdk.CreateDatasetInput{
					Name:            aws.String("ds1"),
					DatasetGroupArn: aws.String(dgArn),
					DatasetType:     aws.String("INTERACTIONS"),
					SchemaArn:       aws.String(schemaArn),
				})
				require.NoError(t, err)
				dsArn := aws.ToString(created.DatasetArn)
				require.NotEmpty(t, dsArn)

				described, err := client.DescribeDataset(t.Context(), &personalizesdk.DescribeDatasetInput{
					DatasetArn: aws.String(dsArn),
				})
				require.NoError(t, err)
				assert.Equal(t, "INTERACTIONS", aws.ToString(described.Dataset.DatasetType))
				assert.Equal(t, schemaArn, aws.ToString(described.Dataset.SchemaArn))
				assert.Equal(t, "ACTIVE", aws.ToString(described.Dataset.Status))

				_, err = client.UpdateDataset(t.Context(), &personalizesdk.UpdateDatasetInput{
					DatasetArn: aws.String(dsArn),
					SchemaArn:  aws.String(schemaArn2),
				})
				require.NoError(t, err)

				described, err = client.DescribeDataset(t.Context(), &personalizesdk.DescribeDatasetInput{
					DatasetArn: aws.String(dsArn),
				})
				require.NoError(t, err)
				assert.Equal(t, schemaArn2, aws.ToString(described.Dataset.SchemaArn))

				listed, err := client.ListDatasets(t.Context(), &personalizesdk.ListDatasetsInput{
					DatasetGroupArn: aws.String(dgArn),
				})
				require.NoError(t, err)
				require.Len(t, listed.Datasets, 1)
				assert.Equal(t, "ds1", aws.ToString(listed.Datasets[0].Name))

				_, err = client.DeleteDataset(t.Context(), &personalizesdk.DeleteDatasetInput{
					DatasetArn: aws.String(dsArn),
				})
				require.NoError(t, err)

				_, err = client.DescribeDataset(t.Context(), &personalizesdk.DescribeDatasetInput{
					DatasetArn: aws.String(dsArn),
				})
				require.Error(t, err)
			},
		},
		{
			name: "dataset_jobs",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				dsArn := personalizeCreateDataset(t, h, "job-ds")

				importCreated, err := client.CreateDatasetImportJob(
					t.Context(),
					&personalizesdk.CreateDatasetImportJobInput{
						JobName:    aws.String("import1"),
						DatasetArn: aws.String(dsArn),
						RoleArn:    aws.String("arn:aws:iam::000000000000:role/personalize-role"),
						DataSource: &types.DataSource{DataLocation: aws.String("s3://bucket/import.csv")},
					},
				)
				require.NoError(t, err)
				importArn := aws.ToString(importCreated.DatasetImportJobArn)
				require.NotEmpty(t, importArn)

				importDescribed, err := client.DescribeDatasetImportJob(
					t.Context(),
					&personalizesdk.DescribeDatasetImportJobInput{
						DatasetImportJobArn: aws.String(importArn),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "import1", aws.ToString(importDescribed.DatasetImportJob.JobName))
				require.NotNil(t, importDescribed.DatasetImportJob.DataSource)
				assert.Equal(t, "s3://bucket/import.csv",
					aws.ToString(importDescribed.DatasetImportJob.DataSource.DataLocation))
				assert.Equal(t, "ACTIVE", aws.ToString(importDescribed.DatasetImportJob.Status))

				importListed, err := client.ListDatasetImportJobs(
					t.Context(),
					&personalizesdk.ListDatasetImportJobsInput{
						DatasetArn: aws.String(dsArn),
					},
				)
				require.NoError(t, err)
				require.Len(t, importListed.DatasetImportJobs, 1)
				assert.Equal(t, "import1", aws.ToString(importListed.DatasetImportJobs[0].JobName))

				exportCreated, err := client.CreateDatasetExportJob(
					t.Context(),
					&personalizesdk.CreateDatasetExportJobInput{
						JobName:    aws.String("export1"),
						DatasetArn: aws.String(dsArn),
						RoleArn:    aws.String("arn:aws:iam::000000000000:role/personalize-role"),
						JobOutput: &types.DatasetExportJobOutput{
							S3DataDestination: &types.S3DataConfig{Path: aws.String("s3://bucket/export/")},
						},
					},
				)
				require.NoError(t, err)
				exportArn := aws.ToString(exportCreated.DatasetExportJobArn)
				require.NotEmpty(t, exportArn)

				exportDescribed, err := client.DescribeDatasetExportJob(
					t.Context(),
					&personalizesdk.DescribeDatasetExportJobInput{
						DatasetExportJobArn: aws.String(exportArn),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "export1", aws.ToString(exportDescribed.DatasetExportJob.JobName))
				require.NotNil(t, exportDescribed.DatasetExportJob.JobOutput)
				require.NotNil(t, exportDescribed.DatasetExportJob.JobOutput.S3DataDestination)
				assert.Equal(t, "s3://bucket/export/",
					aws.ToString(exportDescribed.DatasetExportJob.JobOutput.S3DataDestination.Path))

				exportListed, err := client.ListDatasetExportJobs(
					t.Context(),
					&personalizesdk.ListDatasetExportJobsInput{
						DatasetArn: aws.String(dsArn),
					},
				)
				require.NoError(t, err)
				require.Len(t, exportListed.DatasetExportJobs, 1)
				assert.Equal(t, "export1", aws.ToString(exportListed.DatasetExportJobs[0].JobName))
			},
		},
		{
			name: "solution_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				solArn := personalizeCreateSolution(t, h, "sol-x")

				svCreated, err := client.CreateSolutionVersion(
					t.Context(),
					&personalizesdk.CreateSolutionVersionInput{
						SolutionArn: aws.String(solArn),
					},
				)
				require.NoError(t, err)
				svArn := aws.ToString(svCreated.SolutionVersionArn)
				require.NotEmpty(t, svArn)

				listedVersions, err := client.ListSolutionVersions(
					t.Context(),
					&personalizesdk.ListSolutionVersionsInput{
						SolutionArn: aws.String(solArn),
					},
				)
				require.NoError(t, err)
				require.Len(t, listedVersions.SolutionVersions, 1)
				assert.Equal(t, svArn, aws.ToString(listedVersions.SolutionVersions[0].SolutionVersionArn))

				metrics, err := client.GetSolutionMetrics(
					t.Context(),
					&personalizesdk.GetSolutionMetricsInput{
						SolutionVersionArn: aws.String(svArn),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, svArn, aws.ToString(metrics.SolutionVersionArn))
				assert.Contains(t, metrics.Metrics, "coverage")

				_, err = client.StopSolutionVersionCreation(
					t.Context(),
					&personalizesdk.StopSolutionVersionCreationInput{
						SolutionVersionArn: aws.String(svArn),
					},
				)
				require.NoError(t, err)

				listedSolutions, err := client.ListSolutions(
					t.Context(),
					&personalizesdk.ListSolutionsInput{},
				)
				require.NoError(t, err)
				var found bool
				for _, s := range listedSolutions.Solutions {
					if aws.ToString(s.SolutionArn) == solArn {
						found = true
					}
				}
				assert.True(t, found, "ListSolutions should include %q", solArn)

				delSolArn := personalizeCreateSolution(t, h, "sol-del")
				_, err = client.DeleteSolution(t.Context(), &personalizesdk.DeleteSolutionInput{
					SolutionArn: aws.String(delSolArn),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "campaign_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				const campaignName = "camp-x"
				personalizeCreateCampaign(t, h, campaignName)
				campaignArn := "arn:aws:personalize:us-east-1:000000000000:campaign/" + campaignName

				described, err := client.DescribeCampaign(
					t.Context(),
					&personalizesdk.DescribeCampaignInput{
						CampaignArn: aws.String(campaignArn),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, campaignName, aws.ToString(described.Campaign.Name))
				assert.Equal(t, "ACTIVE", aws.ToString(described.Campaign.Status))

				svArn2 := personalizeCreateSolutionVersion(t, h, campaignName+"-sol2")

				_, err = client.UpdateCampaign(t.Context(), &personalizesdk.UpdateCampaignInput{
					CampaignArn:        aws.String(campaignArn),
					SolutionVersionArn: aws.String(svArn2),
					MinProvisionedTPS:  aws.Int32(2),
				})
				require.NoError(t, err)

				described, err = client.DescribeCampaign(t.Context(), &personalizesdk.DescribeCampaignInput{
					CampaignArn: aws.String(campaignArn),
				})
				require.NoError(t, err)
				assert.Equal(t, svArn2, aws.ToString(described.Campaign.SolutionVersionArn))
				require.NotNil(t, described.Campaign.MinProvisionedTPS)
				assert.Equal(t, int32(2), *described.Campaign.MinProvisionedTPS)
				require.NotNil(t, described.Campaign.LatestCampaignUpdate)

				_, err = client.DeleteCampaign(t.Context(), &personalizesdk.DeleteCampaignInput{
					CampaignArn: aws.String(campaignArn),
				})
				require.NoError(t, err)

				_, err = client.DescribeCampaign(t.Context(), &personalizesdk.DescribeCampaignInput{
					CampaignArn: aws.String(campaignArn),
				})
				require.Error(t, err)
			},
		},
		{
			name: "event_tracker_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				dgArn := personalizeCreateDatasetGroup(t, h, "et-dg")

				created, err := client.CreateEventTracker(
					t.Context(),
					&personalizesdk.CreateEventTrackerInput{
						Name:            aws.String("et1"),
						DatasetGroupArn: aws.String(dgArn),
					},
				)
				require.NoError(t, err)
				trackerArn := aws.ToString(created.EventTrackerArn)
				require.NotEmpty(t, trackerArn)
				require.NotEmpty(t, aws.ToString(created.TrackingId))

				listed, err := client.ListEventTrackers(t.Context(), &personalizesdk.ListEventTrackersInput{
					DatasetGroupArn: aws.String(dgArn),
				})
				require.NoError(t, err)
				require.Len(t, listed.EventTrackers, 1)
				assert.Equal(t, "et1", aws.ToString(listed.EventTrackers[0].Name))

				_, err = client.DeleteEventTracker(t.Context(), &personalizesdk.DeleteEventTrackerInput{
					EventTrackerArn: aws.String(trackerArn),
				})
				require.NoError(t, err)

				listed, err = client.ListEventTrackers(t.Context(), &personalizesdk.ListEventTrackersInput{
					DatasetGroupArn: aws.String(dgArn),
				})
				require.NoError(t, err)
				assert.Empty(t, listed.EventTrackers)
			},
		},
		{
			name: "filter_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				dgArn := personalizeCreateDatasetGroup(t, h, "filter-dg")

				created, err := client.CreateFilter(t.Context(), &personalizesdk.CreateFilterInput{
					Name:             aws.String("f1"),
					DatasetGroupArn:  aws.String(dgArn),
					FilterExpression: aws.String("INCLUDE ItemID WHERE Items.CATEGORY IN ($CATEGORIES)"),
				})
				require.NoError(t, err)
				filterArn := aws.ToString(created.FilterArn)
				require.NotEmpty(t, filterArn)

				described, err := client.DescribeFilter(t.Context(), &personalizesdk.DescribeFilterInput{
					FilterArn: aws.String(filterArn),
				})
				require.NoError(t, err)
				assert.Equal(t, "f1", aws.ToString(described.Filter.Name))
				assert.Equal(t, "INCLUDE ItemID WHERE Items.CATEGORY IN ($CATEGORIES)",
					aws.ToString(described.Filter.FilterExpression))

				_, err = client.DeleteFilter(t.Context(), &personalizesdk.DeleteFilterInput{
					FilterArn: aws.String(filterArn),
				})
				require.NoError(t, err)

				_, err = client.DescribeFilter(t.Context(), &personalizesdk.DescribeFilterInput{
					FilterArn: aws.String(filterArn),
				})
				require.Error(t, err)
			},
		},
		{
			name: "recommender_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				dgArn := personalizeCreateDatasetGroup(t, h, "rec-dg")
				const recipeArn = "arn:aws:personalize:::recipe/aws-user-personalization"

				created, err := client.CreateRecommender(
					t.Context(),
					&personalizesdk.CreateRecommenderInput{
						Name:            aws.String("rec1"),
						DatasetGroupArn: aws.String(dgArn),
						RecipeArn:       aws.String(recipeArn),
					},
				)
				require.NoError(t, err)
				recommenderArn := aws.ToString(created.RecommenderArn)
				require.NotEmpty(t, recommenderArn)

				_, err = client.UpdateRecommender(t.Context(), &personalizesdk.UpdateRecommenderInput{
					RecommenderArn: aws.String(recommenderArn),
					RecommenderConfig: &types.RecommenderConfig{
						ItemExplorationConfig: map[string]string{"explorationWeight": "0.3"},
					},
				})
				require.NoError(t, err)

				listed, err := client.ListRecommenders(t.Context(), &personalizesdk.ListRecommendersInput{
					DatasetGroupArn: aws.String(dgArn),
				})
				require.NoError(t, err)
				require.Len(t, listed.Recommenders, 1)
				assert.Equal(t, "rec1", aws.ToString(listed.Recommenders[0].Name))
				require.NotNil(t, listed.Recommenders[0].RecommenderConfig)
				assert.Equal(
					t,
					"0.3",
					listed.Recommenders[0].RecommenderConfig.ItemExplorationConfig["explorationWeight"],
				)

				stopped, err := client.StopRecommender(t.Context(), &personalizesdk.StopRecommenderInput{
					RecommenderArn: aws.String(recommenderArn),
				})
				require.NoError(t, err)
				assert.Equal(t, recommenderArn, aws.ToString(stopped.RecommenderArn))

				started, err := client.StartRecommender(t.Context(), &personalizesdk.StartRecommenderInput{
					RecommenderArn: aws.String(recommenderArn),
				})
				require.NoError(t, err)
				assert.Equal(t, recommenderArn, aws.ToString(started.RecommenderArn))

				_, err = client.DeleteRecommender(t.Context(), &personalizesdk.DeleteRecommenderInput{
					RecommenderArn: aws.String(recommenderArn),
				})
				require.NoError(t, err)

				listed, err = client.ListRecommenders(t.Context(), &personalizesdk.ListRecommendersInput{
					DatasetGroupArn: aws.String(dgArn),
				})
				require.NoError(t, err)
				assert.Empty(t, listed.Recommenders)
			},
		},
		{
			name: "metric_attribution_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				dgArn := personalizeCreateDatasetGroup(t, h, "ma-dg")

				created, err := client.CreateMetricAttribution(
					t.Context(),
					&personalizesdk.CreateMetricAttributionInput{
						Name:            aws.String("ma1"),
						DatasetGroupArn: aws.String(dgArn),
						Metrics: []types.MetricAttribute{
							{
								EventType:  aws.String("purchase"),
								Expression: aws.String("SUM(Items.PRICE)"),
								MetricName: aws.String("m1"),
							},
						},
						MetricsOutputConfig: &types.MetricAttributionOutput{
							RoleArn: aws.String("arn:aws:iam::000000000000:role/personalize-role"),
						},
					},
				)
				require.NoError(t, err)
				maArn := aws.ToString(created.MetricAttributionArn)
				require.NotEmpty(t, maArn)

				described, err := client.DescribeMetricAttribution(
					t.Context(),
					&personalizesdk.DescribeMetricAttributionInput{MetricAttributionArn: aws.String(maArn)},
				)
				require.NoError(t, err)
				assert.Equal(t, "ma1", aws.ToString(described.MetricAttribution.Name))
				require.NotNil(t, described.MetricAttribution.MetricsOutputConfig)
				assert.Equal(t, "arn:aws:iam::000000000000:role/personalize-role",
					aws.ToString(described.MetricAttribution.MetricsOutputConfig.RoleArn))

				_, err = client.UpdateMetricAttribution(
					t.Context(),
					&personalizesdk.UpdateMetricAttributionInput{
						MetricAttributionArn: aws.String(maArn),
						AddMetrics: []types.MetricAttribute{
							{
								EventType:  aws.String("click"),
								Expression: aws.String("SAMPLECOUNT()"),
								MetricName: aws.String("m2"),
							},
						},
						RemoveMetrics: []string{"m1"},
					},
				)
				require.NoError(t, err)

				metricsListed, err := client.ListMetricAttributionMetrics(
					t.Context(),
					&personalizesdk.ListMetricAttributionMetricsInput{
						MetricAttributionArn: aws.String(maArn),
					},
				)
				require.NoError(t, err)
				require.Len(t, metricsListed.Metrics, 1)
				assert.Equal(t, "m2", aws.ToString(metricsListed.Metrics[0].MetricName))

				maListed, err := client.ListMetricAttributions(
					t.Context(),
					&personalizesdk.ListMetricAttributionsInput{
						DatasetGroupArn: aws.String(dgArn),
					},
				)
				require.NoError(t, err)
				require.Len(t, maListed.MetricAttributions, 1)
				assert.Equal(t, "ma1", aws.ToString(maListed.MetricAttributions[0].Name))

				_, err = client.DeleteMetricAttribution(
					t.Context(),
					&personalizesdk.DeleteMetricAttributionInput{
						MetricAttributionArn: aws.String(maArn),
					},
				)
				require.NoError(t, err)

				_, err = client.DescribeMetricAttribution(
					t.Context(),
					&personalizesdk.DescribeMetricAttributionInput{MetricAttributionArn: aws.String(maArn)},
				)
				require.Error(t, err)
			},
		},
		{
			name: "batch_inference_job",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				svArn := personalizeCreateSolutionVersion(t, h, "bij-sol")

				created, err := client.CreateBatchInferenceJob(
					t.Context(),
					&personalizesdk.CreateBatchInferenceJobInput{
						JobName:            aws.String("bij1"),
						SolutionVersionArn: aws.String(svArn),
						RoleArn:            aws.String("arn:aws:iam::000000000000:role/personalize-role"),
						JobInput: &types.BatchInferenceJobInput{
							S3DataSource: &types.S3DataConfig{Path: aws.String("s3://bucket/bij-in.json")},
						},
						JobOutput: &types.BatchInferenceJobOutput{
							S3DataDestination: &types.S3DataConfig{
								Path: aws.String("s3://bucket/bij-out/"),
							},
						},
					},
				)
				require.NoError(t, err)
				jobArn := aws.ToString(created.BatchInferenceJobArn)
				require.NotEmpty(t, jobArn)

				described, err := client.DescribeBatchInferenceJob(
					t.Context(),
					&personalizesdk.DescribeBatchInferenceJobInput{
						BatchInferenceJobArn: aws.String(jobArn),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "bij1", aws.ToString(described.BatchInferenceJob.JobName))
				require.NotNil(t, described.BatchInferenceJob.JobInput)
				require.NotNil(t, described.BatchInferenceJob.JobInput.S3DataSource)
				assert.Equal(
					t,
					"s3://bucket/bij-in.json",
					aws.ToString(described.BatchInferenceJob.JobInput.S3DataSource.Path),
				)
				assert.Equal(t, "ACTIVE", aws.ToString(described.BatchInferenceJob.Status))

				listed, err := client.ListBatchInferenceJobs(
					t.Context(),
					&personalizesdk.ListBatchInferenceJobsInput{
						SolutionVersionArn: aws.String(svArn),
					},
				)
				require.NoError(t, err)
				require.Len(t, listed.BatchInferenceJobs, 1)
				assert.Equal(t, "bij1", aws.ToString(listed.BatchInferenceJobs[0].JobName))
			},
		},
		{
			name: "batch_segment_job",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				svArn := personalizeCreateSolutionVersion(t, h, "bsj-sol")

				created, err := client.CreateBatchSegmentJob(
					t.Context(),
					&personalizesdk.CreateBatchSegmentJobInput{
						JobName:            aws.String("bsj1"),
						SolutionVersionArn: aws.String(svArn),
						RoleArn:            aws.String("arn:aws:iam::000000000000:role/personalize-role"),
						JobInput: &types.BatchSegmentJobInput{
							S3DataSource: &types.S3DataConfig{Path: aws.String("s3://bucket/bsj-in.json")},
						},
						JobOutput: &types.BatchSegmentJobOutput{
							S3DataDestination: &types.S3DataConfig{
								Path: aws.String("s3://bucket/bsj-out/"),
							},
						},
					},
				)
				require.NoError(t, err)
				jobArn := aws.ToString(created.BatchSegmentJobArn)
				require.NotEmpty(t, jobArn)

				described, err := client.DescribeBatchSegmentJob(
					t.Context(),
					&personalizesdk.DescribeBatchSegmentJobInput{BatchSegmentJobArn: aws.String(jobArn)},
				)
				require.NoError(t, err)
				assert.Equal(t, "bsj1", aws.ToString(described.BatchSegmentJob.JobName))
				require.NotNil(t, described.BatchSegmentJob.JobOutput)
				require.NotNil(t, described.BatchSegmentJob.JobOutput.S3DataDestination)
				assert.Equal(t, "s3://bucket/bsj-out/",
					aws.ToString(described.BatchSegmentJob.JobOutput.S3DataDestination.Path))

				listed, err := client.ListBatchSegmentJobs(
					t.Context(),
					&personalizesdk.ListBatchSegmentJobsInput{
						SolutionVersionArn: aws.String(svArn),
					},
				)
				require.NoError(t, err)
				require.Len(t, listed.BatchSegmentJobs, 1)
				assert.Equal(t, "bsj1", aws.ToString(listed.BatchSegmentJobs[0].JobName))
			},
		},
		{
			name: "data_deletion_job",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				dgArn := personalizeCreateDatasetGroup(t, h, "ddj-dg")

				created, err := client.CreateDataDeletionJob(
					t.Context(),
					&personalizesdk.CreateDataDeletionJobInput{
						JobName:         aws.String("ddj1"),
						DatasetGroupArn: aws.String(dgArn),
						RoleArn:         aws.String("arn:aws:iam::000000000000:role/personalize-role"),
						DataSource: &types.DataSource{
							DataLocation: aws.String("s3://bucket/deletions.csv"),
						},
					},
				)
				require.NoError(t, err)
				jobArn := aws.ToString(created.DataDeletionJobArn)
				require.NotEmpty(t, jobArn)

				described, err := client.DescribeDataDeletionJob(
					t.Context(),
					&personalizesdk.DescribeDataDeletionJobInput{DataDeletionJobArn: aws.String(jobArn)},
				)
				require.NoError(t, err)
				assert.Equal(t, "ddj1", aws.ToString(described.DataDeletionJob.JobName))
				require.NotNil(t, described.DataDeletionJob.DataSource)
				assert.Equal(
					t,
					"s3://bucket/deletions.csv",
					aws.ToString(described.DataDeletionJob.DataSource.DataLocation),
				)
				require.NotNil(t, described.DataDeletionJob.NumDeleted)
				assert.Equal(t, int32(0), *described.DataDeletionJob.NumDeleted)

				listed, err := client.ListDataDeletionJobs(
					t.Context(),
					&personalizesdk.ListDataDeletionJobsInput{
						DatasetGroupArn: aws.String(dgArn),
					},
				)
				require.NoError(t, err)
				require.Len(t, listed.DataDeletionJobs, 1)
				assert.Equal(t, "ddj1", aws.ToString(listed.DataDeletionJobs[0].JobName))
			},
		},
		{
			name: "recipe_algorithm_feature_transformation",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newPersonalizeClient(t)

				const recipeArn = "arn:aws:personalize:::recipe/aws-user-personalization"

				describedRecipe, err := client.DescribeRecipe(
					t.Context(),
					&personalizesdk.DescribeRecipeInput{
						RecipeArn: aws.String(recipeArn),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "aws-user-personalization", aws.ToString(describedRecipe.Recipe.Name))
				assert.Equal(t, recipeArn, aws.ToString(describedRecipe.Recipe.RecipeArn))

				listedRecipes, err := client.ListRecipes(t.Context(), &personalizesdk.ListRecipesInput{})
				require.NoError(t, err)
				assert.NotEmpty(t, listedRecipes.Recipes)

				describedAlgo, err := client.DescribeAlgorithm(
					t.Context(),
					&personalizesdk.DescribeAlgorithmInput{
						AlgorithmArn: aws.String(
							"arn:aws:personalize:::algorithm/aws-user-personalization",
						),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "aws-user-personalization", aws.ToString(describedAlgo.Algorithm.Name))

				describedFT, err := client.DescribeFeatureTransformation(
					t.Context(),
					&personalizesdk.DescribeFeatureTransformationInput{
						FeatureTransformationArn: aws.String(
							"arn:aws:personalize:us-east-1:000000000000:feature-transformation/aws-feature-transformation",
						),
					},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					"aws-feature-transformation",
					aws.ToString(describedFT.FeatureTransformation.Name),
				)
			},
		},
		{
			name: "tags",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)

				dgArn := personalizeCreateDatasetGroup(t, h, "tag-dg")

				_, err := client.TagResource(t.Context(), &personalizesdk.TagResourceInput{
					ResourceArn: aws.String(dgArn),
					Tags: []types.Tag{
						{TagKey: aws.String("env"), TagValue: aws.String("prod")},
					},
				})
				require.NoError(t, err)

				listed, err := client.ListTagsForResource(
					t.Context(),
					&personalizesdk.ListTagsForResourceInput{
						ResourceArn: aws.String(dgArn),
					},
				)
				require.NoError(t, err)
				require.Len(t, listed.Tags, 1)
				assert.Equal(t, "env", aws.ToString(listed.Tags[0].TagKey))
				assert.Equal(t, "prod", aws.ToString(listed.Tags[0].TagValue))

				_, err = client.UntagResource(t.Context(), &personalizesdk.UntagResourceInput{
					ResourceArn: aws.String(dgArn),
					TagKeys:     []string{"env"},
				})
				require.NoError(t, err)

				listed, err = client.ListTagsForResource(
					t.Context(),
					&personalizesdk.ListTagsForResourceInput{
						ResourceArn: aws.String(dgArn),
					},
				)
				require.NoError(t, err)
				assert.Empty(t, listed.Tags)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
