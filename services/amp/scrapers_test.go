package amp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/amp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const eksARN = "arn:aws:eks:us-east-1:000000000000:cluster/prod"

func scraperInput(wsARN string) *ampsdk.CreateScraperInput {
	return &ampsdk.CreateScraperInput{
		Alias: aws.String("collector"),
		Destination: &types.DestinationMemberAmpConfiguration{
			Value: types.AmpConfiguration{WorkspaceArn: aws.String(wsARN)},
		},
		Source: &types.SourceMemberEksConfiguration{Value: types.EksConfiguration{
			ClusterArn: aws.String(
				eksARN,
			),
			SubnetIds:        []string{"subnet-1", "subnet-2"},
			SecurityGroupIds: []string{"sg-1"},
		}},
		ScrapeConfiguration: &types.ScrapeConfigurationMemberConfigurationBlob{Value: b64(validScrape)},
		Tags:                map[string]string{"team": "obs"},
	}
}

func workspaceARN(t *testing.T, f *fixture) string {
	t.Helper()

	out, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})
	require.NoError(t, err)
	f.settle()

	return aws.ToString(out.Arn)
}

func TestScrapers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, wsARN string)
		name string
	}{
		{name: "lifecycle", run: func(t *testing.T, f *fixture, wsARN string) {
			t.Helper()

			c, err := f.client.CreateScraper(t.Context(), scraperInput(wsARN))
			require.NoError(t, err)
			assert.Equal(t, types.ScraperStatusCodeCreating, c.Status.StatusCode)
			assert.Regexp(t, `:scraper/s-[0-9a-f-]{36}$`, aws.ToString(c.Arn))

			f.settle()

			d, err := f.client.DescribeScraper(t.Context(), &ampsdk.DescribeScraperInput{ScraperId: c.ScraperId})
			require.NoError(t, err)

			s := d.Scraper
			assert.Equal(t, types.ScraperStatusCodeActive, s.Status.StatusCode)
			assert.Equal(t, "collector", aws.ToString(s.Alias))
			assert.Equal(
				t,
				validScrape,
				string(s.ScrapeConfiguration.(*types.ScrapeConfigurationMemberConfigurationBlob).Value),
			)
			assert.Equal(
				t,
				wsARN,
				aws.ToString(s.Destination.(*types.DestinationMemberAmpConfiguration).Value.WorkspaceArn),
			)
			assert.Equal(t, eksARN, aws.ToString(s.Source.(*types.SourceMemberEksConfiguration).Value.ClusterArn))
			assert.Contains(t, aws.ToString(s.RoleArn), "AWSServiceRoleForAmazonPrometheusScraper_")
			assert.Equal(t, map[string]string{"team": "obs"}, s.Tags)

			up, err := f.client.UpdateScraper(
				t.Context(),
				&ampsdk.UpdateScraperInput{ScraperId: c.ScraperId, Alias: aws.String("renamed")},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ScraperStatusCodeUpdating, up.Status.StatusCode)
			f.settle()

			d, err = f.client.DescribeScraper(t.Context(), &ampsdk.DescribeScraperInput{ScraperId: c.ScraperId})
			require.NoError(t, err)
			assert.Equal(t, "renamed", aws.ToString(d.Scraper.Alias))
			assert.Equal(
				t,
				validScrape,
				string(d.Scraper.ScrapeConfiguration.(*types.ScrapeConfigurationMemberConfigurationBlob).Value),
			)

			del, err := f.client.DeleteScraper(t.Context(), &ampsdk.DeleteScraperInput{ScraperId: c.ScraperId})
			require.NoError(t, err)
			assert.Equal(t, types.ScraperStatusCodeDeleting, del.Status.StatusCode)
			f.settle()

			_, err = f.client.DescribeScraper(t.Context(), &ampsdk.DescribeScraperInput{ScraperId: c.ScraperId})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "list filters", run: func(t *testing.T, f *fixture, wsARN string) {
			t.Helper()

			a, err := f.client.CreateScraper(t.Context(), scraperInput(wsARN))
			require.NoError(t, err)

			in := scraperInput(wsARN)
			in.Alias = aws.String("other")
			_, err = f.client.CreateScraper(t.Context(), in)
			require.NoError(t, err)

			tests := []struct {
				filters map[string][]string
				want    int
			}{
				{filters: nil, want: 2},
				{filters: map[string][]string{"alias": {"collector"}}, want: 1},
				{filters: map[string][]string{"alias": {"collector", "other"}}, want: 2},
				{filters: map[string][]string{"status": {"ACTIVE"}}, want: 0},
				{filters: map[string][]string{"status": {"CREATING"}, "alias": {"other"}}, want: 1},
				{filters: map[string][]string{"destinationArn": {wsARN}}, want: 2},
				{filters: map[string][]string{"sourceArn": {eksARN}}, want: 2},
				{
					filters: map[string][]string{"sourceArn": {"arn:aws:eks:us-east-1:000000000000:cluster/none"}},
					want:    0,
				},
			}
			for _, tt := range tests {
				page, lerr := f.client.ListScrapers(t.Context(), &ampsdk.ListScrapersInput{Filters: tt.filters})
				require.NoError(t, lerr)
				assert.Len(t, page.Scrapers, tt.want)
			}

			out, err := f.client.ListScrapers(t.Context(), &ampsdk.ListScrapersInput{MaxResults: aws.Int32(1)})
			require.NoError(t, err)
			require.Len(t, out.Scrapers, 1)
			assert.NotNil(t, out.NextToken)

			_ = a
		}},
		{name: "validation", run: func(t *testing.T, f *fixture, wsARN string) {
			t.Helper()

			bad := scraperInput(wsARN)
			bad.ScrapeConfiguration = &types.ScrapeConfigurationMemberConfigurationBlob{Value: b64("global: {}\n")}
			_, err := f.client.CreateScraper(t.Context(), bad)
			assert.Equal(t, "ValidationException", apiCode(err))

			bad = scraperInput(wsARN)
			bad.Source = &types.SourceMemberEksConfiguration{Value: types.EksConfiguration{
				ClusterArn: aws.String("not-an-arn"), SubnetIds: []string{"subnet-1"},
			}}
			_, err = f.client.CreateScraper(t.Context(), bad)
			assert.Equal(t, "ValidationException", apiCode(err))

			bad = scraperInput(wsARN)
			bad.Exporters = []types.ExporterConfiguration{
				&types.ExporterConfigurationMemberOpenSearchConfiguration{
					Value: types.OpenSearchExporterConfiguration{DomainArn: aws.String("bad")},
				},
			}
			_, err = f.client.CreateScraper(t.Context(), bad)
			assert.Equal(t, "ValidationException", apiCode(err))

			bad = scraperInput(wsARN)
			bad.RoleConfiguration = &types.RoleConfiguration{SourceRoleArn: aws.String("bad")}
			_, err = f.client.CreateScraper(t.Context(), bad)
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
		{name: "unknown destination workspace", run: func(t *testing.T, f *fixture, _ string) {
			t.Helper()

			_, err := f.client.CreateScraper(
				t.Context(),
				scraperInput("arn:aws:aps:us-east-1:000000000000:workspace/ws-0000"),
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "vpc source and cloudwatch destination", run: func(t *testing.T, f *fixture, _ string) {
			t.Helper()

			in := &ampsdk.CreateScraperInput{
				Destination: &types.DestinationMemberCloudWatchConfiguration{Value: types.CloudWatchConfiguration{
					DatasetArn: aws.String("arn:aws:cloudwatch:us-east-1:000000000000:dataset/default"),
				}},
				Source: &types.SourceMemberVpcConfiguration{Value: types.VpcConfiguration{
					SubnetIds: []string{"subnet-1"}, SecurityGroupIds: []string{"sg-1"},
				}},
				ScrapeConfiguration: &types.ScrapeConfigurationMemberConfigurationBlob{Value: b64(validScrape)},
			}
			c, err := f.client.CreateScraper(t.Context(), in)
			require.NoError(t, err)

			d, err := f.client.DescribeScraper(t.Context(), &ampsdk.DescribeScraperInput{ScraperId: c.ScraperId})
			require.NoError(t, err)
			assert.IsType(t, &types.SourceMemberVpcConfiguration{}, d.Scraper.Source)
			assert.IsType(t, &types.DestinationMemberCloudWatchConfiguration{}, d.Scraper.Destination)
		}},
		{name: "mutation while not active conflicts", run: func(t *testing.T, f *fixture, wsARN string) {
			t.Helper()

			c, err := f.client.CreateScraper(t.Context(), scraperInput(wsARN))
			require.NoError(t, err)

			_, err = f.client.DeleteScraper(t.Context(), &ampsdk.DeleteScraperInput{ScraperId: c.ScraperId})
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "logging configuration", run: func(t *testing.T, f *fixture, wsARN string) {
			t.Helper()

			c, err := f.client.CreateScraper(t.Context(), scraperInput(wsARN))
			require.NoError(t, err)
			f.settle()

			_, err = f.client.DescribeScraperLoggingConfiguration(
				t.Context(),
				&ampsdk.DescribeScraperLoggingConfigurationInput{ScraperId: c.ScraperId},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			up, err := f.client.UpdateScraperLoggingConfiguration(
				t.Context(),
				&ampsdk.UpdateScraperLoggingConfigurationInput{
					ScraperId: c.ScraperId,
					LoggingDestination: &types.ScraperLoggingDestinationMemberCloudWatchLogs{
						Value: types.CloudWatchLogDestination{LogGroupArn: aws.String(logGroup)},
					},
					ScraperComponents: []types.ScraperComponent{
						{Type: types.ScraperComponentTypeCollector},
						{
							Type:   types.ScraperComponentTypeServiceDiscovery,
							Config: &types.ComponentConfig{Options: map[string]string{"k": "v"}},
						},
					},
				},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ScraperLoggingConfigurationStatusCodeCreating, up.Status.StatusCode)
			f.settle()

			d, err := f.client.DescribeScraperLoggingConfiguration(
				t.Context(),
				&ampsdk.DescribeScraperLoggingConfigurationInput{ScraperId: c.ScraperId},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ScraperLoggingConfigurationStatusCodeActive, d.Status.StatusCode)
			require.Len(t, d.ScraperComponents, 2)
			assert.Equal(t, map[string]string{"k": "v"}, d.ScraperComponents[1].Config.Options)
			assert.Equal(
				t,
				logGroup,
				aws.ToString(
					d.LoggingDestination.(*types.ScraperLoggingDestinationMemberCloudWatchLogs).Value.LogGroupArn,
				),
			)

			_, err = f.client.DeleteScraperLoggingConfiguration(
				t.Context(),
				&ampsdk.DeleteScraperLoggingConfigurationInput{ScraperId: c.ScraperId},
			)
			require.NoError(t, err)
			f.settle()

			_, err = f.client.DescribeScraperLoggingConfiguration(
				t.Context(),
				&ampsdk.DescribeScraperLoggingConfigurationInput{ScraperId: c.ScraperId},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "tags", run: func(t *testing.T, f *fixture, wsARN string) {
			t.Helper()

			c, err := f.client.CreateScraper(t.Context(), scraperInput(wsARN))
			require.NoError(t, err)

			_, err = f.client.TagResource(
				t.Context(),
				&ampsdk.TagResourceInput{ResourceArn: c.Arn, Tags: map[string]string{"x": "y"}},
			)
			require.NoError(t, err)

			out, err := f.client.ListTagsForResource(t.Context(), &ampsdk.ListTagsForResourceInput{ResourceArn: c.Arn})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"team": "obs", "x": "y"}, out.Tags)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			tt.run(t, f, workspaceARN(t, f))
		})
	}
}

func TestScraperEKSSourceIsChecked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		clusters fakeClusters
		code     string
		name     string
	}{
		{name: "cluster exists", clusters: fakeClusters{"us-east-1/prod": true}},
		{name: "cluster missing", clusters: fakeClusters{}, code: "ResourceNotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetClusterResolver(tt.clusters)

			_, err := f.client.CreateScraper(t.Context(), scraperInput(workspaceARN(t, f)))
			if tt.code == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestDefaultScraperConfiguration(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	out, err := f.client.GetDefaultScraperConfiguration(t.Context(), &ampsdk.GetDefaultScraperConfigurationInput{})
	require.NoError(t, err)
	assert.Contains(t, string(out.Configuration), "scrape_configs:")
}

func detectorInput(ws *string) *ampsdk.CreateAnomalyDetectorInput {
	return &ampsdk.CreateAnomalyDetectorInput{
		WorkspaceId: ws,
		Alias:       aws.String("cpu"),
		Configuration: &types.AnomalyDetectorConfigurationMemberRandomCutForest{
			Value: types.RandomCutForestConfiguration{
				Query: aws.String(`avg by (instance) (rate(node_cpu_seconds_total[5m]))`),
			},
		},
		MissingDataAction: &types.AnomalyDetectorMissingDataActionMemberSkip{Value: true},
		Labels:            map[string]string{"team": "a"},
		Tags:              map[string]string{"env": "x"},
	}
}

func TestAnomalyDetectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, ws *string)
		name string
	}{
		{name: "lifecycle", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			c, err := f.client.CreateAnomalyDetector(t.Context(), detectorInput(ws))
			require.NoError(t, err)
			assert.Equal(t, types.AnomalyDetectorStatusCodeCreating, c.Status.StatusCode)
			f.settle()

			d, err := f.client.DescribeAnomalyDetector(
				t.Context(),
				&ampsdk.DescribeAnomalyDetectorInput{WorkspaceId: ws, AnomalyDetectorId: c.AnomalyDetectorId},
			)
			require.NoError(t, err)

			ad := d.AnomalyDetector
			assert.Equal(t, int32(60), aws.ToInt32(ad.EvaluationIntervalInSeconds))
			assert.Equal(t, map[string]string{"team": "a"}, ad.Labels)
			assert.IsType(t, &types.AnomalyDetectorMissingDataActionMemberSkip{}, ad.MissingDataAction)

			rcf := ad.Configuration.(*types.AnomalyDetectorConfigurationMemberRandomCutForest).Value
			assert.Equal(t, int32(8), aws.ToInt32(rcf.ShingleSize))
			assert.Equal(t, int32(256), aws.ToInt32(rcf.SampleSize))

			next := detectorInput(ws)
			p, err := f.client.PutAnomalyDetector(t.Context(), &ampsdk.PutAnomalyDetectorInput{
				WorkspaceId: ws, AnomalyDetectorId: c.AnomalyDetectorId, Configuration: next.Configuration,
				EvaluationIntervalInSeconds: aws.Int32(120),
			})
			require.NoError(t, err)
			assert.Equal(t, types.AnomalyDetectorStatusCodeUpdating, p.Status.StatusCode)
			f.settle()

			d, err = f.client.DescribeAnomalyDetector(
				t.Context(),
				&ampsdk.DescribeAnomalyDetectorInput{WorkspaceId: ws, AnomalyDetectorId: c.AnomalyDetectorId},
			)
			require.NoError(t, err)
			assert.Equal(t, int32(120), aws.ToInt32(d.AnomalyDetector.EvaluationIntervalInSeconds))
			assert.Equal(t, "cpu", aws.ToString(d.AnomalyDetector.Alias))

			l, err := f.client.ListAnomalyDetectors(
				t.Context(),
				&ampsdk.ListAnomalyDetectorsInput{WorkspaceId: ws, Alias: aws.String("cpu")},
			)
			require.NoError(t, err)
			assert.Len(t, l.AnomalyDetectors, 1)

			l, err = f.client.ListAnomalyDetectors(
				t.Context(),
				&ampsdk.ListAnomalyDetectorsInput{WorkspaceId: ws, Alias: aws.String("zzz")},
			)
			require.NoError(t, err)
			assert.Empty(t, l.AnomalyDetectors)

			_, err = f.client.DeleteAnomalyDetector(
				t.Context(),
				&ampsdk.DeleteAnomalyDetectorInput{WorkspaceId: ws, AnomalyDetectorId: c.AnomalyDetectorId},
			)
			require.NoError(t, err)
			f.settle()

			_, err = f.client.DescribeAnomalyDetector(
				t.Context(),
				&ampsdk.DescribeAnomalyDetectorInput{WorkspaceId: ws, AnomalyDetectorId: c.AnomalyDetectorId},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "query must be aggregated", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			for _, q := range []string{"up", "rate(up[5m])", "sum(up", "foo bar"} {
				in := detectorInput(ws)
				in.Configuration = &types.AnomalyDetectorConfigurationMemberRandomCutForest{
					Value: types.RandomCutForestConfiguration{Query: aws.String(q)},
				}
				_, err := f.client.CreateAnomalyDetector(t.Context(), in)
				assert.Equal(t, "ValidationException", apiCode(err), q)
			}
		}},
		{name: "bounds", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			in := detectorInput(ws)
			in.EvaluationIntervalInSeconds = aws.Int32(0)
			_, err := f.client.CreateAnomalyDetector(t.Context(), in)
			assert.Equal(t, "ValidationException", apiCode(err))

			in = detectorInput(ws)
			in.Configuration = &types.AnomalyDetectorConfigurationMemberRandomCutForest{
				Value: types.RandomCutForestConfiguration{
					Query:                       aws.String("sum(up)"),
					IgnoreNearExpectedFromAbove: &types.IgnoreNearExpectedMemberAmount{Value: 1},
				},
			}
			_, err = f.client.CreateAnomalyDetector(t.Context(), in)
			require.NoError(t, err)
		}},
		{name: "unknown", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			_, err := f.client.DescribeAnomalyDetector(
				t.Context(),
				&ampsdk.DescribeAnomalyDetectorInput{WorkspaceId: ws, AnomalyDetectorId: aws.String("ad-x")},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			tt.run(t, f, aws.String(f.workspace(t)))
		})
	}
}
