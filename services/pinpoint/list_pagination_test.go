package pinpoint_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

type pagedEnv struct {
	client *pinpointsdk.Client
	appID  string
}

func seedPagedPinpoint(t *testing.T) pagedEnv {
	t.Helper()

	c := newTestPinpointClient(t, pinpoint.NewHandler(pinpoint.NewInMemoryBackend("us-east-1", "123456789012")))
	app, err := c.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("paged-app")},
	})
	require.NoError(t, err)

	appID := aws.ToString(app.ApplicationResponse.Id)

	for i := range 3 {
		_, err = c.CreateExportJob(t.Context(), &pinpointsdk.CreateExportJobInput{
			ApplicationId: aws.String(appID),
			ExportJobRequest: &types.ExportJobRequest{
				RoleArn: aws.String("arn:aws:iam::123456789012:role/r"), S3UrlPrefix: aws.String("s3://b/p"),
				SegmentId: aws.String(fmt.Sprintf("seg-%d", i%2)),
			},
		})
		require.NoError(t, err)

		_, err = c.CreateImportJob(t.Context(), &pinpointsdk.CreateImportJobInput{
			ApplicationId: aws.String(appID),
			ImportJobRequest: &types.ImportJobRequest{
				RoleArn: aws.String("arn:aws:iam::123456789012:role/r"), S3Url: aws.String("s3://b/f.csv"),
				Format: types.FormatCsv,
			},
		})
		require.NoError(t, err)

		_, err = c.CreateRecommenderConfiguration(t.Context(), &pinpointsdk.CreateRecommenderConfigurationInput{
			CreateRecommenderConfiguration: &types.CreateRecommenderConfigurationShape{
				RecommendationProviderRoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
				RecommendationProviderUri:     aws.String("arn:aws:personalize:us-east-1:123456789012:campaign/c"),
			},
		})
		require.NoError(t, err)

		_, err = c.CreateEmailTemplate(t.Context(), &pinpointsdk.CreateEmailTemplateInput{
			TemplateName:         aws.String(fmt.Sprintf("tmpl-%d", i)),
			EmailTemplateRequest: &types.EmailTemplateRequest{Subject: aws.String("s")},
		})
		require.NoError(t, err)
	}

	return pagedEnv{client: c, appID: appID}
}

// TestRealClient_ListOpsHonourPageSize pages ops whose page-size/token query
// members were ignored (serializers.go:3485, pinpoint@v1.42.4).
func TestRealClient_ListOpsHonourPageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fetch func(t *testing.T, e pagedEnv, size, token *string) (int, *string)
		name  string
		total int
	}{
		{name: "apps", total: 1, fetch: func(t *testing.T, e pagedEnv, sz, tok *string) (int, *string) {
			t.Helper()

			out, err := e.client.GetApps(t.Context(), &pinpointsdk.GetAppsInput{PageSize: sz, Token: tok})
			require.NoError(t, err)

			return len(out.ApplicationsResponse.Item), out.ApplicationsResponse.NextToken
		}},
		{name: "export_jobs", total: 3, fetch: func(t *testing.T, e pagedEnv, sz, tok *string) (int, *string) {
			t.Helper()

			out, err := e.client.GetExportJobs(t.Context(), &pinpointsdk.GetExportJobsInput{
				ApplicationId: aws.String(e.appID), PageSize: sz, Token: tok,
			})
			require.NoError(t, err)

			return len(out.ExportJobsResponse.Item), out.ExportJobsResponse.NextToken
		}},
		{name: "import_jobs", total: 3, fetch: func(t *testing.T, e pagedEnv, sz, tok *string) (int, *string) {
			t.Helper()

			out, err := e.client.GetImportJobs(t.Context(), &pinpointsdk.GetImportJobsInput{
				ApplicationId: aws.String(e.appID), PageSize: sz, Token: tok,
			})
			require.NoError(t, err)

			return len(out.ImportJobsResponse.Item), out.ImportJobsResponse.NextToken
		}},
		{name: "recommenders", total: 3, fetch: func(t *testing.T, e pagedEnv, sz, tok *string) (int, *string) {
			t.Helper()

			out, err := e.client.GetRecommenderConfigurations(
				t.Context(),
				&pinpointsdk.GetRecommenderConfigurationsInput{
					PageSize: sz, Token: tok,
				},
			)
			require.NoError(t, err)

			return len(
				out.ListRecommenderConfigurationsResponse.Item,
			), out.ListRecommenderConfigurationsResponse.NextToken
		}},
		{name: "templates", total: 3, fetch: func(t *testing.T, e pagedEnv, sz, tok *string) (int, *string) {
			t.Helper()

			out, err := e.client.ListTemplates(
				t.Context(),
				&pinpointsdk.ListTemplatesInput{PageSize: sz, NextToken: tok},
			)
			require.NoError(t, err)

			return len(out.TemplatesResponse.Item), out.TemplatesResponse.NextToken
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := seedPagedPinpoint(t)

			var token *string

			got, pages := 0, 0

			for {
				n, next := tt.fetch(t, e, aws.String("1"), token)
				got += n
				pages++

				if next == nil {
					break
				}

				token = next

				require.LessOrEqual(t, pages, tt.total+1)
			}

			require.Equal(t, tt.total, got)
			require.Equal(t, tt.total, pages)
		})
	}
}

func TestRealClient_SegmentExportJobsFilterBySegment(t *testing.T) {
	t.Parallel()

	e := seedPagedPinpoint(t)

	out, err := e.client.GetSegmentExportJobs(t.Context(), &pinpointsdk.GetSegmentExportJobsInput{
		ApplicationId: aws.String(e.appID), SegmentId: aws.String("seg-0"),
	})
	require.NoError(t, err)
	require.Len(t, out.ExportJobsResponse.Item, 2)
}

func TestRealClient_ListOpsRejectBadPaging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		size  *string
		token *string
		name  string
	}{
		{name: "bad_token", token: aws.String("%%%")},
		{name: "non_numeric_size", size: aws.String("abc")},
		{name: "zero_size", size: aws.String("0")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := seedPagedPinpoint(t)

			_, err := e.client.GetExportJobs(t.Context(), &pinpointsdk.GetExportJobsInput{
				ApplicationId: aws.String(e.appID), PageSize: tt.size, Token: tt.token,
			})

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, "BadRequestException", apiErr.ErrorCode())
		})
	}
}
