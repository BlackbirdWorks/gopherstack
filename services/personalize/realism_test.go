package personalize_test

import (
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"
	"github.com/aws/aws-sdk-go-v2/service/personalize/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

func TestCreateNames_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "simple", input: "my-group_1"},
		{name: "space", input: "my group", wantErr: true},
		{name: "leading hyphen", input: "-group", wantErr: true},
		{name: "dot", input: "a.b", wantErr: true},
		{name: "too long", input: strings.Repeat("a", 64), wantErr: true},
		{name: "max length", input: strings.Repeat("a", 63)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newPersonalizeClient(t)
			_, err := client.CreateDatasetGroup(t.Context(), &personalizesdk.CreateDatasetGroupInput{
				Name: aws.String(tt.input),
			})

			if tt.wantErr {
				var apiErr *types.InvalidInputException
				require.ErrorAs(t, err, &apiErr)
				assert.NotContains(t, apiErr.ErrorMessage(), "InvalidInputException:")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCreateSchema_Document(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{
			name:   "record",
			schema: `{"type":"record","name":"Interactions","namespace":"com.amazonaws.personalize.schema","fields":[]}`,
		},
		{name: "not json", schema: `not json`, wantErr: true},
		{name: "wrong type", schema: `{"type":"array"}`, wantErr: true},
		{name: "empty", schema: ``, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newPersonalizeClient(t)
			_, err := client.CreateSchema(t.Context(), &personalizesdk.CreateSchemaInput{
				Name: aws.String("s"), Schema: aws.String(tt.schema),
			})

			if tt.wantErr {
				var apiErr *types.InvalidInputException
				require.ErrorAs(t, err, &apiErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCreateFilter_Expression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{name: "include", expr: "INCLUDE ItemID WHERE Items.GENRE IN ('x')"},
		{name: "exclude", expr: "EXCLUDE ItemID WHERE Items.GENRE IN ('x')"},
		{name: "garbage", expr: "select * from items", wantErr: true},
		{name: "empty", expr: "  ", wantErr: true},
		{name: "too long", expr: "INCLUDE " + strings.Repeat("x", 2500), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, client := newPersonalizeClient(t)
			dg := personalizeCreateDatasetGroup(t, h, "fdg")

			_, err := client.CreateFilter(t.Context(), &personalizesdk.CreateFilterInput{
				Name: aws.String("f"), DatasetGroupArn: aws.String(dg), FilterExpression: aws.String(tt.expr),
			})

			if tt.wantErr {
				var apiErr *types.InvalidInputException
				require.ErrorAs(t, err, &apiErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCreateSolution_RecipeRequired(t *testing.T) {
	t.Parallel()

	h, client := newPersonalizeClient(t)
	dg := personalizeCreateDatasetGroup(t, h, "sdg")

	_, err := client.CreateSolution(t.Context(), &personalizesdk.CreateSolutionInput{
		Name: aws.String("no-recipe"), DatasetGroupArn: aws.String(dg),
	})

	var apiErr *types.InvalidInputException
	require.ErrorAs(t, err, &apiErr)

	_, err = client.CreateSolution(t.Context(), &personalizesdk.CreateSolutionInput{
		Name: aws.String("auto-ml"), DatasetGroupArn: aws.String(dg), PerformAutoML: true,
	})
	require.NoError(t, err)
}

func TestListPagination_NoItemLostAndTokensOpaque(t *testing.T) {
	t.Parallel()

	_, client := newPersonalizeClient(t)
	for _, n := range []string{"g1", "g2", "g3", "g4", "g5"} {
		_, err := client.CreateDatasetGroup(t.Context(), &personalizesdk.CreateDatasetGroupInput{Name: aws.String(n)})
		require.NoError(t, err)
	}

	var seen []string

	pager := personalizesdk.NewListDatasetGroupsPaginator(client, &personalizesdk.ListDatasetGroupsInput{
		MaxResults: aws.Int32(2),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, g := range page.DatasetGroups {
			seen = append(seen, aws.ToString(g.Name))
		}
	}

	assert.Equal(t, []string{"g1", "g2", "g3", "g4", "g5"}, seen)
}

func TestListPagination_BadInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input *personalizesdk.ListDatasetGroupsInput
		want  string
	}{
		{
			name:  "garbage token",
			input: &personalizesdk.ListDatasetGroupsInput{NextToken: aws.String("garbage")},
			want:  "token",
		},
		{
			name:  "max too big",
			input: &personalizesdk.ListDatasetGroupsInput{MaxResults: aws.Int32(101)},
			want:  "input",
		},
		{
			name:  "max zero",
			input: &personalizesdk.ListDatasetGroupsInput{MaxResults: aws.Int32(0)},
			want:  "input",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newPersonalizeClient(t)
			_, err := client.ListDatasetGroups(t.Context(), tt.input)

			if tt.want == "token" {
				var apiErr *types.InvalidNextTokenException
				require.ErrorAs(t, err, &apiErr)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())
		})
	}
}

func TestNotFound_MessageHasNoCodePrefix(t *testing.T) {
	t.Parallel()

	_, client := newPersonalizeClient(t)
	_, err := client.DescribeCampaign(t.Context(), &personalizesdk.DescribeCampaignInput{
		CampaignArn: aws.String("arn:aws:personalize:us-east-1:000000000000:campaign/nope"),
	})

	var apiErr *types.ResourceNotFoundException
	require.ErrorAs(t, err, &apiErr)
	assert.NotContains(t, apiErr.ErrorMessage(), "ResourceNotFoundException")
	assert.Contains(t, apiErr.ErrorMessage(), "not found")
}

func TestCreateDatasetImportJob_RoleArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		role    string
		wantErr bool
	}{
		{name: "valid", role: "arn:aws:iam::000000000000:role/svc/PersonalizeRole"},
		{name: "empty", role: "", wantErr: true},
		{name: "not iam", role: "arn:aws:s3:::bucket", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, client := newPersonalizeClient(t)
			dsArn := personalizeCreateDataset(t, h, "jobds")

			_, err := client.CreateDatasetImportJob(t.Context(), &personalizesdk.CreateDatasetImportJobInput{
				JobName: aws.String("job"), DatasetArn: aws.String(dsArn), RoleArn: aws.String(tt.role),
				DataSource: &types.DataSource{DataLocation: aws.String("s3://bucket/key")},
			})

			if tt.wantErr {
				var apiErr *types.InvalidInputException
				require.ErrorAs(t, err, &apiErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDataDeletionJob_CompletesNotActive(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dg := personalizeCreateDatasetGroup(t, h, "ddg")

	rec := personalizeDo(t, h, "CreateDataDeletionJob", map[string]any{
		"jobName": "del", "datasetGroupArn": dg,
		"roleArn": "arn:aws:iam::000000000000:role/r", "dataSource": map[string]any{"dataLocation": "s3://b/k"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	arn := personalizeUnmarshal(t, rec)["dataDeletionJobArn"]

	rec = personalizeDo(t, h, "DescribeDataDeletionJob", map[string]any{"dataDeletionJobArn": arn})
	job := personalizeUnmarshal(t, rec)["dataDeletionJob"].(map[string]any)
	assert.Equal(t, "COMPLETED", job["status"])
}

func TestResourceLifecycle_Delay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		delay time.Duration
		wait  time.Duration
	}{
		{name: "no delay", delay: 0, wait: 0, want: "ACTIVE"},
		{name: "pending", delay: 9 * time.Second, wait: time.Second, want: "CREATE PENDING"},
		{name: "in progress", delay: 9 * time.Second, wait: 4 * time.Second, want: "CREATE IN_PROGRESS"},
		{name: "active", delay: 9 * time.Second, wait: 9 * time.Second, want: "ACTIVE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
				b.SetLifecycleDelay(tt.delay)
				h := personalize.NewHandler(b)

				dg := personalizeCreateDatasetGroup(t, h, "lc")
				time.Sleep(tt.wait)

				rec := personalizeDo(t, h, "DescribeDatasetGroup", map[string]any{"datasetGroupArn": dg})
				got := personalizeUnmarshal(t, rec)["datasetGroup"].(map[string]any)
				assert.Equal(t, tt.want, got["status"])

				rec = personalizeDo(t, h, "ListDatasetGroups", map[string]any{})
				list := personalizeUnmarshal(t, rec)["datasetGroups"].([]any)
				assert.Equal(t, tt.want, list[0].(map[string]any)["status"])
			})
		})
	}
}

func TestJobLifecycle_Delay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		wait time.Duration
	}{
		{name: "pending", wait: time.Second, want: "PENDING"},
		{name: "in progress", wait: 4 * time.Second, want: "IN PROGRESS"},
		{name: "active", wait: 9 * time.Second, want: "ACTIVE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := personalize.NewInMemoryBackend("000000000000", "us-east-1")
				h := personalize.NewHandler(b)
				svArn := personalizeCreateSolutionVersion(t, h, "jsol")

				b.SetLifecycleDelay(9 * time.Second)

				rec := personalizeDo(t, h, "CreateBatchInferenceJob", map[string]any{
					"jobName": "bij", "solutionVersionArn": svArn,
					"roleArn":   "arn:aws:iam::000000000000:role/r",
					"jobInput":  map[string]any{"s3DataSource": map[string]any{"path": "s3://b/in"}},
					"jobOutput": map[string]any{"s3DataDestination": map[string]any{"path": "s3://b/out"}},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				arn := personalizeUnmarshal(t, rec)["batchInferenceJobArn"]

				time.Sleep(tt.wait)

				rec = personalizeDo(t, h, "DescribeBatchInferenceJob", map[string]any{"batchInferenceJobArn": arn})
				job := personalizeUnmarshal(t, rec)["batchInferenceJob"].(map[string]any)
				assert.Equal(t, tt.want, job["status"])
			})
		})
	}
}
