package bedrock_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// newCompletedCustomizationJob drives a real CreateModelCustomizationJob call
// through client, then advances the janitor-simulated delay so the job's
// output model materializes as a CustomModel (see
// InMemoryBackend.AdvanceCustomizationJobStatuses). Returns the job ARN and
// the resolved output model ARN.
func newCompletedCustomizationJob(
	t *testing.T, b *bedrock.InMemoryBackend, client *bedrocksdk.Client, jobName, customModelName string,
) (string, string) {
	t.Helper()

	createOut, err := client.CreateModelCustomizationJob(t.Context(), &bedrocksdk.CreateModelCustomizationJobInput{
		JobName:             aws.String(jobName),
		CustomModelName:     aws.String(customModelName),
		BaseModelIdentifier: aws.String("amazon.titan-text-express-v1"),
		RoleArn:             aws.String("arn:aws:iam::000000000000:role/customize"),
		CustomizationType:   types.CustomizationTypeFineTuning,
		TrainingDataConfig:  &types.TrainingDataConfig{S3Uri: aws.String("s3://bucket/training")},
		OutputDataConfig:    &types.OutputDataConfig{S3Uri: aws.String("s3://bucket/output")},
	})
	require.NoError(t, err)

	require.Equal(t, 1, b.AdvanceCustomizationJobStatuses(0))

	getOut, err := client.GetModelCustomizationJob(t.Context(), &bedrocksdk.GetModelCustomizationJobInput{
		JobIdentifier: createOut.JobArn,
	})
	require.NoError(t, err)

	return aws.ToString(createOut.JobArn), aws.ToString(getOut.OutputModelArn)
}

// TestListCustomModels_BaseModelFilters proves baseModelArnEquals/
// foundationModelArnEquals match a completed CreateModelCustomizationJob's
// output model (real base model from baseModelIdentifier) and never match a
// CreateCustomModel import (no base model in the wire input --
// bedrock@v1.66.4 CreateCustomModelRequest carries only a data source, never
// a base model reference).
func TestListCustomModels_BaseModelFilters(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	client := newTestBedrockClient(t, h)

	_, _ = newCompletedCustomizationJob(t, b, client, "filter-job", "filter-job-model")

	_, setupErr := client.CreateCustomModel(t.Context(), &bedrocksdk.CreateCustomModelInput{
		ModelName: aws.String("filter-imported-model"),
	})
	require.NoError(t, setupErr)

	const (
		baseArn  = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1"
		otherArn = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
	)

	tests := []struct {
		in        *bedrocksdk.ListCustomModelsInput
		name      string
		wantNames []string
	}{
		{
			name:      "basemodelarnequals matches job output only",
			in:        &bedrocksdk.ListCustomModelsInput{BaseModelArnEquals: aws.String(baseArn)},
			wantNames: []string{"filter-job-model"},
		},
		{
			name:      "basemodelarnequals no match for unrelated foundation model",
			in:        &bedrocksdk.ListCustomModelsInput{BaseModelArnEquals: aws.String(otherArn)},
			wantNames: nil,
		},
		{
			name:      "foundationmodelarnequals matches job output only",
			in:        &bedrocksdk.ListCustomModelsInput{FoundationModelArnEquals: aws.String(baseArn)},
			wantNames: []string{"filter-job-model"},
		},
		{
			name:      "no filter returns both origins",
			in:        &bedrocksdk.ListCustomModelsInput{},
			wantNames: []string{"filter-job-model", "filter-imported-model"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.ListCustomModels(t.Context(), tt.in)
			require.NoError(t, err)

			gotNames := make([]string, 0, len(out.ModelSummaries))
			for _, m := range out.ModelSummaries {
				gotNames = append(gotNames, aws.ToString(m.ModelName))
			}

			assert.ElementsMatch(t, tt.wantNames, gotNames)
		})
	}
}

// TestListCustomModels_JobOutputSummaryFields proves the completed job's
// CustomModelSummary carries a real baseModelArn/baseModelName (required
// fields in bedrock@v1.66.4 CustomModelSummary) and a customizationType of
// FINE_TUNING, not fabricated placeholder values.
func TestListCustomModels_JobOutputSummaryFields(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	client := newTestBedrockClient(t, h)

	_, outputModelARN := newCompletedCustomizationJob(t, b, client, "summary-job", "summary-job-model")

	out, err := client.ListCustomModels(t.Context(), &bedrocksdk.ListCustomModelsInput{})
	require.NoError(t, err)
	require.Len(t, out.ModelSummaries, 1)

	summary := out.ModelSummaries[0]
	assert.Equal(t, outputModelARN, aws.ToString(summary.ModelArn))
	assert.Equal(
		t,
		"arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1",
		aws.ToString(summary.BaseModelArn),
	)
	assert.Equal(t, "Titan Text G1 - Express", aws.ToString(summary.BaseModelName))
	assert.Equal(t, types.CustomizationTypeFineTuning, summary.CustomizationType)
}

// TestGetCustomModel_JobArnDistinguishesOrigin proves GetCustomModel's jobArn
// is the field that distinguishes a customization job's output model from a
// CreateCustomModel import (bedrock@v1.66.4 GetCustomModelResponse: "For
// models that you create with the CreateCustomModel API operation, this is
// NULL").
func TestGetCustomModel_JobArnDistinguishesOrigin(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	client := newTestBedrockClient(t, h)

	jobARN, outputModelARN := newCompletedCustomizationJob(t, b, client, "origin-job", "origin-job-model")

	importedOut, setupErr := client.CreateCustomModel(t.Context(), &bedrocksdk.CreateCustomModelInput{
		ModelName: aws.String("origin-imported-model"),
	})
	require.NoError(t, setupErr)

	tests := []struct {
		name        string
		modelARN    string
		wantJobARN  string
		wantBaseArn bool
	}{
		{
			name:        "job output has jobarn and base model",
			modelARN:    outputModelARN,
			wantJobARN:  jobARN,
			wantBaseArn: true,
		},
		{
			name:        "imported model has no jobarn or base model",
			modelARN:    aws.ToString(importedOut.ModelArn),
			wantJobARN:  "",
			wantBaseArn: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.GetCustomModel(t.Context(), &bedrocksdk.GetCustomModelInput{
				ModelIdentifier: aws.String(tt.modelARN),
			})
			require.NoError(t, err)

			assert.Equal(t, tt.wantJobARN, aws.ToString(out.JobArn))
			if tt.wantBaseArn {
				assert.NotEmpty(t, aws.ToString(out.BaseModelArn))
			} else {
				assert.Empty(t, aws.ToString(out.BaseModelArn))
			}
		})
	}
}

// TestListModelCustomizationJobs_SummaryUsesCustomModelArnName proves
// ListModelCustomizationJobs' per-item shape uses customModelArn/
// customModelName (bedrock@v1.66.4 ModelCustomizationJobSummary), the wire
// key GetModelCustomizationJob uses outputModelArn/outputModelName for
// instead (bedrock@v1.66.4 GetModelCustomizationJobResponse) -- reusing one
// key set for both would silently drop the field for whichever operation it
// doesn't match.
func TestListModelCustomizationJobs_SummaryUsesCustomModelArnName(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	client := newTestBedrockClient(t, h)

	jobARN, outputModelARN := newCompletedCustomizationJob(t, b, client, "summary-key-job", "summary-key-model")

	out, err := client.ListModelCustomizationJobs(t.Context(), &bedrocksdk.ListModelCustomizationJobsInput{})
	require.NoError(t, err)
	require.Len(t, out.ModelCustomizationJobSummaries, 1)

	summary := out.ModelCustomizationJobSummaries[0]
	assert.Equal(t, jobARN, aws.ToString(summary.JobArn))
	assert.Equal(t, outputModelARN, aws.ToString(summary.CustomModelArn))
	assert.Equal(t, "summary-key-model", aws.ToString(summary.CustomModelName))
}

// TestCreateModelCustomizationJob_MissingCustomModelName proves customModelName
// is a required field like jobName -- the real aws-sdk-go-v2 client refuses
// to even send a request missing it (validators.go
// validateOpCreateModelCustomizationJobInput), so this drives the raw HTTP
// path the same way the pre-existing MissingJobName test does.
func TestCreateModelCustomizationJob_MissingCustomModelName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs", map[string]any{
		"jobName":             "no-model-name-job",
		"baseModelIdentifier": "amazon.titan-text-express-v1",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
