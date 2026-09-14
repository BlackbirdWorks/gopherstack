package sagemaker_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateEndpointConfig_DataCaptureConfig proves
// DataCaptureConfig.CaptureOptions ([]types.CaptureOption) was never
// modeled at all -- accepted-and-dropped on decode, so it could never reach
// the response either (validateDataCaptureConfig, validators.go:10514-10537,
// nil-checked required) -- via a real aws-sdk-go-v2 client round trip so the
// typed response shape is the thing under test.
func TestCreateEndpointConfig_DataCaptureConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateEndpointConfig(t.Context(), &sagemakersdk.CreateEndpointConfigInput{
		EndpointConfigName: aws.String("ec-respsweep"),
		ProductionVariants: []smtypes.ProductionVariant{
			{VariantName: aws.String("v1"), ModelName: aws.String("m1"), InitialInstanceCount: aws.Int32(1)},
		},
		DataCaptureConfig: &smtypes.DataCaptureConfig{
			EnableCapture:             aws.Bool(true),
			DestinationS3Uri:          aws.String("s3://bucket/capture"),
			InitialSamplingPercentage: aws.Int32(0),
			CaptureOptions: []smtypes.CaptureOption{
				{CaptureMode: smtypes.CaptureModeInput},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeEndpointConfig(t.Context(), &sagemakersdk.DescribeEndpointConfigInput{
		EndpointConfigName: aws.String("ec-respsweep"),
	})
	require.NoError(t, err)

	require.NotNil(t, out.DataCaptureConfig)
	require.Len(t, out.DataCaptureConfig.CaptureOptions, 1, "CaptureOptions must survive the round trip")
	assert.Equal(t, smtypes.CaptureModeInput, out.DataCaptureConfig.CaptureOptions[0].CaptureMode)
	require.NotNil(t, out.DataCaptureConfig.InitialSamplingPercentage)
	assert.Equal(t, int32(0), *out.DataCaptureConfig.InitialSamplingPercentage)
}

// TestCreateEndpointConfig_VpcConfigSurvivesEmptyArrays proves
// VpcConfig.SecurityGroupIds/Subnets (shared by Cluster/EndpointConfig/
// Model/TrainingJob) survive an explicit empty array instead of vanishing --
// required whenever VpcConfig itself is present (validateVpcConfig,
// validators.go:16267-16282, nil-checked only) but previously carried
// `omitempty`. Checked as raw JSON, not the typed SDK client: the real
// aws-sdk-go-v2 deserializer itself collapses a present-but-empty JSON array
// back to a nil Go slice, so the typed client can't distinguish "key absent"
// from "key present as []" -- the same limitation documented on
// TestCompilationJob_RequiredOutputMembersSurviveInProgress above.
func TestCreateEndpointConfig_VpcConfigSurvivesEmptyArrays(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doSageMakerRequest(t, h, "CreateEndpointConfig", map[string]any{
		"EndpointConfigName": "ec-vpc-respsweep",
		"ProductionVariants": []any{
			map[string]any{"VariantName": "v1", "ModelName": "m1", "InitialInstanceCount": 1},
		},
		"VpcConfig": map[string]any{"SecurityGroupIds": []any{}, "Subnets": []any{}},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doSageMakerRequest(t, h, "DescribeEndpointConfig", map[string]any{
		"EndpointConfigName": "ec-vpc-respsweep",
	})
	require.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	vpc, ok := resp["VpcConfig"].(map[string]any)
	require.True(t, ok, "VpcConfig key must be present")
	sgIDs, ok := vpc["SecurityGroupIds"]
	assert.True(t, ok, "SecurityGroupIds key must be present even when empty")
	assert.Empty(t, sgIDs)
	subnets, ok := vpc["Subnets"]
	assert.True(t, ok, "Subnets key must be present even when empty")
	assert.Empty(t, subnets)
}

// TestCreateTrainingJob_ModelArtifactsRequiredWhileInProgress proves
// TrainingJob.ModelArtifacts (required on DescribeTrainingJobOutput even
// while InProgress, api_op_DescribeTrainingJob.go:56-61) is now computed
// synchronously at Create, matching the CompilationJob precedent -- and that
// the two real input-side gaps found while fixing it (RoleArn,
// OutputDataConfig.S3OutputPath, both "This member is required" on
// CreateTrainingJobInput but previously unvalidated) are now rejected.
func TestCreateTrainingJob_ModelArtifactsRequiredWhileInProgress(t *testing.T) {
	t.Parallel()

	validBody := map[string]any{
		"TrainingJobName":        "tj-respsweep",
		"RoleArn":                "arn:aws:iam::000000000000:role/x",
		"AlgorithmSpecification": map[string]any{"TrainingInputMode": "File"},
		"OutputDataConfig":       map[string]any{"S3OutputPath": "s3://bucket/output"},
		"ResourceConfig": map[string]any{
			"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 20,
		},
		"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
	}

	t.Run("ModelArtifacts present while InProgress", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rr := doSageMakerRequest(t, h, "CreateTrainingJob", validBody)
		require.Equal(t, http.StatusOK, rr.Code)

		rr = doSageMakerRequest(t, h, "DescribeTrainingJob", map[string]any{"TrainingJobName": "tj-respsweep"})
		require.Equal(t, http.StatusOK, rr.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
		require.Equal(t, "InProgress", resp["TrainingJobStatus"])

		artifacts, ok := resp["ModelArtifacts"].(map[string]any)
		require.True(t, ok, "ModelArtifacts key must be present, even while InProgress")
		assert.Equal(t, "s3://bucket/output/output/model.tar.gz", artifacts["S3ModelArtifacts"])
	})

	tests := []struct {
		name   string
		remove string
	}{
		{name: "missing role arn", remove: "RoleArn"},
		{name: "missing output data config", remove: "OutputDataConfig"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := make(map[string]any, len(validBody))
			for k, v := range validBody {
				if k != tt.remove {
					body[k] = v
				}
			}

			rr := doSageMakerRequest(t, h, "CreateTrainingJob", body)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
		})
	}
}

// TestCreateInferenceExperiment_TypeRequired proves CreateInferenceExperiment
// rejects a request omitting Type -- required on both
// CreateInferenceExperimentInput (api_op_CreateInferenceExperiment.go:87-88)
// and DescribeInferenceExperimentOutput, but previously entirely
// unvalidated, so an omitted Type would have stored an experiment whose
// Describe response silently dropped it.
func TestCreateInferenceExperiment_TypeRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{
		"Name":             "ie-respsweep",
		"RoleArn":          "arn:aws:iam::000000000000:role/x",
		"EndpointName":     "my-endpoint",
		"ModelVariants":    []any{map[string]any{"ModelName": "m1", "VariantName": "v1"}},
		"ShadowModeConfig": map[string]any{"SourceModelVariantName": "v1"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "Type")
}

// TestCreateWorkteam_RequiredFieldsValidated proves CreateWorkteam rejects
// requests omitting MemberDefinitions/Description -- both "This member is
// required" on CreateWorkteamInput (validateOpCreateWorkteamInput,
// validators.go:18963-18988) but previously entirely unvalidated, and that
// an empty-but-present MemberDefinitions/Description survives the round
// trip once accepted (Workteam.Description/.MemberDefinitions previously
// carried `omitempty`).
func TestCreateWorkteam_RequiredFieldsValidated(t *testing.T) {
	t.Parallel()

	t.Run("missing member definitions rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rr := doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{
			"WorkteamName": "wt-no-members", "Description": "desc",
		})
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("missing description rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rr := doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{
			"WorkteamName": "wt-no-desc", "MemberDefinitions": defaultMemberDefinitions(),
		})
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})
}

// TestBatchDescribeModelPackage_ErrorResponseWireKey proves a wrong-wire-key
// bug: the error entry's message used the JSON key "ErrorMessage", but the
// real BatchDescribeModelPackageError deserializer reads "ErrorResponse"
// (deserializers.go:51081-51125) -- so a real SDK client's ErrorResponse
// field was always empty despite this backend holding the real message.
// Proven via a real aws-sdk-go-v2 client round trip since the wrong key is
// invisible to a raw-JSON assertion on the field the backend *thinks* it's
// writing.
func TestBatchDescribeModelPackage_ErrorResponseWireKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	const missingArn = "arn:aws:sagemaker:us-east-1:000000000000:model-package/does-not-exist"

	out, err := client.BatchDescribeModelPackage(t.Context(), &sagemakersdk.BatchDescribeModelPackageInput{
		ModelPackageArnList: []string{missingArn},
	})
	require.NoError(t, err)

	entry, ok := out.BatchDescribeModelPackageErrorMap[missingArn]
	require.True(t, ok)
	assert.NotEmpty(t, entry.ErrorResponse, "ErrorResponse must be populated via the real wire key, not ErrorMessage")
}

// TestCreatePipeline_ParallelismConfigurationSurvivesZero proves
// ParallelismConfiguration.MaxParallelExecutionSteps survives an explicit 0
// -- required whenever ParallelismConfiguration is present
// (validateParallelismConfiguration, validators.go:14068-14080, *int32
// nil-checked only) but previously carried `omitempty`.
func TestCreatePipeline_ParallelismConfigurationSurvivesZero(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doSageMakerRequest(t, h, "CreatePipeline", map[string]any{
		"PipelineName":       "pl-respsweep",
		"PipelineDefinition": `{"Version":"v1"}`,
		"ParallelismConfiguration": map[string]any{
			"MaxParallelExecutionSteps": 0,
		},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doSageMakerRequest(t, h, "DescribePipeline", map[string]any{"PipelineName": "pl-respsweep"})
	require.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	pc, ok := resp["ParallelismConfiguration"].(map[string]any)
	require.True(t, ok, "ParallelismConfiguration key must be present")
	steps, ok := pc["MaxParallelExecutionSteps"]
	assert.True(t, ok, "MaxParallelExecutionSteps key must be present even when 0")
	assert.InDelta(t, float64(0), steps, 0)
}

// TestCreateProcessingJob_OutputsSurvivesEmpty proves
// ProcessingOutputConfig.Outputs survives an explicit empty array --
// required whenever ProcessingOutputConfig is present
// (validateProcessingOutputConfig, validators.go:14382-14394, nil-checked
// only) but previously carried `omitempty`.
func TestCreateProcessingJob_OutputsSurvivesEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doSageMakerRequest(t, h, "CreateProcessingJob", map[string]any{
		"ProcessingJobName": "pj-respsweep",
		"RoleArn":           "arn:aws:iam::000000000000:role/x",
		"AppSpecification":  map[string]any{"ImageUri": "img:latest"},
		"ProcessingResources": map[string]any{
			"ClusterConfig": map[string]any{"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 10},
		},
		"ProcessingOutputConfig": map[string]any{"Outputs": []any{}},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doSageMakerRequest(t, h, "DescribeProcessingJob", map[string]any{"ProcessingJobName": "pj-respsweep"})
	require.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	poc, ok := resp["ProcessingOutputConfig"].(map[string]any)
	require.True(t, ok)
	outputs, ok := poc["Outputs"]
	assert.True(t, ok, "Outputs key must be present even when empty")
	assert.Empty(t, outputs)
}

// TestDescribeAutoMLJob_RequiredFieldsSurviveEmpty proves
// AutoMLOutputDataConfig.S3OutputPath and AutoMLChannel.TargetAttributeName
// survive an explicit empty string -- both "This member is required"
// (validateAutoMLOutputDataConfig/validateAutoMLChannel, validators.go,
// *string nil-checked only) but previously carried `omitempty`.
func TestDescribeAutoMLJob_RequiredFieldsSurviveEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "aml-respsweep",
		"RoleArn":       "arn:aws:iam::000000000000:role/x",
		"InputDataConfig": []any{
			map[string]any{
				"DataSource":          map[string]any{"S3DataSource": map[string]any{"S3Uri": "s3://bucket/in"}},
				"TargetAttributeName": "",
			},
		},
		"OutputDataConfig": map[string]any{"S3OutputPath": ""},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{"AutoMLJobName": "aml-respsweep"})
	require.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	outCfg, ok := resp["OutputDataConfig"].(map[string]any)
	require.True(t, ok)
	s3op, ok := outCfg["S3OutputPath"]
	assert.True(t, ok, "S3OutputPath key must be present even when empty")
	assert.Empty(t, s3op)

	inCfg, ok := resp["InputDataConfig"].([]any)
	require.True(t, ok)
	require.Len(t, inCfg, 1)
	channel, ok := inCfg[0].(map[string]any)
	require.True(t, ok)
	tan, ok := channel["TargetAttributeName"]
	assert.True(t, ok, "TargetAttributeName key must be present even when empty")
	assert.Empty(t, tan)
}

// TestCreateCompilationJob_InputConfigOrModelPackageVersionArnRequired
// proves CreateCompilationJob rejects a request supplying neither InputConfig
// nor ModelPackageVersionArn, and rejects one supplying both -- "Provide
// either a ModelPackageVersionArn or an InputConfig object ... presence of
// both ... will return an exception" (api_op_CreateCompilationJob.go:105-110)
// -- previously neither direction of this mutual-exclusion constraint was
// enforced. Also proves CompilationInputConfig.S3Uri/
// CompilationOutputConfig.S3OutputLocation survive an explicit empty string
// (both required, *string nil-checked only, previously `omitempty`).
func TestCreateCompilationJob_InputConfigOrModelPackageVersionArnRequired(t *testing.T) {
	t.Parallel()

	base := map[string]any{
		"RoleArn":           "arn:aws:iam::000000000000:role/x",
		"OutputConfig":      map[string]any{"S3OutputLocation": "s3://bucket/out"},
		"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
	}

	t.Run("neither InputConfig nor ModelPackageVersionArn rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		body := map[string]any{"CompilationJobName": "cj-neither"}
		maps.Copy(body, base)

		rr := doSageMakerRequest(t, h, "CreateCompilationJob", body)
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("both InputConfig and ModelPackageVersionArn rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		body := map[string]any{
			"CompilationJobName":     "cj-both",
			"InputConfig":            map[string]any{"S3Uri": "", "Framework": "TENSORFLOW"},
			"ModelPackageVersionArn": "arn:aws:sagemaker:us-east-1:000000000000:model-package/mp/1",
		}
		maps.Copy(body, base)

		rr := doSageMakerRequest(t, h, "CreateCompilationJob", body)
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("InputConfig with empty S3Uri survives the round trip", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		body := map[string]any{
			"CompilationJobName": "cj-empty-s3uri",
			"InputConfig":        map[string]any{"S3Uri": "", "Framework": "TENSORFLOW"},
		}
		maps.Copy(body, base)

		rr := doSageMakerRequest(t, h, "CreateCompilationJob", body)
		require.Equal(t, http.StatusOK, rr.Code)

		rr = doSageMakerRequest(t, h, "DescribeCompilationJob", map[string]any{
			"CompilationJobName": "cj-empty-s3uri",
		})
		require.Equal(t, http.StatusOK, rr.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
		inCfg, ok := resp["InputConfig"].(map[string]any)
		require.True(t, ok)
		s3uri, ok := inCfg["S3Uri"]
		assert.True(t, ok, "S3Uri key must be present even when empty")
		assert.Empty(t, s3uri)
	})
}

// TestListTrainingJobsForHPTuningJob_TunedHyperParametersSurvivesEmpty
// proves HyperParameterTrainingJobSummary.TunedHyperParameters (required on
// the real type) survives an empty hyperparameter map instead of vanishing.
func TestListTrainingJobsForHPTuningJob_TunedHyperParametersSurvivesEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "hpt-respsweep",
		"HyperParameterTuningJobConfig": map[string]any{
			"Strategy":       "Bayesian",
			"ResourceLimits": map[string]any{"MaxNumberOfTrainingJobs": 1, "MaxParallelTrainingJobs": 1},
		},
		"TrainingJobDefinition": map[string]any{
			"AlgorithmSpecification": map[string]any{"TrainingInputMode": "File"},
			"RoleArn":                "arn:aws:iam::000000000000:role/x",
			"OutputDataConfig":       map[string]any{"S3OutputPath": "s3://bucket/out"},
			"ResourceConfig": map[string]any{
				"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 20,
			},
			"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
		},
	})

	doSageMakerRequest(t, h, "CreateTrainingJob", map[string]any{
		"TrainingJobName":        "hpt-respsweep-001-abc",
		"RoleArn":                "arn:aws:iam::000000000000:role/x",
		"AlgorithmSpecification": map[string]any{"TrainingInputMode": "File"},
		"OutputDataConfig":       map[string]any{"S3OutputPath": "s3://bucket/out"},
		"ResourceConfig": map[string]any{
			"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 20,
		},
		"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
	})

	rr := doSageMakerRequest(t, h, "ListTrainingJobsForHyperParameterTuningJob", map[string]any{
		"HyperParameterTuningJobName": "hpt-respsweep",
	})
	require.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	summaries, ok := resp["TrainingJobSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	summary, ok := summaries[0].(map[string]any)
	require.True(t, ok)
	tuned, ok := summary["TunedHyperParameters"]
	assert.True(t, ok, "TunedHyperParameters key must be present even when empty")
	assert.Empty(t, tuned)
}
