package sagemaker_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_OpaqueConfigShapeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		target  string
		wantMsg string
	}{
		{
			name:    "benchmark_endpoint_identifier",
			target:  "CreateAIBenchmarkJob",
			body:    map[string]any{"BenchmarkTarget": map[string]any{"Endpoint": map[string]any{}}},
			wantMsg: "BenchmarkTarget.Endpoint.Identifier is required",
		},
		{
			name:    "benchmark_output_s3",
			target:  "CreateAIBenchmarkJob",
			body:    map[string]any{"OutputConfig": map[string]any{}},
			wantMsg: "OutputConfig.S3OutputLocation is required",
		},
		{
			name:   "recommendation_constraint_metric",
			target: "CreateAIRecommendationJob",
			body: map[string]any{
				"PerformanceTarget": map[string]any{"Constraints": []any{map[string]any{}}},
			},
			wantMsg: "PerformanceTarget.Constraints[0].Metric is required",
		},
		{
			name:   "inference_recommendations_vpc",
			target: "CreateInferenceRecommendationsJob",
			body: map[string]any{
				"JobName":     "j",
				"InputConfig": map[string]any{"VpcConfig": map[string]any{"Subnets": []string{"s"}}},
			},
			wantMsg: "InputConfig.VpcConfig.SecurityGroupIds is required",
		},
		{
			name:   "automl_tabular_target",
			target: "CreateAutoMLJobV2",
			body: map[string]any{
				"AutoMLJobName":           "a",
				"AutoMLProblemTypeConfig": map[string]any{"TabularJobConfig": map[string]any{}},
			},
			wantMsg: "TabularJobConfig.TargetAttributeName is required",
		},
		{
			name:   "domain_custom_image",
			target: "CreateDomain",
			body: map[string]any{
				"DomainName": "d",
				"DefaultUserSettings": map[string]any{
					"CodeEditorAppSettings": map[string]any{
						"CustomImages": []any{map[string]any{"ImageName": "x"}},
					},
				},
			},
			wantMsg: "CustomImages[0].AppImageConfigName is required",
		},
		{
			name:    "space_settings_not_object",
			target:  "CreateSpace",
			body:    map[string]any{"DomainId": "d", "SpaceName": "s", "SpaceSettings": "oops"},
			wantMsg: "SpaceSettings must be an object",
		},
		{
			name:    "user_settings_not_object",
			target:  "UpdateUserProfile",
			body:    map[string]any{"DomainId": "d", "UserProfileName": "u", "UserSettings": []any{}},
			wantMsg: "UserSettings must be an object",
		},
		{
			name:   "algorithm_training_image",
			target: "CreateAlgorithm",
			body: map[string]any{
				"AlgorithmName":         "a",
				"TrainingSpecification": map[string]any{"SupportedTrainingInstanceTypes": []string{"ml.m5.large"}},
			},
			wantMsg: "TrainingSpecification.TrainingImage is required",
		},
		{
			name:   "explainer_shap_config",
			target: "CreateEndpointConfig",
			body: map[string]any{
				"EndpointConfigName": "c",
				"ProductionVariants": []any{map[string]any{"VariantName": "v", "ModelName": "m"}},
				"ExplainerConfig":    map[string]any{"ClarifyExplainerConfig": map[string]any{}},
			},
			wantMsg: "ClarifyExplainerConfig.ShapConfig is required",
		},
		{
			name:    "model_package_containers",
			target:  "CreateModelPackage",
			body:    map[string]any{"ModelPackageName": "m", "InferenceSpecification": map[string]any{}},
			wantMsg: "InferenceSpecification.Containers is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, newTestHandler(t), tt.target, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantMsg)
		})
	}
}
