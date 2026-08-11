package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// AutoMLJobChannel is a named input source for CreateAutoMLJobV2/
// DescribeAutoMLJobV2. SDK ref: types.AutoMLJobChannel — narrower than
// AutoMLChannel: no TargetAttributeName/SampleWeightAttributeName, since V2
// carries those inside AutoMLProblemTypeConfig instead.
type AutoMLJobChannel struct {
	DataSource      *AutoMLDataSource `json:"DataSource,omitempty"`
	ChannelType     string            `json:"ChannelType,omitempty"`
	CompressionType string            `json:"CompressionType,omitempty"`
	ContentType     string            `json:"ContentType,omitempty"`
}

// EmrServerlessComputeConfig mirrors types.EmrServerlessComputeConfig.
type EmrServerlessComputeConfig struct {
	ExecutionRoleARN string `json:"ExecutionRoleARN"`
}

// AutoMLComputeConfig mirrors types.AutoMLComputeConfig.
type AutoMLComputeConfig struct {
	EmrServerlessComputeConfig *EmrServerlessComputeConfig `json:"EmrServerlessComputeConfig,omitempty"`
}

// AutoMLDataSplitConfig mirrors types.AutoMLDataSplitConfig.
type AutoMLDataSplitConfig struct {
	ValidationFraction *float32 `json:"ValidationFraction,omitempty"`
}

// AutoMLSecurityConfig mirrors types.AutoMLSecurityConfig. VpcConfig reuses
// the shared type from training_jobs.go (identical SecurityGroupIds/Subnets shape).
type AutoMLSecurityConfig struct {
	VpcConfig                             *VpcConfig `json:"VpcConfig,omitempty"`
	EnableInterContainerTrafficEncryption *bool      `json:"EnableInterContainerTrafficEncryption,omitempty"`
	VolumeKmsKeyID                        string     `json:"VolumeKmsKeyId,omitempty"`
}

// ModelDeployConfig mirrors types.ModelDeployConfig.
type ModelDeployConfig struct {
	AutoGenerateEndpointName *bool  `json:"AutoGenerateEndpointName,omitempty"`
	EndpointName             string `json:"EndpointName,omitempty"`
}

// automlProblemTypeConfigNames maps the AutoMLProblemTypeConfig tagged
// union's single top-level member key to the AutoMLProblemTypeConfigName
// enum value DescribeAutoMLJobV2Output returns. SDK ref: the member->JSON-key
// mapping in serializers.go's awsAwsjson11_serializeDocumentAutoMLProblemTypeConfig,
// cross-referenced against enums.go's AutoMLProblemTypeConfigName values.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at package load
var automlProblemTypeConfigNames = map[string]string{
	"ImageClassificationJobConfig":   "ImageClassification",
	"TabularJobConfig":               "Tabular",
	"TextClassificationJobConfig":    "TextClassification",
	"TextGenerationJobConfig":        "TextGeneration",
	"TimeSeriesForecastingJobConfig": "TimeSeriesForecasting",
}

// automlProblemTypeConfigName derives AutoMLProblemTypeConfigName from the
// single member key present in an opaque AutoMLProblemTypeConfig payload.
func automlProblemTypeConfigName(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}

	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		return ""
	}

	for key := range members {
		if name, ok := automlProblemTypeConfigNames[key]; ok {
			return name
		}
	}

	return ""
}

// AutoMLJobV2Extras bundles the CreateAutoMLJobV2-only fields set after the
// shared CreateAutoMLJob record is created.
type AutoMLJobV2Extras struct {
	AutoMLProblemTypeConfig  json.RawMessage
	OutputDataConfig         *AutoMLOutputDataConfig
	AutoMLJobObjective       *AutoMLJobObjective
	AutoMLComputeConfig      *AutoMLComputeConfig
	DataSplitConfig          *AutoMLDataSplitConfig
	SecurityConfig           *AutoMLSecurityConfig
	ModelDeployConfig        *ModelDeployConfig
	AutoMLJobInputDataConfig []AutoMLJobChannel
}

// SetAutoMLJobV2Extras sets the CreateAutoMLJobV2-only fields on an existing
// AutoML job — the fields CreateAutoMLJobV2Input requires or accepts that
// CreateAutoMLJobInput (V1) does not, most importantly the required
// AutoMLJobInputDataConfig ([]types.AutoMLJobChannel, CreateAutoMLJobV2Input:91).
func (b *InMemoryBackend) SetAutoMLJobV2Extras(ctx context.Context, name string, extras AutoMLJobV2Extras) error {
	b.mu.Lock("SetAutoMLJobV2Extras")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.autoMLJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: AutoML job %q not found", ErrAutoMLJobNotFound, name)
	}

	j.AutoMLJobInputDataConfig = append([]AutoMLJobChannel(nil), extras.AutoMLJobInputDataConfig...)

	if extras.AutoMLProblemTypeConfig != nil {
		j.AutoMLProblemTypeConfig = append(json.RawMessage(nil), extras.AutoMLProblemTypeConfig...)
	}

	if extras.OutputDataConfig != nil {
		odc := *extras.OutputDataConfig
		j.OutputDataConfig = &odc
	}

	if extras.AutoMLJobObjective != nil {
		obj := *extras.AutoMLJobObjective
		j.AutoMLJobObjective = &obj
	}

	if extras.AutoMLComputeConfig != nil {
		cc := *extras.AutoMLComputeConfig
		j.AutoMLComputeConfig = &cc
	}

	if extras.DataSplitConfig != nil {
		dsc := *extras.DataSplitConfig
		j.DataSplitConfig = &dsc
	}

	if extras.SecurityConfig != nil {
		sc := *extras.SecurityConfig
		j.SecurityConfig = &sc
	}

	if extras.ModelDeployConfig != nil {
		mdc := *extras.ModelDeployConfig
		j.ModelDeployConfig = &mdc
	}

	return nil
}
