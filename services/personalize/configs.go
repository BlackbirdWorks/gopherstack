package personalize

// SolutionConfig mirrors aws-sdk-go-v2/service/personalize/types.SolutionConfig
// (types.go:1945).
type SolutionConfig struct {
	AutoMLConfig                    *AutoMLConfig          `json:"autoMLConfig,omitempty"`
	AutoTrainingConfig              *AutoTrainingConfig    `json:"autoTrainingConfig,omitempty"`
	EventsConfig                    *EventsConfig          `json:"eventsConfig,omitempty"`
	HpoConfig                       *HPOConfig             `json:"hpoConfig,omitempty"`
	OptimizationObjective           *OptimizationObjective `json:"optimizationObjective,omitempty"`
	TrainingDataConfig              *TrainingDataConfig    `json:"trainingDataConfig,omitempty"`
	AlgorithmHyperParameters        map[string]string      `json:"algorithmHyperParameters,omitempty"`
	FeatureTransformationParameters map[string]string      `json:"featureTransformationParameters,omitempty"`
	EventValueThreshold             string                 `json:"eventValueThreshold,omitempty"`
}

// AutoMLConfig mirrors types.AutoMLConfig (types.go:68).
type AutoMLConfig struct {
	MetricName string   `json:"metricName,omitempty"`
	RecipeList []string `json:"recipeList,omitempty"`
}

// AutoTrainingConfig mirrors types.AutoTrainingConfig (types.go:92).
type AutoTrainingConfig struct {
	SchedulingExpression string `json:"schedulingExpression,omitempty"`
}

// EventsConfig mirrors types.EventsConfig (types.go:1214).
type EventsConfig struct {
	EventParametersList []EventParameters `json:"eventParametersList,omitempty"`
}

// EventParameters mirrors types.EventParameters (types.go:1197).
type EventParameters struct {
	EventType           string  `json:"eventType,omitempty"`
	EventValueThreshold float64 `json:"eventValueThreshold,omitempty"`
	Weight              float64 `json:"weight,omitempty"`
}

// HPOConfig mirrors types.HPOConfig (types.go:1397).
type HPOConfig struct {
	AlgorithmHyperParameterRanges *HyperParameterRanges `json:"algorithmHyperParameterRanges,omitempty"`
	HpoObjective                  *HPOObjective         `json:"hpoObjective,omitempty"`
	HpoResourceConfig             *HPOResourceConfig    `json:"hpoResourceConfig,omitempty"`
}

// HPOObjective mirrors types.HPOObjective (types.go:1416).
type HPOObjective struct {
	MetricName  string `json:"metricName,omitempty"`
	MetricRegex string `json:"metricRegex,omitempty"`
	Type        string `json:"type,omitempty"`
}

// HPOResourceConfig mirrors types.HPOResourceConfig (types.go:1431).
type HPOResourceConfig struct {
	MaxNumberOfTrainingJobs string `json:"maxNumberOfTrainingJobs,omitempty"`
	MaxParallelTrainingJobs string `json:"maxParallelTrainingJobs,omitempty"`
}

// HyperParameterRanges mirrors types.HyperParameterRanges (types.go:1446).
type HyperParameterRanges struct {
	CategoricalHyperParameterRanges []CategoricalHyperParameterRange `json:"categoricalHyperParameterRanges,omitempty"`
	ContinuousHyperParameterRanges  []ContinuousHyperParameterRange  `json:"continuousHyperParameterRanges,omitempty"`
	IntegerHyperParameterRanges     []IntegerHyperParameterRange     `json:"integerHyperParameterRanges,omitempty"`
}

// CategoricalHyperParameterRange mirrors types.CategoricalHyperParameterRange
// (types.go:548).
type CategoricalHyperParameterRange struct {
	Name   string   `json:"name,omitempty"`
	Values []string `json:"values,omitempty"`
}

// ContinuousHyperParameterRange mirrors types.ContinuousHyperParameterRange
// (types.go:560).
type ContinuousHyperParameterRange struct {
	Name     string  `json:"name,omitempty"`
	MaxValue float64 `json:"maxValue,omitempty"`
	MinValue float64 `json:"minValue,omitempty"`
}

// IntegerHyperParameterRange mirrors types.IntegerHyperParameterRange
// (types.go:1461).
type IntegerHyperParameterRange struct {
	Name     string `json:"name,omitempty"`
	MaxValue int32  `json:"maxValue,omitempty"`
	MinValue int32  `json:"minValue,omitempty"`
}

// OptimizationObjective mirrors types.OptimizationObjective (types.go:1586).
type OptimizationObjective struct {
	ItemAttribute        string `json:"itemAttribute,omitempty"`
	ObjectiveSensitivity string `json:"objectiveSensitivity,omitempty"`
}

// TrainingDataConfig mirrors types.TrainingDataConfig (types.go:2232).
type TrainingDataConfig struct {
	ExcludedDatasetColumns map[string][]string `json:"excludedDatasetColumns,omitempty"`
	IncludedDatasetColumns map[string][]string `json:"includedDatasetColumns,omitempty"`
}

// SolutionUpdateConfig mirrors types.SolutionUpdateConfig (types.go:2020) --
// the narrower subset of SolutionConfig that UpdateSolution can change after
// creation (AutoTrainingConfig/EventsConfig only; the other SolutionConfig
// members are creation-time-only).
type SolutionUpdateConfig struct {
	AutoTrainingConfig *AutoTrainingConfig `json:"autoTrainingConfig,omitempty"`
	EventsConfig       *EventsConfig       `json:"eventsConfig,omitempty"`
}

// CampaignConfig mirrors types.CampaignConfig (types.go:433).
type CampaignConfig struct {
	ItemExplorationConfig             map[string]string  `json:"itemExplorationConfig,omitempty"`
	RankingInfluence                  map[string]float64 `json:"rankingInfluence,omitempty"`
	EnableMetadataWithRecommendations *bool              `json:"enableMetadataWithRecommendations,omitempty"`
	SyncWithLatestSolutionVersion     *bool              `json:"syncWithLatestSolutionVersion,omitempty"`
}

// RecommenderConfig mirrors types.RecommenderConfig (types.go:1728).
type RecommenderConfig struct {
	TrainingDataConfig                 *TrainingDataConfig `json:"trainingDataConfig,omitempty"`
	ItemExplorationConfig              map[string]string   `json:"itemExplorationConfig,omitempty"`
	EnableMetadataWithRecommendations  *bool               `json:"enableMetadataWithRecommendations,omitempty"`
	MinRecommendationRequestsPerSecond *int32              `json:"minRecommendationRequestsPerSecond,omitempty"`
}
