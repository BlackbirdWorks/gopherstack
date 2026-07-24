package transcribe

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- CreateLanguageModel ---

type createLanguageModelInput struct {
	InputDataConfig *InputDataConfig `json:"InputDataConfig"`
	ModelName       string           `json:"ModelName"`
	BaseModelName   string           `json:"BaseModelName"`
	LanguageCode    string           `json:"LanguageCode"`
	Tags            []transcribeTag  `json:"Tags"`
}

type createLanguageModelOutput struct {
	InputDataConfig *InputDataConfig `json:"InputDataConfig,omitempty"`
	ModelName       string           `json:"ModelName"`
	BaseModelName   string           `json:"BaseModelName"`
	LanguageCode    string           `json:"LanguageCode"`
	ModelStatus     string           `json:"ModelStatus"`
}

func (h *Handler) handleCreateLanguageModel(
	_ context.Context,
	in *createLanguageModelInput,
) (*createLanguageModelOutput, error) {
	m, err := h.Backend.CreateLanguageModel(&LanguageModel{
		ModelName:       in.ModelName,
		BaseModelName:   in.BaseModelName,
		LanguageCode:    in.LanguageCode,
		InputDataConfig: in.InputDataConfig,
		Tags:            tagsToMap(in.Tags),
	})
	if err != nil {
		return nil, err
	}

	return &createLanguageModelOutput{
		ModelName:       m.ModelName,
		BaseModelName:   m.BaseModelName,
		LanguageCode:    m.LanguageCode,
		ModelStatus:     m.ModelStatus,
		InputDataConfig: m.InputDataConfig,
	}, nil
}

// --- DeleteLanguageModel ---

type deleteLanguageModelInput struct {
	ModelName string `json:"ModelName"`
}

func (h *Handler) handleDeleteLanguageModel(
	_ context.Context,
	in *deleteLanguageModelInput,
) (*struct{}, error) {
	if err := h.Backend.DeleteLanguageModel(in.ModelName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- DescribeLanguageModel ---

type describeLanguageModelInput struct {
	ModelName string `json:"ModelName"`
}

type languageModelOutput struct {
	InputDataConfig     *InputDataConfig `json:"InputDataConfig,omitempty"`
	CreateTime          *float64         `json:"CreateTime,omitempty"`
	LastModifiedTime    *float64         `json:"LastModifiedTime,omitempty"`
	ModelName           string           `json:"ModelName"`
	BaseModelName       string           `json:"BaseModelName"`
	LanguageCode        string           `json:"LanguageCode"`
	ModelStatus         string           `json:"ModelStatus"`
	FailureReason       string           `json:"FailureReason,omitempty"`
	UpgradeAvailability bool             `json:"UpgradeAvailability"`
}

type describeLanguageModelOutput struct {
	LanguageModel *languageModelOutput `json:"LanguageModel"`
}

func toLanguageModelOutput(m *LanguageModel) languageModelOutput {
	out := languageModelOutput{
		ModelName:           m.ModelName,
		BaseModelName:       m.BaseModelName,
		LanguageCode:        m.LanguageCode,
		ModelStatus:         m.ModelStatus,
		FailureReason:       m.FailureReason,
		InputDataConfig:     m.InputDataConfig,
		UpgradeAvailability: m.UpgradeAvailability,
	}

	if !m.CreateTime.IsZero() {
		s := awstime.Epoch(m.CreateTime)
		out.CreateTime = &s
	}

	if !m.LastModifiedTime.IsZero() {
		s := awstime.Epoch(m.LastModifiedTime)
		out.LastModifiedTime = &s
	}

	return out
}

func (h *Handler) handleDescribeLanguageModel(
	_ context.Context,
	in *describeLanguageModelInput,
) (*describeLanguageModelOutput, error) {
	m, err := h.Backend.DescribeLanguageModel(in.ModelName)
	if err != nil {
		return nil, err
	}

	out := toLanguageModelOutput(m)

	return &describeLanguageModelOutput{
		LanguageModel: &out,
	}, nil
}

// --- ListLanguageModels ---

type listLanguageModelsInput struct {
	NextToken    string `json:"NextToken"`
	StatusEquals string `json:"StatusEquals"`
	NameContains string `json:"NameContains"`
}

type listLanguageModelsOutput struct {
	NextToken string                `json:"NextToken,omitempty"`
	Models    []languageModelOutput `json:"Models"`
}

func (h *Handler) handleListLanguageModels(
	_ context.Context,
	in *listLanguageModelsInput,
) (*listLanguageModelsOutput, error) {
	models, nextToken := h.Backend.ListLanguageModels(in.StatusEquals, in.NameContains, in.NextToken)

	result := make([]languageModelOutput, 0, len(models))
	for i := range models {
		result = append(result, toLanguageModelOutput(&models[i]))
	}

	return &listLanguageModelsOutput{
		Models:    result,
		NextToken: nextToken,
	}, nil
}
