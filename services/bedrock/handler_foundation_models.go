package bedrock

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func extractFoundationModelOperation(path, method string) (string, bool) {
	switch {
	case path == foundationModelsPrefix && method == http.MethodGet:
		return "ListFoundationModels", true
	case strings.HasPrefix(path, foundationModelsPrefix+"/") && method == http.MethodGet:
		return "GetFoundationModel", true
	default:
		return "", false
	}
}

func (h *Handler) routeFoundationModel(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == foundationModelsPrefix && method == http.MethodGet:
		return true, h.handleListFoundationModels(c)
	case strings.HasPrefix(path, foundationModelsPrefix+"/") && method == http.MethodGet:
		id := decodePath(strings.TrimPrefix(path, foundationModelsPrefix+"/"))

		return true, h.handleGetFoundationModel(c, id)
	default:
		return false, nil
	}
}

type foundationModelSummaryOutput struct {
	ModelLifecycle             *FoundationModelLifecycle `json:"modelLifecycle,omitempty"`
	ModelArn                   string                    `json:"modelArn"`
	ModelID                    string                    `json:"modelId"`
	ModelName                  string                    `json:"modelName"`
	ProviderName               string                    `json:"providerName"`
	InputModalities            []string                  `json:"inputModalities,omitempty"`
	OutputModalities           []string                  `json:"outputModalities,omitempty"`
	InferenceTypesSupported    []string                  `json:"inferenceTypesSupported,omitempty"`
	CustomizationsSupported    []string                  `json:"customizationsSupported,omitempty"`
	ResponseStreamingSupported bool                      `json:"responseStreamingSupported"`
}

type listFoundationModelsOutput struct {
	NextToken      string                         `json:"nextToken,omitempty"`
	ModelSummaries []foundationModelSummaryOutput `json:"modelSummaries"`
}

func (h *Handler) handleListFoundationModels(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	models, outToken := h.Backend.ListFoundationModels(nextToken)
	summaries := make([]foundationModelSummaryOutput, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, foundationModelToOutput(m))
	}

	return c.JSON(
		http.StatusOK,
		listFoundationModelsOutput{ModelSummaries: summaries, NextToken: outToken},
	)
}

type getFoundationModelOutput struct {
	ModelDetails foundationModelSummaryOutput `json:"modelDetails"`
}

func (h *Handler) handleGetFoundationModel(c *echo.Context, modelID string) error {
	m, err := h.Backend.GetFoundationModel(modelID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, getFoundationModelOutput{
		ModelDetails: foundationModelToOutput(m),
	})
}

func foundationModelToOutput(m *FoundationModelSummary) foundationModelSummaryOutput {
	return foundationModelSummaryOutput{
		ModelArn:                   m.ModelArn,
		ModelID:                    m.ModelID,
		ModelName:                  m.ModelName,
		ProviderName:               m.ProviderName,
		InputModalities:            m.InputModalities,
		OutputModalities:           m.OutputModalities,
		InferenceTypesSupported:    m.InferenceTypesSupported,
		CustomizationsSupported:    m.CustomizationsSupported,
		ResponseStreamingSupported: m.ResponseStreamingSupported,
		ModelLifecycle:             m.ModelLifecycle,
	}
}
