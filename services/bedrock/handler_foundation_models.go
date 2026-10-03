package bedrock

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
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
	q := c.Request().URL.Query()

	if err := validateListFoundationModelsEnums(q); err != nil {
		return h.writeError(c, err)
	}

	models, outToken := h.Backend.ListFoundationModels(ListFoundationModelsFilter{
		ByCustomizationType: q.Get("byCustomizationType"),
		ByInferenceType:     q.Get("byInferenceType"),
		ByOutputModality:    q.Get("byOutputModality"),
		ByProvider:          q.Get("byProvider"),
		NextToken:           q.Get("nextToken"),
	})
	summaries := make([]foundationModelSummaryOutput, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, foundationModelToOutput(m))
	}

	return c.JSON(
		http.StatusOK,
		listFoundationModelsOutput{ModelSummaries: summaries, NextToken: outToken},
	)
}

// validateListFoundationModelsEnums rejects values outside the SDK enums
// (types/enums.go ModelCustomization, InferenceType, ModelModality).
func validateListFoundationModelsEnums(q url.Values) error {
	checks := []struct {
		param   string
		allowed []string
	}{
		{"byCustomizationType", []string{"FINE_TUNING", "CONTINUED_PRE_TRAINING", "DISTILLATION"}},
		{"byInferenceType", []string{"ON_DEMAND", "PROVISIONED"}},
		{"byOutputModality", []string{"TEXT", "IMAGE", "EMBEDDING"}},
	}

	for _, c := range checks {
		if v := q.Get(c.param); v != "" && !slices.Contains(c.allowed, v) {
			return fmt.Errorf("%w: invalid %s %q", ErrValidation, c.param, v)
		}
	}

	return nil
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

// validateListSortParams rejects sortBy/sortOrder outside the SDK enums; every List
// sortBy enum has only CreationTime (types/enums.go).
func validateListSortParams(q url.Values) error {
	if v := q.Get("sortBy"); v != "" && v != "CreationTime" {
		return fmt.Errorf("%w: invalid sortBy %q", ErrValidation, v)
	}

	if v := q.Get("sortOrder"); v != "" && v != "Ascending" && v != sortOrderDescending {
		return fmt.Errorf("%w: invalid sortOrder %q", ErrValidation, v)
	}

	return nil
}
