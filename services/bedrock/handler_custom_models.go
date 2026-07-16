package bedrock

import (
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

func extractCustomModelOperation(path, method string) (string, bool) {
	if op, ok := extractCustomModelDeploymentSubOp(path, method); ok {
		return op, true
	}

	if method != http.MethodPost {
		return "", false
	}

	switch path {
	case customModelsCreate:
		return "CreateCustomModel", true
	case customModelDeploymentsPath:
		return "CreateCustomModelDeployment", true
	case foundationModelAgreement:
		return "CreateFoundationModelAgreement", true
	default:
		return "", false
	}
}

func (h *Handler) routeCustomModel(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	if method != http.MethodPost {
		return false, nil
	}

	switch path {
	case customModelsCreate:
		return true, h.handleCreateCustomModel(c, body)
	case customModelDeploymentsPath:
		return true, h.handleCreateCustomModelDeployment(c, body)
	case foundationModelAgreement:
		return true, h.handleCreateFoundationModelAgreement(c, body)
	default:
		return false, nil
	}
}

type createCustomModelInput struct {
	ModelName string `json:"modelName"`
	Tags      []Tag  `json:"tags,omitempty"`
}

type createCustomModelOutput struct {
	ModelArn string `json:"modelArn"`
}

func (h *Handler) handleCreateCustomModel(c *echo.Context, body []byte) error {
	in, err := parseBody[createCustomModelInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	model, opErr := h.Backend.CreateCustomModel(in.ModelName, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createCustomModelOutput{ModelArn: model.ModelArn})
}

func extractCustomModelListOperation(path, method string) (string, bool) {
	isSubPath := strings.HasPrefix(path, customModelsPrefix+"/")

	switch {
	case path == customModelsPrefix && method == http.MethodGet:
		return "ListCustomModels", true
	case isSubPath && method == http.MethodGet:
		return "GetCustomModel", true
	case isSubPath && method == http.MethodDelete:
		return "DeleteCustomModel", true
	default:
		return "", false
	}
}

func (h *Handler) routeCustomModelList(c *echo.Context, path, method string) (bool, error) {
	isSubPath := strings.HasPrefix(path, customModelsPrefix+"/")

	switch {
	case path == customModelsPrefix && method == http.MethodGet:
		return true, h.handleListCustomModels(c)
	case isSubPath && method == http.MethodGet:
		id := decodePath(strings.TrimPrefix(path, customModelsPrefix+"/"))

		return true, h.handleGetCustomModel(c, id)
	case isSubPath && method == http.MethodDelete:
		id := decodePath(strings.TrimPrefix(path, customModelsPrefix+"/"))

		return true, h.handleDeleteCustomModel(c, id)
	default:
		return false, nil
	}
}

type customModelOutput struct {
	CreationTime string `json:"creationTime"`
	ModelArn     string `json:"modelArn"`
	ModelName    string `json:"modelName"`
	Tags         []Tag  `json:"tags,omitempty"`
}

func customModelToOutput(m *CustomModel) customModelOutput {
	return customModelOutput{
		ModelArn:     m.ModelArn,
		ModelName:    m.ModelName,
		CreationTime: m.CreationTime.Format(time.RFC3339),
		Tags:         m.Tags,
	}
}

func (h *Handler) handleGetCustomModel(c *echo.Context, id string) error {
	m, err := h.Backend.GetCustomModel(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, customModelToOutput(m))
}

type listCustomModelsOutput struct {
	NextToken      string              `json:"nextToken,omitempty"`
	ModelSummaries []customModelOutput `json:"modelSummaries"`
}

func (h *Handler) handleListCustomModels(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	models, outToken := h.Backend.ListCustomModels(nextToken)
	summaries := make([]customModelOutput, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, customModelToOutput(m))
	}

	return c.JSON(
		http.StatusOK,
		listCustomModelsOutput{ModelSummaries: summaries, NextToken: outToken},
	)
}

func (h *Handler) handleDeleteCustomModel(c *echo.Context, id string) error {
	if err := h.Backend.DeleteCustomModel(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
