package bedrock

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func extractPMTOperation(path, method string) (string, bool) {
	switch {
	case path == provisionedModelThroughput && method == http.MethodPost:
		return "CreateProvisionedModelThroughput", true
	case path == provisionedModelThroughputs && method == http.MethodGet:
		return "ListProvisionedModelThroughputs", true
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodGet:
		return "GetProvisionedModelThroughput", true
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodPatch:
		return "UpdateProvisionedModelThroughput", true
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodDelete:
		return "DeleteProvisionedModelThroughput", true
	default:
		return "", false
	}
}

func (h *Handler) routePMT(c *echo.Context, path, method string, body []byte) (bool, error) {
	id := decodePath(strings.TrimPrefix(path, provisionedModelThroughput+"/"))

	switch {
	case path == provisionedModelThroughput && method == http.MethodPost:
		return true, h.handleCreateProvisionedModelThroughput(c, body)
	case path == provisionedModelThroughputs && method == http.MethodGet:
		return true, h.handleListProvisionedModelThroughputs(c)
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodGet:
		return true, h.handleGetProvisionedModelThroughput(c, id)
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodPatch:
		return true, h.handleUpdateProvisionedModelThroughput(c, id, body)
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodDelete:
		return true, h.handleDeleteProvisionedModelThroughput(c, id)
	default:
		return false, nil
	}
}

type createProvisionedModelThroughputInput struct {
	ProvisionedModelName string `json:"provisionedModelName"`
	ModelID              string `json:"modelId"`
	CommitmentDuration   string `json:"commitmentDuration,omitempty"`
	Tags                 []Tag  `json:"tags"`
	ModelUnits           int32  `json:"modelUnits"`
}

type createProvisionedModelThroughputOutput struct {
	ProvisionedModelArn string `json:"provisionedModelArn"`
}

func (h *Handler) handleCreateProvisionedModelThroughput(c *echo.Context, body []byte) error {
	in, err := parseBody[createProvisionedModelThroughputInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	pmt, opErr := h.Backend.CreateProvisionedModelThroughput(
		in.ProvisionedModelName,
		in.ModelID,
		in.ModelUnits,
		in.CommitmentDuration,
		in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createProvisionedModelThroughputOutput{
		ProvisionedModelArn: pmt.ProvisionedModelArn,
	})
}

type provisionedModelSummaryOutput struct {
	CreationTime         isoTime `json:"creationTime"`
	LastModifiedTime     isoTime `json:"lastModifiedTime"`
	ProvisionedModelArn  string  `json:"provisionedModelArn"`
	ProvisionedModelName string  `json:"provisionedModelName"`
	ModelArn             string  `json:"modelArn"`
	DesiredModelArn      string  `json:"desiredModelArn"`
	FoundationModelArn   string  `json:"foundationModelArn"`
	Status               string  `json:"status"`
	CommitmentDuration   string  `json:"commitmentDuration,omitempty"`
	ModelUnits           int32   `json:"modelUnits"`
	DesiredModelUnits    int32   `json:"desiredModelUnits"`
}

func pmtToOutput(pmt *ProvisionedModelThroughput) provisionedModelSummaryOutput {
	return provisionedModelSummaryOutput{
		ProvisionedModelArn:  pmt.ProvisionedModelArn,
		ProvisionedModelName: pmt.ProvisionedModelName,
		ModelArn:             pmt.ModelArn,
		DesiredModelArn:      pmt.DesiredModelArn,
		FoundationModelArn:   pmt.FoundationModelArn,
		Status:               pmt.Status,
		ModelUnits:           pmt.ModelUnits,
		DesiredModelUnits:    pmt.DesiredModelUnits,
		CommitmentDuration:   pmt.CommitmentDuration,
		CreationTime:         isoTime{pmt.CreationTime},
		LastModifiedTime:     isoTime{pmt.LastModifiedTime},
	}
}

func (h *Handler) handleGetProvisionedModelThroughput(c *echo.Context, id string) error {
	pmt, err := h.Backend.GetProvisionedModelThroughput(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, pmtToOutput(pmt))
}

type listProvisionedModelThroughputsOutput struct {
	NextToken                 string                          `json:"nextToken,omitempty"`
	ProvisionedModelSummaries []provisionedModelSummaryOutput `json:"provisionedModelSummaries"`
}

func (h *Handler) handleListProvisionedModelThroughputs(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	pmts, outToken := h.Backend.ListProvisionedModelThroughputs(nextToken)
	summaries := make([]provisionedModelSummaryOutput, 0, len(pmts))

	for _, pmt := range pmts {
		summaries = append(summaries, pmtToOutput(pmt))
	}

	return c.JSON(
		http.StatusOK,
		listProvisionedModelThroughputsOutput{
			ProvisionedModelSummaries: summaries,
			NextToken:                 outToken,
		},
	)
}

// updateProvisionedModelThroughputInput mirrors the real
// UpdateProvisionedModelThroughputInput wire shape: only desiredModelId and
// desiredProvisionedModelName are updatable. AWS has no modelUnits update field.
type updateProvisionedModelThroughputInput struct {
	DesiredModelID              string `json:"desiredModelId,omitempty"`
	DesiredProvisionedModelName string `json:"desiredProvisionedModelName,omitempty"`
}

func (h *Handler) handleUpdateProvisionedModelThroughput(
	c *echo.Context,
	id string,
	body []byte,
) error {
	in, err := parseBody[updateProvisionedModelThroughputInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	_, opErr := h.Backend.UpdateProvisionedModelThroughput(id, in.DesiredModelID, in.DesiredProvisionedModelName)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteProvisionedModelThroughput(c *echo.Context, id string) error {
	if err := h.Backend.DeleteProvisionedModelThroughput(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
