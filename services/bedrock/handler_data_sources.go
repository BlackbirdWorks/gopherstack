package bedrock

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *AgentsHandler) dispatchDataSourceRoutes(
	c *echo.Context,
	kbID, suffix, method string,
	body []byte,
) error {
	// ListDataSources is real bedrock-agent@v1.58.4 serializers.go:4634: POST
	// .../datasources/, the SAME path+method family CreateDataSource's PUT
	// uses -- method alone disambiguates them. GET is accepted too as
	// harmless extra leniency for this package's own tests.
	if suffix == suffixDataSources && method == http.MethodPut {
		return h.handleCreateDataSource(c, kbID, body)
	}

	if suffix == suffixDataSources && (method == http.MethodPost || method == http.MethodGet) {
		return h.handleListDataSources(c, kbID)
	}

	if rest, dsOK := strings.CutPrefix(suffix, "/datasources/"); dsOK {
		parts := strings.SplitN(rest, "/", splitInTwo)
		dsID := parts[0]
		dsSuffix := ""

		if len(parts) > splitInTwo-1 {
			dsSuffix = "/" + parts[1]
		}

		if handled, err := h.dispatchDataSourceIDRoutes(c, kbID, dsID, dsSuffix, method, body); handled {
			return err
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown data source operation"),
	)
}

// dispatchDataSourceIDRoutes handles routes for a specific data source ID.
// Returns (true, err) when the path was matched; (false, nil) when it was not.
func (h *AgentsHandler) dispatchDataSourceIDRoutes(
	c *echo.Context,
	kbID, dsID, dsSuffix, method string,
	body []byte,
) (bool, error) {
	switch {
	case dsSuffix == "" && method == http.MethodGet:
		return true, h.handleGetDataSource(c, kbID, dsID)
	case dsSuffix == "" && method == http.MethodPut:
		return true, h.handleUpdateDataSource(c, kbID, dsID, body)
	case dsSuffix == "" && method == http.MethodDelete:
		return true, h.handleDeleteDataSource(c, kbID, dsID)
	case strings.HasPrefix(dsSuffix, suffixIngestionJobs):
		return h.dispatchDataSourceIngestionRoutes(c, kbID, dsID, dsSuffix, method, body)
	case strings.HasPrefix(dsSuffix, suffixDocuments):
		return h.dispatchDataSourceDocumentRoutes(c, kbID, dsID, dsSuffix, method, body)
	}

	return false, nil
}

// dispatchDataSourceIngestionRoutes handles the .../ingestionjobs[...] suffix
// family, split out of dispatchDataSourceIDRoutes to keep its cyclomatic
// complexity down.
func (h *AgentsHandler) dispatchDataSourceIngestionRoutes(
	c *echo.Context,
	kbID, dsID, dsSuffix, method string,
	body []byte,
) (bool, error) {
	// ListIngestionJobs is real bedrock-agent@v1.58.4 serializers.go:4961:
	// POST .../ingestionjobs/; StartIngestionJob is real serializers.go:5663:
	// PUT .../ingestionjobs/ (the SAME path) -- method alone disambiguates
	// them. GET is accepted too as harmless extra leniency for this
	// package's own tests.
	switch {
	case dsSuffix == suffixIngestionJobs && method == http.MethodPut:
		return true, h.handleStartIngestionJob(c, kbID, dsID, body)
	case dsSuffix == suffixIngestionJobs && (method == http.MethodPost || method == http.MethodGet):
		return true, h.handleListIngestionJobs(c, kbID, dsID)
	case strings.HasPrefix(dsSuffix, "/ingestionjobs/"):
		return true, h.dispatchIngestionJobRoutes(c, kbID, dsID, dsSuffix, method)
	}

	return false, nil
}

// dispatchDataSourceDocumentRoutes handles the .../documents[...] suffix
// family, split out of dispatchDataSourceIDRoutes to keep its cyclomatic
// complexity down. GetKnowledgeBaseDocuments (POST .../getDocuments) and
// DeleteKnowledgeBaseDocuments (POST .../deleteDocuments) are carved out
// here by their real sub-paths; the base .../documents collection path
// (Ingest=PUT, List=POST) is handled by dispatchDocumentOps.
func (h *AgentsHandler) dispatchDataSourceDocumentRoutes(
	c *echo.Context,
	kbID, dsID, dsSuffix, method string,
	body []byte,
) (bool, error) {
	switch {
	case dsSuffix == "/documents/getDocuments" && method == http.MethodPost:
		return true, h.handleGetKBDocuments(c, kbID, dsID, body)
	case dsSuffix == "/documents/deleteDocuments" && method == http.MethodPost:
		return true, h.handleDeleteKBDocuments(c, kbID, dsID, body)
	default:
		return true, h.dispatchDocumentOps(c, kbID, dsID, dsSuffix, method, body)
	}
}

func (h *AgentsHandler) handleCreateDataSource(c *echo.Context, kbID string, body []byte) error {
	var req struct {
		DataSourceConfiguration map[string]any `json:"dataSourceConfiguration"`
		VectorIngestionConfig   map[string]any `json:"vectorIngestionConfiguration"`
		Name                    string         `json:"name"`
		Description             string         `json:"description"`
		DataDeletionPolicy      string         `json:"dataDeletionPolicy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ds, err := h.Backend.CreateDataSourceWithConfiguration(
		kbID,
		req.Name,
		req.Description,
		req.DataDeletionPolicy,
		req.DataSourceConfiguration,
		req.VectorIngestionConfig,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respDataSource: ds})
}

func (h *AgentsHandler) handleGetDataSource(c *echo.Context, kbID, dsID string) error {
	ds, err := h.Backend.GetDataSource(kbID, dsID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respDataSource: ds})
}

func (h *AgentsHandler) handleListDataSources(c *echo.Context, kbID string) error {
	list, outToken := h.Backend.ListDataSources(kbID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"dataSourceSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateDataSource(
	c *echo.Context,
	kbID, dsID string,
	body []byte,
) error {
	var req struct {
		DataSourceConfiguration map[string]any `json:"dataSourceConfiguration"`
		VectorIngestionConfig   map[string]any `json:"vectorIngestionConfiguration"`
		Name                    string         `json:"name"`
		Description             string         `json:"description"`
		DataDeletionPolicy      string         `json:"dataDeletionPolicy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ds, err := h.Backend.UpdateDataSourceWithConfiguration(
		kbID,
		dsID,
		req.Name,
		req.Description,
		req.DataDeletionPolicy,
		req.DataSourceConfiguration,
		req.VectorIngestionConfig,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respDataSource: ds})
}

func (h *AgentsHandler) handleDeleteDataSource(c *echo.Context, kbID, dsID string) error {
	if err := h.Backend.DeleteDataSource(kbID, dsID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{
			"dataSourceId":     dsID,
			"knowledgeBaseId":  kbID,
			"dataSourceStatus": "DELETING",
		},
	)
}
