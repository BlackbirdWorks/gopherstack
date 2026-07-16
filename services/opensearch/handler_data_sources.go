package opensearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// marshalDataSourceType converts any DataSourceType value (typically a
// JSON-decoded map[string]any) to a JSON string for backend storage.
// Falls back to fmt.Sprintf only if marshalling fails.
func marshalDataSourceType(v any) string {
	if v == nil {
		return ""
	}

	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}

	return string(b)
}

// handleDirectQueryRoutes handles direct query data source routes.
func (h *Handler) handleDirectQueryRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchDirectQueryPath)

	switch {
	// POST /2021-01-01/opensearch/directQueryDataSource → AddDirectQueryDataSource
	case (rest == "" || rest == "/") && r.Method == http.MethodPost:
		h.handleAddDirectQueryDataSource(w, r)
	// GET /2021-01-01/opensearch/directQueryDataSource → ListDirectQueryDataSources
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		sources := h.Backend.ListDirectQueryDataSources()
		h.writeJSON(r, w, map[string]any{"DirectQueryDataSources": sources})
	// GET /2021-01-01/opensearch/directQueryDataSource/{dataSourceName} → GetDirectQueryDataSource
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodGet:
		h.handleGetDirectQueryDataSource(w, r, strings.TrimPrefix(rest, "/"))
	// DELETE /2021-01-01/opensearch/directQueryDataSource/{dataSourceName} → DeleteDirectQueryDataSource
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodDelete:
		h.handleDeleteDirectQueryDataSource(w, r, strings.TrimPrefix(rest, "/"))
	// PUT /2021-01-01/opensearch/directQueryDataSource/{dataSourceName} → UpdateDirectQueryDataSource
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodPut:
		h.handleUpdateDirectQueryDataSource(w, r, strings.TrimPrefix(rest, "/"))
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func (h *Handler) handleGetDirectQueryDataSource(
	w http.ResponseWriter,
	r *http.Request,
	name string,
) {
	ds, err := h.Backend.GetDirectQueryDataSource(name)
	if err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return
	}
	h.writeJSON(r, w, ds)
}

func (h *Handler) handleDeleteDirectQueryDataSource(
	w http.ResponseWriter,
	_ *http.Request,
	name string,
) {
	_ = h.Backend.DeleteDirectQueryDataSource(name)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleUpdateDirectQueryDataSource(
	w http.ResponseWriter,
	r *http.Request,
	name string,
) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}
	var req struct {
		Description    string   `json:"Description"`
		OpenSearchArns []string `json:"OpenSearchArns"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}
	ds, updateErr := h.Backend.UpdateDirectQueryDataSource(
		name,
		req.Description,
		req.OpenSearchArns,
	)
	if updateErr != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

		return
	}
	h.writeJSON(r, w, map[string]any{"DataSourceArn": ds.DataSourceArn})
}

// addDataSourceRequest is the JSON request body for AddDataSource.
type addDataSourceRequest struct {
	DataSourceType any    `json:"DataSourceType"`
	Name           string `json:"Name"`
	Description    string `json:"Description"`
}

// addDataSourceOutput is the JSON response for AddDataSource.
type addDataSourceOutput struct {
	Message string `json:"Message"`
}

func (h *Handler) handleAddDataSource(w http.ResponseWriter, r *http.Request, domainName string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req addDataSourceRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	msg, addErr := h.Backend.AddDataSource(
		domainName,
		req.Name,
		req.Description,
		marshalDataSourceType(req.DataSourceType),
	)
	if addErr != nil {
		switch {
		case errors.Is(addErr, ErrDomainNotFound):
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", addErr.Error())
		case errors.Is(addErr, ErrDataSourceAlreadyExists):
			h.writeError(
				r,
				w,
				http.StatusConflict,
				"ResourceAlreadyExistsException",
				addErr.Error(),
			)
		default:
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())
		}

		return
	}

	h.writeJSON(r, w, addDataSourceOutput{Message: msg})
}

// addDirectQueryDataSourceRequest is the JSON request body for AddDirectQueryDataSource.
type addDirectQueryDataSourceRequest struct {
	DataSourceName string   `json:"DataSourceName"`
	Description    string   `json:"Description"`
	DataSourceType any      `json:"DataSourceType"`
	OpenSearchArns []string `json:"OpenSearchArns"`
}

// addDirectQueryDataSourceOutput is the JSON response for AddDirectQueryDataSource.
type addDirectQueryDataSourceOutput struct {
	DataSourceArn string `json:"DataSourceArn"`
}

func (h *Handler) handleAddDirectQueryDataSource(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req addDirectQueryDataSourceRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	dsARN, addErr := h.Backend.AddDirectQueryDataSource(
		req.DataSourceName,
		req.Description,
		marshalDataSourceType(req.DataSourceType),
		req.OpenSearchArns,
	)
	if addErr != nil {
		if errors.Is(addErr, ErrDataSourceAlreadyExists) {
			h.writeError(
				r,
				w,
				http.StatusConflict,
				"ResourceAlreadyExistsException",
				addErr.Error(),
			)
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())
		}

		return
	}

	h.writeJSON(r, w, addDirectQueryDataSourceOutput{DataSourceArn: dsARN})
}
