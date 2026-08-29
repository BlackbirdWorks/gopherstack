package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// directQueryDataSourceJSON renders a DirectQueryDataSource as the wire-shape
// types.DirectQueryDataSource / GetDirectQueryDataSourceOutput object. Note
// the domain-level DataSource type uses "Name" but this type uses
// "DataSourceName" -- they are genuinely different field names on the real
// API, not a typo.
type directQueryDataSourceJSON struct {
	DataSourceArn  string          `json:"DataSourceArn"`
	DataSourceName string          `json:"DataSourceName"`
	DataSourceType json.RawMessage `json:"DataSourceType,omitempty"`
	Description    string          `json:"Description,omitempty"`
	OpenSearchArns []string        `json:"OpenSearchArns"`
}

func toDirectQueryDataSourceJSON(ds *DirectQueryDataSource) directQueryDataSourceJSON {
	return directQueryDataSourceJSON{
		DataSourceArn:  ds.DataSourceArn,
		DataSourceName: ds.Name,
		DataSourceType: ds.DataSourceType,
		Description:    ds.Description,
		OpenSearchArns: ds.OpenSearchArns,
	}
}

// dataSourceJSON renders a DataSource as the wire-shape types.DataSourceDetails
// / GetDataSourceOutput object (top-level fields, no envelope).
type dataSourceJSON struct {
	Description    string          `json:"Description,omitempty"`
	Name           string          `json:"Name"`
	Status         string          `json:"Status,omitempty"`
	DataSourceType json.RawMessage `json:"DataSourceType,omitempty"`
}

func toDataSourceJSON(ds *DataSource) dataSourceJSON {
	return dataSourceJSON{
		DataSourceType: ds.DataSourceType,
		Description:    ds.Description,
		Name:           ds.Name,
		Status:         ds.Status,
	}
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
		items := make([]directQueryDataSourceJSON, 0, len(sources))
		for _, ds := range sources {
			items = append(items, toDirectQueryDataSourceJSON(ds))
		}
		h.writeJSON(r, w, map[string]any{"DirectQueryDataSources": items})
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
	h.writeJSON(r, w, toDirectQueryDataSourceJSON(ds))
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
		DataSourceType json.RawMessage `json:"DataSourceType"`
		Description    string          `json:"Description"`
		OpenSearchArns []string        `json:"OpenSearchArns"`
	}
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}
	ds, updateErr := h.Backend.UpdateDirectQueryDataSource(
		name,
		req.Description,
		req.DataSourceType,
		req.OpenSearchArns,
	)
	if updateErr != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

		return
	}
	h.writeJSON(r, w, map[string]any{"DataSourceArn": ds.DataSourceArn})
}

// updateDataSourceRequest is the JSON request body for UpdateDataSource
// (types.UpdateDataSourceInput -- DomainName and Name come from the URL
// path, not the body).
type updateDataSourceRequest struct {
	Description    string          `json:"Description"`
	Status         string          `json:"Status"`
	DataSourceType json.RawMessage `json:"DataSourceType"`
}

// handleUpdateDataSource handles PUT /2021-01-01/opensearch/domain/{DomainName}/dataSource/{Name}.
func (h *Handler) handleUpdateDataSource(w http.ResponseWriter, r *http.Request, domainName, name string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req updateDataSourceRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}

	updateErr := h.Backend.UpdateDataSource(domainName, name, req.Description, req.DataSourceType, req.Status)
	if updateErr != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

		return
	}

	h.writeJSON(r, w, map[string]any{"Message": "Data source updated successfully"})
}

// addDataSourceRequest is the JSON request body for AddDataSource.
type addDataSourceRequest struct {
	Name           string          `json:"Name"`
	Description    string          `json:"Description"`
	DataSourceType json.RawMessage `json:"DataSourceType"`
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
		req.DataSourceType,
	)
	if addErr != nil {
		switch {
		case errors.Is(addErr, ErrDomainNotFound):
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", addErr.Error())
		default:
			// AddDataSource's own deserializer (opensearch@v1.75.4
			// deserializers.go) has no ResourceAlreadyExistsException case --
			// a duplicate name, like any other client-fault, is
			// ValidationException.
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())
		}

		return
	}

	h.writeJSON(r, w, addDataSourceOutput{Message: msg})
}

// addDirectQueryDataSourceRequest is the JSON request body for AddDirectQueryDataSource.
type addDirectQueryDataSourceRequest struct {
	DataSourceName string          `json:"DataSourceName"`
	Description    string          `json:"Description"`
	DataSourceType json.RawMessage `json:"DataSourceType"`
	OpenSearchArns []string        `json:"OpenSearchArns"`
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
		req.DataSourceType,
		req.OpenSearchArns,
	)
	if addErr != nil {
		// AddDirectQueryDataSource's own deserializer (opensearch@v1.75.4
		// deserializers.go) has no ResourceAlreadyExistsException case -- a
		// duplicate name, like any other client-fault, is ValidationException.
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())

		return
	}

	h.writeJSON(r, w, addDirectQueryDataSourceOutput{DataSourceArn: dsARN})
}
