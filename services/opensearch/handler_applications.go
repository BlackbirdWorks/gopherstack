package opensearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// applicationEndpoint synthesizes a plausible OpenSearch application
// endpoint. gopherstack has no real DNS-routable application layer, so this
// is a reasonable non-stub placeholder -- the important behavioral property
// (the Endpoint field being present at all, unlike before) is what matters.
func applicationEndpoint(appID, region string) string {
	return fmt.Sprintf("%s.%s.opensearch-applications.amazonaws.com", appID, region)
}

// handleApplicationRoutes handles application routes.
func (h *Handler) handleApplicationRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchApplicationPath)

	// Root: Create/List applications.
	if rest == "" || rest == "/" {
		h.handleApplicationRootRoutes(w, r)

		return
	}

	trimmed := strings.TrimPrefix(rest, "/")

	// Sub-routes under /application/{id}/... -- data source attachments
	// (handler_data_source_attachments.go) and capabilities
	// (handler_capabilities.go).
	if appID, subPath, ok := strings.Cut(trimmed, "/"); ok {
		h.handleApplicationSubRoutes(w, r, appID, subPath)

		return
	}

	// Per-app-ID routes.
	h.handleApplicationIDRoutes(w, r, rest)
}

// handleApplicationSubRoutes dispatches every /application/{id}/{subPath}
// route to its owning family.
func (h *Handler) handleApplicationSubRoutes(
	w http.ResponseWriter,
	r *http.Request,
	appID, subPath string,
) {
	switch subPath {
	case subPathAttachDataSource, subPathDetachDataSource,
		subPathDescribeDataSourceAttachment, subPathListDataSourceAttachments:
		h.handleDataSourceAttachmentRoutes(w, r, appID, subPath)

		return
	}

	if capPath, ok := strings.CutPrefix(subPath, "capability/"); ok {
		h.handleCapabilityRoutes(w, r, appID, capPath)

		return
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleApplicationRootRoutes handles /application and /application/ requests
// (CreateApplication only -- ListApplications is NOT here, see
// handleListApplications).
func (h *Handler) handleApplicationRootRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handleCreateApplication(w, r)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleListApplications serves ListApplications: GET
// /2021-01-01/opensearch/list-applications (a sibling of, not nested under,
// the /application prefix -- api_op_ListApplications.go, opensearch@v1.75.4
// serializers.go) -- gopherstack-l5ir.
func (h *Handler) handleListApplications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	apps := h.Backend.ListApplications()
	summaries := make([]map[string]any, 0, len(apps))

	for _, app := range apps {
		summaries = append(summaries, map[string]any{
			"id":                 app.ID,
			jsonKeyAppName:       app.Name,
			jsonKeyAppArn:        app.ARN,
			jsonKeyStatusLower:   pkgStateActive,
			"endpoint":           applicationEndpoint(app.ID, h.Backend.Region()),
			jsonKeyCreatedAt:     app.CreatedAt,
			jsonKeyLastUpdatedAt: app.LastUpdatedAt,
		})
	}

	h.writeJSON(r, w, map[string]any{"ApplicationSummaries": summaries})
}

// handleDefaultApplicationSettingRoutes handles
// GET/PUT /2021-01-01/opensearch/defaultApplicationSetting
// (GetDefaultApplicationSetting / PutDefaultApplicationSetting). This is a
// top-level path, NOT nested under /application -- and the real SDK wire
// shape carries a single lowercase "applicationArn" field (plus
// "setAsDefault" on the PUT request), not a per-ApplicationType settings
// list.
func (h *Handler) handleDefaultApplicationSettingRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.writeJSON(r, w, map[string]any{"applicationArn": h.Backend.GetDefaultApplicationSetting()})
	case http.MethodPut:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}

		var req struct {
			ApplicationArn string `json:"applicationArn"`
			SetAsDefault   bool   `json:"setAsDefault"`
		}

		if len(body) > 0 {
			if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
				h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

				return
			}
		}

		newArn, putErr := h.Backend.PutDefaultApplicationSetting(req.ApplicationArn, req.SetAsDefault)
		if putErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", putErr.Error())

			return
		}

		h.writeJSON(r, w, map[string]any{"applicationArn": newArn})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleApplicationIDRoutes handles /application/{appId} requests.
func (h *Handler) handleApplicationIDRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	appID := strings.TrimPrefix(rest, "/")
	if strings.Contains(appID, "/") {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	switch r.Method {
	case http.MethodGet:
		app, err := h.Backend.GetApplication(appID)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{
			"id": app.ID, jsonKeyAppName: app.Name, jsonKeyAppArn: app.ARN,
			"appConfigs": app.AppConfigs, "dataSources": app.DataSources,
			jsonKeyStatusLower:   pkgStateActive,
			"endpoint":           applicationEndpoint(app.ID, h.Backend.Region()),
			jsonKeyCreatedAt:     app.CreatedAt,
			jsonKeyLastUpdatedAt: app.LastUpdatedAt,
		})
	case http.MethodDelete:
		if err := h.Backend.DeleteApplication(appID); err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			AppConfigs  []appConfigJSON `json:"AppConfigs"`
			DataSources []appDSJSON     `json:"DataSources"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		appConfigs := make([]AppConfig, len(req.AppConfigs))
		for i, ac := range req.AppConfigs {
			appConfigs[i] = AppConfig(ac)
		}
		dataSources := make([]AppDataSource, len(req.DataSources))
		for i, ds := range req.DataSources {
			dataSources[i] = AppDataSource(ds)
		}
		app, updateErr := h.Backend.UpdateApplication(appID, appConfigs, dataSources)
		if updateErr != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

			return
		}
		// UpdateApplicationOutput carries no Status field (unlike
		// GetApplication/ListApplications) -- do not add one here.
		h.writeJSON(r, w, map[string]any{
			"id": app.ID, jsonKeyAppName: app.Name, jsonKeyAppArn: app.ARN,
			"appConfigs": app.AppConfigs, "dataSources": app.DataSources,
			jsonKeyCreatedAt:     app.CreatedAt,
			jsonKeyLastUpdatedAt: app.LastUpdatedAt,
		})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// createApplicationRequest is the JSON request body for CreateApplication.
type createApplicationRequest struct {
	Name        string          `json:"name"`
	AppConfigs  []appConfigJSON `json:"appConfigs"`
	DataSources []appDSJSON     `json:"dataSources"`
	TagList     []svcTags.KV    `json:"tagList,omitempty"`
}

// appConfigJSON is the JSON representation of an AppConfig.
type appConfigJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// appDSJSON is the JSON representation of an application data source.
type appDSJSON struct {
	DataSourceArn string `json:"dataSourceArn"`
}

// createApplicationOutput is the JSON response for CreateApplication. Note
// CreateApplicationOutput has no Status or LastUpdatedAt field (unlike
// GetApplication) -- do not add them here. Field names are lowerCamelCase,
// matching the real deserializer's case-sensitive key switch (opensearch@v1.75.4
// deserializers.go:1830 awsRestjson1_deserializeOpDocumentCreateApplicationOutput)
// -- unlike the older Domain API surface, which is PascalCase.
type createApplicationOutput struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	ARN         string          `json:"arn"`
	AppConfigs  []appConfigJSON `json:"appConfigs"`
	DataSources []appDSJSON     `json:"dataSources"`
	CreatedAt   float64         `json:"createdAt"`
}

func (h *Handler) handleCreateApplication(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createApplicationRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	appConfigs := make([]AppConfig, 0, len(req.AppConfigs))
	for _, ac := range req.AppConfigs {
		appConfigs = append(appConfigs, AppConfig(ac))
	}

	dataSources := make([]AppDataSource, 0, len(req.DataSources))
	for _, ds := range req.DataSources {
		dataSources = append(dataSources, AppDataSource(ds))
	}

	app, createErr := h.Backend.CreateApplication(req.Name, appConfigs, dataSources, svcTags.MapFromKV(req.TagList))
	if createErr != nil {
		if errors.Is(createErr, ErrApplicationAlreadyExists) {
			// CreateApplication's own deserializer (opensearch@v1.75.4
			// deserializers.go) models ConflictException, not
			// ResourceAlreadyExistsException, for this case.
			h.writeError(r, w, http.StatusConflict, "ConflictException", createErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())
		}

		return
	}

	outConfigs := make([]appConfigJSON, 0, len(app.AppConfigs))
	for _, ac := range app.AppConfigs {
		outConfigs = append(outConfigs, appConfigJSON(ac))
	}

	outDS := make([]appDSJSON, 0, len(app.DataSources))
	for _, ds := range app.DataSources {
		outDS = append(outDS, appDSJSON(ds))
	}

	h.writeJSON(r, w, createApplicationOutput{
		ID:          app.ID,
		Name:        app.Name,
		ARN:         app.ARN,
		AppConfigs:  outConfigs,
		DataSources: outDS,
		CreatedAt:   app.CreatedAt,
	})
}
