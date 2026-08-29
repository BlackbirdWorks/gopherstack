package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// migrationSourceJSON matches types.MigrationSource.
type migrationSourceJSON struct {
	DatasourceArn string `json:"datasourceArn"`
}

// migrationWorkspaceJSON matches types.MigrationWorkspace.
type migrationWorkspaceJSON struct {
	WorkspaceID     string `json:"workspaceId"`
	Name            string `json:"name"`
	Type            string `json:"type"`
	CreateWorkspace bool   `json:"createWorkspace"`
}

// savedObjectIdentifierJSON matches types.SavedObjectIdentifier.
type savedObjectIdentifierJSON struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// exportOptionsJSON matches types.ExportOptions.
type exportOptionsJSON struct {
	Objects               []savedObjectIdentifierJSON `json:"objects"`
	Types                 []string                    `json:"types"`
	IncludeReferencesDeep bool                        `json:"includeReferencesDeep"`
}

// startMigrationRequest is the JSON request body for StartMigration, field-
// diffed against types.MigrationOptions: source.datasourceArn, workspace
// (required by the SDK), exportOptions, and conflictResolution.
type startMigrationRequest struct {
	ApplicationID    string `json:"applicationId"`
	MigrationOptions struct {
		Source             migrationSourceJSON     `json:"source"`
		Workspace          *migrationWorkspaceJSON `json:"workspace"`
		ExportOptions      *exportOptionsJSON      `json:"exportOptions"`
		ConflictResolution string                  `json:"conflictResolution"`
	} `json:"migrationOptions"`
}

// startMigrationOutput matches StartMigrationOutput.
type startMigrationOutput struct {
	MigrationID string `json:"migrationId"`
	Status      string `json:"status"`
}

// migrationJSON matches GetMigrationOutput/the MigrationSummary element shape
// shared by ListMigrationsOutput.
type migrationJSON struct {
	ApplicationID string              `json:"applicationId"`
	MigrationID   string              `json:"migrationId"`
	Status        string              `json:"status"`
	Source        migrationSourceJSON `json:"source"`
	CreatedAt     float64             `json:"createdAt"`
	UpdatedAt     float64             `json:"updatedAt"`
	ExportedCount int                 `json:"exportedCount"`
	ImportedCount int                 `json:"importedCount"`
}

func toMigrationJSON(m *Migration) migrationJSON {
	return migrationJSON{
		ApplicationID: m.ApplicationID,
		CreatedAt:     float64(m.CreatedAt.Unix()),
		MigrationID:   m.MigrationID,
		Source:        migrationSourceJSON{DatasourceArn: m.SourceArn},
		Status:        m.Status,
		UpdatedAt:     float64(m.UpdatedAt.Unix()),
		ExportedCount: m.ExportedCount,
		ImportedCount: m.ImportedCount,
	}
}

// handleAppMigrationsRoutes handles /2021-01-01/opensearch/app-migrations and
// /2021-01-01/opensearch/app-migrations/{migrationId}.
func (h *Handler) handleAppMigrationsRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchAppMigrationsPath)

	switch {
	case (rest == "" || rest == "/") && r.Method == http.MethodPost:
		h.handleStartMigration(w, r)
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		h.handleListMigrations(w, r)
	case strings.HasPrefix(rest, "/") && len(rest) > 1 && r.Method == http.MethodGet:
		h.handleGetMigration(w, r, strings.TrimPrefix(rest, "/"))
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func (h *Handler) handleStartMigration(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req startMigrationRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}

	m, startErr := h.Backend.StartMigration(
		req.ApplicationID,
		req.MigrationOptions.Source.DatasourceArn,
		toMigrationWorkspaceInput(req.MigrationOptions.Workspace),
		toExportOptionsInput(req.MigrationOptions.ExportOptions),
		req.MigrationOptions.ConflictResolution,
	)
	if startErr != nil {
		h.writeMigrationError(r, w, startErr)

		return
	}

	h.writeJSON(r, w, startMigrationOutput{MigrationID: m.MigrationID, Status: m.Status})
}

// toMigrationWorkspaceInput converts the wire shape to the backend's
// MigrationWorkspaceInput. nil in, nil out (the backend treats a nil pointer
// as "Workspace was omitted", matching MigrationOptions.Workspace's required
// -ness check in resolveMigrationWorkspaceLocked).
func toMigrationWorkspaceInput(ws *migrationWorkspaceJSON) *MigrationWorkspaceInput {
	if ws == nil {
		return nil
	}

	return &MigrationWorkspaceInput{
		WorkspaceID:     ws.WorkspaceID,
		Name:            ws.Name,
		Type:            ws.Type,
		CreateWorkspace: ws.CreateWorkspace,
	}
}

// toExportOptionsInput converts the wire shape to the backend's
// ExportOptionsInput.
func toExportOptionsInput(opts *exportOptionsJSON) *ExportOptionsInput {
	if opts == nil {
		return nil
	}

	objects := make([]SavedObjectIdentifierInput, 0, len(opts.Objects))
	for _, o := range opts.Objects {
		objects = append(objects, SavedObjectIdentifierInput(o))
	}

	return &ExportOptionsInput{
		Objects:               objects,
		Types:                 opts.Types,
		IncludeReferencesDeep: opts.IncludeReferencesDeep,
	}
}

func (h *Handler) handleGetMigration(w http.ResponseWriter, r *http.Request, migrationID string) {
	m, err := h.Backend.GetMigration(migrationID)
	if err != nil {
		h.writeMigrationError(r, w, err)

		return
	}

	h.writeJSON(r, w, toMigrationJSON(m))
}

func (h *Handler) handleListMigrations(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	migrations, err := h.Backend.ListMigrations(q.Get("applicationId"), q.Get("status"))
	if err != nil {
		// Unlike GetMigration/StartMigration, ListMigrations's own deserializer
		// (opensearch@v1.75.4 deserializers.go) has no ResourceNotFoundException
		// case -- only ValidationException.
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())

		return
	}

	items := make([]migrationJSON, 0, len(migrations))
	for _, m := range migrations {
		items = append(items, toMigrationJSON(m))
	}

	h.writeJSON(r, w, map[string]any{"migrations": items})
}

// writeMigrationError maps migration errors to their documented HTTP status
// codes -- ResourceNotFoundException at 409, matching the same "application"
// API family convention as writeAttachmentError/writeCapabilityError.
func (h *Handler) writeMigrationError(r *http.Request, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrApplicationNotFound),
		errors.Is(err, ErrDataSourceNotFound),
		errors.Is(err, ErrMigrationNotFound),
		errors.Is(err, ErrWorkspaceNotFound):
		h.writeError(r, w, http.StatusConflict, "ResourceNotFoundException", err.Error())
	default:
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
	}
}
