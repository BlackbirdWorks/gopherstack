package cosmosdb

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// createDatabaseBody is the request body shape for POST /dbs.
type createDatabaseBody struct {
	ID string `json:"id"`
}

func (h *Handler) handleDatabases(c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createDatabase(c)
	case http.MethodGet:
		return h.listDatabases(c)
	default:
		return h.writeError(
			c,
			http.StatusMethodNotAllowed,
			"MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.",
		)
	}
}

func (h *Handler) handleDatabaseItem(c *echo.Context, dbID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getDatabase(c, dbID)
	case http.MethodDelete:
		return h.deleteDatabase(c, dbID)
	default:
		return h.writeError(
			c,
			http.StatusMethodNotAllowed,
			"MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.",
		)
	}
}

func (h *Handler) createDatabase(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var req createDatabaseBody
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
	}

	if req.ID == "" {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "id must not be empty.")
	}

	info, createErr := h.Backend.CreateDatabase(req.ID)
	if createErr != nil {
		if errors.Is(createErr, ErrDatabaseAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "Conflict", "A database with the specified id already exists.")
		}

		return h.writeError(c, http.StatusInternalServerError, "InternalError", createErr.Error())
	}

	c.Response().Header().Set("ETag", databaseETag(info))

	return h.writeJSON(c, http.StatusCreated, databaseBody(info))
}

func (h *Handler) listDatabases(c *echo.Context) error {
	infos := h.Backend.ListDatabases()

	values := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		values = append(values, databaseBody(info))
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{
		sysPropRID: "", "Databases": values, sysPropCount: len(values),
	})
}

func (h *Handler) getDatabase(c *echo.Context, dbID string) error {
	info, err := h.Backend.GetDatabase(dbID)
	if err != nil {
		return h.writeDatabaseNotFoundError(c)
	}

	c.Response().Header().Set("ETag", databaseETag(info))

	return h.writeJSON(c, http.StatusOK, databaseBody(info))
}

func (h *Handler) deleteDatabase(c *echo.Context, dbID string) error {
	if err := h.Backend.DeleteDatabase(dbID); err != nil {
		return h.writeDatabaseNotFoundError(c)
	}

	return c.NoContent(http.StatusNoContent)
}

// databaseETag returns a static, non-versioned ETag for a database. Real
// Cosmos databases carry an _etag too, but nothing in this milestone ever
// mutates a database in place (there is no "update database" operation), so
// a fixed value derived from its RID is sufficient -- see PARITY.md.
func databaseETag(info DatabaseInfo) string {
	return `"` + info.RID + `"`
}

// databaseBody builds a Cosmos DB database resource's JSON response body.
func databaseBody(info DatabaseInfo) map[string]any {
	return map[string]any{
		"id":        info.ID,
		sysPropRID:  info.RID,
		sysPropSelf: "dbs/" + info.RID + "/",
		sysPropETag: databaseETag(info),
		sysPropTS:   1,
		"_colls":    collsSegment + "/",
		"_users":    "users/",
	}
}

// writeDatabaseNotFoundError maps a missing-database StorageBackend error to
// the corresponding Cosmos error code/status.
func (h *Handler) writeDatabaseNotFoundError(c *echo.Context) error {
	return h.writeError(c, http.StatusNotFound, "NotFound", "Owner resource does not exist")
}
