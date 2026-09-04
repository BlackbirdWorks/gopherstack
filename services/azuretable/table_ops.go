package azuretable

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// preferReturnNoContent is the Prefer request header value that makes a
// Create Table/Insert Entity respond 204 (with Preference-Applied echoed
// back) instead of 201 + body. aztables sends this on every Create/Insert
// call whose ResponsePreference isn't overridden -- see
// TableClientCreateOptions.ResponsePreference in aztables' generated client.
const preferReturnNoContent = "return-no-content"

// createTableBody is the request body shape for POST /<account>/Tables.
type createTableBody struct {
	TableName string `json:"TableName"`
}

func (h *Handler) createTable(c *echo.Context) error {
	r := c.Request()

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var req createTableBody
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInput", "The input is not valid.")
	}

	if req.TableName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInput", "TableName must not be empty.")
	}

	if createErr := h.Backend.CreateTable(req.TableName); createErr != nil {
		if errors.Is(createErr, ErrTableAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "TableAlreadyExists",
				"The table specified already exists.")
		}

		return h.writeError(c, http.StatusInternalServerError, "InternalError", createErr.Error())
	}

	if r.Header.Get("Prefer") == preferReturnNoContent {
		c.Response().Header().Set("Preference-Applied", preferReturnNoContent)

		return c.NoContent(http.StatusNoContent)
	}

	level := odataLevelFromAccept(r.Header.Get("Accept"))

	return h.writeJSON(c, http.StatusCreated, h.tableEntityBody(req.TableName, level))
}

func (h *Handler) listTables(c *echo.Context) error {
	infos := h.Backend.ListTables()
	level := odataLevelFromAccept(c.Request().Header.Get("Accept"))

	values := make([]map[string]any, 0, len(infos))
	for _, ti := range infos {
		values = append(values, h.tableEntityBody(ti.Name, level))
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{"value": values})
}

func (h *Handler) deleteTable(c *echo.Context, quotedName string) error {
	name, ok := unquoteODataString(quotedName)
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "InvalidInput", "The specified table name is invalid.")
	}

	if err := h.Backend.DeleteTable(name); err != nil {
		return h.writeTableNotFoundError(c)
	}

	return c.NoContent(http.StatusNoContent)
}

// tableEntityBody builds a Table Storage table entity's OData JSON body,
// varying by metadata level: nometadata carries only TableName;
// minimalmetadata (the default) adds odata.metadata; fullmetadata further
// adds odata.type/odata.id/odata.editLink.
func (h *Handler) tableEntityBody(name, level string) map[string]any {
	m := map[string]any{"TableName": name}

	if level == odataLevelNoMetadata {
		return m
	}

	endpoint := h.serviceEndpoint()
	m["odata.metadata"] = endpoint + "/$metadata#Tables/@Element"

	if level == odataLevelFullMetadata {
		m["odata.type"] = devstoreAccountName + ".Tables"
		m["odata.id"] = endpoint + "/Tables('" + escapeODataKey(name) + "')"
		m["odata.editLink"] = "Tables('" + escapeODataKey(name) + "')"
	}

	return m
}
