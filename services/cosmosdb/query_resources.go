package cosmosdb

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// queryResources runs the SQL query in the request body over resource bodies (databases or containers), as
// the SDK's query-databases and query-containers pagers do via POST with the query header.
func (h *Handler) queryResources(c *echo.Context, listKey string, bodies []map[string]any) error {
	token, limit, reqErr := pageRequest(c.Request())
	if reqErr != nil {
		return h.writeContinuationError(c)
	}

	raw, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var req queryRequestBody

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	if decErr := dec.Decode(&req); decErr != nil || rejectTrailingJSON(dec) != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
	}

	stmt, parseErr := parseQuery(req.Query)
	if parseErr != nil {
		return h.writeDocumentError(c, parseErr)
	}

	res, execErr := executeStatement(stmt, req.Parameters, mapRows(bodies))
	if execErr != nil {
		return h.writeDocumentError(c, execErr)
	}

	pg := paginate(res.Rows, token, limit)
	setPageHeaders(c, len(pg.Data), pg.Next)
	setCharge(c, ruMetadata)

	return h.writeJSON(c, http.StatusOK, map[string]any{sysPropRID: "", listKey: pg.Data, sysPropCount: len(pg.Data)})
}
