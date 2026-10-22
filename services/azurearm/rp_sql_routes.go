package azurearm

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	sqlNsIdx       = 5
	sqlTypeIdx     = 6
	sqlServerSegs  = 8
	sqlChildSegs   = 10
	sqlListSegs    = 9
	sqlDBListSegs  = 11
	sqlChildType   = 8
	sqlOpSegs      = 8
	sqlOpProviders = 2
	sqlOpNsIdx     = 3
	sqlOpLocIdx    = 4
	sqlOpKindIdx   = 6
	sqlAsyncSuffix = "AzureAsyncOperation"
	sqlResultsSfx  = "OperationResults"
)

func isSQLScope(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, sqlNsIdx, namespaceMicrosoftSQL) && segAt(segs, sqlTypeIdx, sqlServersType)
}

func isSQLOperation(segs []string) bool {
	if len(segs) != sqlOpSegs || !isSubscriptionScoped(segs) || !segAt(segs, sqlOpProviders, providersSegment) ||
		!segAt(segs, sqlOpNsIdx, namespaceMicrosoftSQL) || !segAt(segs, sqlOpLocIdx, "locations") {
		return false
	}

	_, ok := sqlOperationKind(segs[sqlOpKindIdx])

	return ok
}

// sqlOperationKind splits an operation collection name such as serverAzureAsyncOperation into the resource
// kind ("server") and whether it is the Azure-AsyncOperation (true) or the Location (false) flavour.
func sqlOperationKind(seg string) (bool, bool) {
	for _, kind := range []string{"server", "database", "elasticPool"} {
		switch {
		case strings.EqualFold(seg, kind+sqlAsyncSuffix):
			return true, true
		case strings.EqualFold(seg, kind+sqlResultsSfx):
			return false, true
		}
	}

	return false, false
}

func isSQLWrite(segs []string, r *http.Request) bool {
	switch r.Method {
	case http.MethodPut, http.MethodDelete:
	case http.MethodPatch:
		patchable := isSQLScope(segs, sqlServerSegs) ||
			(isSQLScope(segs, sqlChildSegs) && segAt(segs, sqlChildType, sqlDatabasesType))
		if !patchable {
			return false
		}
	default:
		return false
	}

	return isSQLScope(segs, sqlServerSegs) ||
		(isSQLScope(segs, sqlChildSegs) &&
			(segAt(segs, sqlChildType, sqlDatabasesType) || segAt(segs, sqlChildType, sqlPoolsType)))
}

func isSQLChildList(segs []string, r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}

	return isSQLScope(segs, sqlListSegs) ||
		(isSQLScope(segs, sqlDBListSegs) && segAt(segs, sqlChildType, sqlDatabasesType))
}

//nolint:gochecknoglobals // static route table, read-only after init
var sqlRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isSQLOperation(segs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSQLOperation(c, segs) },
	},
	{
		match:  isSQLWrite,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSQLWrite(c, segs) },
	},
	{
		match:  isSQLChildList,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSQLChildList(c, segs) },
	},
}

func (h *Handler) sqlFor(c *echo.Context, segs []string) (*SQLProvider, ResourceID, error) {
	p, ok := h.sqlProvider()
	if !ok {
		return nil, ResourceID{}, h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs))
	if err != nil {
		return nil, ResourceID{}, h.writeAPIError(c, err)
	}

	return p, id, nil
}

// setSQLOperation points the response's Azure-AsyncOperation and Location headers at status resources.
func (h *Handler) setSQLOperation(c *echo.Context, p *SQLProvider, id ResourceID) {
	kind := "server"

	switch classifySQLPath(id) {
	case sqlKindDatabase:
		kind = "database"
	case sqlKindPool:
		kind = "elasticPool"
	default:
	}

	opID := p.CreateOperation()
	base := h.baseURLFor(c.Request()) + "/subscriptions/" + id.SubscriptionID + "/providers/" +
		namespaceMicrosoftSQL + "/locations/" + DefaultLocation + "/"
	query := "?api-version=" + c.Request().URL.Query().Get("api-version")

	hdr := c.Response().Header()
	hdr.Set("Azure-Asyncoperation", base+kind+sqlAsyncSuffix+"/"+opID+query)
	hdr.Set("Location", base+kind+sqlResultsSfx+"/"+opID+query)
	hdr.Set("Retry-After", "1")
}

func (h *Handler) handleSQLOperation(c *echo.Context, segs []string) error {
	p, ok := h.sqlProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	opID := segs[len(segs)-1]
	if !p.OperationKnown(opID) {
		return h.writeAPIError(c, ErrSQLChildNotFound)
	}

	if async, _ := sqlOperationKind(segs[sqlOpKindIdx]); async {
		return h.writeJSON(c, http.StatusOK, map[string]any{fieldName: opID, sqlPropStatus: sqlStatusSucceeded})
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleSQLWrite(c *echo.Context, segs []string) error {
	p, id, err := h.sqlFor(c, segs)
	if p == nil {
		return err
	}

	if c.Request().Method == http.MethodDelete {
		return h.deleteSQL(c, p, id)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	status := http.StatusOK
	apply := p.Put

	switch {
	case c.Request().Method == http.MethodPatch && classifySQLPath(id) == sqlKindServer:
		apply = func(_ context.Context, id ResourceID, b map[string]any) (map[string]any, error) {
			return p.Patch(id, b)
		}
	case c.Request().Method == http.MethodPatch:
		apply = func(_ context.Context, id ResourceID, b map[string]any) (map[string]any, error) {
			return p.PatchDatabase(id, b)
		}
	default:
		if _, getErr := p.Get(c.Request().Context(), id); getErr != nil {
			status = http.StatusCreated
		}
	}

	out, err := apply(c.Request().Context(), id, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	h.setSQLOperation(c, p, id)

	return h.writeJSON(c, status, out)
}

func (h *Handler) deleteSQL(c *echo.Context, p *SQLProvider, id ResourceID) error {
	if err := p.Delete(c.Request().Context(), id); err != nil {
		if errors.Is(err, ErrSQLServerNotFound) || errors.Is(err, ErrSQLChildNotFound) || isNotFoundOnDelete(err) {
			return c.NoContent(http.StatusNoContent)
		}

		return h.writeAPIError(c, err)
	}

	h.setSQLOperation(c, p, id)

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleSQLChildList(c *echo.Context, segs []string) error {
	p, id, err := h.sqlFor(c, segs[:len(segs)-1])
	if p == nil {
		return err
	}

	values, err := p.ListChildren(id, segs[len(segs)-1])
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: values})
}
