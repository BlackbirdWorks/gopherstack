package lakeformation

import (
	"cmp"
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGrantPermissions(ctx context.Context, c *echo.Context, body []byte) error {
	var in grantPermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	entry := &PermissionEntry{
		Principal:                  in.Principal,
		Resource:                   withCatalogID(in.Resource, in.CatalogID),
		Permissions:                in.Permissions,
		PermissionsWithGrantOption: in.PermissionsWithGrantOption,
		Condition:                  in.Condition,
	}

	if err := h.Backend.GrantPermissions(ctx, entry); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, grantPermissionsOutput{})
}

func (h *Handler) handleRevokePermissions(ctx context.Context, c *echo.Context, body []byte) error {
	var in revokePermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	entry := &PermissionEntry{
		Principal:                  in.Principal,
		Resource:                   withCatalogID(in.Resource, in.CatalogID),
		Permissions:                in.Permissions,
		PermissionsWithGrantOption: in.PermissionsWithGrantOption,
		Condition:                  in.Condition,
	}

	if err := h.Backend.RevokePermissions(ctx, entry); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, revokePermissionsOutput{})
}

func (h *Handler) handleListPermissions(_ context.Context, c *echo.Context, body []byte) error {
	var in listPermissionsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	includeRelated := strings.EqualFold(in.IncludeRelated, "TRUE")
	if includeRelated && in.Principal != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException",
			"Principal must not be specified when IncludeRelated is TRUE")
	}

	entries, nextToken := h.Backend.ListPermissionsInCatalog(
		in.Resource,
		in.MaxResults,
		in.NextToken,
		in.Principal,
		in.ResourceType,
		in.CatalogID,
		h.AccountID,
		includeRelated,
	)

	return c.JSON(http.StatusOK, listPermissionsOutput{
		PrincipalResourcePermissions: toPermissionEntryWireList(entries),
		NextToken:                    nextToken,
	})
}

func (h *Handler) handleBatchGrantPermissions(ctx context.Context, c *echo.Context, body []byte) error {
	var in batchGrantPermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	failures, err := h.runBatchPermissions(ctx, in.Entries, in.CatalogID, h.Backend.BatchGrantPermissions)
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	return c.JSON(http.StatusOK, batchGrantPermissionsOutput{Failures: failures})
}

func (h *Handler) handleBatchRevokePermissions(ctx context.Context, c *echo.Context, body []byte) error {
	var in batchRevokePermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	failures, err := h.runBatchPermissions(ctx, in.Entries, in.CatalogID, h.Backend.BatchRevokePermissions)
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	return c.JSON(http.StatusOK, batchRevokePermissionsOutput{Failures: failures})
}

// runBatchPermissions validates the entries, applies the request CatalogId,
// runs apply, and returns its failures echoing the caller's original entries.
func (h *Handler) runBatchPermissions(
	ctx context.Context,
	entries []*BatchPermissionsRequestEntry,
	catalogID string,
	apply func(context.Context, []*BatchPermissionsRequestEntry) []*BatchFailureEntry,
) ([]BatchFailureEntry, error) {
	if err := validateBatchPermissionsEntries(entries); err != nil {
		return nil, err
	}

	scoped, originals := entriesWithCatalogID(entries, catalogID)
	failures := restoreFailureEntries(apply(ctx, scoped), originals)
	out := make([]BatchFailureEntry, 0, len(failures))

	for _, f := range failures {
		if f != nil {
			out = append(out, *f)
		}
	}

	return out, nil
}

func (h *Handler) handleGetEffectivePermissionsForPath(_ context.Context, c *echo.Context, body []byte) error {
	var in getEffectivePermissionsForPathInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	entries, nextToken := h.Backend.GetEffectivePermissionsForPathInCatalog(
		in.ResourceArn, in.MaxResults, in.NextToken, in.CatalogID, h.AccountID,
	)

	return c.JSON(http.StatusOK, getEffectivePermissionsForPathOutput{
		Permissions: toPermissionEntryWireList(entries),
		NextToken:   nextToken,
	})
}

// withCatalogID returns a copy of r whose resource-level catalog id defaults
// to catalogID, the request's CatalogId. A catalog id already on r wins.
func withCatalogID(r *Resource, catalogID string) *Resource {
	if r == nil || catalogID == "" {
		return r
	}

	cp := copyResource(r)

	switch {
	case cp.Database != nil:
		cp.Database.CatalogID = cmp.Or(cp.Database.CatalogID, catalogID)
	case cp.Table != nil:
		cp.Table.CatalogID = cmp.Or(cp.Table.CatalogID, catalogID)
	case cp.TableWithColumns != nil:
		cp.TableWithColumns.CatalogID = cmp.Or(cp.TableWithColumns.CatalogID, catalogID)
	case cp.DataLocation != nil:
		cp.DataLocation.CatalogID = cmp.Or(cp.DataLocation.CatalogID, catalogID)
	case cp.DataCellsFilter != nil:
		cp.DataCellsFilter.TableCatalogID = cmp.Or(cp.DataCellsFilter.TableCatalogID, catalogID)
	case cp.LFTag != nil:
		cp.LFTag.CatalogID = cmp.Or(cp.LFTag.CatalogID, catalogID)
	case cp.LFTagExpression != nil:
		cp.LFTagExpression.CatalogID = cmp.Or(cp.LFTagExpression.CatalogID, catalogID)
	case cp.LFTagPolicy != nil:
		cp.LFTagPolicy.CatalogID = cmp.Or(cp.LFTagPolicy.CatalogID, catalogID)
	default:
	}

	return cp
}

// entriesWithCatalogID applies withCatalogID to copies of the batch entries,
// returning a copy-to-original map so failures can echo the caller's entry.
func entriesWithCatalogID(
	entries []*BatchPermissionsRequestEntry, catalogID string,
) ([]*BatchPermissionsRequestEntry, map[*BatchPermissionsRequestEntry]*BatchPermissionsRequestEntry) {
	if catalogID == "" {
		return entries, nil
	}

	copies := make([]*BatchPermissionsRequestEntry, 0, len(entries))
	originals := make(map[*BatchPermissionsRequestEntry]*BatchPermissionsRequestEntry, len(entries))

	for _, e := range entries {
		if e == nil {
			copies = append(copies, e)

			continue
		}

		cp := *e
		cp.Resource = withCatalogID(e.Resource, catalogID)
		copies = append(copies, &cp)
		originals[&cp] = e
	}

	return copies, originals
}

func restoreFailureEntries(
	failures []*BatchFailureEntry, originals map[*BatchPermissionsRequestEntry]*BatchPermissionsRequestEntry,
) []*BatchFailureEntry {
	for _, f := range failures {
		if f != nil && f.RequestEntry != nil {
			if orig, ok := originals[f.RequestEntry]; ok {
				f.RequestEntry = orig
			}
		}
	}

	return failures
}
