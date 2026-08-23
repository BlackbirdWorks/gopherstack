package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGrantPermissions(ctx context.Context, c *echo.Context, body []byte) error {
	var in grantPermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	entry := &PermissionEntry{
		Principal:                  in.Principal,
		Resource:                   in.Resource,
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
		Resource:                   in.Resource,
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

	entries, nextToken := h.Backend.ListPermissions(
		in.Resource,
		in.MaxResults,
		in.NextToken,
		in.Principal,
		in.ResourceType,
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

	if err := validateBatchPermissionsEntries(in.Entries); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	failures := h.Backend.BatchGrantPermissions(ctx, in.Entries)

	result := batchGrantPermissionsOutput{Failures: make([]BatchFailureEntry, 0, len(failures))}

	for _, f := range failures {
		if f != nil {
			result.Failures = append(result.Failures, *f)
		}
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleBatchRevokePermissions(ctx context.Context, c *echo.Context, body []byte) error {
	var in batchRevokePermissionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := validateBatchPermissionsEntries(in.Entries); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	failures := h.Backend.BatchRevokePermissions(ctx, in.Entries)

	result := batchRevokePermissionsOutput{Failures: make([]BatchFailureEntry, 0, len(failures))}

	for _, f := range failures {
		if f != nil {
			result.Failures = append(result.Failures, *f)
		}
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleGetEffectivePermissionsForPath(_ context.Context, c *echo.Context, body []byte) error {
	var in getEffectivePermissionsForPathInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	entries, nextToken := h.Backend.GetEffectivePermissionsForPath(in.ResourceArn, in.MaxResults, in.NextToken)

	return c.JSON(http.StatusOK, getEffectivePermissionsForPathOutput{
		Permissions: toPermissionEntryWireList(entries),
		NextToken:   nextToken,
	})
}
