package ram

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

func usesClientToken(op string) bool {
	switch op {
	case opAcceptResourceShareInvitation, opRejectResourceShareInvitation,
		opUpdateResourceShare, opDeleteResourceShare,
		opDeletePermission, opDeletePermissionVersion, opSetDefaultPermissionVersion,
		opPromotePermissionCreatedFromPolicy, opReplacePermissionAssociations,
		opAssociateResourceShare, opDisassociateResourceShare,
		opAssociateResourceSharePermission, opDisassociateResourceSharePermission:
		return true
	default:
		return false
	}
}

// replayClientToken returns the first response for a repeated (op, clientToken) with unchanged
// parameters, and IdempotentParameterMismatchException when the parameters changed.
func (h *Handler) replayClientToken(
	ctx context.Context, op string, c *echo.Context, body []byte,
) ([]byte, error) {
	query := c.Request().URL.Query()
	token := query.Get("clientToken")

	var fields map[string]any
	if len(body) > 0 && json.Unmarshal(body, &fields) == nil {
		if t, ok := fields["clientToken"].(string); ok && token == "" {
			token = t
		}

		delete(fields, "clientToken")
	}

	query.Del("clientToken")

	resp, err := idempotency.Replay(
		h.idem, op, token, idempotency.Fingerprint([]any{c.Request().URL.Path, query, fields}),
		func() (*[]byte, error) {
			r, _, dispErr := h.dispatchMutateOps(ctx, op, c, body)
			if dispErr != nil {
				return nil, dispErr
			}

			return &r, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return *resp, nil
}
