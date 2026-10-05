package backup

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCancelLegalHold(c *echo.Context, legalHoldID string) error {
	if legalHoldID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "LegalHoldId is required"),
		)
	}

	q := c.Request().URL.Query()
	if err := h.Backend.CancelLegalHold(
		legalHoldID, q.Get("cancelDescription"), int64(parseInt(q.Get("retainRecordInDays"))),
	); err != nil {
		return h.handleError(c, err)
	}

	// Real AWS: responseCode 201 (unusual for a DELETE, but confirmed against
	// the service model -- CancelLegalHold really does respond 201, not 204).
	return c.NoContent(http.StatusCreated)
}

type dateRangeJSON struct {
	FromDate float64 `json:"FromDate"`
	ToDate   float64 `json:"ToDate"`
}

type recoveryPointSelectionJSON struct {
	DateRange           *dateRangeJSON `json:"DateRange,omitempty"`
	ResourceIdentifiers []string       `json:"ResourceIdentifiers,omitempty"`
	VaultNames          []string       `json:"VaultNames,omitempty"`
}

// recoveryPointSelectionFromJSON converts the wire shape to the domain type,
// returning nil for a zero-value (all-empty) selection so
// recoveryPointMatchesSelection's "nil = matches everything" short-circuit
// applies uniformly whether the field was omitted or sent empty.
func recoveryPointSelectionFromJSON(in *recoveryPointSelectionJSON) *RecoveryPointSelection {
	if in == nil || (in.DateRange == nil && len(in.ResourceIdentifiers) == 0 && len(in.VaultNames) == 0) {
		return nil
	}

	sel := &RecoveryPointSelection{
		ResourceIdentifiers: in.ResourceIdentifiers,
		VaultNames:          in.VaultNames,
	}
	if in.DateRange != nil {
		from := time.Unix(int64(in.DateRange.FromDate), 0).UTC()
		to := time.Unix(int64(in.DateRange.ToDate), 0).UTC()
		sel.DateRange = &DateRange{FromDate: &from, ToDate: &to}
	}

	return sel
}

type createLegalHoldBody struct {
	RecoveryPointSelection *recoveryPointSelectionJSON `json:"RecoveryPointSelection,omitempty"`
	Title                  string                      `json:"Title"`
	Description            string                      `json:"Description"`
	IdempotencyToken       string                      `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleCreateLegalHold(c *echo.Context, body []byte) error {
	var in createLegalHoldBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	if in.Title == "" {
		return c.JSON(http.StatusBadRequest, errResp("MissingParameterValueException", "Title is required"))
	}

	if in.Description == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "Description is required"),
		)
	}

	lh, err := h.Backend.CreateLegalHoldWithToken(
		in.Title, in.Description, recoveryPointSelectionFromJSON(in.RecoveryPointSelection), in.IdempotencyToken,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyLegalHoldID:  lh.LegalHoldID,
		keyLegalHoldArn: lh.LegalHoldArn,
		keyTitle:        lh.Title,
		keyDescription:  lh.Description,
		keyStatus:       lh.Status,
		keyCreationDate: epochSeconds(lh.CreationDate),
	})
}

// recoveryPointSelectionToJSON renders a RecoveryPointSelection for
// GetLegalHold, with DateRange bounds as epoch-seconds (restjson1 wire format).
func recoveryPointSelectionToJSON(sel *RecoveryPointSelection) map[string]any {
	out := map[string]any{}
	if len(sel.VaultNames) > 0 {
		out["VaultNames"] = sel.VaultNames
	}
	if len(sel.ResourceIdentifiers) > 0 {
		out["ResourceIdentifiers"] = sel.ResourceIdentifiers
	}
	if sel.DateRange != nil {
		dr := map[string]any{}
		if sel.DateRange.FromDate != nil {
			dr["FromDate"] = epochSeconds(*sel.DateRange.FromDate)
		}
		if sel.DateRange.ToDate != nil {
			dr["ToDate"] = epochSeconds(*sel.DateRange.ToDate)
		}
		out["DateRange"] = dr
	}

	return out
}

// dispatchLegalHoldOps handles legal-hold describe/list operations.
func (h *Handler) dispatchLegalHoldOps(c *echo.Context, route backupRoute) (bool, error) {
	switch route.operation {
	case opGetLegalHold:
		lh, err := h.Backend.GetLegalHold(route.resource)
		if err != nil {
			return true, c.JSON(
				http.StatusBadRequest,
				errResp("ResourceNotFoundException", err.Error()),
			)
		}

		resp := map[string]any{
			keyLegalHoldID: lh.LegalHoldID, keyTitle: lh.Title,
			keyStatus: lh.Status, keyLegalHoldArn: lh.LegalHoldArn,
			keyDescription: lh.Description, keyCreationDate: epochSeconds(lh.CreationDate),
		}
		setOptionalStr(resp, "CancelDescription", lh.CancelDescription)
		if lh.CancellationDate != nil {
			resp["CancellationDate"] = epochSeconds(*lh.CancellationDate)
		}
		if lh.RetainRecordUntil != nil {
			resp["RetainRecordUntil"] = epochSeconds(*lh.RetainRecordUntil)
		}
		if lh.RecoveryPointSelection != nil {
			resp["RecoveryPointSelection"] = recoveryPointSelectionToJSON(lh.RecoveryPointSelection)
		}

		return true, c.JSON(http.StatusOK, resp)
	case opListLegalHolds:
		q := c.Request().URL.Query()
		lhs, nextToken := h.Backend.ListLegalHolds(parseInt(q.Get("maxResults")), q.Get("nextToken"))
		items := make([]map[string]any, 0, len(lhs))
		for _, lh := range lhs {
			item := map[string]any{
				keyLegalHoldID:  lh.LegalHoldID,
				keyTitle:        lh.Title,
				keyStatus:       lh.Status,
				keyLegalHoldArn: lh.LegalHoldArn,
				keyDescription:  lh.Description,
				keyCreationDate: epochSeconds(lh.CreationDate),
			}
			if lh.CancellationDate != nil {
				item["CancellationDate"] = epochSeconds(*lh.CancellationDate)
			}
			items = append(items, item)
		}

		resp := map[string]any{"LegalHolds": items}
		if nextToken != "" {
			resp["NextToken"] = nextToken
		}

		return true, c.JSON(http.StatusOK, resp)
	}

	return false, nil
}
