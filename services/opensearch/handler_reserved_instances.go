package opensearch

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleReservedInstancesRoutes handles reserved instance routes.
func (h *Handler) handleReservedInstancesRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchReservedPath)

	switch {
	// GET /reservedInstances → DescribeReservedInstances
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		instances := h.Backend.DescribeReservedInstances()
		if instances == nil {
			instances = []*ReservedInstance{}
		}
		h.writeJSON(r, w, map[string]any{"ReservedInstances": instances})
	// GET /reservedInstances/offerings → DescribeReservedInstanceOfferings
	case rest == "/offerings" && r.Method == http.MethodGet:
		offerings := h.Backend.DescribeReservedInstanceOfferings()
		h.writeJSON(r, w, map[string]any{"ReservedInstanceOfferings": offerings})
	// POST /reservedInstances/offerings/{offeringId} → PurchaseReservedInstanceOffering
	case strings.HasPrefix(rest, "/offerings/") && r.Method == http.MethodPost:
		offeringID := strings.TrimPrefix(rest, "/offerings/")
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			ReservationName string `json:"ReservationName"`
			InstanceCount   int    `json:"InstanceCount"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		if req.InstanceCount == 0 {
			req.InstanceCount = 1
		}
		ri, purchaseErr := h.Backend.PurchaseReservedInstanceOffering(
			offeringID,
			req.ReservationName,
			req.InstanceCount,
		)
		if purchaseErr != nil {
			h.writeError(
				r,
				w,
				http.StatusNotFound,
				"ResourceNotFoundException",
				purchaseErr.Error(),
			)

			return
		}
		h.writeJSON(r, w, map[string]any{
			"ReservedInstanceId": ri.ReservedInstanceID,
			"ReservationName":    ri.ReservationName,
		})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}
