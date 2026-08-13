package opensearch

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleReservedInstancesRoutes handles GET /reservedInstances → DescribeReservedInstances.
// DescribeReservedInstanceOfferings and PurchaseReservedInstanceOffering are
// NOT here: their real paths are siblings of this prefix, not nested under it
// -- see handleReservedInstanceOfferings / handlePurchaseReservedInstanceOffering.
func (h *Handler) handleReservedInstancesRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchReservedPath)

	if (rest != "" && rest != "/") || r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	instances := h.Backend.DescribeReservedInstances(r.URL.Query().Get("reservationId"))
	if instances == nil {
		instances = []*ReservedInstance{}
	}

	h.writeJSON(r, w, map[string]any{"ReservedInstances": instances})
}

// handleReservedInstanceOfferings serves DescribeReservedInstanceOfferings:
// GET /2021-01-01/opensearch/reservedInstanceOfferings.
func (h *Handler) handleReservedInstanceOfferings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	offerings := h.Backend.DescribeReservedInstanceOfferings(r.URL.Query().Get("offeringId"))
	h.writeJSON(r, w, map[string]any{"ReservedInstanceOfferings": offerings})
}

// handlePurchaseReservedInstanceOffering serves PurchaseReservedInstanceOffering:
// POST /2021-01-01/opensearch/purchaseReservedInstanceOffering, with
// ReservedInstanceOfferingId in the body (api_op_PurchaseReservedInstanceOffering.go,
// opensearch@v1.75.4: literal path, no {Param} URL binding) -- gopherstack-l5ir.
func (h *Handler) handlePurchaseReservedInstanceOffering(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req struct {
		ReservedInstanceOfferingID string `json:"ReservedInstanceOfferingId"`
		ReservationName            string `json:"ReservationName"`
		InstanceCount              int    `json:"InstanceCount"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	if req.InstanceCount == 0 {
		req.InstanceCount = 1
	}

	ri, purchaseErr := h.Backend.PurchaseReservedInstanceOffering(
		req.ReservedInstanceOfferingID,
		req.ReservationName,
		req.InstanceCount,
	)
	if purchaseErr != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", purchaseErr.Error())

		return
	}

	h.writeJSON(r, w, map[string]any{
		"ReservedInstanceId": ri.ReservedInstanceID,
		"ReservationName":    ri.ReservationName,
	})
}
