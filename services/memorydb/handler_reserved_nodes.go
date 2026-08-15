package memorydb

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDescribeReservedNodes(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeReservedNodesRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	nodes, err := h.Backend.DescribeReservedNodes(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	nodes, nextToken := paginateItems(
		nodes, req.NextToken, req.MaxResults, func(rn *ReservedNode) string { return rn.ReservationID },
	)

	return c.JSON(
		http.StatusOK,
		describeReservedNodesResponse{ReservedNodes: toReservedNodeSlice(nodes), NextToken: nextToken},
	)
}

func (h *Handler) handleDescribeReservedNodesOfferings(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeReservedNodesOfferingsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	offerings, err := h.Backend.DescribeReservedNodesOfferings(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	offerings, nextToken := paginateItems(
		offerings, req.NextToken, req.MaxResults,
		func(o *ReservedNodesOffering) string { return o.ReservedNodesOfferingID },
	)

	return c.JSON(
		http.StatusOK,
		describeReservedNodesOfferingsResponse{
			ReservedNodesOfferings: toReservedNodesOfferingSlice(offerings),
			NextToken:              nextToken,
		},
	)
}

func (h *Handler) handlePurchaseReservedNodesOffering(ctx context.Context, c *echo.Context, body []byte) error {
	var req purchaseReservedNodesOfferingRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ReservedNodesOfferingID == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"ReservedNodesOfferingId is required",
		)
	}

	rn, err := h.Backend.PurchaseReservedNodesOffering(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, purchaseReservedNodesOfferingResponse{ReservedNode: rn})
}

// -- DescribeMultiRegionParameters handler ---------------------------------------

// toReservedNodeSlice converts a slice of ReservedNode pointers to values.
func toReservedNodeSlice(nodes []*ReservedNode) []ReservedNode {
	result := make([]ReservedNode, 0, len(nodes))

	for _, n := range nodes {
		result = append(result, *n)
	}

	return result
}

// toReservedNodesOfferingSlice converts a slice of ReservedNodesOffering pointers to values.
func toReservedNodesOfferingSlice(offerings []*ReservedNodesOffering) []ReservedNodesOffering {
	result := make([]ReservedNodesOffering, 0, len(offerings))

	for _, o := range offerings {
		result = append(result, *o)
	}

	return result
}
