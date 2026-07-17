package glacier

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListProvisionedCapacity(c *echo.Context, accountID string) error {
	caps := h.Backend.ListProvisionedCapacity(accountID)
	items := make([]ProvisionedCapacity, 0, len(caps))

	for _, item := range caps {
		items = append(items, *item)
	}

	return c.JSON(http.StatusOK, listProvisionedCapacityResponse{
		ProvisionedCapacityList: items,
	})
}

func (h *Handler) handlePurchaseProvisionedCapacity(c *echo.Context, accountID string) error {
	provCap, err := h.Backend.PurchaseProvisionedCapacity(accountID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	c.Response().Header().Set("X-Amz-Capacity-Id", provCap.CapacityID)

	return c.JSON(http.StatusCreated, purchaseProvisionedCapacityResponse{
		CapacityID: provCap.CapacityID,
	})
}
