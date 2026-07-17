package glacier

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleVaultLock(c *echo.Context, op, resource string, body []byte) error {
	vaultName := extractVaultName(resource)

	switch op {
	case opAbortVaultLock:
		if err := h.Backend.AbortVaultLock(h.AccountID, h.DefaultRegion, vaultName); err != nil {
			return h.writeBackendError(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	case opInitiateVaultLock:
		var req vaultLockPolicyRequest
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}

		lockID := generateID(lockIDLength)
		if err := h.Backend.SetVaultLock(h.AccountID, h.DefaultRegion, vaultName, req.Policy, lockID); err != nil {
			return h.writeBackendError(c, err)
		}

		return c.JSON(http.StatusCreated, map[string]string{"lockId": lockID})
	case opCompleteVaultLock:
		lockID := extractSubID(resource)
		if err := h.Backend.CompleteVaultLock(h.AccountID, h.DefaultRegion, vaultName, lockID); err != nil {
			return h.writeBackendError(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	case opGetVaultLock:
		return h.handleGetVaultLock(c, vaultName)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetVaultLock(c *echo.Context, vaultName string) error {
	lock, err := h.Backend.GetVaultLock(h.AccountID, h.DefaultRegion, vaultName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getVaultLockResponse{
		Policy:         lock.Policy,
		State:          lock.State,
		CreationDate:   lock.CreationDate,
		ExpirationDate: lock.ExpirationDate,
	})
}
