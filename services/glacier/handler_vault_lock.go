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
			if err := json.Unmarshal(body, &req); err != nil {
				return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
					"invalid request body: "+err.Error())
			}
		}

		lockID := generateID(lockIDLength)
		if err := h.Backend.SetVaultLock(h.AccountID, h.DefaultRegion, vaultName, req.Policy, lockID); err != nil {
			return h.writeBackendError(c, err)
		}

		// Real AWS returns the lock ID via the x-amz-lock-id header (see
		// aws-sdk-go-v2's InitiateVaultLock deserializer, which reads it from
		// there and ignores the body); the JSON body field is kept too for
		// any client that reads it directly.
		c.Response().Header().Set("X-Amz-Lock-Id", lockID)

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
