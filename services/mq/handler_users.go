package mq

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createUserBody struct {
	Username string   `json:"username"`
	Password string   `json:"password"`
	Groups   []string `json:"groups"`
	Console  bool     `json:"consoleAccess"`
}

func (h *Handler) handleCreateUser(c *echo.Context, brokerID, username string, body []byte) error {
	var in createUserBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if err := h.Backend.CreateUser(brokerID, username, in.Password, in.Groups, in.Console); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDescribeUser(c *echo.Context, brokerID, username string) error {
	u, err := h.Backend.DescribeUser(brokerID, username)
	if err != nil {
		return h.writeError(c, err)
	}

	groups := u.Groups
	if groups == nil {
		groups = []string{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBrokerID:     brokerID,
		"username":      u.Username,
		"consoleAccess": u.Console,
		"groups":        groups,
	})
}

func (h *Handler) handleDeleteUser(c *echo.Context, brokerID, username string) error {
	if err := h.Backend.DeleteUser(brokerID, username); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type updateUserBody struct {
	Console  *bool    `json:"consoleAccess"`
	Password string   `json:"password"`
	Groups   []string `json:"groups"`
}

func (h *Handler) handleUpdateUser(c *echo.Context, brokerID, username string, body []byte) error {
	var in updateUserBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if err := h.Backend.UpdateUser(brokerID, username, in.Password, in.Groups, in.Console); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListUsers(c *echo.Context, brokerID string) error {
	users, err := h.Backend.ListUsers(brokerID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBrokerID: brokerID,
		"users":     users,
	})
}
