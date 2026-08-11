package mq

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// mqUsersDefaultPageSize is ListUsers' documented default MaxResults (20),
// distinct from mqDefaultPageSize used by ListBrokers/ListConfigurations --
// see ListUsersInput.MaxResults in aws-sdk-go-v2/service/mq ("20 by
// default... must be an integer from 5 to 100").
const mqUsersDefaultPageSize = 20

type createUserBody struct {
	Username        string   `json:"username"`
	Password        string   `json:"password"`
	Groups          []string `json:"groups"`
	Console         bool     `json:"consoleAccess"`
	ReplicationUser bool     `json:"replicationUser"`
}

func (h *Handler) handleCreateUser(c *echo.Context, brokerID, username string, body []byte) error {
	var in createUserBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if err := h.Backend.CreateUser(
		brokerID, username, in.Password, in.Groups, in.Console, in.ReplicationUser,
	); err != nil {
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

	resp := map[string]any{
		keyBrokerID:       brokerID,
		"username":        u.Username,
		"consoleAccess":   u.Console,
		"groups":          groups,
		"replicationUser": u.ReplicationUser,
	}

	if u.Pending != nil {
		resp["pending"] = u.Pending
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteUser(c *echo.Context, brokerID, username string) error {
	if err := h.Backend.DeleteUser(brokerID, username); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type updateUserBody struct {
	Console         *bool    `json:"consoleAccess"`
	ReplicationUser *bool    `json:"replicationUser"`
	Password        string   `json:"password"`
	Groups          []string `json:"groups"`
}

func (h *Handler) handleUpdateUser(c *echo.Context, brokerID, username string, body []byte) error {
	var in updateUserBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if err := h.Backend.UpdateUser(
		brokerID, username, in.Password, in.Groups, in.Console, in.ReplicationUser,
	); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListUsers(c *echo.Context, brokerID string) error {
	users, err := h.Backend.ListUsers(brokerID)
	if err != nil {
		return h.writeError(c, err)
	}

	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults := 0

	if s := q.Get("maxResults"); s != "" {
		if n, parseErr := strconv.Atoi(s); parseErr == nil && n > 0 && n <= 100 {
			maxResults = n
		}
	}

	// Use opaque index-based tokens so the page boundary is stable regardless
	// of insertions or deletions between requests, matching ListBrokers.
	pg := page.New(users, nextToken, maxResults, mqUsersDefaultPageSize)

	resp := map[string]any{
		keyBrokerID: brokerID,
		"users":     pg.Data,
	}
	if pg.Next != "" {
		resp["nextToken"] = pg.Next
	}

	return c.JSON(http.StatusOK, resp)
}
