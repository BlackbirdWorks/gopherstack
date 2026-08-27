package sns

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// parseMemberList reads Name.member.N values from the form and returns the slice.
func parseMemberList(c *echo.Context, prefix string) []string {
	var items []string

	for i := 1; ; i++ {
		v := c.Request().FormValue(fmt.Sprintf("%s.member.%d", prefix, i))
		if v == "" {
			return items
		}

		items = append(items, v)
	}
}

func (h *Handler) handleAddPermission(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	label := c.Request().FormValue("Label")

	if topicArn == "" || label == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"TopicArn and Label are required",
		)
	}

	accounts := parseMemberList(c, "AWSAccountId")
	actions := parseMemberList(c, "ActionName")

	if err := h.Backend.AddPermission(topicArn, label, accounts, actions); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, AddPermissionResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}

func (h *Handler) handleRemovePermission(c *echo.Context) error {
	topicArn := c.Request().FormValue("TopicArn")
	label := c.Request().FormValue("Label")

	if topicArn == "" || label == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameter",
			"TopicArn and Label are required",
		)
	}

	if err := h.Backend.RemovePermission(topicArn, label); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, RemovePermissionResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.NewString()},
	})
}
