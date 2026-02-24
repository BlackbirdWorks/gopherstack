package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	cwlogsbackend "github.com/blackbirdworks/gopherstack/cloudwatchlogs"
)

func (h *DashboardHandler) cloudWatchLogsIndex(c *echo.Context) error {
	if h.CloudWatchLogsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	groups, _, _ := h.CloudWatchLogsOps.Backend.DescribeLogGroups("", "", 0)
	data := struct {
		PageData

		LogGroups []cwlogsbackend.LogGroup
	}{
		PageData:  PageData{Title: "CloudWatch Logs", ActiveTab: "cloudwatchlogs"},
		LogGroups: groups,
	}

	h.renderTemplate(c.Response(), "cloudwatchlogs/index.html", data)

	return nil
}

func (h *DashboardHandler) cloudWatchLogsGroupDetail(c *echo.Context) error {
	if h.CloudWatchLogsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	groupName := c.Request().URL.Query().Get("name")

	streams, _, _ := h.CloudWatchLogsOps.Backend.DescribeLogStreams(groupName, "", "", 0)
	data := struct {
		PageData

		GroupName string
		Streams   []cwlogsbackend.LogStream
	}{
		PageData:  PageData{Title: "Log Group: " + groupName, ActiveTab: "cloudwatchlogs"},
		GroupName: groupName,
		Streams:   streams,
	}

	h.renderTemplate(c.Response(), "cloudwatchlogs/group_detail.html", data)

	return nil
}
