package dashboard

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/sns"
)

// snsIndex renders the list of all SNS topics.
func (h *DashboardHandler) snsIndex(c *echo.Context) error {
	w := c.Response()

	if h.SNSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	topics := h.SNSOps.Backend.ListAllTopics()

	data := struct {
		PageData

		Topics []any
	}{
		PageData: PageData{
			Title:     "SNS Topics",
			ActiveTab: "sns",
		},
		Topics: make([]any, 0),
	}

	for _, t := range topics {
		data.Topics = append(data.Topics, t)
	}

	h.renderTemplate(w, "sns/index.html", data)

	return nil
}

// snsCreateTopic handles creation of a new SNS topic from the dashboard.
func (h *DashboardHandler) snsCreateTopic(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.SNSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := r.ParseForm(); err != nil {
		h.Logger.Error("Failed to parse form", "error", err)

		return c.String(http.StatusBadRequest, "Invalid request")
	}

	name := r.FormValue("name")
	if name == "" {
		return c.String(http.StatusBadRequest, "Topic name is required")
	}

	_, err := h.SNSOps.Backend.CreateTopic(name, nil)
	if err != nil {
		h.Logger.Error("Failed to create SNS topic", "name", name, "error", err)

		if errors.Is(err, sns.ErrTopicAlreadyExists) {
			return c.String(http.StatusConflict, "Topic already exists: "+name)
		}

		return c.String(http.StatusInternalServerError, "Failed to create topic: "+err.Error())
	}

	w.Header().Set("Hx-Redirect", "/dashboard/sns")

	return c.NoContent(http.StatusOK)
}

// snsDeleteTopic handles deletion of an SNS topic from the dashboard.
func (h *DashboardHandler) snsDeleteTopic(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.SNSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	arn := r.URL.Query().Get("arn")
	if arn == "" {
		return c.String(http.StatusBadRequest, "Missing arn")
	}

	if err := h.SNSOps.Backend.DeleteTopic(arn); err != nil {
		h.Logger.Error("Failed to delete SNS topic", "arn", arn, "error", err)

		return c.String(http.StatusInternalServerError, "Failed to delete topic")
	}

	w.Header().Set("Hx-Redirect", "/dashboard/sns")

	return c.NoContent(http.StatusOK)
}

// snsTopicDetail renders the detail view for a single SNS topic including its subscriptions.
func (h *DashboardHandler) snsTopicDetail(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.SNSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	arn := r.URL.Query().Get("arn")
	if arn == "" {
		return c.String(http.StatusBadRequest, "Missing arn")
	}

	attrs, err := h.SNSOps.Backend.GetTopicAttributes(arn)
	if err != nil {
		h.Logger.Error("Failed to get SNS topic attributes", "arn", arn, "error", err)

		return c.String(http.StatusNotFound, "Topic not found")
	}

	subs, _, err := h.SNSOps.Backend.ListSubscriptionsByTopic(arn, "")
	if err != nil {
		h.Logger.Warn("Failed to list subscriptions for topic", "arn", arn, "error", err)
	}

	data := struct {
		PageData

		TopicArn      string
		Attributes    map[string]string
		Subscriptions []any
	}{
		PageData: PageData{
			Title:     "SNS Topic",
			ActiveTab: "sns",
		},
		TopicArn:      arn,
		Attributes:    attrs,
		Subscriptions: make([]any, 0),
	}

	for _, s := range subs {
		data.Subscriptions = append(data.Subscriptions, s)
	}

	h.renderTemplate(w, "sns/topic_detail.html", data)

	return nil
}
