package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
)
// sqsQueueView is a view model for the SQS dashboard queue list.
type sqsQueueView struct {
	Name             string
	URL              string
	IsFIFO           bool
	MessageCount     string
	InFlightMessages string
}

// sqsIndex renders the list of all SQS queues.
func (h *DashboardHandler) sqsIndex(c *echo.Context) error {
	w := c.Response()

	if h.SQSOps == nil {
		h.renderTemplate(w, "sqs/index.html", sqsIndexData{
			PageData: PageData{Title: "SQS Queues", ActiveTab: "sqs"},
			Queues:   []sqsQueueView{},
		})

		return nil
	}

	queues := h.SQSOps.Backend.ListAll()
	views := make([]sqsQueueView, 0, len(queues))

	for _, q := range queues {
		views = append(views, sqsQueueView{
			Name:             q.Name,
			URL:              q.URL,
			IsFIFO:           q.IsFIFO,
			MessageCount:     q.Attributes["ApproximateNumberOfMessages"],
			InFlightMessages: q.Attributes["ApproximateNumberOfMessagesNotVisible"],
		})
	}

	h.renderTemplate(w, "sqs/index.html", sqsIndexData{
		PageData: PageData{Title: "SQS Queues", ActiveTab: "sqs"},
		Queues:   views,
	})

	return nil
}

// sqsIndexData is the template data for the SQS index page.
type sqsIndexData struct {
	PageData
	Queues []sqsQueueView
}

// sqsCreateQueueModal renders the create-queue modal form.
func (h *DashboardHandler) sqsCreateQueueModal(c *echo.Context) error {
	w := c.Response()
	h.renderFragment(w, "sqs/create_modal.html", struct {
		PageData
	}{
		PageData: PageData{Title: "Create Queue", ActiveTab: "sqs"},
	})

	return nil
}

// sqsCreateQueue handles queue creation form submissions.
func (h *DashboardHandler) sqsCreateQueue(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if parseErr := r.ParseForm(); parseErr != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	queueName := r.FormValue("queue_name")
	if queueName == "" {
		return c.String(http.StatusBadRequest, "Queue name is required")
	}

	if h.SQSOps != nil {
		_, err := h.SQSOps.Backend.CreateQueue(&sqsbackend.CreateQueueInput{
			QueueName: queueName,
			Attributes: map[string]string{
				"VisibilityTimeout": r.FormValue("visibility_timeout"),
			},
		})
		if err != nil {
			return c.String(http.StatusInternalServerError, "Failed to create queue: "+err.Error())
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/sqs")

	return c.NoContent(http.StatusOK)
}

// sqsDeleteQueue handles queue deletion.
func (h *DashboardHandler) sqsDeleteQueue(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	queueURL := r.URL.Query().Get("url")
	if queueURL == "" {
		return c.String(http.StatusBadRequest, "Missing queue URL")
	}

	if h.SQSOps != nil {
		if err := h.SQSOps.Backend.DeleteQueue(&sqsbackend.DeleteQueueInput{QueueURL: queueURL}); err != nil {
			return c.String(http.StatusInternalServerError, "Failed to delete queue")
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/sqs")

	return c.NoContent(http.StatusOK)
}

// sqsPurgeQueue handles purging all messages from a queue.
func (h *DashboardHandler) sqsPurgeQueue(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	queueURL := r.URL.Query().Get("url")
	if queueURL == "" {
		return c.String(http.StatusBadRequest, "Missing queue URL")
	}

	if h.SQSOps != nil {
		if err := h.SQSOps.Backend.PurgeQueue(&sqsbackend.PurgeQueueInput{QueueURL: queueURL}); err != nil {
			return c.String(http.StatusInternalServerError, "Failed to purge queue")
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/sqs")

	return c.NoContent(http.StatusOK)
}

// sqsQueueDetail renders the detail view for a specific queue.
func (h *DashboardHandler) sqsQueueDetail(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	queueURL := r.URL.Query().Get("url")
	if queueURL == "" {
		return c.String(http.StatusBadRequest, "Missing queue URL")
	}

	if h.SQSOps == nil {
		return c.String(http.StatusInternalServerError, "SQS not available")
	}

	out, err := h.SQSOps.Backend.GetQueueAttributes(&sqsbackend.GetQueueAttributesInput{
		QueueURL:       queueURL,
		AttributeNames: []string{"All"},
	})
	if err != nil {
		return c.String(http.StatusNotFound, "Queue not found")
	}

	h.renderTemplate(w, "sqs/queue_detail.html", struct {
		PageData
		QueueURL   string
		Attributes map[string]string
	}{
		PageData: PageData{
			Title:     "Queue Detail",
			ActiveTab: "sqs",
		},
		QueueURL:   queueURL,
		Attributes: out.Attributes,
	})

	return nil
}
