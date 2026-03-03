package dashboard

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
)

// maxReceiveMessages is the maximum number of messages to receive in a dashboard peek.
const maxReceiveMessages = 10

// sqsQueueView is a view model for the SQS dashboard queue list.
type sqsQueueView struct {
	Name             string
	URL              string
	MessageCount     string
	InFlightMessages string
	IsFIFO           bool
}

// sqsIndexData is the template data for the SQS index page.
type sqsIndexData struct {
	PageData

	Queues []sqsQueueView
}

// sqsQueueDetailData is the template data for the SQS queue detail page.
type sqsQueueDetailData struct {
	PageData

	Attributes map[string]string
	QueueURL   string
}

// sqsIndex renders the list of all SQS queues.
func (h *DashboardHandler) sqsIndex(c *echo.Context) error {
	w := c.Response()

	if h.SQSOps == nil {
		h.renderTemplate(w, "sqs/index.html", sqsIndexData{
			PageData: PageData{Title: "SQS Queues", ActiveTab: "sqs",
				Snippet: &SnippetData{
					ID:    "sqs-operations",
					Title: "Using Sqs",
					Cli:   `aws sqs help --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Using Sqs
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := sqs.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Using Sqs
import boto3

client = boto3.client('sqs', endpoint_url='http://localhost:8000')`,
				}},
			Queues: []sqsQueueView{},
		})

		return nil
	}

	queues := h.SQSOps.Backend.ListAll()
	views := make([]sqsQueueView, 0, len(queues))

	for _, q := range queues {
		var msgCount, inFlightMessages string

		out, err := h.SQSOps.Backend.GetQueueAttributes(&sqsbackend.GetQueueAttributesInput{
			QueueURL: q.URL,
			AttributeNames: []string{
				sqsbackend.AttrApproxMessages,
				sqsbackend.AttrApproxMessagesNotVisible,
			},
		})
		if err == nil {
			msgCount = out.Attributes[sqsbackend.AttrApproxMessages]
			inFlightMessages = out.Attributes[sqsbackend.AttrApproxMessagesNotVisible]
		}

		views = append(views, sqsQueueView{
			Name:             q.Name,
			URL:              q.URL,
			IsFIFO:           q.IsFIFO,
			MessageCount:     msgCount,
			InFlightMessages: inFlightMessages,
		})
	}

	h.renderTemplate(w, "sqs/index.html", sqsIndexData{
		PageData: PageData{Title: "SQS Queues", ActiveTab: "sqs",
			Snippet: &SnippetData{
				ID:    "sqs-operations",
				Title: "Using Sqs",
				Cli:   `aws sqs help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Sqs
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := sqs.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Sqs
import boto3

client = boto3.client('sqs', endpoint_url='http://localhost:8000')`,
			}},
		Queues: views,
	})

	return nil
}

// sqsCreateQueueModal renders the create-queue modal form.
func (h *DashboardHandler) sqsCreateQueueModal(c *echo.Context) error {
	w := c.Response()
	h.renderFragment(w, "sqs/create_modal.html", struct {
		PageData
	}{
		PageData: PageData{Title: "Create Queue", ActiveTab: "sqs",
			Snippet: &SnippetData{
				ID:    "sqs-operations",
				Title: "Using Sqs",
				Cli:   `aws sqs help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Sqs
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := sqs.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Sqs
import boto3

client = boto3.client('sqs', endpoint_url='http://localhost:8000')`,
			}},
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

// sqsMessagesData is the template data for the SQS messages fragment.
type sqsMessagesData struct {
	PageData

	QueueURL string
	Messages []sqsMessageView
}

// sqsMessageView is a view model for a single SQS message.
type sqsMessageView struct {
	MessageID     string
	Body          string
	ReceiptHandle string
}

// sqsSendMessage handles sending a message to an SQS queue.
func (h *DashboardHandler) sqsSendMessage(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	queueURL := r.URL.Query().Get("url")
	if queueURL == "" {
		return c.String(http.StatusBadRequest, "Missing queue URL")
	}

	if parseErr := r.ParseForm(); parseErr != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	messageBody := r.FormValue("message_body")

	var delaySeconds int
	if d := r.FormValue("delay"); d != "" {
		if _, err := fmt.Sscanf(d, "%d", &delaySeconds); err != nil {
			delaySeconds = 0
		}
	}

	if h.SQSOps != nil {
		_, err := h.SQSOps.Backend.SendMessage(&sqsbackend.SendMessageInput{
			QueueURL:     queueURL,
			MessageBody:  messageBody,
			DelaySeconds: delaySeconds,
		})
		if err != nil {
			return c.String(http.StatusInternalServerError, "Failed to send message: "+err.Error())
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/sqs/queue?url="+queueURL)

	return c.NoContent(http.StatusOK)
}

// sqsReceiveMessages handles receiving messages from an SQS queue and returns an HTMX fragment.
func (h *DashboardHandler) sqsReceiveMessages(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	queueURL := r.URL.Query().Get("url")
	if queueURL == "" {
		return c.String(http.StatusBadRequest, "Missing queue URL")
	}

	data := sqsMessagesData{
		QueueURL: queueURL,
		Messages: []sqsMessageView{},
	}

	if h.SQSOps != nil {
		out, err := h.SQSOps.Backend.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
			QueueURL:            queueURL,
			MaxNumberOfMessages: maxReceiveMessages,
			WaitTimeSeconds:     0,
		})
		if err == nil {
			for _, m := range out.Messages {
				data.Messages = append(data.Messages, sqsMessageView{
					MessageID:     m.MessageID,
					Body:          m.Body,
					ReceiptHandle: m.ReceiptHandle,
				})
			}
		}
	}

	h.renderFragment(w, "messages_fragment", data)

	return nil
}
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

	h.renderTemplate(w, "sqs/queue_detail.html", sqsQueueDetailData{
		PageData: PageData{
			Title:     "Queue Detail",
			ActiveTab: "sqs",
			Snippet: &SnippetData{
				ID:    "sqs-operations",
				Title: "Using Sqs",
				Cli:   `aws sqs help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Sqs
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := sqs.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Sqs
import boto3

client = boto3.client('sqs', endpoint_url='http://localhost:8000')`,
			},
		},
		QueueURL:   queueURL,
		Attributes: out.Attributes,
	})

	return nil
}
