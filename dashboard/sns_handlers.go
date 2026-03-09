package dashboard

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

const (
	// arnResourceIndex is the index of the resource segment in an ARN split by ":".
	arnResourceIndex = 5
	// platformARNResourceParts is the number of slash-delimited segments in the
	// resource component of a platform application ARN: "app/{Platform}/{Name}".
	platformARNResourceParts = 3
)

// snsIndexData is the template data for the SNS topics list page.
type snsIndexData struct {
	PageData

	Topics               []any
	PlatformApplications []snsPlatformApplicationView
}

// snsPlatformApplicationView is the view model for a single SNS platform application.
type snsPlatformApplicationView struct {
	PlatformApplicationArn string
	Platform               string
	Name                   string
}

// snsTopicDetailData is the template data for the SNS topic detail page.
type snsTopicDetailData struct {
	PageData

	TopicArn      string
	Attributes    map[string]string
	Subscriptions []any
}

// snsSubscribeToTopic handles subscribing to an SNS topic.
func (h *DashboardHandler) snsSubscribeToTopic(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.SNSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	arn := r.URL.Query().Get("arn")
	if arn == "" {
		return c.String(http.StatusBadRequest, "Missing arn")
	}

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	protocol := r.FormValue("protocol")
	endpoint := r.FormValue("endpoint")

	if _, err := h.SNSOps.Backend.Subscribe(arn, protocol, endpoint, ""); err != nil {
		h.Logger.Error("Failed to subscribe to SNS topic", "arn", arn, "error", err)

		return c.String(http.StatusInternalServerError, "Failed to subscribe: "+err.Error())
	}

	w.Header().Set("Hx-Redirect", "/dashboard/sns/topic?arn="+arn)

	return c.NoContent(http.StatusOK)
}

// snsUnsubscribeFromTopic handles unsubscribing from an SNS topic.
func (h *DashboardHandler) snsUnsubscribeFromTopic(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.SNSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	subArn := r.URL.Query().Get("sub")
	topicArn := r.URL.Query().Get("arn")

	if subArn == "" {
		return c.String(http.StatusBadRequest, "Missing sub")
	}

	if err := h.SNSOps.Backend.Unsubscribe(subArn); err != nil {
		h.Logger.Error("Failed to unsubscribe from SNS topic", "subArn", subArn, "error", err)

		return c.String(http.StatusInternalServerError, "Failed to unsubscribe: "+err.Error())
	}

	w.Header().Set("Hx-Redirect", "/dashboard/sns/topic?arn="+topicArn)

	return c.NoContent(http.StatusOK)
}

// snsPublishMessage handles publishing a message to an SNS topic.
func (h *DashboardHandler) snsPublishMessage(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.SNSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	arn := r.URL.Query().Get("arn")
	if arn == "" {
		return c.String(http.StatusBadRequest, "Missing arn")
	}

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	message := r.FormValue("message")
	subject := r.FormValue("subject")

	if _, err := h.SNSOps.Backend.Publish(arn, message, subject, "", nil); err != nil {
		h.Logger.Error("Failed to publish SNS message", "arn", arn, "error", err)

		return c.String(http.StatusInternalServerError, "Failed to publish: "+err.Error())
	}

	w.Header().Set("Hx-Redirect", "/dashboard/sns/topic?arn="+arn)

	return c.NoContent(http.StatusOK)
}

// snsIndex renders the list of all SNS topics.
func (h *DashboardHandler) snsIndex(c *echo.Context) error {
	w := c.Response()

	if h.SNSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	topics := h.SNSOps.Backend.ListAllTopics()
	apps := h.SNSOps.Backend.ListAllPlatformApplications()

	data := snsIndexData{
		PageData: PageData{
			Title:     "SNS Topics",
			ActiveTab: "sns",
			Snippet: &SnippetData{
				ID:    "sns-operations",
				Title: "Using Sns",
				Cli:   `aws sns help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Sns
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
client := sns.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Sns
import boto3

client = boto3.client('sns', endpoint_url='http://localhost:8000')`,
			},
		},
		Topics:               make([]any, 0),
		PlatformApplications: make([]snsPlatformApplicationView, 0, len(apps)),
	}

	for _, t := range topics {
		data.Topics = append(data.Topics, t)
	}

	for _, app := range apps {
		platform, name := parsePlatformARN(app.PlatformApplicationArn)
		data.PlatformApplications = append(data.PlatformApplications, snsPlatformApplicationView{
			PlatformApplicationArn: app.PlatformApplicationArn,
			Platform:               platform,
			Name:                   name,
		})
	}

	h.renderTemplate(w, "sns/index.html", data)

	return nil
}

// parsePlatformARN extracts the platform type and application name from a platform
// application ARN in the form "arn:aws:sns:{region}:{account}:app/{Platform}/{Name}".
// Returns empty strings if the ARN does not match the expected format.
func parsePlatformARN(arnStr string) (string, string) {
	parts := strings.SplitN(arnStr, ":", arnResourceIndex+1)
	if len(parts) <= arnResourceIndex {
		return "", ""
	}

	segments := strings.SplitN(parts[arnResourceIndex], "/", platformARNResourceParts)
	if len(segments) != platformARNResourceParts {
		return "", ""
	}

	return segments[1], segments[2]
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

	data := snsTopicDetailData{
		PageData: PageData{
			Title:     "SNS Topic",
			ActiveTab: "sns",
			Snippet: &SnippetData{
				ID:    "sns-operations",
				Title: "Using Sns",
				Cli:   `aws sns help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Sns
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
client := sns.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Sns
import boto3

client = boto3.client('sns', endpoint_url='http://localhost:8000')`,
			},
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
