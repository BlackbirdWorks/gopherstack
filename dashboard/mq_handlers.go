package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// mqBrokerView is the view model for a single Amazon MQ broker row.
type mqBrokerView struct {
	BrokerID       string
	BrokerName     string
	BrokerState    string
	EngineType     string
	EngineVersion  string
	DeploymentMode string
}

// mqIndexData is the template data for the Amazon MQ dashboard page.
type mqIndexData struct {
	PageData

	Brokers []mqBrokerView
}

// mqSnippet returns the shared SnippetData for the Amazon MQ dashboard.
func mqSnippet() *SnippetData {
	return &SnippetData{
		ID:    "mq-operations",
		Title: "Using Amazon MQ",
		Cli:   `aws mq list-brokers --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Amazon MQ
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
)
if err != nil {
    log.Fatal(err)
}
client := mq.NewFromConfig(cfg, func(o *mq.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for Amazon MQ
import boto3

client = boto3.client('mq', endpoint_url='http://localhost:8000')`,
	}
}

// setupMQRoutes registers all Amazon MQ dashboard routes.
func (h *DashboardHandler) setupMQRoutes() {
	h.SubRouter.GET("/dashboard/mq", h.mqIndex)
	h.SubRouter.POST("/dashboard/mq/brokers/create", h.mqCreateBroker)
	h.SubRouter.POST("/dashboard/mq/brokers/delete", h.mqDeleteBroker)
}

// mqIndex renders the main Amazon MQ dashboard page.
func (h *DashboardHandler) mqIndex(c *echo.Context) error {
	w := c.Response()

	if h.MQOps == nil {
		h.renderTemplate(w, "mq/index.html", mqIndexData{
			PageData: PageData{
				Title:     "Amazon MQ Brokers",
				ActiveTab: "mq",
				Snippet:   mqSnippet(),
			},
			Brokers: []mqBrokerView{},
		})

		return nil
	}

	brokers := h.MQOps.Backend.ListBrokers()
	views := make([]mqBrokerView, 0, len(brokers))

	for _, br := range brokers {
		views = append(views, mqBrokerView{
			BrokerID:       br.BrokerID,
			BrokerName:     br.BrokerName,
			BrokerState:    br.BrokerState,
			EngineType:     br.EngineType,
			EngineVersion:  br.EngineVersion,
			DeploymentMode: br.DeploymentMode,
		})
	}

	h.renderTemplate(w, "mq/index.html", mqIndexData{
		PageData: PageData{
			Title:     "Amazon MQ Brokers",
			ActiveTab: "mq",
			Snippet:   mqSnippet(),
		},
		Brokers: views,
	})

	return nil
}

// mqCreateBroker handles POST /dashboard/mq/brokers/create.
func (h *DashboardHandler) mqCreateBroker(c *echo.Context) error {
	if h.MQOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	engineType := c.Request().FormValue("engineType")
	if engineType == "" {
		engineType = "ACTIVEMQ"
	}

	_, err := h.MQOps.Backend.CreateBroker(
		name,
		"SINGLE_INSTANCE",
		engineType,
		"",
		"mq.m5.large",
		false,
		true,
		nil,
		nil,
		nil,
		nil,
	)
	if err != nil {
		h.Logger.ErrorContext(c.Request().Context(), "mq: failed to create broker", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/mq")
}

// mqDeleteBroker handles POST /dashboard/mq/brokers/delete.
func (h *DashboardHandler) mqDeleteBroker(c *echo.Context) error {
	if h.MQOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	brokerID := c.Request().FormValue("brokerId")
	if brokerID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.MQOps.Backend.DeleteBroker(brokerID); err != nil {
		h.Logger.ErrorContext(c.Request().Context(), "mq: failed to delete broker", "brokerId", brokerID, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/mq")
}
