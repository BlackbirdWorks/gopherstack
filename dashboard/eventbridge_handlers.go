package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	ebbackend "github.com/blackbirdworks/gopherstack/eventbridge"
)

// eventBridgeIndexData is the template data for the EventBridge buses list page.
type eventBridgeIndexData struct {
	PageData

	Buses []ebbackend.EventBus
}

// eventBridgeRulesData is the template data for the EventBridge rules page.
type eventBridgeRulesData struct {
	PageData

	BusName string
	Rules   []ebbackend.Rule
}

// eventBridgeEventLogData is the template data for the EventBridge event log page.
type eventBridgeEventLogData struct {
	PageData

	Events []ebbackend.EventLogEntry
}

func (h *DashboardHandler) eventBridgeIndex(c *echo.Context) error {
	if h.EventBridgeOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	buses, _, _ := h.EventBridgeOps.Backend.ListEventBuses("", "")
	data := eventBridgeIndexData{
		PageData: PageData{Title: "EventBridge", ActiveTab: "eventbridge",
			Snippet: &SnippetData{
				ID:    "eventbridge-operations",
				Title: "Using Eventbridge",
				Cli:   `aws eventbridge help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Eventbridge
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
client := eventbridge.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Eventbridge
import boto3

client = boto3.client('eventbridge', endpoint_url='http://localhost:8000')`,
			}},
		Buses: buses,
	}

	h.renderTemplate(c.Response(), "eventbridge/index.html", data)

	return nil
}

func (h *DashboardHandler) eventBridgeRules(c *echo.Context) error {
	if h.EventBridgeOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	busName := c.Request().URL.Query().Get("bus")
	if busName == "" {
		busName = "default"
	}

	rules, _, _ := h.EventBridgeOps.Backend.ListRules(busName, "", "")
	data := eventBridgeRulesData{
		PageData: PageData{Title: "EventBridge Rules", ActiveTab: "eventbridge",
			Snippet: &SnippetData{
				ID:    "eventbridge-operations",
				Title: "Using Eventbridge",
				Cli:   `aws eventbridge help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Eventbridge
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
client := eventbridge.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Eventbridge
import boto3

client = boto3.client('eventbridge', endpoint_url='http://localhost:8000')`,
			}},
		BusName: busName,
		Rules:   rules,
	}

	h.renderTemplate(c.Response(), "eventbridge/rules.html", data)

	return nil
}

func (h *DashboardHandler) eventBridgeEventLog(c *echo.Context) error {
	if h.EventBridgeOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	log := h.EventBridgeOps.Backend.GetEventLog()
	data := eventBridgeEventLogData{
		PageData: PageData{Title: "EventBridge Event Log", ActiveTab: "eventbridge",
			Snippet: &SnippetData{
				ID:    "eventbridge-operations",
				Title: "Using Eventbridge",
				Cli:   `aws eventbridge help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Eventbridge
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
client := eventbridge.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Eventbridge
import boto3

client = boto3.client('eventbridge', endpoint_url='http://localhost:8000')`,
			}},
		Events: log,
	}

	h.renderTemplate(c.Response(), "eventbridge/event_log.html", data)

	return nil
}
