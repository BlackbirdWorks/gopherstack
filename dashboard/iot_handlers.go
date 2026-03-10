package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
)

// iotIndexData is the template data for the IoT dashboard page.
type iotIndexData struct {
	PageData

	Things []iotThingView
	Rules  []iotRuleView
}

// iotThingView is the view model for a single IoT Thing.
type iotThingView struct {
	Name      string
	ARN       string
	ThingType string
	CreatedAt string
}

// iotRuleView is the view model for a single IoT Topic Rule.
type iotRuleView struct {
	RuleName  string
	SQL       string
	CreatedAt string
	Enabled   bool
}

// iotIndex renders the IoT dashboard page.
func (h *DashboardHandler) iotIndex(c *echo.Context) error {
	w := c.Response()

	if h.IoTOps == nil {
		h.renderTemplate(w, "iot/index.html", iotIndexData{
			PageData: PageData{
				Title:     "IoT Core",
				ActiveTab: "iot",
				Snippet:   iotSnippetData(),
			},
			Things: []iotThingView{},
			Rules:  []iotRuleView{},
		})

		return nil
	}

	things := h.IoTOps.Backend.ListThings()
	rules := h.IoTOps.Backend.ListTopicRules()

	thingViews := make([]iotThingView, 0, len(things))

	for _, t := range things {
		thingViews = append(thingViews, iotThingView{
			Name:      t.ThingName,
			ARN:       t.ARN,
			ThingType: t.ThingType,
			CreatedAt: t.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}

	ruleViews := make([]iotRuleView, 0, len(rules))

	for _, r := range rules {
		ruleViews = append(ruleViews, iotRuleView{
			RuleName:  r.RuleName,
			SQL:       r.SQL,
			Enabled:   r.Enabled,
			CreatedAt: r.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}

	h.renderTemplate(w, "iot/index.html", iotIndexData{
		PageData: PageData{
			Title:     "IoT Core",
			ActiveTab: "iot",
			Snippet:   iotSnippetData(),
		},
		Things: thingViews,
		Rules:  ruleViews,
	})

	return nil
}

// iotCreateThing handles POST /dashboard/iot/thing/create.
func (h *DashboardHandler) iotCreateThing(c *echo.Context) error {
	if h.IoTOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.IoTOps.Backend.CreateThing(&iotbackend.CreateThingInput{ThingName: name}); err != nil {
		h.Logger.Error("failed to create IoT thing", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/iot")
}

// iotDeleteThing handles POST /dashboard/iot/thing/delete.
func (h *DashboardHandler) iotDeleteThing(c *echo.Context) error {
	if h.IoTOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.IoTOps.Backend.DeleteThing(name); err != nil {
		h.Logger.Error("failed to delete IoT thing", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/iot")
}

func iotSnippetData() *SnippetData {
	return &SnippetData{
		ID:    "iot-operations",
		Title: "Using IoT Core",
		Cli:   `aws iot list-things --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for IoT
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
)
if err != nil {
    log.Fatal(err)
}
client := iot.NewFromConfig(cfg, func(o *iot.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for IoT
import boto3

client = boto3.client('iot', endpoint_url='http://localhost:8000')`,
	}
}
