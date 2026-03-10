package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// bedrockGuardrailView is the view model for a single Bedrock guardrail.
type bedrockGuardrailView struct {
	ID      string
	Name    string
	ARN     string
	Status  string
	Version string
}

// bedrockFoundationModelView is the view model for a foundation model.
type bedrockFoundationModelView struct {
	ModelID      string
	ModelName    string
	ProviderName string
	ModelArn     string
}

// bedrockProvisionedThroughputView is the view model for a provisioned model throughput.
type bedrockProvisionedThroughputView struct {
	Name       string
	ARN        string
	Status     string
	ModelUnits int32
}

// bedrockIndexData is the template data for the Bedrock dashboard index page.
type bedrockIndexData struct {
	PageData

	Guardrails             []bedrockGuardrailView
	FoundationModels       []bedrockFoundationModelView
	ProvisionedThroughputs []bedrockProvisionedThroughputView
}

// bedrockIndex renders the Bedrock dashboard index page.
func (h *DashboardHandler) bedrockIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "bedrock-operations",
		Title: "Using Amazon Bedrock",
		Cli: `aws bedrock list-guardrails \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Bedrock
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
client := bedrock.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Bedrock
import boto3

client = boto3.client('bedrock', endpoint_url='http://localhost:8000')`,
	}

	if h.BedrockOps == nil {
		h.renderTemplate(w, "bedrock/index.html", bedrockIndexData{
			PageData: PageData{
				Title:     "Bedrock",
				ActiveTab: "bedrock",
				Snippet:   snippet,
			},
			Guardrails:             []bedrockGuardrailView{},
			FoundationModels:       []bedrockFoundationModelView{},
			ProvisionedThroughputs: []bedrockProvisionedThroughputView{},
		})

		return nil
	}

	guardrails := h.BedrockOps.Backend.ListGuardrails()
	gViews := make([]bedrockGuardrailView, 0, len(guardrails))

	for _, g := range guardrails {
		gViews = append(gViews, bedrockGuardrailView{
			ID:      g.GuardrailID,
			Name:    g.Name,
			ARN:     g.Arn,
			Status:  g.Status,
			Version: g.Version,
		})
	}

	models := h.BedrockOps.Backend.ListFoundationModels()
	mViews := make([]bedrockFoundationModelView, 0, len(models))

	for _, m := range models {
		mViews = append(mViews, bedrockFoundationModelView{
			ModelID:      m.ModelID,
			ModelName:    m.ModelName,
			ProviderName: m.ProviderName,
			ModelArn:     m.ModelArn,
		})
	}

	pmts := h.BedrockOps.Backend.ListProvisionedModelThroughputs()
	pmtViews := make([]bedrockProvisionedThroughputView, 0, len(pmts))

	for _, pmt := range pmts {
		pmtViews = append(pmtViews, bedrockProvisionedThroughputView{
			Name:       pmt.ProvisionedModelName,
			ARN:        pmt.ProvisionedModelArn,
			Status:     pmt.Status,
			ModelUnits: pmt.ModelUnits,
		})
	}

	h.renderTemplate(w, "bedrock/index.html", bedrockIndexData{
		PageData: PageData{
			Title:     "Bedrock",
			ActiveTab: "bedrock",
			Snippet:   snippet,
		},
		Guardrails:             gViews,
		FoundationModels:       mViews,
		ProvisionedThroughputs: pmtViews,
	})

	return nil
}

// bedrockCreateGuardrail handles POST /dashboard/bedrock/guardrails/create.
func (h *DashboardHandler) bedrockCreateGuardrail(c *echo.Context) error {
	if h.BedrockOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	description := c.Request().FormValue("description")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.BedrockOps.Backend.CreateGuardrail(name, description, "", "", nil)
	if err != nil {
		h.Logger.Error("failed to create guardrail", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/bedrock")
}

// bedrockDeleteGuardrail handles POST /dashboard/bedrock/guardrails/delete.
func (h *DashboardHandler) bedrockDeleteGuardrail(c *echo.Context) error {
	if h.BedrockOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.BedrockOps.Backend.DeleteGuardrail(id); err != nil {
		h.Logger.Error("failed to delete guardrail", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/bedrock")
}

// bedrockDeleteProvisionedThroughput handles POST /dashboard/bedrock/provisioned-throughputs/delete.
func (h *DashboardHandler) bedrockDeleteProvisionedThroughput(c *echo.Context) error {
	if h.BedrockOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	arn := c.Request().FormValue("arn")
	if arn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.BedrockOps.Backend.DeleteProvisionedModelThroughput(arn); err != nil {
		h.Logger.Error("failed to delete provisioned model throughput", "arn", arn, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/bedrock")
}

// setupBedrockRoutes registers routes for the Bedrock dashboard.
func (h *DashboardHandler) setupBedrockRoutes() {
	h.SubRouter.GET("/dashboard/bedrock", h.bedrockIndex)
	h.SubRouter.POST("/dashboard/bedrock/guardrails/create", h.bedrockCreateGuardrail)
	h.SubRouter.POST("/dashboard/bedrock/guardrails/delete", h.bedrockDeleteGuardrail)
	h.SubRouter.POST("/dashboard/bedrock/provisioned-throughputs/delete", h.bedrockDeleteProvisionedThroughput)
}
