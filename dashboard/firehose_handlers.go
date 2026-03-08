package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// firehoseStreamView is the view model for a single Firehose delivery stream.
type firehoseStreamView struct {
	Name   string
	ARN    string
	Status string
}

// firehoseIndexData is the template data for the Firehose index page.
type firehoseIndexData struct {
	PageData

	Streams []firehoseStreamView
}

// firehoseIndex renders the Firehose dashboard index.
func (h *DashboardHandler) firehoseIndex(c *echo.Context) error {
	w := c.Response()

	if h.FirehoseOps == nil {
		h.renderTemplate(w, "firehose/index.html", firehoseIndexData{
			PageData: PageData{Title: "Firehose Delivery Streams", ActiveTab: "firehose",
				Snippet: &SnippetData{
					ID:    "firehose-operations",
					Title: "Using Firehose",
					Cli:   `aws firehose help --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Using Firehose
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
client := firehose.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Using Firehose
import boto3

client = boto3.client('firehose', endpoint_url='http://localhost:8000')`,
				}},
			Streams: []firehoseStreamView{},
		})

		return nil
	}

	names := h.FirehoseOps.Backend.ListDeliveryStreams()
	views := make([]firehoseStreamView, 0, len(names))

	for _, name := range names {
		s, err := h.FirehoseOps.Backend.DescribeDeliveryStream(name)
		if err != nil {
			continue
		}

		views = append(views, firehoseStreamView{
			Name:   s.Name,
			ARN:    s.ARN,
			Status: s.Status,
		})
	}

	h.renderTemplate(w, "firehose/index.html", firehoseIndexData{
		PageData: PageData{Title: "Firehose Delivery Streams", ActiveTab: "firehose",
			Snippet: &SnippetData{
				ID:    "firehose-operations",
				Title: "Using Firehose",
				Cli:   `aws firehose help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Firehose
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
client := firehose.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Firehose
import boto3

client = boto3.client('firehose', endpoint_url='http://localhost:8000')`,
			}},
		Streams: views,
	})

	return nil
}

// firehoseCreate handles POST /dashboard/firehose/create.
func (h *DashboardHandler) firehoseCreate(c *echo.Context) error {
	if h.FirehoseOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.FirehoseOps.Backend.CreateDeliveryStream(
		firehose.CreateDeliveryStreamInput{Name: name},
	); err != nil {
		h.Logger.Error("failed to create Firehose stream", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/firehose")
}

// firehoseDelete handles POST /dashboard/firehose/delete.
func (h *DashboardHandler) firehoseDelete(c *echo.Context) error {
	if h.FirehoseOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.FirehoseOps.Backend.DeleteDeliveryStream(name); err != nil {
		h.Logger.Error("failed to delete Firehose stream", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/firehose")
}
