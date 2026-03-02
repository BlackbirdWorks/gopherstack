package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	kinesisbackend "github.com/blackbirdworks/gopherstack/kinesis"
)

// kinesisStreamView is the view model for a single stream in the index listing.
type kinesisStreamView struct {
	Name       string
	ARN        string
	Status     string
	ShardCount int
}

// kinesisIndexData is the template data for the Kinesis index page.
type kinesisIndexData struct {
	PageData

	Streams []kinesisStreamView
}

// kinesisStreamDetailData is the template data for the Kinesis stream detail page.
type kinesisStreamDetailData struct {
	PageData

	StreamName string
	StreamARN  string
	Status     string
	Shards     []kinesisbackend.ShardDescription
}

// kinesisIndex renders the list of all Kinesis streams.
func (h *DashboardHandler) kinesisIndex(c *echo.Context) error {
	w := c.Response()

	if h.KinesisOps == nil {
		h.renderTemplate(w, "kinesis/index.html", kinesisIndexData{
			PageData: PageData{Title: "Kinesis Streams", ActiveTab: "kinesis",
		Snippet: &SnippetData{
			ID:    "kinesis-operations",
			Title: "Using Kinesis",
			Cli:   "aws kinesis help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Kinesis */",
			Python: "# Write boto3 code for Kinesis\nimport boto3\nclient = boto3.client('kinesis', endpoint_url='http://localhost:8000')",
		},},
			Streams:  []kinesisStreamView{},
		})

		return nil
	}

	all := h.KinesisOps.Backend.ListAll()
	views := make([]kinesisStreamView, 0, len(all))

	for _, s := range all {
		views = append(views, kinesisStreamView{
			Name:       s.Name,
			ARN:        s.ARN,
			Status:     s.Status,
			ShardCount: s.ShardCount,
		})
	}

	h.renderTemplate(w, "kinesis/index.html", kinesisIndexData{
		PageData: PageData{Title: "Kinesis Streams", ActiveTab: "kinesis",
		Snippet: &SnippetData{
			ID:    "kinesis-operations",
			Title: "Using Kinesis",
			Cli:   "aws kinesis help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Kinesis */",
			Python: `# Write boto3 code for Kinesis
import boto3
client = boto3.client('kinesis', endpoint_url='http://localhost:8000')`,
		},},
		Streams:  views,
	})

	return nil
}

// kinesisStreamDetail renders the detail page for a single Kinesis stream.
func (h *DashboardHandler) kinesisStreamDetail(c *echo.Context) error {
	w := c.Response()

	if h.KinesisOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	name := c.Request().URL.Query().Get("name")

	desc, err := h.KinesisOps.Backend.DescribeStream(&kinesisbackend.DescribeStreamInput{StreamName: name})
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	h.renderTemplate(w, "kinesis/stream_detail.html", kinesisStreamDetailData{
		PageData:   PageData{Title: "Stream: " + name, ActiveTab: "kinesis",
		Snippet: &SnippetData{
			ID:    "kinesis-operations",
			Title: "Using Kinesis",
			Cli:   "aws kinesis help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Kinesis */",
			Python: `# Write boto3 code for Kinesis
import boto3
client = boto3.client('kinesis', endpoint_url='http://localhost:8000')`,
		},},
		StreamName: desc.StreamName,
		StreamARN:  desc.StreamARN,
		Status:     desc.StreamStatus,
		Shards:     desc.Shards,
	})

	return nil
}

// kinesisCreateStream handles the POST /dashboard/kinesis/create form.
func (h *DashboardHandler) kinesisCreateStream(c *echo.Context) error {
	if h.KinesisOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	_ = c.Request().ParseForm()
	name := c.Request().FormValue("stream_name")

	if name == "" {
		return c.String(http.StatusBadRequest, "stream_name is required")
	}

	_ = h.KinesisOps.Backend.CreateStream(&kinesisbackend.CreateStreamInput{
		StreamName: name,
		ShardCount: 1,
	})

	return c.Redirect(http.StatusFound, "/dashboard/kinesis")
}

// kinesisDeleteStream handles the DELETE /dashboard/kinesis/delete request.
func (h *DashboardHandler) kinesisDeleteStream(c *echo.Context) error {
	if h.KinesisOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	name := c.Request().URL.Query().Get("name")

	_ = h.KinesisOps.Backend.DeleteStream(&kinesisbackend.DeleteStreamInput{StreamName: name})

	return c.Redirect(http.StatusFound, "/dashboard/kinesis")
}

// kinesisPutRecord handles the POST /dashboard/kinesis/record form.
func (h *DashboardHandler) kinesisPutRecord(c *echo.Context) error {
	if h.KinesisOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	_ = c.Request().ParseForm()
	streamName := c.Request().FormValue("stream_name")
	partitionKey := c.Request().FormValue("partition_key")
	data := c.Request().FormValue("data")

	if partitionKey == "" {
		partitionKey = "default"
	}

	_, _ = h.KinesisOps.Backend.PutRecord(&kinesisbackend.PutRecordInput{
		StreamName:   streamName,
		PartitionKey: partitionKey,
		Data:         []byte(data),
	})

	return c.Redirect(http.StatusFound, "/dashboard/kinesis/stream?name="+streamName)
}
