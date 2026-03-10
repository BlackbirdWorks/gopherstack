package dashboard

import (
	"github.com/labstack/echo/v5"

	pkgslogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// iotDataPlaneIndexData is the template data for the IoT Data Plane dashboard page.
type iotDataPlaneIndexData struct {
	PageData
}

// iotDataPlaneSnippet returns code snippet data for IoT Data Plane operations.
func (h *DashboardHandler) iotDataPlaneSnippet() *SnippetData {
	return &SnippetData{
		ID:    "iotdataplane-operations",
		Title: "Using IoT Data Plane",
		Cli:   `aws iot-data publish --topic "my/topic" --payload '{"message":"hello"}' --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for IoT Data Plane
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := iotdataplane.NewFromConfig(cfg, func(o *iotdataplane.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})

_, err = client.Publish(context.TODO(), &iotdataplane.PublishInput{
    Topic:   aws.String("my/topic"),
    Payload: []byte(` + "`" + `{"message":"hello"}` + "`" + `),
    Qos:     aws.Int32(0),
})`,
		Python: `# Initialize boto3 client for IoT Data Plane
import boto3

client = boto3.client(
    'iot-data',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
)

client.publish(
    topic='my/topic',
    payload=b'{"message": "hello"}',
    qos=0,
)`,
	}
}

// iotDataPlaneIndex handles GET /dashboard/iotdataplane.
func (h *DashboardHandler) iotDataPlaneIndex(c *echo.Context) error {
	w := c.Response()
	ctx := c.Request().Context()

	if h.IoTDataPlaneOps == nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "IoT Data Plane handler not available")
	}

	h.renderTemplate(w, "iotdataplane/index.html", iotDataPlaneIndexData{
		PageData: PageData{
			Title:     "IoT Data Plane",
			ActiveTab: "iotdataplane",
			Snippet:   h.iotDataPlaneSnippet(),
		},
	})

	return nil
}
