package dashboard

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/labstack/echo/v5"
)

// dynamodbStreamsStreamView is the view model for a single DynamoDB stream.
type dynamodbStreamsStreamView struct {
	TableName   string
	StreamARN   string
	StreamLabel string
}

// dynamodbStreamsIndexData is the template data for the DynamoDB Streams index page.
type dynamodbStreamsIndexData struct {
	PageData

	Streams []dynamodbStreamsStreamView
}

// dynamodbStreamsSnippet returns the shared SnippetData for the DynamoDB Streams dashboard.
func dynamodbStreamsSnippet() *SnippetData {
	return &SnippetData{
		ID:    "dynamodbstreams-operations",
		Title: "Using DynamoDB Streams",
		Cli:   `aws dynamodbstreams list-streams --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for DynamoDB Streams
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
client := dynamodbstreams.NewFromConfig(cfg)
out, err := client.ListStreams(context.TODO(), &dynamodbstreams.ListStreamsInput{})`,
		Python: `# Initialize boto3 client for DynamoDB Streams
import boto3

client = boto3.client('dynamodbstreams', endpoint_url='http://localhost:8000')
response = client.list_streams()`,
	}
}

// dynamodbStreamsIndex renders the DynamoDB Streams dashboard index.
func (h *DashboardHandler) dynamodbStreamsIndex(c *echo.Context) error {
	w := c.Response()

	if h.DynamoDBStreamsOps == nil || h.DynamoDBStreamsOps.Streams == nil {
		h.renderTemplate(w, "dynamodbstreams/index.html", dynamodbStreamsIndexData{
			PageData: PageData{
				Title:     "DynamoDB Streams",
				ActiveTab: "dynamodbstreams",
				Snippet:   dynamodbStreamsSnippet(),
			},
			Streams: []dynamodbStreamsStreamView{},
		})

		return nil
	}

	out, err := h.DynamoDBStreamsOps.Streams.ListStreams(c.Request().Context(), &dynamodbstreams.ListStreamsInput{})
	if err != nil {
		h.Logger.Error("failed to list streams", "error", err)
		h.renderTemplate(w, "dynamodbstreams/index.html", dynamodbStreamsIndexData{
			PageData: PageData{
				Title:     "DynamoDB Streams",
				ActiveTab: "dynamodbstreams",
				Snippet:   dynamodbStreamsSnippet(),
			},
			Streams: []dynamodbStreamsStreamView{},
		})

		return nil
	}

	views := make([]dynamodbStreamsStreamView, 0, len(out.Streams))

	for _, s := range out.Streams {
		tableName := ""
		if s.TableName != nil {
			tableName = *s.TableName
		}

		streamARN := ""
		if s.StreamArn != nil {
			streamARN = *s.StreamArn
		}

		streamLabel := ""
		if s.StreamLabel != nil {
			streamLabel = *s.StreamLabel
		}

		views = append(views, dynamodbStreamsStreamView{
			TableName:   tableName,
			StreamARN:   streamARN,
			StreamLabel: streamLabel,
		})
	}

	h.renderTemplate(w, "dynamodbstreams/index.html", dynamodbStreamsIndexData{
		PageData: PageData{Title: "DynamoDB Streams", ActiveTab: "dynamodbstreams", Snippet: dynamodbStreamsSnippet()},
		Streams:  views,
	})

	return nil
}

// setupDynamoDBStreamsRoutes registers routes for the DynamoDB Streams dashboard.
func (h *DashboardHandler) setupDynamoDBStreamsRoutes() {
	h.SubRouter.GET("/dashboard/dynamodbstreams", h.dynamodbStreamsIndex)
}
