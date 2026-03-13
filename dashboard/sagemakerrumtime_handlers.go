package dashboard

import (
	"github.com/labstack/echo/v5"

	sagemakerruntimebackend "github.com/blackbirdworks/gopherstack/services/sagemakerrumtime"
)

// sagemakerruntimeInvocationView is the view model for a single SageMaker Runtime invocation.
type sagemakerruntimeInvocationView struct {
	Operation    string
	EndpointName string
	Input        string
	Output       string
	CreatedAt    string
}

// sagemakerruntimeIndexData is the template data for the SageMaker Runtime dashboard index page.
type sagemakerruntimeIndexData struct {
	PageData

	Invocations []sagemakerruntimeInvocationView
}

// sagemakerruntimeSnippet returns the code snippet for the SageMaker Runtime dashboard.
func sagemakerruntimeSnippet() *SnippetData {
	return &SnippetData{
		ID:    "sagemakerrumtime-operations",
		Title: "Using AWS SageMaker Runtime",
		Cli: `aws sagemaker-runtime invoke-endpoint \
    --endpoint-name my-endpoint \
    --body '{"data": "test input"}' \
    --content-type application/json \
    --endpoint-url http://localhost:8000 \
    output.json`,
		Go: `package main

import (
	"context"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sagemakerrumtime"
)

func main() {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint("http://localhost:8000"),
	)
	if err != nil {
		log.Fatal(err)
	}

	client := sagemakerrumtime.NewFromConfig(cfg)

	out, err := client.InvokeEndpoint(context.TODO(), &sagemakerrumtime.InvokeEndpointInput{
		EndpointName: aws.String("my-endpoint"),
		Body:         []byte(` + "`" + `{"data": "test input"}` + "`" + `),
		ContentType:  aws.String("application/json"),
	})
	if err != nil {
		log.Fatal(err)
	}

	body, err := io.ReadAll(out.Body)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Response: %s\n", body)
}`,
		Python: `# Initialize boto3 client for SageMaker Runtime
import boto3

client = boto3.client('sagemaker-runtime', endpoint_url='http://localhost:8000')

response = client.invoke_endpoint(
    EndpointName='my-endpoint',
    Body=b'{"data": "test input"}',
    ContentType='application/json'
)

print(response['Body'].read())`,
	}
}

// sagemakerruntimeIndex renders the SageMaker Runtime dashboard index page.
func (h *DashboardHandler) sagemakerruntimeIndex(c *echo.Context) error {
	w := c.Response()
	snippet := sagemakerruntimeSnippet()

	if h.SageMakerRuntimeOps == nil {
		h.renderTemplate(w, "sagemakerrumtime/index.html", sagemakerruntimeIndexData{
			PageData: PageData{
				Title:     "SageMaker Runtime",
				ActiveTab: "sagemakerrumtime",
				Snippet:   snippet,
			},
			Invocations: []sagemakerruntimeInvocationView{},
		})

		return nil
	}

	invViews := sagemakerruntimeInvocationViews(h.SageMakerRuntimeOps.Backend.ListInvocations())

	h.renderTemplate(w, "sagemakerrumtime/index.html", sagemakerruntimeIndexData{
		PageData: PageData{
			Title:     "SageMaker Runtime",
			ActiveTab: "sagemakerrumtime",
			Snippet:   snippet,
		},
		Invocations: invViews,
	})

	return nil
}

// sagemakerruntimeInvocationViews converts a slice of Invocation records to view models.
func sagemakerruntimeInvocationViews(
	invocations []*sagemakerruntimebackend.Invocation,
) []sagemakerruntimeInvocationView {
	views := make([]sagemakerruntimeInvocationView, 0, len(invocations))

	for _, inv := range invocations {
		inputPreview := inv.Input
		if len(inputPreview) > invocationPreviewLen {
			inputPreview = inputPreview[:invocationPreviewLen] + "..."
		}

		outputPreview := inv.Output
		if len(outputPreview) > invocationPreviewLen {
			outputPreview = outputPreview[:invocationPreviewLen] + "..."
		}

		views = append(views, sagemakerruntimeInvocationView{
			Operation:    inv.Operation,
			EndpointName: inv.EndpointName,
			Input:        inputPreview,
			Output:       outputPreview,
			CreatedAt:    inv.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}

	return views
}

// setupSageMakerRuntimeRoutes registers routes for the SageMaker Runtime dashboard.
func (h *DashboardHandler) setupSageMakerRuntimeRoutes() {
	h.SubRouter.GET("/dashboard/sagemakerrumtime", h.sagemakerruntimeIndex)
}
