package dashboard

import (
	"github.com/labstack/echo/v5"

	bedrockruntimebackend "github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

// invocationPreviewLen is the maximum number of characters shown in invocation previews.
const invocationPreviewLen = 200

// bedrockruntimeInvocationView is the view model for a single Bedrock Runtime invocation.
type bedrockruntimeInvocationView struct {
	Operation string
	ModelID   string
	Input     string
	Output    string
	CreatedAt string
}

// bedrockruntimeIndexData is the template data for the Bedrock Runtime dashboard index page.
type bedrockruntimeIndexData struct {
	PageData

	Invocations []bedrockruntimeInvocationView
}

// bedrockruntimeIndex renders the Bedrock Runtime dashboard index page.
func (h *DashboardHandler) bedrockruntimeIndex(c *echo.Context) error {
	w := c.Response()
	snippet := bedrockruntimeSnippet()

	if h.BedrockRuntimeOps == nil {
		h.renderTemplate(w, "bedrockruntime/index.html", bedrockruntimeIndexData{
			PageData: PageData{
				Title:     "Bedrock Runtime",
				ActiveTab: "bedrockruntime",
				Snippet:   snippet,
			},
			Invocations: []bedrockruntimeInvocationView{},
		})

		return nil
	}

	invViews := bedrockruntimeInvocationViews(h.BedrockRuntimeOps.Backend.ListInvocations())

	h.renderTemplate(w, "bedrockruntime/index.html", bedrockruntimeIndexData{
		PageData: PageData{
			Title:     "Bedrock Runtime",
			ActiveTab: "bedrockruntime",
			Snippet:   snippet,
		},
		Invocations: invViews,
	})

	return nil
}

// bedrockruntimeSnippet returns the code snippet for the Bedrock Runtime dashboard.
func bedrockruntimeSnippet() *SnippetData {
	return &SnippetData{
		ID:    "bedrockruntime-operations",
		Title: "Using AWS Bedrock Runtime",
		Cli: `aws bedrock-runtime invoke-model \
    --model-id anthropic.claude-v2 \
    --body '{"prompt": "Hello, world!", "max_tokens_to_sample": 200}' \
    --endpoint-url http://localhost:8000 \
    output.json`,
		Go: `package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
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

	client := bedrockruntime.NewFromConfig(cfg)

	out, err := client.Converse(context.TODO(), &bedrockruntime.ConverseInput{
		ModelId: aws.String("anthropic.claude-3-sonnet-20240229-v1:0"),
		Messages: []types.Message{
			{
				Role: types.ConversationRoleUser,
				Content: []types.ContentBlock{
					&types.ContentBlockMemberText{Value: "Hello!"},
				},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Response: %#v\n", out)
}`,
		Python: `# Initialize boto3 client for Bedrock Runtime
import boto3, json

client = boto3.client('bedrock-runtime', endpoint_url='http://localhost:8000')

response = client.converse(
    modelId='anthropic.claude-3-sonnet-20240229-v1:0',
    messages=[
        {'role': 'user', 'content': [{'text': 'Hello!'}]}
    ]
)`,
	}
}

// bedrockruntimeInvocationViews converts a slice of Invocation records to view models.
func bedrockruntimeInvocationViews(
	invocations []*bedrockruntimebackend.Invocation,
) []bedrockruntimeInvocationView {
	views := make([]bedrockruntimeInvocationView, 0, len(invocations))

	for _, inv := range invocations {
		inputPreview := inv.Input
		if len(inputPreview) > invocationPreviewLen {
			inputPreview = inputPreview[:invocationPreviewLen] + "..."
		}

		outputPreview := inv.Output
		if len(outputPreview) > invocationPreviewLen {
			outputPreview = outputPreview[:invocationPreviewLen] + "..."
		}

		views = append(views, bedrockruntimeInvocationView{
			Operation: inv.Operation,
			ModelID:   inv.ModelID,
			Input:     inputPreview,
			Output:    outputPreview,
			CreatedAt: inv.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}

	return views
}

// setupBedrockRuntimeRoutes registers routes for the Bedrock Runtime dashboard.
func (h *DashboardHandler) setupBedrockRuntimeRoutes() {
	h.SubRouter.GET("/dashboard/bedrockruntime", h.bedrockruntimeIndex)
}
