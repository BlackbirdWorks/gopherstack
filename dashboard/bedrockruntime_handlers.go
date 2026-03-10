package dashboard

import (
	"github.com/labstack/echo/v5"

	bedrockruntimebackend "github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

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

	snippet := &SnippetData{
		ID:    "bedrockruntime-operations",
		Title: "Using AWS Bedrock Runtime",
		Cli: `aws bedrock-runtime invoke-model \
    --model-id anthropic.claude-v2 \
    --body '{"prompt": "Hello, world!", "max_tokens_to_sample": 200}' \
    --endpoint-url http://localhost:8000 \
    output.json`,
		Go: `// Initialize AWS SDK v2 for Bedrock Runtime
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
client := bedrockruntime.NewFromConfig(cfg)

out, err := client.Converse(context.TODO(), &bedrockruntime.ConverseInput{
    ModelId: aws.String("anthropic.claude-3-sonnet-20240229-v1:0"),
    Messages: []types.Message{
        {Role: types.ConversationRoleUser, Content: []types.ContentBlock{
            &types.ContentBlockMemberText{Value: types.TextBlock{Text: aws.String("Hello!")}},
        }},
    },
})`,
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

	invocations := h.BedrockRuntimeOps.Backend.ListInvocations()
	invViews := make([]bedrockruntimeInvocationView, 0, len(invocations))

	for _, inv := range invocations {
		inputPreview := inv.Input
		if len(inputPreview) > 200 {
			inputPreview = inputPreview[:200] + "..."
		}

		outputPreview := inv.Output
		if len(outputPreview) > 200 {
			outputPreview = outputPreview[:200] + "..."
		}

		invViews = append(invViews, bedrockruntimeInvocationView{
			Operation: inv.Operation,
			ModelID:   inv.ModelID,
			Input:     inputPreview,
			Output:    outputPreview,
			CreatedAt: inv.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}

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

// setupBedrockRuntimeRoutes registers routes for the Bedrock Runtime dashboard.
func (h *DashboardHandler) setupBedrockRuntimeRoutes() {
	h.SubRouter.GET("/dashboard/bedrockruntime", h.bedrockruntimeIndex)
}

// bedrockRuntimeDemoData seeds the Bedrock Runtime backend with demo invocations.
func bedrockRuntimeDemoData(bk *bedrockruntimebackend.InMemoryBackend) {
	bk.RecordInvocation("InvokeModel", "anthropic.claude-v2",
		`{"prompt": "Human: What is the capital of France?\n\nAssistant:"}`,
		`{"completion": " Paris is the capital of France.", "stop_reason": "end_turn"}`,
	)
	bk.RecordInvocation("Converse", "anthropic.claude-3-sonnet-20240229-v1:0",
		`{"messages": [{"role": "user", "content": [{"text": "Hello!"}]}]}`,
		`{"output": {"message": {"role": "assistant", "content": [{"text": "Hello! How can I help you today?"}]}}, "stopReason": "end_turn"}`,
	)
}
