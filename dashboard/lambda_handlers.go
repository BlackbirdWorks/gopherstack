package dashboard

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// lambdaIndexData is the template data for the Lambda functions list page.
type lambdaIndexData struct {
	PageData

	Functions []*lambdabackend.FunctionConfiguration
}

// lambdaFunctionDetailData is the template data for the Lambda function detail page.
type lambdaFunctionDetailData struct {
	Function *lambdabackend.FunctionConfiguration
	PageData //nolint:embeddedstructfieldcheck // fieldalignment places this after pointer fields
}

// lambdaIndex renders the Lambda function list page.
func (h *DashboardHandler) lambdaIndex(c *echo.Context) error {
	w := c.Response()

	var fns []*lambdabackend.FunctionConfiguration
	if h.LambdaOps != nil {
		fns = h.LambdaOps.Backend.ListFunctions("", 0).Data
	} else {
		fns = []*lambdabackend.FunctionConfiguration{}
	}

	data := lambdaIndexData{
		PageData: PageData{
			Title:     "Lambda Functions",
			ActiveTab: "lambda",
			Snippet: &SnippetData{
				ID:    "lambda-operations",
				Title: "Using Lambda",
				Cli:   `aws lambda help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Lambda
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
client := lambda.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Lambda
import boto3

client = boto3.client('lambda', endpoint_url='http://localhost:8000')`,
			},
		},
		Functions: fns,
	}

	h.renderTemplate(w, "lambda/index.html", data)

	return nil
}

// lambdaFunctionDetail renders the Lambda function detail page.
func (h *DashboardHandler) lambdaFunctionDetail(c *echo.Context) error {
	name := c.Request().URL.Query().Get("name")
	if name == "" {
		return c.Redirect(http.StatusFound, "/dashboard/lambda")
	}

	if h.LambdaOps == nil {
		return c.Redirect(http.StatusFound, "/dashboard/lambda")
	}

	fn, err := h.LambdaOps.Backend.GetFunction(name)
	if err != nil {
		return c.Redirect(http.StatusFound, "/dashboard/lambda")
	}

	data := lambdaFunctionDetailData{
		PageData: PageData{
			Title:     "Lambda Function",
			ActiveTab: "lambda",
			Snippet: &SnippetData{
				ID:    "lambda-operations",
				Title: "Using Lambda",
				Cli:   `aws lambda help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Lambda
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
client := lambda.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Lambda
import boto3

client = boto3.client('lambda', endpoint_url='http://localhost:8000')`,
			},
		},
		Function: fn,
	}

	h.renderTemplate(c.Response(), "lambda/function_detail.html", data)

	return nil
}

// lambdaInvoke handles the invoke action from the dashboard.
func (h *DashboardHandler) lambdaInvoke(c *echo.Context) error {
	r := c.Request()
	name := r.URL.Query().Get("name")

	if name == "" || h.LambdaOps == nil {
		return c.String(http.StatusBadRequest, "Missing function name")
	}

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid form")
	}

	payload := r.FormValue("payload")
	if payload == "" {
		payload = "{}"
	}

	if !json.Valid([]byte(payload)) {
		return c.String(http.StatusBadRequest, "Payload must be valid JSON")
	}

	result, httpStatus, err := h.LambdaOps.Backend.InvokeFunction(
		r.Context(), name, lambdabackend.InvocationTypeRequestResponse, []byte(payload),
	)
	if err != nil {
		return c.String(http.StatusInternalServerError, "Invoke failed: "+err.Error())
	}

	if len(result) == 0 {
		result = []byte(`{"status":"ok"}`)
	}

	// Pretty-print the result for display.
	var prettyBuf []byte
	if json.Valid(result) {
		prettyBuf, _ = json.MarshalIndent(json.RawMessage(result), "", "  ")
	} else {
		prettyBuf = result
	}

	return c.String(httpStatus, string(prettyBuf))
}
