package dashboard

import (
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/labstack/echo/v5"

	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
)

// ssmIndex renders the list of all parameters in the Parameter Store.
func (h *DashboardHandler) ssmIndex(c *echo.Context) error {
	w := c.Response()

	// Since our mock backend doesn't implement DescribeParameters yet, we will
	// just use a workaround: we know the local memory backend ListAll() could be exposed,
	// but via the SDK, it's safer to fetch what we can or wait for DescribeParameters.
	// For this mock extension, let's actually add the DescribeParameters to the mock.

	// For simplicity in this UI, we will just fetch the list directly from the handler via a
	// backchannel or cast until we implement DescribeParameters natively via SDK.

	params := h.SSMOps.Backend.ListAll()

	data := struct {
		PageData

		Parameters []any
	}{
		PageData: PageData{
			Title:     "SSM Parameter Store",
			ActiveTab: "ssm",
			Snippet: &SnippetData{
				ID:    "ssm-operations",
				Title: "Using Ssm",
				Cli:   "aws ssm help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Ssm */",
				Python: "# Write boto3 code for Ssm\nimport boto3\n" +
					"client = boto3.client('ssm', endpoint_url='http://localhost:8000')",
			},
		},
		Parameters: make([]any, 0),
	}

	for _, p := range params {
		data.Parameters = append(data.Parameters, p)
	}

	h.renderTemplate(w, "ssm/index.html", data)

	return nil
}

// ssmPutModal renders the modal for creating or editing a parameter.
func (h *DashboardHandler) ssmPutModal(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	name := r.URL.Query().Get("name")

	data := struct {
		Name        string
		Type        string
		Value       string
		Description string
		IsEdit      bool
	}{
		IsEdit: false,
		Type:   "String",
	}

	if name != "" {
		ctx := c.Request().Context()
		out, err := h.SSM.GetParameter(ctx, &ssm.GetParameterInput{
			Name:           aws.String(name),
			WithDecryption: aws.Bool(true),
		})

		if err == nil && out.Parameter != nil {
			data.IsEdit = true
			data.Name = *out.Parameter.Name
			data.Type = string(out.Parameter.Type)
			data.Value = *out.Parameter.Value
			// We skip description extraction for now unless it was added to SDK models
			// as it's typically fetched via DescribeParameters
		} else {
			h.Logger.Error("Failed to fetch parameter for edit", "name", name, "error", err)

			return c.String(http.StatusNotFound, "Parameter not found")
		}
	}

	h.renderFragment(w, "ssm/put_modal.html", data)

	return nil
}

// ssmPutParameter handles the form submission to create or update a parameter.
func (h *DashboardHandler) ssmPutParameter(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if err := r.ParseForm(); err != nil {
		h.Logger.Error("Failed to parse form", "error", err)

		return c.String(http.StatusBadRequest, "Invalid request")
	}

	name := r.FormValue("name")
	paramType := r.FormValue("type")
	value := r.FormValue("value")
	description := r.FormValue("description")
	overwrite := r.FormValue("overwrite") == "true"

	ctx := c.Request().Context()
	_, err := h.SSM.PutParameter(ctx, &ssm.PutParameterInput{
		Name:        aws.String(name),
		Type:        types.ParameterType(paramType),
		Value:       aws.String(value),
		Description: aws.String(description),
		Overwrite:   aws.Bool(overwrite),
	})

	if err != nil {
		h.Logger.Error("Failed to put parameter", "name", name, "error", err)
		// Rather than rendering an error, a good HTMX pattern is triggering an alert
		// but for simplicity we'll just return a bad request or an error header.
		h.Logger.Error("failed to create parameter", "error", err)

		return c.String(http.StatusInternalServerError, "Failed to save parameter: "+err.Error())
	}

	w.Header().Set("Hx-Redirect", "/dashboard/ssm")

	return c.NoContent(http.StatusOK)
}

// ssmDeleteParameter handles the deletion of a parameter.
func (h *DashboardHandler) ssmDeleteParameter(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	name := r.URL.Query().Get("name")
	if name == "" {
		return c.String(http.StatusBadRequest, "Missing name")
	}

	ctx := c.Request().Context()
	_, err := h.SSM.DeleteParameter(ctx, &ssm.DeleteParameterInput{
		Name: aws.String(name),
	})

	if err != nil {
		h.Logger.Error("failed to delete parameter", "error", err)

		return c.String(http.StatusInternalServerError, "Failed to delete parameter")
	}

	// Tell HTMX to reload the page to reflect the deletion
	w.Header().Set("Hx-Redirect", "/dashboard/ssm")

	return c.NoContent(http.StatusOK)
}

// ssmHistoryData holds the data for the parameter history page.
type ssmHistoryData struct {
	PageData

	Name    string
	History []ssmHistoryEntry
}

// ssmHistoryEntry is a single version entry in parameter history.
type ssmHistoryEntry struct {
	Value   string
	Type    string
	Version int64
}

// ssmParameterHistory handles GET /dashboard/ssm/history?name=<paramName>.
func (h *DashboardHandler) ssmParameterHistory(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	name := r.URL.Query().Get("name")
	if name == "" {
		return c.String(http.StatusBadRequest, "Missing name")
	}

	out, err := h.SSMOps.Backend.GetParameterHistory(&ssmbackend.GetParameterHistoryInput{Name: name})
	if err != nil {
		h.Logger.Error("failed to get parameter history", "name", name, "error", err)

		return c.String(http.StatusInternalServerError, "Failed to fetch parameter history")
	}

	entries := make([]ssmHistoryEntry, 0, len(out.Parameters))
	for _, p := range out.Parameters {
		entries = append(entries, ssmHistoryEntry{
			Version: p.Version,
			Value:   p.Value,
			Type:    p.Type,
		})
	}

	data := ssmHistoryData{
		PageData: PageData{
			Title:     "Parameter History: " + name,
			ActiveTab: "ssm",
			Snippet: &SnippetData{
				ID:    "ssm-operations",
				Title: "Using Ssm",
				Cli:   "aws ssm help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Ssm */",
				Python: `# Write boto3 code for Ssm
import boto3
client = boto3.client('ssm', endpoint_url='http://localhost:8000')`,
			},
		},
		Name:    name,
		History: entries,
	}

	h.renderTemplate(w, "ssm/history.html", data)

	return nil
}
