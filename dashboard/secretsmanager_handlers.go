package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
)

// secretsManagerView is the view model for a single secret in the dashboard.
type secretsManagerView struct {
	DeletedDate  *float64
	Name         string
	Description  string
	SecretString string
}

// secretsManagerDetailData is the template data for the Secrets Manager secret detail page.
type secretsManagerDetailData struct {
	PageData

	DeletedDate        *float64
	VersionIDsToStages map[string][]string
	Name               string
	ARN                string
	Description        string
	SecretString       string
}

// secretsManagerIndex renders the list of all Secrets Manager secrets.
func (h *DashboardHandler) secretsManagerIndex(c *echo.Context) error {
	w := c.Response()

	data := struct {
		PageData

		Secrets []secretsManagerView
	}{
		PageData: PageData{
			Title:     "Secrets Manager",
			ActiveTab: "secretsmanager",
		Snippet: &SnippetData{
			ID:    "secretsmanager-operations",
			Title: "Using Secretsmanager",
			Cli:   "aws secretsmanager help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Secretsmanager */",
			Python: "# Write boto3 code for Secretsmanager\nimport boto3\nclient = boto3.client('secretsmanager', endpoint_url='http://localhost:8000')",
		},
		},
		Secrets: make([]secretsManagerView, 0),
	}

	if h.SecretsManagerOps != nil {
		out, err := h.SecretsManagerOps.Backend.ListSecrets(&secretsmanagerbackend.ListSecretsInput{})
		if err == nil {
			for _, s := range out.SecretList {
				view := secretsManagerView{
					Name:        s.Name,
					Description: s.Description,
					DeletedDate: s.DeletedDate,
				}
				// Fetch the current secret value
				val, getErr := h.SecretsManagerOps.Backend.GetSecretValue(&secretsmanagerbackend.GetSecretValueInput{
					SecretID: s.Name,
				})
				if getErr == nil {
					view.SecretString = val.SecretString
				}
				data.Secrets = append(data.Secrets, view)
			}
		}
	}

	h.renderTemplate(w, "secretsmanager/index.html", data)

	return nil
}

// secretsManagerCreate handles creating a new secret.
func (h *DashboardHandler) secretsManagerCreate(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.SecretsManagerOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	name := r.FormValue("name")
	if name == "" {
		return c.String(http.StatusBadRequest, "Secret name is required")
	}

	_, err := h.SecretsManagerOps.Backend.CreateSecret(&secretsmanagerbackend.CreateSecretInput{
		Name:         name,
		Description:  r.FormValue("description"),
		SecretString: r.FormValue("secret_string"),
	})
	if err != nil {
		h.Logger.Error("Failed to create secret", "name", name, "error", err)

		return c.String(http.StatusInternalServerError, "Failed to create secret: "+err.Error())
	}

	w.Header().Set("Hx-Redirect", "/dashboard/secretsmanager")

	return c.NoContent(http.StatusOK)
}

// secretsManagerUpdate handles updating a secret's value.
func (h *DashboardHandler) secretsManagerUpdate(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.SecretsManagerOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		return c.String(http.StatusBadRequest, "Missing name")
	}

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	_, err := h.SecretsManagerOps.Backend.PutSecretValue(&secretsmanagerbackend.PutSecretValueInput{
		SecretID:     name,
		SecretString: r.FormValue("secret_string"),
	})
	if err != nil {
		h.Logger.Error("Failed to update secret", "name", name, "error", err)

		return c.String(http.StatusInternalServerError, "Failed to update secret: "+err.Error())
	}

	// Redirect back to the detail page so the updated version history is visible.
	w.Header().Set("Hx-Redirect", "/dashboard/secretsmanager/secret?name="+name)

	return c.NoContent(http.StatusOK)
}

// secretsManagerDelete handles deleting a secret.
func (h *DashboardHandler) secretsManagerDelete(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.SecretsManagerOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		return c.String(http.StatusBadRequest, "Missing name")
	}

	_, err := h.SecretsManagerOps.Backend.DeleteSecret(&secretsmanagerbackend.DeleteSecretInput{
		SecretID:                   name,
		ForceDeleteWithoutRecovery: true,
	})
	if err != nil {
		h.Logger.Error("Failed to delete secret", "name", name, "error", err)

		return c.String(http.StatusInternalServerError, "Failed to delete secret: "+err.Error())
	}

	w.Header().Set("Hx-Redirect", "/dashboard/secretsmanager")

	return c.NoContent(http.StatusOK)
}

// secretsManagerDetail renders the detail view for a specific secret.
func (h *DashboardHandler) secretsManagerDetail(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if h.SecretsManagerOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		return c.String(http.StatusBadRequest, "Missing name")
	}

	descOut, err := h.SecretsManagerOps.Backend.DescribeSecret(&secretsmanagerbackend.DescribeSecretInput{
		SecretID: name,
	})
	if err != nil {
		h.Logger.Error("Failed to describe secret", "name", name, "error", err)

		return c.String(http.StatusNotFound, "Secret not found")
	}

	var secretString string

	valOut, err := h.SecretsManagerOps.Backend.GetSecretValue(&secretsmanagerbackend.GetSecretValueInput{
		SecretID: name,
	})
	if err == nil {
		secretString = valOut.SecretString
	}

	data := secretsManagerDetailData{
		PageData: PageData{
			Title:     "Secret Detail",
			ActiveTab: "secretsmanager",
		Snippet: &SnippetData{
			ID:    "secretsmanager-operations",
			Title: "Using Secretsmanager",
			Cli:   "aws secretsmanager help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Secretsmanager */",
			Python: `# Write boto3 code for Secretsmanager
import boto3
client = boto3.client('secretsmanager', endpoint_url='http://localhost:8000')`,
		},
		},
		Name:               descOut.Name,
		ARN:                descOut.ARN,
		Description:        descOut.Description,
		SecretString:       secretString,
		DeletedDate:        descOut.DeletedDate,
		VersionIDsToStages: descOut.VersionIDsToStages,
	}

	h.renderTemplate(w, "secretsmanager/secret_detail.html", data)

	return nil
}
