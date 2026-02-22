package dashboard

import (
	"github.com/labstack/echo/v5"

	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
)

// secretsManagerIndex renders the list of all Secrets Manager secrets.
func (h *DashboardHandler) secretsManagerIndex(c *echo.Context) error {
	w := c.Response()

	data := struct {
		PageData

		Secrets []any
	}{
		PageData: PageData{
			Title:     "Secrets Manager",
			ActiveTab: "secretsmanager",
		},
		Secrets: make([]any, 0),
	}

	if h.SecretsManagerOps != nil {
		out, err := h.SecretsManagerOps.Backend.ListSecrets(&secretsmanagerbackend.ListSecretsInput{})
		if err == nil {
			for _, s := range out.SecretList {
				data.Secrets = append(data.Secrets, s)
			}
		}
	}

	h.renderTemplate(w, "secretsmanager/index.html", data)

	return nil
}
