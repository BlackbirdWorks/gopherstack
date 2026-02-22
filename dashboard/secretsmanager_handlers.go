package dashboard

import (
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
