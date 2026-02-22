package dashboard

import (
	"github.com/labstack/echo/v5"

	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
)

// kmsIndex renders the list of all KMS keys.
func (h *DashboardHandler) kmsIndex(c *echo.Context) error {
	w := c.Response()

	data := struct {
		PageData

		Keys []any
	}{
		PageData: PageData{
			Title:     "KMS Keys",
			ActiveTab: "kms",
		},
		Keys: make([]any, 0),
	}

	if h.KMSOps != nil {
		out, err := h.KMSOps.Backend.ListKeys(&kmsbackend.ListKeysInput{})
		if err == nil {
			for _, k := range out.Keys {
				data.Keys = append(data.Keys, k)
			}
		}
	}

	h.renderTemplate(w, "kms/index.html", data)

	return nil
}
