package dashboard

import "net/http"

// docIndex renders the documentation page.
func (h *DashboardHandler) docIndex(w http.ResponseWriter, _ *http.Request) {
	data := struct {
		PageData

		DynamoDBOps []string
		S3Ops       []string
	}{
		PageData: PageData{
			Title:     "API Documentation",
			ActiveTab: "docs",
		},
		DynamoDBOps: h.DDBOps.GetSupportedOperations(),
		S3Ops:       h.S3Ops.GetSupportedOperations(),
	}

	h.renderTemplate(w, "doc.html", data)
}
