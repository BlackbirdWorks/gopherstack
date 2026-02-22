package dashboard

import "net/http"

// docIndex renders the documentation page.
func (h *DashboardHandler) docIndex(w http.ResponseWriter, _ *http.Request) {
	data := struct {
		PageData

		DynamoDBOps []string
		S3Ops       []string
		SSMOps      []string
		SQSOps      []string
	}{
		PageData: PageData{
			Title:     "API Documentation",
			ActiveTab: "docs",
		},
		DynamoDBOps: h.DDBOps.GetSupportedOperations(),
		S3Ops:       h.S3Ops.GetSupportedOperations(),
		SSMOps:      h.SSMOps.GetSupportedOperations(),
		SQSOps:      h.sqsOps(),
	}

	h.renderTemplate(w, "doc.html", data)
}

// sqsOps returns the list of supported SQS operations, or nil if SQSOps is not configured.
func (h *DashboardHandler) sqsOps() []string {
	if h.SQSOps == nil {
		return nil
	}

	return h.SQSOps.GetSupportedOperations()
}
