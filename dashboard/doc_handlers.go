package dashboard

import "net/http"

// docPageData is the template data for the documentation page.
type docPageData struct {
	PageData

	DynamoDBOps       []string
	S3Ops             []string
	SSMOps            []string
	SQSOps            []string
	SNSOps            []string
	IAMOps            []string
	STSOps            []string
	KMSOps            []string
	SecretsManagerOps []string
}

// docIndex renders the documentation page.
func (h *DashboardHandler) docIndex(w http.ResponseWriter, _ *http.Request) {
	data := docPageData{
		PageData: PageData{
			Title:     "API Documentation",
			ActiveTab: "docs",
			Snippet: &SnippetData{
				ID:    "docs-operations",
				Title: "Using Docs",
				Cli:   "aws docs help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Docs */",
				Python: "# Write boto3 code for Docs\nimport boto3\n" +
					"client = boto3.client('docs', endpoint_url='http://localhost:8000')",
			},
		},
		DynamoDBOps:       h.DDBOps.GetSupportedOperations(),
		S3Ops:             h.S3Ops.GetSupportedOperations(),
		SSMOps:            h.SSMOps.GetSupportedOperations(),
		SQSOps:            h.sqsOps(),
		SNSOps:            h.snsOps(),
		IAMOps:            h.iamOps(),
		STSOps:            h.stsOps(),
		KMSOps:            h.kmsOps(),
		SecretsManagerOps: h.smOps(),
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

// snsOps returns the list of supported SNS operations, or nil if SNSOps is not configured.
func (h *DashboardHandler) snsOps() []string {
	if h.SNSOps == nil {
		return nil
	}

	return h.SNSOps.GetSupportedOperations()
}

// iamOps returns the list of supported IAM operations, or nil if IAMOps is not configured.
func (h *DashboardHandler) iamOps() []string {
	if h.IAMOps == nil {
		return nil
	}

	return h.IAMOps.GetSupportedOperations()
}

// stsOps returns the list of supported STS operations, or nil if STSOps is not configured.
func (h *DashboardHandler) stsOps() []string {
	if h.STSOps == nil {
		return nil
	}

	return h.STSOps.GetSupportedOperations()
}

// kmsOps returns the list of supported KMS operations, or nil if KMSOps is not configured.
func (h *DashboardHandler) kmsOps() []string {
	if h.KMSOps == nil {
		return nil
	}

	return h.KMSOps.GetSupportedOperations()
}

// smOps returns the list of supported Secrets Manager operations, or nil if SecretsManagerOps is not configured.
func (h *DashboardHandler) smOps() []string {
	if h.SecretsManagerOps == nil {
		return nil
	}

	return h.SecretsManagerOps.GetSupportedOperations()
}
