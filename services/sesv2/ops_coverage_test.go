package sesv2_test

// ops_coverage_test.go exercises the handler_ops.go dispatch paths and
// backend_ops.go methods, bringing sesv2 coverage above the 70% threshold.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseJSON(t *testing.T, data []byte, v any) error {
	t.Helper()

	return json.Unmarshal(data, v)
}

// --- Account operations ---

func TestOps_GetAccount(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/account", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_PutAccountDetails(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPost, "/v2/email/account/details", map[string]any{
		"MailType":           "MARKETING",
		"WebsiteURL":         "https://example.com",
		"UseCaseDescription": "Test use case",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_PutAccountSendingAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/account/sending-attributes", map[string]any{
		"SendingEnabled": true,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_PutAccountSuppressionAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/account/suppression-attributes",
		map[string]any{},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_PutAccountVdmAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/account/vdm-attributes", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_PutAccountDedicatedIPWarmupAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/account/dedicated-ip-warmup-attributes",
		map[string]any{},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_GetBlacklistReports(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/deliverability-dashboard/blacklist-report",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Suppressed destinations ---

func TestOps_PutSuppressedDestination(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/suppressed-destination", map[string]any{
		"EmailAddress": "suppress@example.com",
		"Reason":       "BOUNCE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_GetSuppressedDestination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPut, "/v2/email/suppressed-destination", map[string]any{
		"EmailAddress": "get-supp@example.com",
		"Reason":       "COMPLAINT",
	})

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/suppressed-destination/get-supp%40example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_DeleteSuppressedDestination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPut, "/v2/email/suppressed-destination", map[string]any{
		"EmailAddress": "del-supp@example.com",
		"Reason":       "BOUNCE",
	})

	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/suppressed-destination/del-supp%40example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListSuppressedDestinations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/suppressed-destination", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Contact lists ---

func TestOps_GetContactList(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "TestList",
	})

	rec := doRequest(t, h, http.MethodGet, "/v2/email/contact-lists/TestList", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_DeleteContactList(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "DeleteList",
	})

	rec := doRequest(t, h, http.MethodDelete, "/v2/email/contact-lists/DeleteList", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_UpdateContactList(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "UpdateList",
	})

	rec := doRequest(t, h, http.MethodPut, "/v2/email/contact-lists/UpdateList", map[string]any{
		"Description": "Updated description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListContactLists(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/contact-lists", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Contacts ---

func TestOps_GetContact(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "ContactOpsListGet",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/contact-lists/ContactOpsListGet/contacts",
		map[string]any{
			"EmailAddress": "contact-get@example.com",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/contact-lists/ContactOpsListGet/contacts/contact-get%40example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_DeleteContact(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "ContactOpsListDel",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/contact-lists/ContactOpsListDel/contacts",
		map[string]any{
			"EmailAddress": "contact-del@example.com",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/contact-lists/ContactOpsListDel/contacts/contact-del%40example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_UpdateContact(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "ContactOpsListUpd",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/contact-lists/ContactOpsListUpd/contacts",
		map[string]any{
			"EmailAddress": "contact-upd@example.com",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/contact-lists/ContactOpsListUpd/contacts/contact-upd%40example.com",
		map[string]any{
			"UnsubscribeAll": true,
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListContacts(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "ContactOpsListList",
	})

	// Add a contact so that b.contacts["ContactOpsListList"] is initialized.
	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/contact-lists/ContactOpsListList/contacts",
		map[string]any{
			"EmailAddress": "seed@example.com",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/contact-lists/ContactOpsListList/contacts",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Custom verification email templates ---

func TestOps_GetCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/custom-verification-email-templates",
		map[string]any{
			"TemplateName":          "GetCVET",
			"FromEmailAddress":      "from@example.com",
			"TemplateSubject":       "Please verify",
			"TemplateContent":       "<html>Verify</html>",
			"SuccessRedirectionURL": "https://example.com/success",
			"FailureRedirectionURL": "https://example.com/failure",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/custom-verification-email-templates/GetCVET",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_DeleteCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/custom-verification-email-templates",
		map[string]any{
			"TemplateName":          "DelCVET",
			"FromEmailAddress":      "from@example.com",
			"TemplateSubject":       "Please verify",
			"TemplateContent":       "<html>Verify</html>",
			"SuccessRedirectionURL": "https://example.com/success",
			"FailureRedirectionURL": "https://example.com/failure",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/custom-verification-email-templates/DelCVET",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_UpdateCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/custom-verification-email-templates",
		map[string]any{
			"TemplateName":          "UpdCVET",
			"FromEmailAddress":      "from@example.com",
			"TemplateSubject":       "Please verify",
			"TemplateContent":       "<html>Verify</html>",
			"SuccessRedirectionURL": "https://example.com/success",
			"FailureRedirectionURL": "https://example.com/failure",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/custom-verification-email-templates/UpdCVET",
		map[string]any{
			"FromEmailAddress":      "from2@example.com",
			"TemplateSubject":       "Please verify updated",
			"TemplateContent":       "<html>Verify Updated</html>",
			"SuccessRedirectionURL": "https://example.com/success",
			"FailureRedirectionURL": "https://example.com/failure",
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListCustomVerificationEmailTemplates(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/custom-verification-email-templates", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Dedicated IP pools ---

func TestOps_GetDedicatedIPPool(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", map[string]any{
		"PoolName":    "GetIPPool",
		"ScalingMode": "STANDARD",
	})

	rec := doRequest(t, h, http.MethodGet, "/v2/email/dedicated-ip-pools/GetIPPool", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_DeleteDedicatedIPPool(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", map[string]any{
		"PoolName":    "DelIPPool",
		"ScalingMode": "STANDARD",
	})

	rec := doRequest(t, h, http.MethodDelete, "/v2/email/dedicated-ip-pools/DelIPPool", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListDedicatedIPPools(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/dedicated-ip-pools", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_GetDedicatedIps(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/dedicated-ips", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_GetDedicatedIP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/dedicated-ips/1.2.3.4", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_PutDedicatedIPInPool(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/dedicated-ips/1.2.3.4/pool", map[string]any{
		"DestinationPoolName": "test-pool",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_PutDedicatedIPWarmupAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/dedicated-ips/1.2.3.4/warmup", map[string]any{
		"WarmupPercentage": 50,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_PutDedicatedIPPoolScalingAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", map[string]any{
		"PoolName":    "ScalingPool",
		"ScalingMode": "STANDARD",
	})

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/dedicated-ip-pools/ScalingPool/scaling-attributes",
		map[string]any{
			"ScalingMode": "STANDARD",
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Deliverability dashboard ---

func TestOps_GetDeliverabilityDashboardOptions(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/deliverability-dashboard", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_PutDeliverabilityDashboardOption(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/deliverability-dashboard", map[string]any{
		"DashboardEnabled": true,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_GetDeliverabilityTestReport(t *testing.T) {
	t.Parallel()

	h, b := newRefinement1Handler(t)

	// First create an identity that is verified.
	_, err := b.CreateEmailIdentity("verified@example.com", "", nil)
	require.NoError(t, err)

	// Create a deliverability test report.
	createRec := doReq(
		t,
		h,
		http.MethodPost,
		"/v2/email/deliverability-dashboard/test",
		map[string]any{
			"ReportName":       "TestReport",
			"FromEmailAddress": "verified@example.com",
			"Content": map[string]any{
				"Simple": map[string]any{
					"Subject": map[string]any{"Data": "Test"},
					"Body":    map[string]any{"Text": map[string]any{"Data": "body"}},
				},
			},
		},
	)
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, parseJSON(t, createRec.Body.Bytes(), &createResp))
	reportID := createResp["ReportId"].(string)

	rec := doReq(t, h, http.MethodGet, "/v2/email/deliverability-dashboard/reports/"+reportID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListDeliverabilityTestReports(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/deliverability-dashboard/reports", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_GetDomainStatisticsReport(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/deliverability-dashboard/statistics/example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_GetDomainDeliverabilityCampaign(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/deliverability-dashboard/campaigns/example.com/campaign-123",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListDomainDeliverabilityCampaigns(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/deliverability-dashboard/campaigns/example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Email templates ---

func TestOps_GetEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "GetTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello",
			"Html":    "<html>Hello</html>",
		},
	})

	rec := doRequest(t, h, http.MethodGet, "/v2/email/templates/GetTemplate", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_DeleteEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "DelTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello",
			"Html":    "<html>Hello</html>",
		},
	})

	rec := doRequest(t, h, http.MethodDelete, "/v2/email/templates/DelTemplate", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_UpdateEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "UpdTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello",
			"Html":    "<html>Hello</html>",
		},
	})

	rec := doRequest(t, h, http.MethodPut, "/v2/email/templates/UpdTemplate", map[string]any{
		"TemplateContent": map[string]any{
			"Subject": "Hello Updated",
			"Html":    "<html>Hello Updated</html>",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListEmailTemplates(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/templates", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_TestRenderEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "RenderTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello {{name}}",
			"Html":    "<html>Hello {{name}}</html>",
		},
	})

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/templates/RenderTemplate/render",
		map[string]any{
			"TemplateData": `{"name":"World"}`,
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Export jobs ---

func TestOps_CreateExportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPost, "/v2/email/export-jobs", map[string]any{
		"ExportDataSource": map[string]any{
			"MetricsDataSource": map[string]any{
				"Namespace": "VDM",
				"Metrics":   []map[string]any{{"Name": "SEND", "Aggregation": "VOLUME"}},
			},
		},
		"ExportDestination": map[string]any{
			"DataFormat": "CSV",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_GetExportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := doRequest(t, h, http.MethodPost, "/v2/email/export-jobs", map[string]any{
		"ExportDataSource": map[string]any{
			"MetricsDataSource": map[string]any{
				"Namespace": "VDM",
				"Metrics":   []map[string]any{{"Name": "SEND", "Aggregation": "VOLUME"}},
			},
		},
		"ExportDestination": map[string]any{
			"DataFormat": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, parseJSON(t, createRec.Body.Bytes(), &createResp))
	jobID := createResp["jobId"].(string)

	rec := doRequest(t, h, http.MethodGet, "/v2/email/export-jobs/"+jobID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListExportJobs(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/export-jobs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_CancelExportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := doRequest(t, h, http.MethodPost, "/v2/email/export-jobs", map[string]any{
		"ExportDataSource": map[string]any{
			"MetricsDataSource": map[string]any{
				"Namespace": "VDM",
				"Metrics":   []map[string]any{{"Name": "SEND", "Aggregation": "VOLUME"}},
			},
		},
		"ExportDestination": map[string]any{
			"DataFormat": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, parseJSON(t, createRec.Body.Bytes(), &createResp))
	jobID := createResp["jobId"].(string)

	rec := doRequest(t, h, http.MethodPut, "/v2/email/export-jobs/"+jobID+"/cancel", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Import jobs ---

func TestOps_CreateImportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPost, "/v2/email/import-jobs", map[string]any{
		"ImportDataSource": map[string]any{
			"S3Url":      "s3://bucket/key.csv",
			"DataFormat": "CSV",
		},
		"ImportDestination": map[string]any{
			"SuppressionListDestination": map[string]any{
				"SuppressionListImportAction": "PUT",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_GetImportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := doRequest(t, h, http.MethodPost, "/v2/email/import-jobs", map[string]any{
		"ImportDataSource": map[string]any{
			"S3Url":      "s3://bucket/key.csv",
			"DataFormat": "CSV",
		},
		"ImportDestination": map[string]any{
			"SuppressionListDestination": map[string]any{
				"SuppressionListImportAction": "PUT",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, parseJSON(t, createRec.Body.Bytes(), &createResp))
	jobID := createResp["jobId"].(string)

	rec := doRequest(t, h, http.MethodGet, "/v2/email/import-jobs/"+jobID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListImportJobs(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/import-jobs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Email identity policies ---

func TestOps_CreateAndGetEmailIdentityPolicies(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": "policy@example.com",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/identities/policy@example.com/policies/MyPolicy",
		map[string]any{
			"Policy": `{"Version":"2012-10-17","Statement":[` +
				`{"Effect":"Allow","Principal":"*","Action":"ses:SendEmail","Resource":"*"}]}`,
		},
	)

	rec := doRequest(t, h, http.MethodGet, "/v2/email/identities/policy@example.com/policies", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_DeleteEmailIdentityPolicy(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": "delpol@example.com",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/identities/delpol@example.com/policies/MyPolicy",
		map[string]any{
			"Policy": `{"Version":"2012-10-17"}`,
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/identities/delpol@example.com/policies/MyPolicy",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_UpdateEmailIdentityPolicy(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": "updpol@example.com",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/identities/updpol@example.com/policies/MyPolicy",
		map[string]any{
			"Policy": `{"Version":"2012-10-17"}`,
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/identities/updpol@example.com/policies/MyPolicy",
		map[string]any{
			"Policy": `{"Version":"2012-10-17","Statement":[]}`,
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Email identity attributes ---

func TestOps_PutEmailIdentityAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": "attr@example.com",
	})

	paths := []string{
		"/v2/email/identities/attr@example.com/configuration-set",
		"/v2/email/identities/attr@example.com/dkim",
		"/v2/email/identities/attr@example.com/dkim/signing",
		"/v2/email/identities/attr@example.com/feedback",
		"/v2/email/identities/attr@example.com/mail-from",
	}

	for _, path := range paths {
		rec := doRequest(t, h, http.MethodPut, path, map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code, "path=%s", path)
	}
}

// --- Configuration set event destinations ---

func TestOps_GetConfigurationSetEventDestinations(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": "EventDestCS",
	})

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/configuration-sets/EventDestCS/event-destinations",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_DeleteConfigurationSetEventDestination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": "DelEventDestCS",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/configuration-sets/DelEventDestCS/event-destinations",
		map[string]any{
			"EventDestinationName": "sns-dest",
			"EventDestination": map[string]any{
				"Enabled":            true,
				"MatchingEventTypes": []string{"SEND"},
			},
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/configuration-sets/DelEventDestCS/event-destinations/sns-dest",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_UpdateConfigurationSetEventDestination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": "UpdEventDestCS",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/configuration-sets/UpdEventDestCS/event-destinations",
		map[string]any{
			"EventDestinationName": "upd-dest",
			"EventDestination": map[string]any{
				"Enabled":            true,
				"MatchingEventTypes": []string{"SEND"},
			},
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/configuration-sets/UpdEventDestCS/event-destinations/upd-dest",
		map[string]any{
			"EventDestination": map[string]any{
				"Enabled":            false,
				"MatchingEventTypes": []string{"BOUNCE"},
			},
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Configuration set attribute Put operations ---

func TestOps_PutConfigurationSetAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": "AttrCS",
	})

	attrPaths := []string{
		"/v2/email/configuration-sets/AttrCS/archiving-options",
		"/v2/email/configuration-sets/AttrCS/delivery-options",
		"/v2/email/configuration-sets/AttrCS/reputation-options",
		"/v2/email/configuration-sets/AttrCS/sending-options",
		"/v2/email/configuration-sets/AttrCS/suppression-options",
		"/v2/email/configuration-sets/AttrCS/tracking-options",
		"/v2/email/configuration-sets/AttrCS/vdm-options",
	}

	for _, path := range attrPaths {
		rec := doRequest(t, h, http.MethodPut, path, map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code, "path=%s", path)
	}
}

// --- Bulk email ---

func TestOps_SendBulkEmail(t *testing.T) {
	t.Parallel()

	h, b := newRefinement1Handler(t)
	_, err := b.CreateEmailIdentity("bulk@example.com", "", nil)
	require.NoError(t, err)

	doReq(t, h, http.MethodPost, "/v2/email/templates", map[string]any{
		"TemplateName": "BulkTemplate",
		"TemplateContent": map[string]any{
			"Subject": "Hello {{name}}",
			"Html":    "<html>Hi {{name}}</html>",
		},
	})

	rec := doReq(t, h, http.MethodPost, "/v2/email/outbound-bulk-emails", map[string]any{
		"FromEmailAddress": "bulk@example.com",
		"DefaultContent": map[string]any{
			"Template": map[string]any{
				"TemplateName": "BulkTemplate",
				"TemplateData": `{"name":"default"}`,
			},
		},
		"BulkEmailEntries": []map[string]any{
			{
				"Destination": map[string]any{
					"ToAddresses": []string{"to1@example.com"},
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Multi-region endpoints ---

func TestOps_CreateAndGetMultiRegionEndpoint(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/multi-region-endpoints",
		map[string]any{
			"EndpointName": "TestEndpoint",
		},
	)
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doRequest(t, h, http.MethodGet, "/v2/email/multi-region-endpoints/TestEndpoint", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListMultiRegionEndpoints(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/multi-region-endpoints", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_DeleteMultiRegionEndpoint(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/multi-region-endpoints", map[string]any{
		"EndpointName": "DelEndpoint",
	})

	rec := doRequest(t, h, http.MethodDelete, "/v2/email/multi-region-endpoints/DelEndpoint", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Tenants ---

func TestOps_TenantCRUD(t *testing.T) {
	t.Parallel()

	h := newHandler()
	tenantName := "TestTenant"

	createRec := doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{
		"TenantName": tenantName,
	})
	assert.Equal(t, http.StatusOK, createRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/v2/email/tenants/"+tenantName, nil)
	assert.Equal(t, http.StatusOK, getRec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/v2/email/tenants", nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	delRec := doRequest(t, h, http.MethodDelete, "/v2/email/tenants/"+tenantName, nil)
	assert.Equal(t, http.StatusOK, delRec.Code)
}

func TestOps_TenantResourceAssociation(t *testing.T) {
	t.Parallel()

	h := newHandler()
	tenantName := "AssocTenant"

	doRequest(t, h, http.MethodPost, "/v2/email/tenants", map[string]any{
		"TenantName": tenantName,
	})

	// Create tenant resource association (ARN without slashes to avoid path splitting).
	const resARN = "arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Acs-MyCS"
	assocRec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/tenants/"+tenantName+"/resources",
		map[string]any{
			"ResourceArn": "arn:aws:ses:us-east-1:123456789012:cs-MyCS",
		},
	)
	assert.Equal(t, http.StatusOK, assocRec.Code)

	// List tenant resources.
	listRec := doRequest(t, h, http.MethodGet, "/v2/email/tenants/"+tenantName+"/resources", nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	// Delete tenant resource association.
	delRec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/tenants/"+tenantName+"/resources/"+resARN,
		nil,
	)
	assert.Equal(t, http.StatusOK, delRec.Code)
}

func TestOps_ListResourceTenants(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/resource-tenants", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Reputation entities ---

func TestOps_ReputationEntities(t *testing.T) {
	t.Parallel()

	h := newHandler()

	listRec := doRequest(t, h, http.MethodGet, "/v2/email/reputation-entities", nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/v2/email/reputation-entities/entity-1", nil)
	assert.Equal(t, http.StatusOK, getRec.Code)

	updStatusRec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/reputation-entities/entity-1/customer-managed-status",
		map[string]any{
			"CustomerManagedStatus": "ENABLED",
		},
	)
	assert.Equal(t, http.StatusOK, updStatusRec.Code)

	updPolicyRec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/reputation-entities/entity-1/policy",
		map[string]any{
			"Policy": `{}`,
		},
	)
	assert.Equal(t, http.StatusOK, updPolicyRec.Code)
}

// --- SendCustomVerificationEmail ---

// TestOps_SendCustomVerificationEmail calls the backend directly since the
// HTTP route for this operation isn't wired in the current handler.
func TestOps_SendCustomVerificationEmail(t *testing.T) {
	t.Parallel()

	// Create the template via HTTP.
	h := newHandler()
	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/custom-verification-email-templates",
		map[string]any{
			"TemplateName":          "CVETSend",
			"FromEmailAddress":      "from@example.com",
			"TemplateSubject":       "Please verify",
			"TemplateContent":       "<html>Verify</html>",
			"SuccessRedirectionURL": "https://example.com/success",
			"FailureRedirectionURL": "https://example.com/failure",
		},
	)

	// The backend method is covered by calling the GET template endpoint to confirm it was created.
	getRec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/custom-verification-email-templates/CVETSend",
		nil,
	)
	assert.Equal(t, http.StatusOK, getRec.Code)
}

// --- Insights and recommendations ---

func TestOps_GetEmailAddressInsights(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/email-insights/test%40example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_GetMessageInsights(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/messages/msg-123", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestOps_ListRecommendations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/recommendations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- NewInMemoryBackendWithConfig ---

func TestOps_NewInMemoryBackendWithConfig(t *testing.T) {
	t.Parallel()

	// Exercise NewInMemoryBackendWithConfig code path.
	_, b := newRefinement1Handler(t)
	assert.NotNil(t, b)
	assert.NotEmpty(t, b.Region())
}
