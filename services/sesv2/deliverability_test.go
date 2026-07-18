package sesv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestCreateDeliverabilityTestReport tests report creation.
func TestCreateDeliverabilityTestReport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		reportName       string
		fromEmailAddress string
	}{
		{
			name:             "creates_report",
			reportName:       "my-report",
			fromEmailAddress: "test@example.com",
		},
		{
			name:             "empty_from",
			reportName:       "my-report-2",
			fromEmailAddress: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			report, err := backend.CreateDeliverabilityTestReport(
				tt.reportName,
				tt.fromEmailAddress,
			)

			require.NoError(t, err)
			require.NotNil(t, report)
			assert.NotEmpty(t, report.ReportID)
			assert.Equal(t, "IN_PROGRESS", report.DeliverabilityTestStatus)
		})
	}
}

// TestCreateDeliverabilityTestReportHTTP tests report creation via HTTP.
func TestCreateDeliverabilityTestReportHTTP(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	body := map[string]any{
		"ReportName":       "my-report",
		"FromEmailAddress": "test@example.com",
	}
	rec := doReq(t, h, http.MethodPost, "/v2/email/deliverability-dashboard/test", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetDeliverabilityDashboardOptions tests the GetDeliverabilityDashboardOptions operation.
func TestGetDeliverabilityDashboardOptions(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/deliverability-dashboard", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestPutDeliverabilityDashboardOption tests the PutDeliverabilityDashboardOption operation.
func TestPutDeliverabilityDashboardOption(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/deliverability-dashboard", map[string]any{
		"DashboardEnabled": true,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetDeliverabilityTestReport tests the GetDeliverabilityTestReport operation.
func TestGetDeliverabilityTestReport(t *testing.T) {
	t.Parallel()

	h, b := newSESv2TestHandler(t)

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
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	reportID := createResp["ReportId"].(string)

	rec := doReq(t, h, http.MethodGet, "/v2/email/deliverability-dashboard/reports/"+reportID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListDeliverabilityTestReports tests the ListDeliverabilityTestReports operation.
func TestListDeliverabilityTestReports(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/deliverability-dashboard/reports", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetDomainStatisticsReport tests the GetDomainStatisticsReport operation.
func TestGetDomainStatisticsReport(t *testing.T) {
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

// TestGetDomainDeliverabilityCampaign tests the GetDomainDeliverabilityCampaign operation via HTTP,
// verifying the campaign ID is parsed correctly from the URL path (segments[3] after stripping the
// /v2/email/ prefix: "deliverability-dashboard"/"campaigns"/domain/campaignId).
func TestGetDomainDeliverabilityCampaign(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/deliverability-dashboard/campaigns/example.com/campaign-123",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "campaign-123", out["CampaignId"], "campaign ID must be parsed from the URL path")
}

// TestListDomainDeliverabilityCampaigns tests the ListDomainDeliverabilityCampaigns operation.
func TestListDomainDeliverabilityCampaigns(t *testing.T) {
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

// TestReputationEntities tests ListReputationEntities, GetReputationEntity,
// UpdateReputationEntityCustomerManagedStatus, and UpdateReputationEntityPolicy.
func TestReputationEntities(t *testing.T) {
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

// TestGetEmailAddressInsights tests the GetEmailAddressInsights operation.
func TestGetEmailAddressInsights(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/email-insights/test%40example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListRecommendations tests the ListRecommendations operation.
func TestListRecommendations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/recommendations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetDomainDeliverabilityCampaignFields verifies required fields are present.
//
// NOTE: this calls the backend method directly (not through the HTTP handler), so unlike
// TestGetDomainDeliverabilityCampaign above it does not exercise the handler's URL
// path-segment parsing.
func TestGetDomainDeliverabilityCampaignFields(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	result, err := backend.GetDomainDeliverabilityCampaign("example.com", "camp-123")
	require.NoError(t, err)

	for _, field := range []string{"CampaignId", "FromAddress", "InboxCount", "SpamCount"} {
		assert.Contains(t, result, field, "missing field %s", field)
	}
}

// TestGetDomainStatisticsReportFields verifies required fields are present.
func TestGetDomainStatisticsReportFields(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	result, err := backend.GetDomainStatisticsReport("example.com", "2024-01-01", "2024-01-31")
	require.NoError(t, err)

	assert.Equal(t, "example.com", result["Domain"])
	assert.Contains(t, result, "OverallVolume")
	assert.Contains(t, result, "DailyVolumes")
}
