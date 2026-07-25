package sesv2_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
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

// TestPutDeliverabilityDashboardOption tests that PutDeliverabilityDashboardOption
// actually persists its state, so a subsequent GetDeliverabilityDashboardOptions
// reflects it (previously a true no-op that never affected the Get response).
func TestPutDeliverabilityDashboardOption(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/deliverability-dashboard", map[string]any{
		"DashboardEnabled": true,
		"SubscribedDomains": []map[string]any{
			{"Domain": "example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/v2/email/deliverability-dashboard", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	assert.Equal(t, true, out["DashboardEnabled"])
	assert.Equal(t, "ACTIVE", out["AccountStatus"])

	domains, ok := out["ActiveSubscribedDomains"].([]any)
	require.True(t, ok)
	require.Len(t, domains, 1)

	domain, ok := domains[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "example.com", domain["Domain"])
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

	rec := doReq(t, h, http.MethodGet, "/v2/email/deliverability-dashboard/test-reports/"+reportID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListDeliverabilityTestReports tests the ListDeliverabilityTestReports operation.
func TestListDeliverabilityTestReports(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/deliverability-dashboard/test-reports", nil)
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
		"/v2/email/deliverability-dashboard/statistics-report/example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetDomainDeliverabilityCampaign tests the GetDomainDeliverabilityCampaign
// operation via HTTP. The real SDK path is
// /v2/email/deliverability-dashboard/campaigns/{CampaignId} -- no domain
// segment (confirmed against GetDomainDeliverabilityCampaignInput, which has
// only a CampaignId field).
func TestGetDomainDeliverabilityCampaign(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/deliverability-dashboard/campaigns/campaign-123",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	campaign, ok := out["DomainDeliverabilityCampaign"].(map[string]any)
	require.True(t, ok, "response missing DomainDeliverabilityCampaign wrapper: %s", rec.Body)
	assert.Equal(t, "campaign-123", campaign["CampaignId"], "campaign ID must be parsed from the URL path")
}

// TestListDomainDeliverabilityCampaigns tests the ListDomainDeliverabilityCampaigns
// operation. Real SDK path is
// /v2/email/deliverability-dashboard/domains/{SubscribedDomain}/campaigns.
func TestListDomainDeliverabilityCampaigns(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/deliverability-dashboard/domains/example.com/campaigns",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestReputationEntities tests ListReputationEntities, GetReputationEntity,
// UpdateReputationEntityCustomerManagedStatus, and UpdateReputationEntityPolicy,
// using the real SDK's /v2/email/reputation/entities/... paths (the
// previously-tested /v2/email/reputation-entities/... top-level path was a
// gopherstack-invented duplicate not present in the real SDK and has been
// removed).
func TestReputationEntities(t *testing.T) {
	t.Parallel()

	h := newHandler()

	listRec := doRequest(t, h, http.MethodPost, "/v2/email/reputation/entities", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)

	getRec := doRequest(
		t, h, http.MethodGet, "/v2/email/reputation/entities/CONFIGURATION_SET/entity-1", nil,
	)
	assert.Equal(t, http.StatusOK, getRec.Code)

	updStatusRec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/reputation/entities/CONFIGURATION_SET/entity-1/customer-managed-status",
		map[string]any{
			"SendingStatus": "ENABLED",
		},
	)
	assert.Equal(t, http.StatusOK, updStatusRec.Code)

	updPolicyRec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/reputation/entities/CONFIGURATION_SET/entity-1/policy",
		map[string]any{
			"ReputationEntityPolicy": `{}`,
		},
	)
	assert.Equal(t, http.StatusOK, updPolicyRec.Code)
}

// TestGetEmailAddressInsights tests the GetEmailAddressInsights operation,
// which the real SDK serves as POST /v2/email/email-address-insights with
// EmailAddress in the JSON body (not a GET with the address in the URL).
func TestGetEmailAddressInsights(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPost, "/v2/email/email-address-insights", map[string]any{
		"EmailAddress": "test@example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	validation, ok := out["MailboxValidation"].(map[string]any)
	require.True(t, ok, "response missing MailboxValidation: %s", rec.Body)
	assert.Contains(t, validation, "IsValid")
	assert.Contains(t, validation, "Evaluations")
}

// TestListRecommendations tests the ListRecommendations operation, served as
// POST /v2/email/vdm/recommendations.
func TestListRecommendations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPost, "/v2/email/vdm/recommendations", map[string]any{})
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
	result, err := backend.GetDomainDeliverabilityCampaign("camp-123")
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

// TestGetDomainStatisticsReportDailyVolumesSDKRoundTrip verifies
// GetDomainStatisticsReport emits one DailyVolumes entry per day in the
// requested [StartDate, EndDate] range (the shape real SES v2 documents),
// via the real aws-sdk-go-v2 client so the RFC3339 StartDate/EndDate query
// parameters are encoded the way a genuine client encodes them.
func TestGetDomainStatisticsReportDailyVolumesSDKRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		startBack time.Duration
		wantDays  int
	}{
		{name: "single_day", startBack: 0, wantDays: 1},
		{name: "three_day_range", startBack: 48 * time.Hour, wantDays: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			client := newSESv2SDKClient(t, h)

			end := time.Now().UTC()
			start := end.Add(-tt.startBack)

			out, err := client.GetDomainStatisticsReport(t.Context(), &sesv2sdk.GetDomainStatisticsReportInput{
				Domain:    aws.String("example.com"),
				StartDate: aws.Time(start),
				EndDate:   aws.Time(end),
			})
			require.NoError(t, err)
			assert.Len(t, out.DailyVolumes, tt.wantDays)
			require.NotNil(t, out.OverallVolume)
		})
	}
}

// TestDomainDeliverabilityCampaignSDKRoundTrip verifies that
// GetDomainDeliverabilityCampaign/ListDomainDeliverabilityCampaigns derive
// real CampaignId/FromAddress/Subject/timestamps from gopherstack's own
// SendEmail history (rather than always-empty placeholders), driven through
// the real aws-sdk-go-v2 client end to end: CreateEmailIdentity -> SendEmail
// -> ListDomainDeliverabilityCampaigns -> GetDomainDeliverabilityCampaign.
func TestDomainDeliverabilityCampaignSDKRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newSESv2SDKClient(t, h)

	_, err := client.CreateEmailIdentity(t.Context(), &sesv2sdk.CreateEmailIdentityInput{
		EmailIdentity: aws.String("camp@example.com"),
	})
	require.NoError(t, err)

	sendOut, err := client.SendEmail(t.Context(), &sesv2sdk.SendEmailInput{
		FromEmailAddress: aws.String("camp@example.com"),
		Destination:      &sesv2types.Destination{ToAddresses: []string{"rcpt@example.com"}},
		Content: &sesv2types.EmailContent{
			Simple: &sesv2types.Message{
				Subject: &sesv2types.Content{Data: aws.String("Campaign Subject")},
				Body:    &sesv2types.Body{Text: &sesv2types.Content{Data: aws.String("hi")}},
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(sendOut.MessageId))

	listOut, err := client.ListDomainDeliverabilityCampaigns(
		t.Context(),
		&sesv2sdk.ListDomainDeliverabilityCampaignsInput{
			SubscribedDomain: aws.String("example.com"),
			StartDate:        aws.Time(time.Now().Add(-24 * time.Hour)),
			EndDate:          aws.Time(time.Now().Add(24 * time.Hour)),
		},
	)
	require.NoError(t, err)
	require.Len(t, listOut.DomainDeliverabilityCampaigns, 1)

	campaign := listOut.DomainDeliverabilityCampaigns[0]
	assert.Equal(t, "camp@example.com", aws.ToString(campaign.FromAddress))
	assert.Equal(t, "Campaign Subject", aws.ToString(campaign.Subject))
	require.NotEmpty(t, aws.ToString(campaign.CampaignId))
	assert.NotNil(t, campaign.FirstSeenDateTime)
	assert.NotNil(t, campaign.LastSeenDateTime)

	getOut, err := client.GetDomainDeliverabilityCampaign(t.Context(), &sesv2sdk.GetDomainDeliverabilityCampaignInput{
		CampaignId: campaign.CampaignId,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.DomainDeliverabilityCampaign)
	assert.Equal(t, "camp@example.com", aws.ToString(getOut.DomainDeliverabilityCampaign.FromAddress))
	assert.Equal(t, "Campaign Subject", aws.ToString(getOut.DomainDeliverabilityCampaign.Subject))
}

// TestReputationEntitySDKRoundTrip verifies the reputationEntityOutput
// typed-DTO conversion (wire_output.go) -- notably that CustomerManagedStatus
// decodes as a nested StatusRecord{Status: ...}, not a bare string -- via the
// real aws-sdk-go-v2 client's deserializer.
func TestReputationEntitySDKRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newSESv2SDKClient(t, h)

	_, err := client.UpdateReputationEntityCustomerManagedStatus(
		t.Context(),
		&sesv2sdk.UpdateReputationEntityCustomerManagedStatusInput{
			ReputationEntityReference: aws.String("entity-1"),
			ReputationEntityType:      sesv2types.ReputationEntityTypeResource,
			SendingStatus:             sesv2types.SendingStatusDisabled,
		},
	)
	require.NoError(t, err)

	getOut, err := client.GetReputationEntity(t.Context(), &sesv2sdk.GetReputationEntityInput{
		ReputationEntityReference: aws.String("entity-1"),
		ReputationEntityType:      sesv2types.ReputationEntityTypeResource,
	})
	require.NoError(t, err)

	require.NotNil(t, getOut.ReputationEntity)
	assert.Equal(t, "entity-1", aws.ToString(getOut.ReputationEntity.ReputationEntityReference))
	require.NotNil(t, getOut.ReputationEntity.CustomerManagedStatus)
	assert.Equal(t, sesv2types.SendingStatusDisabled, getOut.ReputationEntity.CustomerManagedStatus.Status)
	assert.Equal(t, sesv2types.SendingStatusDisabled, getOut.ReputationEntity.SendingStatusAggregate)
}

// TestListRecommendationsSDKRoundTrip verifies ListRecommendations derives
// real DKIM/SPF/COMPLAINT recommendations from gopherstack's actual
// configuration state (rather than always returning an empty list), via the
// real aws-sdk-go-v2 client end to end for each derivation gopherstack
// supports.
func TestListRecommendationsSDKRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, client *sesv2sdk.Client)
		wantType sesv2types.RecommendationType
		name     string
	}{
		{
			name:     "dkim_signing_disabled",
			wantType: sesv2types.RecommendationTypeDkim,
			setup: func(t *testing.T, client *sesv2sdk.Client) {
				t.Helper()

				_, err := client.CreateEmailIdentity(t.Context(), &sesv2sdk.CreateEmailIdentityInput{
					EmailIdentity: aws.String("nodkim@example.com"),
				})
				require.NoError(t, err)

				_, err = client.PutEmailIdentityDkimAttributes(
					t.Context(),
					&sesv2sdk.PutEmailIdentityDkimAttributesInput{
						EmailIdentity:  aws.String("nodkim@example.com"),
						SigningEnabled: false,
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:     "mail_from_pending",
			wantType: sesv2types.RecommendationTypeSpf,
			setup: func(t *testing.T, client *sesv2sdk.Client) {
				t.Helper()

				_, err := client.CreateEmailIdentity(t.Context(), &sesv2sdk.CreateEmailIdentityInput{
					EmailIdentity: aws.String("mailfrom.example.com"),
				})
				require.NoError(t, err)

				_, err = client.PutEmailIdentityMailFromAttributes(
					t.Context(),
					&sesv2sdk.PutEmailIdentityMailFromAttributesInput{
						EmailIdentity:  aws.String("mailfrom.example.com"),
						MailFromDomain: aws.String("bounce.mailfrom.example.com"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:     "reputation_entity_disabled",
			wantType: sesv2types.RecommendationTypeComplaint,
			setup: func(t *testing.T, client *sesv2sdk.Client) {
				t.Helper()

				_, err := client.UpdateReputationEntityCustomerManagedStatus(
					t.Context(),
					&sesv2sdk.UpdateReputationEntityCustomerManagedStatusInput{
						ReputationEntityReference: aws.String("bad-entity"),
						ReputationEntityType:      sesv2types.ReputationEntityTypeResource,
						SendingStatus:             sesv2types.SendingStatusDisabled,
					},
				)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			client := newSESv2SDKClient(t, h)

			tt.setup(t, client)

			out, err := client.ListRecommendations(t.Context(), &sesv2sdk.ListRecommendationsInput{
				Filter: map[string]string{"TYPE": string(tt.wantType)},
			})
			require.NoError(t, err)
			require.Len(t, out.Recommendations, 1)

			rec := out.Recommendations[0]
			assert.Equal(t, tt.wantType, rec.Type)
			assert.Equal(t, sesv2types.RecommendationStatusOpen, rec.Status)
			assert.NotEmpty(t, aws.ToString(rec.ResourceArn))
			assert.NotNil(t, rec.CreatedTimestamp)
		})
	}
}
