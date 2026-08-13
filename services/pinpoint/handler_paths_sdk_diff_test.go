package pinpoint_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

// sdkRouteCases is the authoritative method+path for every real pinpoint
// operation, extracted from pinpoint@v1.42.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateApp", "POST", "/v1/apps"},
		{"CreateCampaign", "POST", "/v1/apps/PLACEHOLDER/campaigns"},
		{"CreateEmailTemplate", "POST", "/v1/templates/PLACEHOLDER/email"},
		{"CreateExportJob", "POST", "/v1/apps/PLACEHOLDER/jobs/export"},
		{"CreateImportJob", "POST", "/v1/apps/PLACEHOLDER/jobs/import"},
		{"CreateInAppTemplate", "POST", "/v1/templates/PLACEHOLDER/inapp"},
		{"CreateJourney", "POST", "/v1/apps/PLACEHOLDER/journeys"},
		{"CreatePushTemplate", "POST", "/v1/templates/PLACEHOLDER/push"},
		{"CreateRecommenderConfiguration", "POST", "/v1/recommenders"},
		{"CreateSegment", "POST", "/v1/apps/PLACEHOLDER/segments"},
		{"CreateSmsTemplate", "POST", "/v1/templates/PLACEHOLDER/sms"},
		{"CreateVoiceTemplate", "POST", "/v1/templates/PLACEHOLDER/voice"},
		{"DeleteAdmChannel", "DELETE", "/v1/apps/PLACEHOLDER/channels/adm"},
		{"DeleteApnsChannel", "DELETE", "/v1/apps/PLACEHOLDER/channels/apns"},
		{"DeleteApnsSandboxChannel", "DELETE", "/v1/apps/PLACEHOLDER/channels/apns_sandbox"},
		{"DeleteApnsVoipChannel", "DELETE", "/v1/apps/PLACEHOLDER/channels/apns_voip"},
		{"DeleteApnsVoipSandboxChannel", "DELETE", "/v1/apps/PLACEHOLDER/channels/apns_voip_sandbox"},
		{"DeleteApp", "DELETE", "/v1/apps/PLACEHOLDER"},
		{"DeleteBaiduChannel", "DELETE", "/v1/apps/PLACEHOLDER/channels/baidu"},
		{"DeleteCampaign", "DELETE", "/v1/apps/PLACEHOLDER/campaigns/PLACEHOLDER"},
		{"DeleteEmailChannel", "DELETE", "/v1/apps/PLACEHOLDER/channels/email"},
		{"DeleteEmailTemplate", "DELETE", "/v1/templates/PLACEHOLDER/email"},
		{"DeleteEndpoint", "DELETE", "/v1/apps/PLACEHOLDER/endpoints/PLACEHOLDER"},
		{"DeleteEventStream", "DELETE", "/v1/apps/PLACEHOLDER/eventstream"},
		{"DeleteGcmChannel", "DELETE", "/v1/apps/PLACEHOLDER/channels/gcm"},
		{"DeleteInAppTemplate", "DELETE", "/v1/templates/PLACEHOLDER/inapp"},
		{"DeleteJourney", "DELETE", "/v1/apps/PLACEHOLDER/journeys/PLACEHOLDER"},
		{"DeletePushTemplate", "DELETE", "/v1/templates/PLACEHOLDER/push"},
		{"DeleteRecommenderConfiguration", "DELETE", "/v1/recommenders/PLACEHOLDER"},
		{"DeleteSegment", "DELETE", "/v1/apps/PLACEHOLDER/segments/PLACEHOLDER"},
		{"DeleteSmsChannel", "DELETE", "/v1/apps/PLACEHOLDER/channels/sms"},
		{"DeleteSmsTemplate", "DELETE", "/v1/templates/PLACEHOLDER/sms"},
		{"DeleteUserEndpoints", "DELETE", "/v1/apps/PLACEHOLDER/users/PLACEHOLDER"},
		{"DeleteVoiceChannel", "DELETE", "/v1/apps/PLACEHOLDER/channels/voice"},
		{"DeleteVoiceTemplate", "DELETE", "/v1/templates/PLACEHOLDER/voice"},
		{"GetAdmChannel", "GET", "/v1/apps/PLACEHOLDER/channels/adm"},
		{"GetApnsChannel", "GET", "/v1/apps/PLACEHOLDER/channels/apns"},
		{"GetApnsSandboxChannel", "GET", "/v1/apps/PLACEHOLDER/channels/apns_sandbox"},
		{"GetApnsVoipChannel", "GET", "/v1/apps/PLACEHOLDER/channels/apns_voip"},
		{"GetApnsVoipSandboxChannel", "GET", "/v1/apps/PLACEHOLDER/channels/apns_voip_sandbox"},
		{"GetApp", "GET", "/v1/apps/PLACEHOLDER"},
		{"GetApplicationDateRangeKpi", "GET", "/v1/apps/PLACEHOLDER/kpis/daterange/PLACEHOLDER"},
		{"GetApplicationSettings", "GET", "/v1/apps/PLACEHOLDER/settings"},
		{"GetApps", "GET", "/v1/apps"},
		{"GetBaiduChannel", "GET", "/v1/apps/PLACEHOLDER/channels/baidu"},
		{"GetCampaign", "GET", "/v1/apps/PLACEHOLDER/campaigns/PLACEHOLDER"},
		{"GetCampaignActivities", "GET", "/v1/apps/PLACEHOLDER/campaigns/PLACEHOLDER/activities"},
		{"GetCampaignDateRangeKpi", "GET", "/v1/apps/PLACEHOLDER/campaigns/PLACEHOLDER/kpis/daterange/PLACEHOLDER"},
		{"GetCampaignVersion", "GET", "/v1/apps/PLACEHOLDER/campaigns/PLACEHOLDER/versions/PLACEHOLDER"},
		{"GetCampaignVersions", "GET", "/v1/apps/PLACEHOLDER/campaigns/PLACEHOLDER/versions"},
		{"GetCampaigns", "GET", "/v1/apps/PLACEHOLDER/campaigns"},
		{"GetChannels", "GET", "/v1/apps/PLACEHOLDER/channels"},
		{"GetEmailChannel", "GET", "/v1/apps/PLACEHOLDER/channels/email"},
		{"GetEmailTemplate", "GET", "/v1/templates/PLACEHOLDER/email"},
		{"GetEndpoint", "GET", "/v1/apps/PLACEHOLDER/endpoints/PLACEHOLDER"},
		{"GetEventStream", "GET", "/v1/apps/PLACEHOLDER/eventstream"},
		{"GetExportJob", "GET", "/v1/apps/PLACEHOLDER/jobs/export/PLACEHOLDER"},
		{"GetExportJobs", "GET", "/v1/apps/PLACEHOLDER/jobs/export"},
		{"GetGcmChannel", "GET", "/v1/apps/PLACEHOLDER/channels/gcm"},
		{"GetImportJob", "GET", "/v1/apps/PLACEHOLDER/jobs/import/PLACEHOLDER"},
		{"GetImportJobs", "GET", "/v1/apps/PLACEHOLDER/jobs/import"},
		{"GetInAppMessages", "GET", "/v1/apps/PLACEHOLDER/endpoints/PLACEHOLDER/inappmessages"},
		{"GetInAppTemplate", "GET", "/v1/templates/PLACEHOLDER/inapp"},
		{"GetJourney", "GET", "/v1/apps/PLACEHOLDER/journeys/PLACEHOLDER"},
		{"GetJourneyDateRangeKpi", "GET", "/v1/apps/PLACEHOLDER/journeys/PLACEHOLDER/kpis/daterange/PLACEHOLDER"},
		{
			"GetJourneyExecutionActivityMetrics", "GET",
			"/v1/apps/PLACEHOLDER/journeys/PLACEHOLDER/activities/PLACEHOLDER/execution-metrics",
		},
		{"GetJourneyExecutionMetrics", "GET", "/v1/apps/PLACEHOLDER/journeys/PLACEHOLDER/execution-metrics"},
		{
			"GetJourneyRunExecutionActivityMetrics", "GET",
			"/v1/apps/PLACEHOLDER/journeys/PLACEHOLDER/runs/PLACEHOLDER/activities/PLACEHOLDER/execution-metrics",
		},
		{
			"GetJourneyRunExecutionMetrics", "GET",
			"/v1/apps/PLACEHOLDER/journeys/PLACEHOLDER/runs/PLACEHOLDER/execution-metrics",
		},
		{"GetJourneyRuns", "GET", "/v1/apps/PLACEHOLDER/journeys/PLACEHOLDER/runs"},
		{"GetPushTemplate", "GET", "/v1/templates/PLACEHOLDER/push"},
		{"GetRecommenderConfiguration", "GET", "/v1/recommenders/PLACEHOLDER"},
		{"GetRecommenderConfigurations", "GET", "/v1/recommenders"},
		{"GetSegment", "GET", "/v1/apps/PLACEHOLDER/segments/PLACEHOLDER"},
		{"GetSegmentExportJobs", "GET", "/v1/apps/PLACEHOLDER/segments/PLACEHOLDER/jobs/export"},
		{"GetSegmentImportJobs", "GET", "/v1/apps/PLACEHOLDER/segments/PLACEHOLDER/jobs/import"},
		{"GetSegmentVersion", "GET", "/v1/apps/PLACEHOLDER/segments/PLACEHOLDER/versions/PLACEHOLDER"},
		{"GetSegmentVersions", "GET", "/v1/apps/PLACEHOLDER/segments/PLACEHOLDER/versions"},
		{"GetSegments", "GET", "/v1/apps/PLACEHOLDER/segments"},
		{"GetSmsChannel", "GET", "/v1/apps/PLACEHOLDER/channels/sms"},
		{"GetSmsTemplate", "GET", "/v1/templates/PLACEHOLDER/sms"},
		{"GetUserEndpoints", "GET", "/v1/apps/PLACEHOLDER/users/PLACEHOLDER"},
		{"GetVoiceChannel", "GET", "/v1/apps/PLACEHOLDER/channels/voice"},
		{"GetVoiceTemplate", "GET", "/v1/templates/PLACEHOLDER/voice"},
		{"ListJourneys", "GET", "/v1/apps/PLACEHOLDER/journeys"},
		{"ListTagsForResource", "GET", "/v1/tags/PLACEHOLDER"},
		{"ListTemplateVersions", "GET", "/v1/templates/PLACEHOLDER/PLACEHOLDER/versions"},
		{"ListTemplates", "GET", "/v1/templates"},
		{"PhoneNumberValidate", "POST", "/v1/phone/number/validate"},
		{"PutEventStream", "POST", "/v1/apps/PLACEHOLDER/eventstream"},
		{"PutEvents", "POST", "/v1/apps/PLACEHOLDER/events"},
		{"RemoveAttributes", "PUT", "/v1/apps/PLACEHOLDER/attributes/PLACEHOLDER"},
		{"SendMessages", "POST", "/v1/apps/PLACEHOLDER/messages"},
		{"SendOTPMessage", "POST", "/v1/apps/PLACEHOLDER/otp"},
		{"SendUsersMessages", "POST", "/v1/apps/PLACEHOLDER/users-messages"},
		{"TagResource", "POST", "/v1/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/v1/tags/PLACEHOLDER"},
		{"UpdateAdmChannel", "PUT", "/v1/apps/PLACEHOLDER/channels/adm"},
		{"UpdateApnsChannel", "PUT", "/v1/apps/PLACEHOLDER/channels/apns"},
		{"UpdateApnsSandboxChannel", "PUT", "/v1/apps/PLACEHOLDER/channels/apns_sandbox"},
		{"UpdateApnsVoipChannel", "PUT", "/v1/apps/PLACEHOLDER/channels/apns_voip"},
		{"UpdateApnsVoipSandboxChannel", "PUT", "/v1/apps/PLACEHOLDER/channels/apns_voip_sandbox"},
		{"UpdateApplicationSettings", "PUT", "/v1/apps/PLACEHOLDER/settings"},
		{"UpdateBaiduChannel", "PUT", "/v1/apps/PLACEHOLDER/channels/baidu"},
		{"UpdateCampaign", "PUT", "/v1/apps/PLACEHOLDER/campaigns/PLACEHOLDER"},
		{"UpdateEmailChannel", "PUT", "/v1/apps/PLACEHOLDER/channels/email"},
		{"UpdateEmailTemplate", "PUT", "/v1/templates/PLACEHOLDER/email"},
		{"UpdateEndpoint", "PUT", "/v1/apps/PLACEHOLDER/endpoints/PLACEHOLDER"},
		{"UpdateEndpointsBatch", "PUT", "/v1/apps/PLACEHOLDER/endpoints"},
		{"UpdateGcmChannel", "PUT", "/v1/apps/PLACEHOLDER/channels/gcm"},
		{"UpdateInAppTemplate", "PUT", "/v1/templates/PLACEHOLDER/inapp"},
		{"UpdateJourney", "PUT", "/v1/apps/PLACEHOLDER/journeys/PLACEHOLDER"},
		{"UpdateJourneyState", "PUT", "/v1/apps/PLACEHOLDER/journeys/PLACEHOLDER/state"},
		{"UpdatePushTemplate", "PUT", "/v1/templates/PLACEHOLDER/push"},
		{"UpdateRecommenderConfiguration", "PUT", "/v1/recommenders/PLACEHOLDER"},
		{"UpdateSegment", "PUT", "/v1/apps/PLACEHOLDER/segments/PLACEHOLDER"},
		{"UpdateSmsChannel", "PUT", "/v1/apps/PLACEHOLDER/channels/sms"},
		{"UpdateSmsTemplate", "PUT", "/v1/templates/PLACEHOLDER/sms"},
		{"UpdateTemplateActiveVersion", "PUT", "/v1/templates/PLACEHOLDER/PLACEHOLDER/active-version"},
		{"UpdateVoiceChannel", "PUT", "/v1/apps/PLACEHOLDER/channels/voice"},
		{"UpdateVoiceTemplate", "PUT", "/v1/templates/PLACEHOLDER/voice"},
		{"VerifyOTPMessage", "POST", "/v1/apps/PLACEHOLDER/verify-otp"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real pinpoint op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}
		})
	}
}
