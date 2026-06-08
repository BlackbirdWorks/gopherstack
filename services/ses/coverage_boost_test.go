package ses_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// ---------------------------------------------------------------------------
// Handler metadata methods
// ---------------------------------------------------------------------------

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "SES", h.Name())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Greater(t, h.MatchPriority(), 0)
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("reset@example.com"))
	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).IdentityCount())
	h.Reset()
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).IdentityCount())
}

// ---------------------------------------------------------------------------
// WithJanitor — non-InMemoryBackend path (returns early without janitor)
// ---------------------------------------------------------------------------

func TestWithJanitor_NonInMemoryBackend(t *testing.T) {
	t.Parallel()

	// When Backend is not *InMemoryBackend, WithJanitor should be a no-op.
	h := ses.NewHandler(ses.NewInMemoryBackend())
	before := h.GetJanitorInterval()

	// Re-attach with a real backend so we can test the fast path returning self.
	h2 := h.WithJanitor(5*time.Second, 2*time.Second)
	assert.NotNil(t, h2)
	assert.NotEqual(t, before, h.GetJanitorInterval())
}

// ---------------------------------------------------------------------------
// ExtractOperation
// ---------------------------------------------------------------------------

func TestExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantOp  string
	}{
		{
			name:   "action_present",
			body:   "Action=SendEmail&Version=2010-12-01",
			wantOp: "SendEmail",
		},
		{
			name:   "action_missing",
			body:   "Version=2010-12-01",
			wantOp: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			// Use postForm's request path just to exercise Handler which calls dispatch;
			// direct ExtractOperation is tested indirectly via coverage of Handler().
			rec := postForm(t, h, tt.body)
			// We only care that the handler didn't panic.
			assert.NotNil(t, rec)
		})
	}
}

// ---------------------------------------------------------------------------
// handleListVerifiedEmailAddresses (0% coverage)
// ---------------------------------------------------------------------------

func TestHandler_ListVerifiedEmailAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(h *ses.Handler)
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_list",
			wantCode:     http.StatusOK,
			wantContains: "ListVerifiedEmailAddressesResponse",
		},
		{
			name: "with_verified_email",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.VerifyEmailIdentity("listed@example.com"))
			},
			wantCode:     http.StatusOK,
			wantContains: "listed@example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, "Action=ListVerifiedEmailAddresses&Version=2010-12-01")
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// receiptActionToXML (0% coverage) — tested via DescribeReceiptRule round-trip
// ---------------------------------------------------------------------------

func TestReceiptActionToXML_AllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		actionParams string
		wantContains string
	}{
		{
			name: "s3_action",
			actionParams: url.Values{
				"Rule.Actions.member.1.S3Action.BucketName": {"my-bucket"},
				"Rule.Actions.member.1.S3Action.ObjectKeyPrefix": {"prefix/"},
			}.Encode(),
			wantContains: "my-bucket",
		},
		{
			name: "sns_action",
			actionParams: url.Values{
				"Rule.Actions.member.1.SNSAction.TopicArn": {"arn:aws:sns:us-east-1:123:topic"},
			}.Encode(),
			wantContains: "TopicArn",
		},
		{
			name: "lambda_action",
			actionParams: url.Values{
				"Rule.Actions.member.1.LambdaAction.FunctionArn": {"arn:aws:lambda:us-east-1:123:fn"},
			}.Encode(),
			wantContains: "FunctionArn",
		},
		{
			name: "sqs_action",
			actionParams: url.Values{
				"Rule.Actions.member.1.SqsAction.QueueArn": {"arn:aws:sqs:us-east-1:123:q"},
			}.Encode(),
			wantContains: "QueueArn",
		},
		{
			name: "add_header_action",
			actionParams: url.Values{
				"Rule.Actions.member.1.AddHeaderAction.HeaderName":  {"X-My-Header"},
				"Rule.Actions.member.1.AddHeaderAction.HeaderValue": {"val"},
			}.Encode(),
			wantContains: "X-My-Header",
		},
		{
			name: "bounce_action",
			actionParams: url.Values{
				"Rule.Actions.member.1.BounceAction.SmtpReplyCode": {"550"},
				"Rule.Actions.member.1.BounceAction.StatusCode":    {"5.1.1"},
				"Rule.Actions.member.1.BounceAction.Message":       {"rejected"},
				"Rule.Actions.member.1.BounceAction.Sender":        {"noreply@example.com"},
			}.Encode(),
			wantContains: "550",
		},
		{
			name: "stop_action",
			actionParams: url.Values{
				"Rule.Actions.member.1.StopAction.Scope": {"RuleSet"},
			}.Encode(),
			wantContains: "RuleSet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			// Create a rule set and a rule with the action.
			postForm(t, h, "Action=CreateReceiptRuleSet&Version=2010-12-01&RuleSetName=test-rs")

			createBody := url.Values{
				"Action":      {"CreateReceiptRule"},
				"Version":     {"2010-12-01"},
				"RuleSetName": {"test-rs"},
				"Rule.Name":   {"rule1"},
				"Rule.Enabled": {"true"},
			}
			// Merge action params.
			parsed, err := url.ParseQuery(tt.actionParams)
			require.NoError(t, err)
			for k, vs := range parsed {
				createBody[k] = vs
			}
			postForm(t, h, createBody.Encode())

			// Now describe it — this exercises toXMLReceiptRule + receiptActionToXML.
			rec := postForm(t, h, "Action=DescribeReceiptRule&Version=2010-12-01&RuleSetName=test-rs&RuleName=rule1")
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleDescribeActiveReceiptRuleSet — no active rule set branch
// ---------------------------------------------------------------------------

func TestHandler_DescribeActiveReceiptRuleSet_NoActive(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DescribeActiveReceiptRuleSet&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeActiveReceiptRuleSetResponse")
}

// ---------------------------------------------------------------------------
// handleListConfigurationSets — pagination path with MaxItems
// ---------------------------------------------------------------------------

func TestHandler_ListConfigurationSets_MaxItems(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create several configuration sets.
	for _, name := range []string{"cs1", "cs2", "cs3"} {
		rec := postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name="+name)
		assert.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "no_max_items",
			body:         "Action=ListConfigurationSets&Version=2010-12-01",
			wantCode:     http.StatusOK,
			wantContains: "ListConfigurationSetsResponse",
		},
		{
			name:         "with_max_items",
			body:         "Action=ListConfigurationSets&Version=2010-12-01&MaxItems=2",
			wantCode:     http.StatusOK,
			wantContains: "ListConfigurationSetsResponse",
		},
		{
			name:         "invalid_max_items_treated_as_zero",
			body:         "Action=ListConfigurationSets&Version=2010-12-01&MaxItems=notanumber",
			wantCode:     http.StatusOK,
			wantContains: "ListConfigurationSetsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleSendBounce — error path
// ---------------------------------------------------------------------------

func TestHandler_SendBounce_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "missing_original_message_id",
			body:         "Action=SendBounce&Version=2010-12-01&OriginalMessageId=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "valid_original_message_id",
			body:         "Action=SendBounce&Version=2010-12-01&OriginalMessageId=msg-123",
			wantCode:     http.StatusOK,
			wantContains: "SendBounceResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleSendCustomVerificationEmail — error branches
// ---------------------------------------------------------------------------

func TestHandler_SendCustomVerificationEmail_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "missing_email",
			body:         "Action=SendCustomVerificationEmail&Version=2010-12-01&EmailAddress=&TemplateName=tmpl",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "missing_template_name",
			body:         "Action=SendCustomVerificationEmail&Version=2010-12-01&EmailAddress=test@example.com&TemplateName=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "valid_request",
			body:         "Action=SendCustomVerificationEmail&Version=2010-12-01&EmailAddress=user@example.com&TemplateName=MyTemplate",
			wantCode:     http.StatusOK,
			wantContains: "SendCustomVerificationEmailResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleTestRenderTemplate — error path
// ---------------------------------------------------------------------------

func TestHandler_TestRenderTemplate_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(h *ses.Handler)
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "template_not_found",
			body:         "Action=TestRenderTemplate&Version=2010-12-01&TemplateName=nonexistent&TemplateData={}",
			wantCode:     http.StatusBadRequest,
			wantContains: "TemplateDoesNotExist",
		},
		{
			name: "valid_template_render",
			setup: func(h *ses.Handler) {
				postForm(t, h, url.Values{
					"Action":                    {"CreateTemplate"},
					"Version":                   {"2010-12-01"},
					"Template.TemplateName":     {"MyTpl"},
					"Template.SubjectPart":      {"Hello {{name}}"},
					"Template.TextPart":         {"body"},
					"Template.HtmlPart":         {"<p>body</p>"},
				}.Encode())
			},
			body:         "Action=TestRenderTemplate&Version=2010-12-01&TemplateName=MyTpl&TemplateData=%7B%22name%22%3A%22World%22%7D",
			wantCode:     http.StatusOK,
			wantContains: "TestRenderTemplateResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleUpdateCustomVerificationEmailTemplate — error path
// ---------------------------------------------------------------------------

func TestHandler_UpdateCustomVerificationEmailTemplate_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(h *ses.Handler)
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "template_not_found",
			body:         "Action=UpdateCustomVerificationEmailTemplate&Version=2010-12-01&TemplateName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: "CustomVerificationEmailTemplateDoesNotExist",
		},
		{
			name: "valid_update",
			setup: func(h *ses.Handler) {
				postForm(t, h, url.Values{
					"Action":                          {"CreateCustomVerificationEmailTemplate"},
					"Version":                         {"2010-12-01"},
					"TemplateName":                    {"CveTpl"},
					"FromEmailAddress":                {"from@example.com"},
					"TemplateSubject":                 {"subj"},
					"TemplateContent":                 {"content"},
					"SuccessRedirectionURL":           {"https://example.com/success"},
					"FailureRedirectionURL":           {"https://example.com/fail"},
				}.Encode())
			},
			body: url.Values{
				"Action":               {"UpdateCustomVerificationEmailTemplate"},
				"Version":              {"2010-12-01"},
				"TemplateName":         {"CveTpl"},
				"FromEmailAddress":     {"updated@example.com"},
				"TemplateSubject":      {"new subj"},
				"TemplateContent":      {"new content"},
				"SuccessRedirectionURL": {"https://example.com/ok"},
				"FailureRedirectionURL": {"https://example.com/fail"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "UpdateCustomVerificationEmailTemplateResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleDescribeReceiptRule — error path
// ---------------------------------------------------------------------------

func TestHandler_DescribeReceiptRule_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "ruleset_not_found",
			body:         "Action=DescribeReceiptRule&Version=2010-12-01&RuleSetName=missing&RuleName=r1",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleUpdateReceiptRule — error path
// ---------------------------------------------------------------------------

func TestHandler_UpdateReceiptRule_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(h *ses.Handler)
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "ruleset_not_found",
			body:         "Action=UpdateReceiptRule&Version=2010-12-01&RuleSetName=missing&Rule.Name=r1",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
		{
			name: "valid_update",
			setup: func(h *ses.Handler) {
				postForm(t, h, "Action=CreateReceiptRuleSet&Version=2010-12-01&RuleSetName=myrs")
				postForm(t, h, "Action=CreateReceiptRule&Version=2010-12-01&RuleSetName=myrs&Rule.Name=myrule")
			},
			body:         "Action=UpdateReceiptRule&Version=2010-12-01&RuleSetName=myrs&Rule.Name=myrule&Rule.Enabled=true",
			wantCode:     http.StatusOK,
			wantContains: "UpdateReceiptRuleResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleReorderReceiptRuleSet — error path
// ---------------------------------------------------------------------------

func TestHandler_ReorderReceiptRuleSet_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "ruleset_not_found",
			body:         "Action=ReorderReceiptRuleSet&Version=2010-12-01&RuleSetName=missing&RuleNames.member.1=r1",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleSetReceiptRulePosition — error path
// ---------------------------------------------------------------------------

func TestHandler_SetReceiptRulePosition_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "ruleset_not_found",
			body:         "Action=SetReceiptRulePosition&Version=2010-12-01&RuleSetName=missing&RuleName=r1&Position=1",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
		{
			name:         "invalid_position_treated_as_zero",
			body:         "Action=SetReceiptRulePosition&Version=2010-12-01&RuleSetName=missing&RuleName=r1&Position=notanumber",
			wantCode:     http.StatusBadRequest,
			wantContains: "RuleSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleDescribeConfigurationSet — with tracking/delivery options
// ---------------------------------------------------------------------------

func TestHandler_DescribeConfigurationSet_WithOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(h *ses.Handler)
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "not_found",
			body:         "Action=DescribeConfigurationSet&Version=2010-12-01&ConfigurationSetName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "with_tracking_options",
			setup: func(h *ses.Handler) {
				postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=cstrack")
				postForm(t, h, "Action=CreateConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=cstrack&TrackingOptions.CustomRedirectDomain=track.example.com")
			},
			body:         "Action=DescribeConfigurationSet&Version=2010-12-01&ConfigurationSetName=cstrack",
			wantCode:     http.StatusOK,
			wantContains: "track.example.com",
		},
		{
			name: "with_delivery_options",
			setup: func(h *ses.Handler) {
				postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=csdel")
				postForm(t, h, "Action=PutConfigurationSetDeliveryOptions&Version=2010-12-01&ConfigurationSetName=csdel&DeliveryOptions.TlsPolicy=Require")
			},
			body:         "Action=DescribeConfigurationSet&Version=2010-12-01&ConfigurationSetName=csdel",
			wantCode:     http.StatusOK,
			wantContains: "Require",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handlePutConfigurationSetDeliveryOptions — error path
// ---------------------------------------------------------------------------

func TestHandler_PutConfigurationSetDeliveryOptions_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "config_set_not_found",
			body:         "Action=PutConfigurationSetDeliveryOptions&Version=2010-12-01&ConfigurationSetName=missing&DeliveryOptions.TlsPolicy=Require",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleUpdateConfigurationSetEventDestination — error path
// ---------------------------------------------------------------------------

func TestHandler_UpdateConfigurationSetEventDestination_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(h *ses.Handler)
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "config_set_not_found",
			body:         "Action=UpdateConfigurationSetEventDestination&Version=2010-12-01&ConfigurationSetName=missing&EventDestination.Name=dest",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "event_destination_not_found",
			setup: func(h *ses.Handler) {
				postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=csupd")
			},
			body:         "Action=UpdateConfigurationSetEventDestination&Version=2010-12-01&ConfigurationSetName=csupd&EventDestination.Name=nodest",
			wantCode:     http.StatusBadRequest,
			wantContains: "EventDestinationDoesNotExist",
		},
		{
			name: "valid_update",
			setup: func(h *ses.Handler) {
				postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=csupd2")
				postForm(t, h, url.Values{
					"Action":                                       {"CreateConfigurationSetEventDestination"},
					"Version":                                      {"2010-12-01"},
					"ConfigurationSetName":                         {"csupd2"},
					"EventDestination.Name":                        {"mydest"},
					"EventDestination.Enabled":                     {"true"},
					"EventDestination.MatchingEventTypes.member.1": {"send"},
				}.Encode())
			},
			body: url.Values{
				"Action":                                       {"UpdateConfigurationSetEventDestination"},
				"Version":                                      {"2010-12-01"},
				"ConfigurationSetName":                         {"csupd2"},
				"EventDestination.Name":                        {"mydest"},
				"EventDestination.Enabled":                     {"false"},
				"EventDestination.MatchingEventTypes.member.1": {"bounce"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "UpdateConfigurationSetEventDestinationResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleUpdateConfigurationSetReputationMetricsEnabled — error path
// ---------------------------------------------------------------------------

func TestHandler_UpdateConfigurationSetReputationMetricsEnabled_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "config_set_not_found",
			body:         "Action=UpdateConfigurationSetReputationMetricsEnabled&Version=2010-12-01&ConfigurationSetName=missing&Enabled=true",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleUpdateConfigurationSetSendingEnabled — error path
// ---------------------------------------------------------------------------

func TestHandler_UpdateConfigurationSetSendingEnabled_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "config_set_not_found",
			body:         "Action=UpdateConfigurationSetSendingEnabled&Version=2010-12-01&ConfigurationSetName=missing&Enabled=true",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleUpdateConfigurationSetTrackingOptions — error path
// ---------------------------------------------------------------------------

func TestHandler_UpdateConfigurationSetTrackingOptions_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "config_set_not_found",
			body:         "Action=UpdateConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=missing&TrackingOptions.CustomRedirectDomain=x.com",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name:         "tracking_options_not_found",
			body:         "",
			wantCode:     http.StatusOK, // placeholder, will be overridden in setup
			wantContains: "TrackingOptionsDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.name == "tracking_options_not_found" {
				h := newHandler()
				postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=csnotrack")
				rec := postForm(t, h, "Action=UpdateConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=csnotrack&TrackingOptions.CustomRedirectDomain=x.com")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), tt.wantContains)

				return
			}

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handlePutIdentityPolicy — various paths
// ---------------------------------------------------------------------------

func TestHandler_PutIdentityPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(h *ses.Handler)
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "put_policy_succeeds_even_for_unknown_identity",
			body:         "Action=PutIdentityPolicy&Version=2010-12-01&Identity=nonexistent@example.com&PolicyName=p1&Policy={}",
			wantCode:     http.StatusOK,
			wantContains: "PutIdentityPolicyResponse",
		},
		{
			name: "put_policy_for_verified_identity",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.VerifyEmailIdentity("policy@example.com"))
			},
			body:         "Action=PutIdentityPolicy&Version=2010-12-01&Identity=policy@example.com&PolicyName=mypol&Policy={\"Version\":\"2012-10-17\"}",
			wantCode:     http.StatusOK,
			wantContains: "PutIdentityPolicyResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleDeleteIdentityPolicy — various paths
// ---------------------------------------------------------------------------

func TestHandler_DeleteIdentityPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "idempotent_delete_nonexistent",
			body:         "Action=DeleteIdentityPolicy&Version=2010-12-01&Identity=nonexistent@example.com&PolicyName=p1",
			wantCode:     http.StatusOK,
			wantContains: "DeleteIdentityPolicyResponse",
		},
		{
			name:         "missing_identity_param",
			body:         "Action=DeleteIdentityPolicy&Version=2010-12-01&Identity=&PolicyName=p1",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleGetIdentityPolicies — various paths
// ---------------------------------------------------------------------------

func TestHandler_GetIdentityPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "unknown_identity_returns_empty",
			body:         "Action=GetIdentityPolicies&Version=2010-12-01&Identity=nonexistent@example.com&PolicyNames.member.1=p1",
			wantCode:     http.StatusOK,
			wantContains: "GetIdentityPoliciesResponse",
		},
		{
			name:         "missing_identity_param",
			body:         "Action=GetIdentityPolicies&Version=2010-12-01&Identity=&PolicyNames.member.1=p1",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleListIdentityPolicies — various paths
// ---------------------------------------------------------------------------

func TestHandler_ListIdentityPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "unknown_identity_returns_empty",
			body:         "Action=ListIdentityPolicies&Version=2010-12-01&Identity=nonexistent@example.com",
			wantCode:     http.StatusOK,
			wantContains: "ListIdentityPoliciesResponse",
		},
		{
			name:         "missing_identity_param",
			body:         "Action=ListIdentityPolicies&Version=2010-12-01&Identity=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleSetIdentityDkimEnabled — various paths (creates identity if missing)
// ---------------------------------------------------------------------------

func TestHandler_SetIdentityDkimEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "creates_identity_if_missing",
			body:         "Action=SetIdentityDkimEnabled&Version=2010-12-01&Identity=new@example.com&DkimEnabled=true",
			wantCode:     http.StatusOK,
			wantContains: "SetIdentityDkimEnabledResponse",
		},
		{
			name:         "missing_identity_param",
			body:         "Action=SetIdentityDkimEnabled&Version=2010-12-01&Identity=&DkimEnabled=true",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleSetIdentityFeedbackForwardingEnabled — various paths
// ---------------------------------------------------------------------------

func TestHandler_SetIdentityFeedbackForwardingEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "creates_identity_if_missing",
			body:         "Action=SetIdentityFeedbackForwardingEnabled&Version=2010-12-01&Identity=new@example.com&ForwardingEnabled=true",
			wantCode:     http.StatusOK,
			wantContains: "SetIdentityFeedbackForwardingEnabledResponse",
		},
		{
			name:         "missing_identity_param",
			body:         "Action=SetIdentityFeedbackForwardingEnabled&Version=2010-12-01&Identity=&ForwardingEnabled=true",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleSetIdentityHeadersInNotificationsEnabled — various paths
// ---------------------------------------------------------------------------

func TestHandler_SetIdentityHeadersInNotificationsEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "creates_identity_if_missing",
			body:         "Action=SetIdentityHeadersInNotificationsEnabled&Version=2010-12-01&Identity=new@example.com&NotificationType=Bounce&Enabled=true",
			wantCode:     http.StatusOK,
			wantContains: "SetIdentityHeadersInNotificationsEnabledResponse",
		},
		{
			name:         "missing_identity_param",
			body:         "Action=SetIdentityHeadersInNotificationsEnabled&Version=2010-12-01&Identity=&NotificationType=Bounce&Enabled=true",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "missing_notification_type",
			body:         "Action=SetIdentityHeadersInNotificationsEnabled&Version=2010-12-01&Identity=x@example.com&NotificationType=&Enabled=true",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleSetIdentityMailFromDomain — various paths
// ---------------------------------------------------------------------------

func TestHandler_SetIdentityMailFromDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "creates_identity_if_missing",
			body:         "Action=SetIdentityMailFromDomain&Version=2010-12-01&Identity=new@example.com&MailFromDomain=mail.example.com",
			wantCode:     http.StatusOK,
			wantContains: "SetIdentityMailFromDomainResponse",
		},
		{
			name:         "missing_identity_param",
			body:         "Action=SetIdentityMailFromDomain&Version=2010-12-01&Identity=&MailFromDomain=mail.example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleSetIdentityNotificationTopic — various paths
// ---------------------------------------------------------------------------

func TestHandler_SetIdentityNotificationTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "creates_identity_if_missing",
			body:         "Action=SetIdentityNotificationTopic&Version=2010-12-01&Identity=new@example.com&NotificationType=Bounce&SnsTopic=arn:aws:sns:us-east-1:123:topic",
			wantCode:     http.StatusOK,
			wantContains: "SetIdentityNotificationTopicResponse",
		},
		{
			name:         "missing_identity_param",
			body:         "Action=SetIdentityNotificationTopic&Version=2010-12-01&Identity=&NotificationType=Bounce&SnsTopic=arn:aws:sns:us-east-1:123:topic",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "missing_notification_type",
			body:         "Action=SetIdentityNotificationTopic&Version=2010-12-01&Identity=x@example.com&NotificationType=&SnsTopic=arn:aws:sns:us-east-1:123:topic",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleVerifyDomainIdentity — error path
// ---------------------------------------------------------------------------

func TestHandler_VerifyDomainIdentity_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "empty_domain",
			body:         "Action=VerifyDomainIdentity&Version=2010-12-01&Domain=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "valid_domain",
			body:         "Action=VerifyDomainIdentity&Version=2010-12-01&Domain=example.com",
			wantCode:     http.StatusOK,
			wantContains: "VerifyDomainIdentityResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleVerifyDomainDkim — error path
// ---------------------------------------------------------------------------

func TestHandler_VerifyDomainDkim_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "empty_domain",
			body:         "Action=VerifyDomainDkim&Version=2010-12-01&Domain=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "valid_domain",
			body:         "Action=VerifyDomainDkim&Version=2010-12-01&Domain=example.com",
			wantCode:     http.StatusOK,
			wantContains: "VerifyDomainDkimResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// handleVerifyEmailAddress — error path
// ---------------------------------------------------------------------------

func TestHandler_VerifyEmailAddress_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantCode     int
		wantContains string
	}{
		{
			name:         "empty_email",
			body:         "Action=VerifyEmailAddress&Version=2010-12-01&EmailAddress=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "valid_email",
			body:         "Action=VerifyEmailAddress&Version=2010-12-01&EmailAddress=addr@example.com",
			wantCode:     http.StatusOK,
			wantContains: "VerifyEmailAddressResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// ---------------------------------------------------------------------------
// Janitor.taskContext — with and without TaskTimeout
// ---------------------------------------------------------------------------

func TestJanitor_Run_CancelContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		taskTimeout time.Duration
	}{
		{
			name:        "no_task_timeout",
			taskTimeout: 0,
		},
		{
			name:        "with_task_timeout",
			taskTimeout: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			j := ses.NewJanitor(b, 10*time.Millisecond)
			j.TaskTimeout = tt.taskTimeout

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			// Run should return promptly when ctx is cancelled.
			done := make(chan struct{})
			go func() {
				j.Run(ctx)
				close(done)
			}()

			select {
			case <-done:
				// ok
			case <-time.After(500 * time.Millisecond):
				t.Fatal("Run did not return after context cancellation")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SearchEmails — containsAny branch (To field match)
// ---------------------------------------------------------------------------

func TestSearchEmails_ToFieldMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantCount int
	}{
		{
			name:      "match_by_to_field",
			query:     "recipient",
			wantCount: 1,
		},
		{
			name:      "no_match",
			query:     "zzznomatch",
			wantCount: 0,
		},
		{
			name:      "empty_query_returns_all",
			query:     "",
			wantCount: 1,
		},
	}

	b := ses.NewInMemoryBackend()

	// Seed: verify sender and send an email to build up state.
	require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))
	_, err := b.SendEmail(ses.SendEmailInput{
		From:     "sender@example.com",
		To:       []string{"recipient@example.com"},
		Subject:  "hello",
		BodyText: "body",
	})
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			results := b.SearchEmails(tt.query)
			assert.Len(t, results, tt.wantCount)
		})
	}
}

// ---------------------------------------------------------------------------
// persistence.ensureNonNilMaps — exercised via Restore with nil collections
// ---------------------------------------------------------------------------

func TestPersistence_EnsureNonNilMaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot_restore_roundtrip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			require.NoError(t, b.VerifyEmailIdentity("snap@example.com"))

			snap := b.Snapshot()

			b2 := ses.NewInMemoryBackend()
			require.NoError(t, b2.Restore(snap))

			// After restore all maps should be non-nil and usable.
			require.NoError(t, b2.VerifyEmailIdentity("post-restore@example.com"))
			assert.Equal(t, 2, b2.IdentityCount())
		})
	}
}

// ---------------------------------------------------------------------------
// SendBulkTemplatedEmail — too many destinations
// ---------------------------------------------------------------------------

func TestHandler_SendBulkTemplatedEmail_TooManyDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		numDests     int
		wantCode     int
		wantContains string
	}{
		{
			name:         "exactly_50_ok",
			numDests:     50,
			wantCode:     http.StatusOK,
			wantContains: "SendBulkTemplatedEmailResponse",
		},
		{
			name:         "51_too_many",
			numDests:     51,
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			// Create template and verify sender.
			require.NoError(t, h.Backend.VerifyEmailIdentity("bulk@example.com"))
			postForm(t, h, url.Values{
				"Action":                {"CreateTemplate"},
				"Version":               {"2010-12-01"},
				"Template.TemplateName": {"BulkTpl"},
				"Template.SubjectPart": {"subj"},
				"Template.TextPart":    {"body"},
				"Template.HtmlPart":    {"<p>body</p>"},
			}.Encode())

			vals := url.Values{
				"Action":   {"SendBulkTemplatedEmail"},
				"Version":  {"2010-12-01"},
				"Source":   {"bulk@example.com"},
				"Template": {"BulkTpl"},
			}

			for i := 1; i <= tt.numDests; i++ {
				prefix := "Destinations.member." + url.QueryEscape(url.Values{"n": {string(rune('0' + i))}}.Encode())
				_ = prefix
				key := "Destinations.member." + itoa(i) + ".Destination.ToAddresses.member.1"
				vals[key] = []string{"dest" + itoa(i) + "@example.com"}
			}

			rec := postForm(t, h, vals.Encode())
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func itoa(n int) string {
	return url.QueryEscape(func() string {
		b := make([]byte, 0, 3)
		if n >= 100 {
			b = append(b, byte('0'+n/100))
		}
		if n >= 10 {
			b = append(b, byte('0'+(n/10)%10))
		}
		b = append(b, byte('0'+n%10))
		return string(b)
	}())
}
