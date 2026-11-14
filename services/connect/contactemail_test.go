package connect_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cn "github.com/blackbirdworks/gopherstack/services/connect"
)

var errSESDown = errors.New("ses down")

type fakeMailer struct {
	err  error
	sent []cn.EmailMessage
	mu   sync.Mutex
}

func (m *fakeMailer) SendEmail(_ context.Context, msg cn.EmailMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return m.err
	}

	m.sent = append(m.sent, msg)

	return nil
}

func (e *contactEnv) emailAddress(t *testing.T) {
	t.Helper()

	_, err := e.f.client.CreateEmailAddress(t.Context(), &sdk.CreateEmailAddressInput{
		InstanceId: aws.String(e.inst), EmailAddress: aws.String("support@example.com"),
	})
	require.NoError(t, err)
}

func raw(subject, body string) *types.InboundEmailContent {
	return &types.InboundEmailContent{
		MessageSourceType: types.InboundMessageSourceTypeRaw,
		RawMessage: &types.InboundRawMessage{
			Subject: aws.String(subject), Body: aws.String(body), ContentType: aws.String("text/plain"),
		},
	}
}

func TestStartEmailContact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut      func(in *sdk.StartEmailContactInput)
		name     string
		wantCode string
	}{
		{name: "ok", mut: func(*sdk.StartEmailContactInput) {}},
		{
			name: "unknown destination", wantCode: "ResourceNotFoundException",
			mut: func(in *sdk.StartEmailContactInput) { in.DestinationEmailAddress = aws.String("other@example.com") },
		},
		{
			name: "bad destination", wantCode: "InvalidParameterException",
			mut: func(in *sdk.StartEmailContactInput) { in.DestinationEmailAddress = aws.String("nope") },
		},
		{
			name:     "bad content type",
			wantCode: "InvalidParameterException",
			mut:      func(in *sdk.StartEmailContactInput) { in.EmailMessage.RawMessage.ContentType = aws.String("image/png") },
		},
		{
			name: "unknown flow", wantCode: "ResourceNotFoundException",
			mut: func(in *sdk.StartEmailContactInput) { in.ContactFlowId = aws.String("nope") },
		},
		{
			name: "unknown related", wantCode: "ResourceNotFoundException",
			mut: func(in *sdk.StartEmailContactInput) { in.RelatedContactId = aws.String("nope") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			e.emailAddress(t)

			in := &sdk.StartEmailContactInput{
				InstanceId: aws.String(e.inst), DestinationEmailAddress: aws.String("support@example.com"),
				FromEmailAddress: &types.EmailAddressInfo{EmailAddress: aws.String("cust@example.org")},
				EmailMessage:     raw("help", "please"), Name: aws.String("mail"),
				Attributes: map[string]string{"a": "1"},
				AdditionalRecipients: &types.InboundAdditionalRecipients{
					CcAddresses: []types.EmailAddressInfo{{EmailAddress: aws.String("cc@example.org")}},
				},
			}
			tt.mut(in)

			out, err := e.f.client.StartEmailContact(t.Context(), in)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)

			c := e.describe(t, aws.ToString(out.ContactId))
			assert.Equal(t, types.ChannelEmail, c.Channel)
			assert.Equal(t, types.ContactInitiationMethodInbound, c.InitiationMethod)
			assert.Equal(t, "cust@example.org", aws.ToString(c.CustomerEndpoint.Address))
			assert.Equal(t, types.EndpointTypeEmailAddress, c.SystemEndpoint.Type)
			require.Len(t, c.AdditionalEmailRecipients.CcList, 1)
			assert.Equal(t, "cc@example.org", aws.ToString(c.AdditionalEmailRecipients.CcList[0].Address))
		})
	}
}

func TestStartEmailContactCopiesRelatedAttributes(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	e.emailAddress(t)
	start := func(attrs map[string]string, related *string) string {
		out, err := e.f.client.StartEmailContact(t.Context(), &sdk.StartEmailContactInput{
			InstanceId: aws.String(e.inst), DestinationEmailAddress: aws.String("support@example.com"),
			FromEmailAddress: &types.EmailAddressInfo{EmailAddress: aws.String("cust@example.org")},
			EmailMessage:     raw("s", "b"), Attributes: attrs, RelatedContactId: related,
		})
		require.NoError(t, err)

		return aws.ToString(out.ContactId)
	}

	first := start(map[string]string{"a": "1", "b": "2"}, nil)
	second := start(map[string]string{"b": "3"}, aws.String(first))
	c := e.describe(t, second)
	assert.Equal(t, map[string]string{"a": "1", "b": "3"}, c.Attributes)
	assert.Equal(t, first, aws.ToString(c.RelatedContactId))
}

func TestStartOutboundEmailContact(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	mailer := &fakeMailer{}
	e.f.backend.SetMailer(mailer)
	e.emailAddress(t)

	create := func(method types.ContactInitiationMethod) string {
		out, err := e.f.client.CreateContact(t.Context(), &sdk.CreateContactInput{
			InstanceId: aws.String(e.inst), Channel: types.ChannelEmail, InitiationMethod: method,
			UserInfo: &types.UserInfo{UserId: aws.String(e.user)},
		})
		require.NoError(t, err)

		return aws.ToString(out.ContactId)
	}
	send := func(id string, from *types.EmailAddressInfo) error {
		_, err := e.f.client.StartOutboundEmailContact(t.Context(), &sdk.StartOutboundEmailContactInput{
			InstanceId: aws.String(e.inst), ContactId: aws.String(id), FromEmailAddress: from,
			DestinationEmailAddress: &types.EmailAddressInfo{EmailAddress: aws.String("cust@example.org")},
			EmailMessage: &types.OutboundEmailContent{
				MessageSourceType: types.OutboundMessageSourceTypeRaw,
				RawMessage: &types.OutboundRawMessage{
					Subject: aws.String("hi"), Body: aws.String("<p>x</p>"), ContentType: aws.String("text/html"),
				},
			},
			AdditionalRecipients: &types.OutboundAdditionalRecipients{
				CcEmailAddresses: []types.EmailAddressInfo{{EmailAddress: aws.String("cc@example.org")}},
			},
		})

		return err
	}
	known := &types.EmailAddressInfo{EmailAddress: aws.String("SUPPORT@example.com")}

	id := create(types.ContactInitiationMethodOutbound)
	assert.Equal(t, "ResourceNotFoundException",
		apiCode(send(id, &types.EmailAddressInfo{EmailAddress: aws.String("x@example.com")})))
	assert.Equal(t, "InvalidRequestException", apiCode(send(id, nil)))
	require.NoError(t, send(id, known))
	assert.Equal(t, "InvalidRequestException", apiCode(send(id, known)))

	require.Len(t, mailer.sent, 1)
	assert.Equal(t, []string{"cust@example.org"}, mailer.sent[0].To)
	assert.Equal(t, []string{"cc@example.org"}, mailer.sent[0].Cc)
	assert.Equal(t, "<p>x</p>", mailer.sent[0].HTML)

	c := e.describe(t, id)
	assert.Equal(t, "cust@example.org", aws.ToString(c.CustomerEndpoint.Address))
	assert.Equal(t, "SUPPORT@example.com", aws.ToString(c.SystemEndpoint.Address))

	task := e.task(t)
	assert.Equal(t, "InvalidRequestException", apiCode(send(task, known)))
	assert.Equal(t, "ResourceNotFoundException", apiCode(send("nope", known)))

	mailer.err = errSESDown
	failing := create(types.ContactInitiationMethodAgentReply)
	require.NoError(t, send(failing, known))
}

func TestSendOutboundEmail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut      func(in *sdk.SendOutboundEmailInput)
		name     string
		wantCode string
		wantSent int
	}{
		{name: "campaign", mut: func(*sdk.SendOutboundEmailInput) {}, wantSent: 1},
		{
			name: "general unsupported", wantCode: "InvalidRequestException",
			mut: func(in *sdk.SendOutboundEmailInput) { in.TrafficType = types.TrafficTypeGeneral },
		},
		{
			name: "campaign needed", wantCode: "InvalidRequestException",
			mut: func(in *sdk.SendOutboundEmailInput) { in.SourceCampaign = nil },
		},
		{
			name: "unknown from", wantCode: "ResourceNotFoundException",
			mut: func(in *sdk.SendOutboundEmailInput) {
				in.FromEmailAddress = &types.EmailAddressInfo{EmailAddress: aws.String("x@example.com")}
			},
		},
		{
			name: "template unsupported", wantCode: "InvalidRequestException",
			mut: func(in *sdk.SendOutboundEmailInput) {
				in.EmailMessage.MessageSourceType = types.OutboundMessageSourceTypeTemplate
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			mailer := &fakeMailer{}
			e.f.backend.SetMailer(mailer)
			e.emailAddress(t)

			in := &sdk.SendOutboundEmailInput{
				InstanceId: aws.String(e.inst), TrafficType: types.TrafficTypeCampaign,
				SourceCampaign:          &types.SourceCampaign{CampaignId: aws.String("camp")},
				FromEmailAddress:        &types.EmailAddressInfo{EmailAddress: aws.String("support@example.com")},
				DestinationEmailAddress: &types.EmailAddressInfo{EmailAddress: aws.String("cust@example.org")},
				EmailMessage: &types.OutboundEmailContent{
					MessageSourceType: types.OutboundMessageSourceTypeRaw,
					RawMessage: &types.OutboundRawMessage{
						Subject: aws.String("s"), Body: aws.String("b"), ContentType: aws.String("text/plain"),
					},
				},
			}
			tt.mut(in)

			_, err := e.f.client.SendOutboundEmail(t.Context(), in)
			assert.Equal(t, tt.wantCode, apiCode(err))
			assert.Len(t, mailer.sent, tt.wantSent)
		})
	}
}
