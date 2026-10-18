package connect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type contactEnv struct {
	f     *fixture
	inst  string
	flow  string
	queue string
	user  string
	phone string
}

func newContactEnv(t *testing.T) *contactEnv {
	t.Helper()

	f := newFixture(t)
	inst := f.newInstance(t, "contacts")
	queue, _, profile := f.defaultQueue(t, inst)

	flow, err := f.client.CreateContactFlow(t.Context(), &sdk.CreateContactFlowInput{
		InstanceId: aws.String(
			inst,
		),
		Name:    aws.String("outbound"),
		Type:    types.ContactFlowTypeContactFlow,
		Content: aws.String(validFlow),
	})
	require.NoError(t, err)

	instARN := "arn:aws:connect:us-east-1:" + testAccount + ":instance/" + inst
	search, err := f.client.SearchAvailablePhoneNumbers(t.Context(), &sdk.SearchAvailablePhoneNumbersInput{
		TargetArn: aws.String(
			instARN,
		),
		PhoneNumberCountryCode: types.PhoneNumberCountryCodeUs,
		PhoneNumberType:        types.PhoneNumberTypeDid,
	})
	require.NoError(t, err)

	number := aws.ToString(search.AvailableNumbersList[0].PhoneNumber)
	_, err = f.client.ClaimPhoneNumber(
		t.Context(),
		&sdk.ClaimPhoneNumberInput{TargetArn: aws.String(instARN), PhoneNumber: aws.String(number)},
	)
	require.NoError(t, err)

	return &contactEnv{
		f:     f,
		inst:  inst,
		flow:  aws.ToString(flow.ContactFlowId),
		queue: queue,
		user:  aws.ToString(f.newUser(t, inst, "agent1", profile).UserId),
		phone: number,
	}
}

func (e *contactEnv) outbound(t *testing.T, dest string, queued bool) string {
	t.Helper()

	in := &sdk.StartOutboundVoiceContactInput{
		InstanceId: aws.String(e.inst), ContactFlowId: aws.String(e.flow), DestinationPhoneNumber: aws.String(dest),
		SourcePhoneNumber: aws.String(e.phone), Attributes: map[string]string{"k": "v"}, Name: aws.String("call"),
	}
	if queued {
		in.QueueId = aws.String(e.queue)
	}

	out, err := e.f.client.StartOutboundVoiceContact(t.Context(), in)
	require.NoError(t, err)

	return aws.ToString(out.ContactId)
}

func TestOutboundVoiceContactLifecycle(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	id := e.outbound(t, "+442071230000", true)

	got, err := e.f.client.DescribeContact(
		t.Context(),
		&sdk.DescribeContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id)},
	)
	require.NoError(t, err)
	c := got.Contact
	assert.Equal(t, types.ChannelVoice, c.Channel)
	assert.Equal(t, types.ContactInitiationMethodOutbound, c.InitiationMethod)
	assert.Equal(t, "+442071230000", aws.ToString(c.CustomerEndpoint.Address))
	assert.Equal(t, e.phone, aws.ToString(c.SystemEndpoint.Address))
	assert.Equal(t, e.queue, aws.ToString(c.QueueInfo.Id))
	assert.Nil(t, c.DisconnectTimestamp)
	assert.Equal(t, map[string]string{"k": "v"}, c.Attributes)

	_, err = e.f.client.UpdateContactAttributes(t.Context(), &sdk.UpdateContactAttributesInput{
		InstanceId: aws.String(
			e.inst,
		),
		InitialContactId: aws.String(id),
		Attributes:       map[string]string{"stage": "ringing"},
	})
	require.NoError(t, err)

	attrs, err := e.f.client.GetContactAttributes(
		t.Context(),
		&sdk.GetContactAttributesInput{InstanceId: aws.String(e.inst), InitialContactId: aws.String(id)},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v", "stage": "ringing"}, attrs.Attributes)

	_, err = e.f.client.TagContact(
		t.Context(),
		&sdk.TagContactInput{
			InstanceId: aws.String(e.inst),
			ContactId:  aws.String(id),
			Tags:       map[string]string{"a": "1", "b": "2"},
		},
	)
	require.NoError(t, err)

	_, err = e.f.client.UntagContact(
		t.Context(),
		&sdk.UntagContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id), TagKeys: []string{"a"}},
	)
	require.NoError(t, err)

	_, err = e.f.client.UpdateContact(t.Context(), &sdk.UpdateContactInput{
		InstanceId: aws.String(
			e.inst,
		),
		ContactId:   aws.String(id),
		Description: aws.String("answered"),
		UserInfo:    &types.UserInfo{UserId: aws.String(e.user)},
	})
	require.NoError(t, err)

	got, err = e.f.client.DescribeContact(
		t.Context(),
		&sdk.DescribeContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id)},
	)
	require.NoError(t, err)
	assert.Equal(t, e.user, aws.ToString(got.Contact.AgentInfo.Id))
	assert.NotNil(t, got.Contact.AgentInfo.ConnectedToAgentTimestamp)
	assert.Equal(t, map[string]string{"b": "2"}, got.Contact.Tags)

	e.f.clock.advance(time.Minute)

	_, err = e.f.client.StopContact(
		t.Context(),
		&sdk.StopContactInput{
			InstanceId:       aws.String(e.inst),
			ContactId:        aws.String(id),
			DisconnectReason: &types.DisconnectReason{Code: aws.String("AGENT_DISCONNECT")},
		},
	)
	require.NoError(t, err)

	got, err = e.f.client.DescribeContact(
		t.Context(),
		&sdk.DescribeContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id)},
	)
	require.NoError(t, err)
	assert.NotNil(t, got.Contact.DisconnectTimestamp)
	assert.Equal(t, "AGENT_DISCONNECT", aws.ToString(got.Contact.DisconnectReason))

	_, err = e.f.client.StopContact(
		t.Context(),
		&sdk.StopContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id)},
	)
	assert.Equal(t, "ContactNotFoundException", apiCode(err))

	_, err = e.f.client.UpdateContact(
		t.Context(),
		&sdk.UpdateContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id), Name: aws.String("late")},
	)
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = e.f.client.DescribeContact(
		t.Context(),
		&sdk.DescribeContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String("missing")},
	)
	assert.Equal(t, "ContactNotFoundException", apiCode(err))
}

func TestOutboundVoiceValidation(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)

	tests := []struct {
		mutate func(in *sdk.StartOutboundVoiceContactInput)
		name   string
		code   string
	}{
		{
			name:   "bad destination",
			mutate: func(in *sdk.StartOutboundVoiceContactInput) { in.DestinationPhoneNumber = aws.String("5551234") },
			code:   "InvalidParameterException",
		},
		{
			name:   "unclaimed source",
			mutate: func(in *sdk.StartOutboundVoiceContactInput) { in.SourcePhoneNumber = aws.String("+14155550100") },
			code:   "InvalidParameterException",
		},
		{
			name:   "no source or queue",
			mutate: func(in *sdk.StartOutboundVoiceContactInput) { in.SourcePhoneNumber = nil },
			code:   "InvalidParameterException",
		},
		{
			name:   "unknown flow",
			mutate: func(in *sdk.StartOutboundVoiceContactInput) { in.ContactFlowId = aws.String("nope") },
			code:   "ResourceNotFoundException",
		},
		{
			name:   "unknown queue",
			mutate: func(in *sdk.StartOutboundVoiceContactInput) { in.QueueId = aws.String("nope") },
			code:   "ResourceNotFoundException",
		},
	}

	t.Run("cases", func(t *testing.T) {
		t.Parallel()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				in := &sdk.StartOutboundVoiceContactInput{
					InstanceId: aws.String(
						e.inst,
					),
					ContactFlowId:          aws.String(e.flow),
					DestinationPhoneNumber: aws.String("+442071230000"),
					SourcePhoneNumber:      aws.String(e.phone),
				}
				tt.mutate(in)
				_, err := e.f.client.StartOutboundVoiceContact(t.Context(), in)
				assert.Equal(t, tt.code, apiCode(err))
			})
		}
	})

	t.Run("outbound disabled", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		inst := f.newInstance(t, "nooutbound")
		_, err := f.client.UpdateInstanceAttribute(t.Context(), &sdk.UpdateInstanceAttributeInput{
			InstanceId: aws.String(
				inst,
			),
			AttributeType: types.InstanceAttributeTypeOutboundCalls,
			Value:         aws.String("false"),
		})
		require.NoError(t, err)

		_, err = f.client.StartOutboundVoiceContact(t.Context(), &sdk.StartOutboundVoiceContactInput{
			InstanceId: aws.String(
				inst,
			),
			ContactFlowId:          aws.String("f"),
			DestinationPhoneNumber: aws.String("+442071230000"),
			SourcePhoneNumber:      aws.String("+14155550100"),
		})
		assert.Equal(t, "OutboundContactNotPermittedException", apiCode(err))
	})
}

func TestChatAndTaskContacts(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)

	chat, err := e.f.client.StartChatContact(t.Context(), &sdk.StartChatContactInput{
		InstanceId:         inst,
		ContactFlowId:      aws.String(e.flow),
		ParticipantDetails: &types.ParticipantDetails{DisplayName: aws.String("Cust")},
		InitialMessage:     &types.ChatMessage{ContentType: aws.String("text/plain"), Content: aws.String("hello")},
		Attributes:         map[string]string{"channel": "web"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(chat.ParticipantToken))
	assert.NotEmpty(t, aws.ToString(chat.ParticipantId))

	_, err = e.f.client.StartChatContact(t.Context(), &sdk.StartChatContactInput{
		InstanceId:         inst,
		ContactFlowId:      aws.String(e.flow),
		ParticipantDetails: &types.ParticipantDetails{DisplayName: aws.String("Cust")},
		PersistentChat: &types.PersistentChat{
			RehydrationType: types.RehydrationTypeEntirePastSession,
			SourceContactId: chat.ContactId,
		},
	})
	assert.Equal(t, "ContactNotTerminatedException", apiCode(err))

	_, err = e.f.client.StopContact(t.Context(), &sdk.StopContactInput{InstanceId: inst, ContactId: chat.ContactId})
	require.NoError(t, err)

	next, err := e.f.client.StartChatContact(t.Context(), &sdk.StartChatContactInput{
		InstanceId:         inst,
		ContactFlowId:      aws.String(e.flow),
		ParticipantDetails: &types.ParticipantDetails{DisplayName: aws.String("Cust")},
		PersistentChat: &types.PersistentChat{
			RehydrationType: types.RehydrationTypeEntirePastSession,
			SourceContactId: chat.ContactId,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(chat.ContactId), aws.ToString(next.ContinuedFromContactId))

	desc, err := e.f.client.DescribeContact(
		t.Context(),
		&sdk.DescribeContactInput{InstanceId: inst, ContactId: next.ContactId},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(chat.ContactId), aws.ToString(desc.Contact.PreviousContactId))
	assert.Equal(t, aws.ToString(chat.ContactId), aws.ToString(desc.Contact.InitialContactId))

	_, err = e.f.client.StartChatContact(
		t.Context(),
		&sdk.StartChatContactInput{
			InstanceId:         inst,
			ContactFlowId:      aws.String(e.flow),
			ParticipantDetails: &types.ParticipantDetails{DisplayName: aws.String("  ")},
		},
	)
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	when := e.f.clock.now().Add(time.Hour)
	task, err := e.f.client.StartTaskContact(t.Context(), &sdk.StartTaskContactInput{
		InstanceId:    inst,
		ContactFlowId: aws.String(e.flow),
		Name:          aws.String("Call back"),
		ScheduledTime: aws.Time(when),
	})
	require.NoError(t, err)

	td, err := e.f.client.DescribeContact(
		t.Context(),
		&sdk.DescribeContactInput{InstanceId: inst, ContactId: task.ContactId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ChannelTask, td.Contact.Channel)
	assert.True(t, td.Contact.ScheduledTimestamp.Equal(when))

	_, err = e.f.client.StartTaskContact(t.Context(), &sdk.StartTaskContactInput{
		InstanceId:    inst,
		ContactFlowId: aws.String(e.flow),
		Name:          aws.String("x"),
		ScheduledTime: aws.Time(e.f.clock.now().Add(-time.Hour)),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = e.f.client.StartTaskContact(
		t.Context(),
		&sdk.StartTaskContactInput{
			InstanceId:     inst,
			ContactFlowId:  aws.String(e.flow),
			Name:           aws.String("x"),
			TaskTemplateId: aws.String("tmpl"),
		},
	)
	assert.Equal(t, "InvalidRequestException", apiCode(err))
}

func TestContactIdempotency(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	in := &sdk.StartOutboundVoiceContactInput{
		InstanceId: aws.String(
			e.inst,
		),
		ContactFlowId:          aws.String(e.flow),
		DestinationPhoneNumber: aws.String("+442071230000"),
		SourcePhoneNumber:      aws.String(e.phone),
		ClientToken:            aws.String("same-token"),
	}

	first, err := e.f.client.StartOutboundVoiceContact(t.Context(), in)
	require.NoError(t, err)

	second, err := e.f.client.StartOutboundVoiceContact(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.ContactId), aws.ToString(second.ContactId))

	changed := *in
	changed.DestinationPhoneNumber = aws.String("+442071239999")
	_, err = e.f.client.StartOutboundVoiceContact(t.Context(), &changed)
	assert.Equal(t, "IdempotencyException", apiCode(err))
}

func TestSearchContacts(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)
	start := e.f.clock.now().Add(-time.Hour)

	a := e.outbound(t, "+442071230001", true)
	e.f.clock.advance(time.Minute)
	b := e.outbound(t, "+442071230002", false)
	e.f.clock.advance(time.Minute)

	chat, err := e.f.client.StartChatContact(t.Context(), &sdk.StartChatContactInput{
		InstanceId:         inst,
		ContactFlowId:      aws.String(e.flow),
		ParticipantDetails: &types.ParticipantDetails{DisplayName: aws.String("c")},
		Attributes:         map[string]string{"tier": "gold"},
	})
	require.NoError(t, err)

	_, err = e.f.client.UpdateContactAttributes(
		t.Context(),
		&sdk.UpdateContactAttributesInput{
			InstanceId:       inst,
			InitialContactId: chat.ContactId,
			Attributes:       map[string]string{"tier": "gold"},
		},
	)
	require.NoError(t, err)

	timeRange := &types.SearchContactsTimeRange{
		Type:      types.SearchContactsTimeRangeTypeInitiationTimestamp,
		StartTime: aws.Time(start),
		EndTime:   aws.Time(e.f.clock.now().Add(time.Hour)),
	}

	ids := func(criteria *types.SearchCriteria, sort *types.Sort) []string {
		out, serr := e.f.client.SearchContacts(
			t.Context(),
			&sdk.SearchContactsInput{InstanceId: inst, TimeRange: timeRange, SearchCriteria: criteria, Sort: sort},
		)
		require.NoError(t, serr)

		got := make([]string, 0, len(out.Contacts))
		for _, c := range out.Contacts {
			got = append(got, aws.ToString(c.Id))
		}

		return got
	}

	assert.Equal(t, []string{a, b, aws.ToString(chat.ContactId)}, ids(nil, nil))
	assert.Equal(
		t,
		[]string{aws.ToString(chat.ContactId), b, a},
		ids(nil, &types.Sort{FieldName: types.SortableFieldNameInitiationTimestamp, Order: types.SortOrderDescending}),
	)
	assert.Equal(
		t,
		[]string{aws.ToString(chat.ContactId)},
		ids(&types.SearchCriteria{Channels: []types.Channel{types.ChannelChat}}, nil),
	)
	assert.Equal(t, []string{a}, ids(&types.SearchCriteria{QueueIds: []string{e.queue}}, nil))
	assert.Equal(
		t,
		[]string{aws.ToString(chat.ContactId)},
		ids(&types.SearchCriteria{SearchableContactAttributes: &types.SearchableContactAttributes{
			MatchType: types.SearchContactsMatchTypeMatchAll,
			Criteria:  []types.SearchableContactAttributesCriteria{{Key: aws.String("tier"), Values: []string{"gold"}}},
		}}, nil),
	)

	_, err = e.f.client.SearchContacts(t.Context(), &sdk.SearchContactsInput{
		InstanceId:     inst,
		TimeRange:      timeRange,
		SearchCriteria: &types.SearchCriteria{ContactTags: &types.ControlPlaneTagFilter{}},
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = e.f.client.SearchContacts(t.Context(), &sdk.SearchContactsInput{
		InstanceId: inst,
		TimeRange: &types.SearchContactsTimeRange{
			Type:      types.SearchContactsTimeRangeTypeInitiationTimestamp,
			StartTime: aws.Time(start),
			EndTime:   aws.Time(start.Add(-time.Hour)),
		},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}
