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

func (e *contactEnv) chat(t *testing.T) string {
	t.Helper()

	out, err := e.f.client.StartChatContact(t.Context(), &sdk.StartChatContactInput{
		InstanceId:         aws.String(e.inst),
		ContactFlowId:      aws.String(e.flow),
		ParticipantDetails: &types.ParticipantDetails{DisplayName: aws.String("Cust")},
	})
	require.NoError(t, err)

	return aws.ToString(out.ContactId)
}

func (e *contactEnv) task(t *testing.T) string {
	t.Helper()

	out, err := e.f.client.StartTaskContact(t.Context(), &sdk.StartTaskContactInput{
		InstanceId: aws.String(e.inst), ContactFlowId: aws.String(e.flow), Name: aws.String("task"),
	})
	require.NoError(t, err)

	return aws.ToString(out.ContactId)
}

func (e *contactEnv) describe(t *testing.T, id string) *types.Contact {
	t.Helper()

	out, err := e.f.client.DescribeContact(
		t.Context(), &sdk.DescribeContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id)})
	require.NoError(t, err)

	return out.Contact
}

func (e *contactEnv) stop(t *testing.T, id string) {
	t.Helper()

	_, err := e.f.client.StopContact(
		t.Context(), &sdk.StopContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id)})
	require.NoError(t, err)
}

func (e *contactEnv) connectedVoice(t *testing.T) string {
	t.Helper()

	id := e.outbound(t, "+442071230000", true)
	_, err := e.f.client.AssociateContactWithUser(t.Context(), &sdk.AssociateContactWithUserInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(id), UserId: aws.String(e.user),
	})
	require.NoError(t, err)

	return id
}

func TestCreateContact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in       func(e *contactEnv) *sdk.CreateContactInput
		check    func(t *testing.T, c *types.Contact)
		name     string
		wantCode string
	}{
		{
			name: "task api with expiry",
			in: func(*contactEnv) *sdk.CreateContactInput {
				return &sdk.CreateContactInput{
					Channel: types.ChannelTask, InitiationMethod: types.ContactInitiationMethodApi,
					Name: aws.String("t"), Attributes: map[string]string{"k_1": "v"},
					ExpiryDurationInMinutes: aws.Int32(90),
				}
			},
			check: func(t *testing.T, c *types.Contact) {
				t.Helper()
				assert.Equal(t, types.ChannelTask, c.Channel)
				assert.Equal(t, map[string]string{"k_1": "v"}, c.Attributes)
				assert.Equal(t, int32(90),
					aws.ToInt32(c.SegmentAttributes["connect:ContactExpiry"].ValueMap["ExpiryDuration"].ValueInteger))
			},
		},
		{
			name: "task completed",
			in: func(*contactEnv) *sdk.CreateContactInput {
				return &sdk.CreateContactInput{
					Channel: types.ChannelTask, InitiationMethod: types.ContactInitiationMethodApi,
					InitiateAs: types.InitiateAsCompleted,
				}
			},
			check: func(t *testing.T, c *types.Contact) {
				t.Helper()
				assert.NotNil(t, c.DisconnectTimestamp)
			},
		},
		{
			name: "email connected to user",
			in: func(e *contactEnv) *sdk.CreateContactInput {
				return &sdk.CreateContactInput{
					Channel: types.ChannelEmail, InitiationMethod: types.ContactInitiationMethodOutbound,
					InitiateAs: types.InitiateAsConnectedToUser, UserInfo: &types.UserInfo{UserId: aws.String(e.user)},
				}
			},
			check: func(t *testing.T, c *types.Contact) {
				t.Helper()
				require.NotNil(t, c.AgentInfo)
			},
		},
		{
			name: "chat unsupported",
			in: func(*contactEnv) *sdk.CreateContactInput {
				return &sdk.CreateContactInput{
					Channel:          types.ChannelChat,
					InitiationMethod: types.ContactInitiationMethodApi,
				}
			},
			wantCode: "InvalidParameterException",
		},
		{
			name: "wrong method for channel",
			in: func(*contactEnv) *sdk.CreateContactInput {
				return &sdk.CreateContactInput{
					Channel:          types.ChannelTask,
					InitiationMethod: types.ContactInitiationMethodInbound,
				}
			},
			wantCode: "InvalidParameterException",
		},
		{
			name: "email outbound needs user",
			in: func(*contactEnv) *sdk.CreateContactInput {
				return &sdk.CreateContactInput{
					Channel: types.ChannelEmail, InitiationMethod: types.ContactInitiationMethodOutbound,
				}
			},
			wantCode: "InvalidRequestException",
		},
		{
			name: "completed only for tasks",
			in: func(*contactEnv) *sdk.CreateContactInput {
				return &sdk.CreateContactInput{
					Channel: types.ChannelEmail, InitiationMethod: types.ContactInitiationMethodFlow,
					InitiateAs: types.InitiateAsCompleted,
				}
			},
			wantCode: "InvalidRequestException",
		},
		{
			name: "bad attribute key",
			in: func(*contactEnv) *sdk.CreateContactInput {
				return &sdk.CreateContactInput{
					Channel: types.ChannelTask, InitiationMethod: types.ContactInitiationMethodApi,
					Attributes: map[string]string{"bad key": "v"},
				}
			},
			wantCode: "InvalidParameterException",
		},
		{
			name: "unknown user",
			in: func(*contactEnv) *sdk.CreateContactInput {
				return &sdk.CreateContactInput{
					Channel: types.ChannelEmail, InitiationMethod: types.ContactInitiationMethodAgentReply,
					UserInfo: &types.UserInfo{UserId: aws.String("nope")},
				}
			},
			wantCode: "ResourceNotFoundException",
		},
		{
			name: "unknown related contact",
			in: func(*contactEnv) *sdk.CreateContactInput {
				return &sdk.CreateContactInput{
					Channel: types.ChannelTask, InitiationMethod: types.ContactInitiationMethodApi,
					RelatedContactId: aws.String("nope"),
				}
			},
			wantCode: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			in := tt.in(e)
			in.InstanceId = aws.String(e.inst)

			out, err := e.f.client.CreateContact(t.Context(), in)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.ContactArn), aws.ToString(out.ContactId))
			tt.check(t, e.describe(t, aws.ToString(out.ContactId)))
		})
	}
}

func TestBatchPutContact(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	phone := func(n string) *types.Endpoint {
		return &types.Endpoint{Type: types.EndpointTypeTelephoneNumber, Address: aws.String(n)}
	}

	out, err := e.f.client.BatchPutContact(t.Context(), &sdk.BatchPutContactInput{
		InstanceId: aws.String(e.inst),
		ContactDataRequestList: []types.ContactDataRequest{
			{
				RequestIdentifier: aws.String("ok"),
				CustomerEndpoint:  phone("+442071230000"),
				QueueId:           aws.String(e.queue),
			},
			{
				RequestIdentifier: aws.String("ok"),
				CustomerEndpoint:  phone("+442071230001"),
				QueueId:           aws.String(e.queue),
			},
			{RequestIdentifier: aws.String("nocust"), QueueId: aws.String(e.queue)},
			{RequestIdentifier: aws.String("badcust"), CustomerEndpoint: phone("0207"), QueueId: aws.String(e.queue)},
			{RequestIdentifier: aws.String("noq"), CustomerEndpoint: phone("+442071230000")},
			{
				RequestIdentifier: aws.String("badq"),
				CustomerEndpoint:  phone("+442071230000"),
				QueueId:           aws.String("nope"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.SuccessfulRequestList, 1)

	got := map[string]types.FailureReasonCode{}
	for _, f := range out.FailedRequestList {
		got[aws.ToString(f.RequestIdentifier)] = f.FailureReasonCode
	}

	assert.Equal(t, types.FailureReasonCodeIdempotencyException, got["ok"])
	assert.Equal(t, types.FailureReasonCodeMissingCustomerEndpoint, got["nocust"])
	assert.Equal(t, types.FailureReasonCodeInvalidCustomerEndpoint, got["badcust"])
	assert.Equal(t, types.FailureReasonCodeMissingQueueIdAndSystemEndpoint, got["noq"])
	assert.Equal(t, types.FailureReasonCodeInvalidQueue, got["badq"])

	c := e.describe(t, aws.ToString(out.SuccessfulRequestList[0].ContactId))
	assert.Equal(t, types.ContactInitiationMethodOutbound, c.InitiationMethod)
	assert.Equal(t, e.queue, aws.ToString(c.QueueInfo.Id))

	_, err = e.f.client.BatchPutContact(t.Context(), &sdk.BatchPutContactInput{
		InstanceId: aws.String(e.inst), ContactDataRequestList: []types.ContactDataRequest{},
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))
}

func TestAssociateContactWithUser(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	queued := e.outbound(t, "+442071230000", true)
	unqueued := e.outbound(t, "+442071230001", false)

	tests := []struct {
		name     string
		contact  string
		user     string
		wantCode string
	}{
		{name: "unknown user", contact: queued, user: "nope", wantCode: "ResourceNotFoundException"},
		{name: "not queued", contact: unqueued, user: e.user, wantCode: "InvalidRequestException"},
		{name: "unknown contact", contact: "nope", user: e.user, wantCode: "ResourceNotFoundException"},
		{name: "ok", contact: queued, user: e.user},
		{name: "already offered", contact: queued, user: e.user, wantCode: "InvalidRequestException"},
	}

	for _, tt := range tests {
		_, err := e.f.client.AssociateContactWithUser(t.Context(), &sdk.AssociateContactWithUserInput{
			InstanceId: aws.String(e.inst), ContactId: aws.String(tt.contact), UserId: aws.String(tt.user),
		})
		assert.Equal(t, tt.wantCode, apiCode(err), tt.name)
	}

	assert.Equal(t, e.user, aws.ToString(e.describe(t, queued).AgentInfo.Id))
}

func TestCreateParticipantAndChatMetrics(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	chat := e.chat(t)
	voice := e.outbound(t, "+442071230000", false)
	bot := &types.ParticipantDetailsToAdd{
		DisplayName: aws.String("bot"), ParticipantRole: types.ParticipantRoleCustomBot,
	}

	out, err := e.f.client.CreateParticipant(t.Context(), &sdk.CreateParticipantInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(chat), ParticipantDetails: bot,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.ParticipantId))
	assert.NotEmpty(t, aws.ToString(out.ParticipantCredentials.ParticipantToken))
	_, perr := time.Parse("2006-01-02T15:04:05.000Z", aws.ToString(out.ParticipantCredentials.Expiry))
	require.NoError(t, perr)

	_, err = e.f.client.CreateParticipant(t.Context(), &sdk.CreateParticipantInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(chat),
		ParticipantDetails: &types.ParticipantDetailsToAdd{
			DisplayName: aws.String("x"), ParticipantRole: types.ParticipantRoleAgent,
		},
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = e.f.client.CreateParticipant(t.Context(), &sdk.CreateParticipantInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(voice), ParticipantDetails: bot,
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	e.f.clock.advance(2 * time.Second)
	e.stop(t, chat)

	m := e.describe(t, chat).ChatMetrics.ChatContactMetrics
	assert.Equal(t, int64(2000), aws.ToInt64(m.ConversationCloseTimeInMillis))
	assert.True(t, aws.ToBool(m.MultiParty))

	_, err = e.f.client.CreateParticipant(t.Context(), &sdk.CreateParticipantInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(chat), ParticipantDetails: bot,
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))
}

func TestCreatePersistentContactAssociation(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	past := e.chat(t)
	cur := e.chat(t)
	call := func(src string, rt types.RehydrationType, current string) (string, error) {
		out, err := e.f.client.CreatePersistentContactAssociation(t.Context(),
			&sdk.CreatePersistentContactAssociationInput{
				InstanceId: aws.String(e.inst), InitialContactId: aws.String(current),
				SourceContactId: aws.String(src), RehydrationType: rt,
			})
		if err != nil {
			return "", err
		}

		return aws.ToString(out.ContinuedFromContactId), nil
	}

	_, err := call(past, types.RehydrationTypeEntirePastSession, cur)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = call(past, types.RehydrationTypeFromSegment, cur)
	assert.Equal(t, "ContactNotTerminatedException", apiCode(err))

	_, err = call(cur, types.RehydrationTypeFromSegment, cur)
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	e.stop(t, past)

	from, err := call(past, types.RehydrationTypeEntirePastSession, cur)
	require.NoError(t, err)
	assert.Equal(t, past, from)
	assert.Equal(t, past, aws.ToString(e.describe(t, cur).PreviousContactId))

	_, err = call("nope", types.RehydrationTypeFromSegment, cur)
	assert.Equal(t, "ContactNotFoundException", apiCode(err))
}

func TestPauseResumeContact(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	task := e.task(t)
	voice := e.outbound(t, "+442071230000", false)
	pause := func(id string) error {
		_, err := e.f.client.PauseContact(t.Context(),
			&sdk.PauseContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id)})

		return err
	}
	resume := func(id string) error {
		_, err := e.f.client.ResumeContact(t.Context(),
			&sdk.ResumeContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(id)})

		return err
	}

	assert.Equal(t, "ConflictException", apiCode(resume(task)))
	assert.Equal(t, "InvalidRequestException", apiCode(pause(voice)))
	require.NoError(t, pause(task))
	assert.Equal(t, "ConflictException", apiCode(pause(task)))

	e.f.clock.advance(30 * time.Second)
	require.NoError(t, resume(task))

	c := e.describe(t, task)
	assert.Equal(t, int32(1), aws.ToInt32(c.TotalPauseCount))
	assert.Equal(t, int32(30), aws.ToInt32(c.TotalPauseDurationInSeconds))
	assert.NotNil(t, c.LastPausedTimestamp)
	assert.NotNil(t, c.LastResumedTimestamp)

	require.NoError(t, pause(task))
	e.f.clock.advance(10 * time.Second)
	e.stop(t, task)
	assert.Equal(t, int32(40), aws.ToInt32(e.describe(t, task).TotalPauseDurationInSeconds))
	assert.Equal(t, "InvalidRequestException", apiCode(pause(task)))
	assert.Equal(t, "ResourceNotFoundException", apiCode(pause("nope")))
}

func TestContactRecordingLifecycle(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	id := e.connectedVoice(t)
	initial := aws.ToString(e.describe(t, id).InitialContactId)
	base := func() (*string, *string, *string) { return aws.String(e.inst), aws.String(id), aws.String(initial) }
	start := func() error {
		i, c, ini := base()
		_, err := e.f.client.StartContactRecording(t.Context(), &sdk.StartContactRecordingInput{
			InstanceId: i, ContactId: c, InitialContactId: ini,
			VoiceRecordingConfiguration: &types.VoiceRecordingConfiguration{
				VoiceRecordingTrack: types.VoiceRecordingTrackAll,
			},
		})

		return err
	}
	suspend := func() error {
		i, c, ini := base()
		_, err := e.f.client.SuspendContactRecording(t.Context(), &sdk.SuspendContactRecordingInput{
			InstanceId: i, ContactId: c, InitialContactId: ini, ContactRecordingType: types.ContactRecordingTypeAgent,
		})

		return err
	}
	resume := func() error {
		i, c, ini := base()
		_, err := e.f.client.ResumeContactRecording(t.Context(), &sdk.ResumeContactRecordingInput{
			InstanceId: i, ContactId: c, InitialContactId: ini, ContactRecordingType: types.ContactRecordingTypeAgent,
		})

		return err
	}
	stop := func() error {
		i, c, ini := base()
		_, err := e.f.client.StopContactRecording(t.Context(), &sdk.StopContactRecordingInput{
			InstanceId: i, ContactId: c, InitialContactId: ini, ContactRecordingType: types.ContactRecordingTypeAgent,
		})

		return err
	}

	assert.Equal(t, "InvalidRequestException", apiCode(suspend()))
	require.NoError(t, start())
	assert.Equal(t, "InvalidRequestException", apiCode(start()))
	assert.Equal(t, "InvalidRequestException", apiCode(resume()))
	require.NoError(t, suspend())
	assert.Equal(t, "InvalidRequestException", apiCode(suspend()))
	require.NoError(t, resume())

	rec := e.describe(t, id).Recordings
	require.Len(t, rec, 1)
	assert.Equal(t, types.ParticipantTypeAll, rec[0].ParticipantType)
	assert.Nil(t, rec[0].StopTimestamp)

	require.NoError(t, stop())
	assert.Equal(t, "InvalidRequestException", apiCode(start()))
	assert.Equal(t, "InvalidRequestException", apiCode(resume()))
	assert.NotNil(t, e.describe(t, id).Recordings[0].StopTimestamp)

	other := e.connectedVoice(t)
	_, err := e.f.client.StartContactRecording(t.Context(), &sdk.StartContactRecordingInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(other), InitialContactId: aws.String("wrong"),
		VoiceRecordingConfiguration: &types.VoiceRecordingConfiguration{
			VoiceRecordingTrack: types.VoiceRecordingTrackFromAgent,
		},
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))
}

func TestStopContactClosesRecordings(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	id := e.connectedVoice(t)

	_, err := e.f.client.StartContactRecording(t.Context(), &sdk.StartContactRecordingInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(id),
		InitialContactId: aws.String(aws.ToString(e.describe(t, id).InitialContactId)),
		VoiceRecordingConfiguration: &types.VoiceRecordingConfiguration{
			IvrRecordingTrack: types.IvrRecordingTrackAll, VoiceRecordingTrack: types.VoiceRecordingTrackToAgent,
		},
	})
	require.NoError(t, err)
	e.stop(t, id)

	rec := e.describe(t, id).Recordings
	require.Len(t, rec, 2)

	for _, r := range rec {
		assert.NotNil(t, r.StopTimestamp)
	}
}

func TestMonitorContact(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	connected := e.connectedVoice(t)
	queued := e.outbound(t, "+442071230001", true)
	monitor := func(contact, user string, caps ...types.MonitorCapability) (*sdk.MonitorContactOutput, error) {
		return e.f.client.MonitorContact(t.Context(), &sdk.MonitorContactInput{
			InstanceId: aws.String(e.inst), ContactId: aws.String(contact), UserId: aws.String(user),
			AllowedMonitorCapabilities: caps,
		})
	}

	_, err := monitor(queued, e.user)
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = monitor(connected, "nope")
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = monitor(connected, e.user, "LISTEN")
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = monitor("nope", e.user)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	out, err := monitor(connected, e.user, types.MonitorCapabilitySilentMonitor, types.MonitorCapabilityBarge)
	require.NoError(t, err)

	m := e.describe(t, aws.ToString(out.ContactId))
	assert.Equal(t, types.ContactInitiationMethodMonitor, m.InitiationMethod)
	assert.Equal(t, connected, aws.ToString(m.RelatedContactId))
	assert.Equal(t, e.user, aws.ToString(m.AgentInfo.Id))
	assert.Nil(t, m.DisconnectTimestamp)

	e.stop(t, connected)
	assert.NotNil(t, e.describe(t, aws.ToString(out.ContactId)).DisconnectTimestamp)

	_, err = monitor(connected, e.user)
	assert.Equal(t, "InvalidRequestException", apiCode(err))
}

func TestContactStreaming(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	chat := e.chat(t)
	topic := "arn:aws:sns:us-east-1:" + testAccount + ":chat-stream"
	start := func(arn string) (*sdk.StartContactStreamingOutput, error) {
		return e.f.client.StartContactStreaming(t.Context(), &sdk.StartContactStreamingInput{
			InstanceId: aws.String(e.inst), ContactId: aws.String(chat), ClientToken: aws.String("tok-" + arn),
			ChatStreamingConfiguration: &types.ChatStreamingConfiguration{StreamingEndpointArn: aws.String(arn)},
		})
	}
	stop := func(id string) error {
		_, err := e.f.client.StopContactStreaming(t.Context(), &sdk.StopContactStreamingInput{
			InstanceId: aws.String(e.inst), ContactId: aws.String(chat), StreamingId: aws.String(id),
		})

		return err
	}

	_, err := start(topic)
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = e.f.client.UpdateInstanceAttribute(t.Context(), &sdk.UpdateInstanceAttributeInput{
		InstanceId: aws.String(e.inst), AttributeType: types.InstanceAttributeTypeMessageStreaming,
		Value: aws.String("true"),
	})
	require.NoError(t, err)

	_, err = start("not-an-arn")
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	out, err := start(topic)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.StreamingId))

	assert.Equal(t, "ResourceNotFoundException", apiCode(stop("nope")))
	require.NoError(t, stop(aws.ToString(out.StreamingId)))
	assert.Equal(t, "ResourceNotFoundException", apiCode(stop(aws.ToString(out.StreamingId))))

	voice := e.outbound(t, "+442071230000", false)
	_, err = e.f.client.StartContactStreaming(t.Context(), &sdk.StartContactStreamingInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(voice), ClientToken: aws.String("v"),
		ChatStreamingConfiguration: &types.ChatStreamingConfiguration{StreamingEndpointArn: aws.String(topic)},
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))
}

func TestContactMediaProcessing(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	chat := e.chat(t)
	fn := "arn:aws:lambda:us-east-1:" + testAccount + ":function:processor"
	start := func(arn string) error {
		_, err := e.f.client.StartContactMediaProcessing(t.Context(), &sdk.StartContactMediaProcessingInput{
			InstanceId: aws.String(e.inst), ContactId: aws.String(chat), ProcessorArn: aws.String(arn),
			FailureMode: types.ContactMediaProcessingFailureModeDeliverUnprocessedMessage,
		})

		return err
	}
	stop := func() error {
		_, err := e.f.client.StopContactMediaProcessing(t.Context(),
			&sdk.StopContactMediaProcessingInput{InstanceId: aws.String(e.inst), ContactId: aws.String(chat)})

		return err
	}

	assert.Equal(t, "InvalidRequestException", apiCode(stop()))
	assert.Equal(t, "InvalidParameterException", apiCode(start("nope")))
	require.NoError(t, start(fn))
	assert.Equal(t, "InvalidRequestException", apiCode(start(fn)))
	require.NoError(t, stop())
	assert.Equal(t, "InvalidRequestException", apiCode(stop()))
}

func TestContactControlStateSurvivesRestore(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	task := e.task(t)
	voice := e.connectedVoice(t)
	initial := aws.ToString(e.describe(t, voice).InitialContactId)

	_, err := e.f.client.PauseContact(t.Context(),
		&sdk.PauseContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(task)})
	require.NoError(t, err)

	_, err = e.f.client.StartContactRecording(t.Context(), &sdk.StartContactRecordingInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(voice), InitialContactId: aws.String(initial),
		VoiceRecordingConfiguration: &types.VoiceRecordingConfiguration{
			VoiceRecordingTrack: types.VoiceRecordingTrackAll,
		},
	})
	require.NoError(t, err)

	dst := e.f.restored(t)
	re := &contactEnv{f: dst, inst: e.inst, user: e.user}

	assert.Equal(t, int32(1), aws.ToInt32(re.describe(t, task).TotalPauseCount))
	require.Len(t, re.describe(t, voice).Recordings, 1)

	_, err = dst.client.ResumeContact(t.Context(),
		&sdk.ResumeContactInput{InstanceId: aws.String(e.inst), ContactId: aws.String(task)})
	require.NoError(t, err)

	_, err = dst.client.SuspendContactRecording(t.Context(), &sdk.SuspendContactRecordingInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(voice), InitialContactId: aws.String(initial),
	})
	require.NoError(t, err)
}
