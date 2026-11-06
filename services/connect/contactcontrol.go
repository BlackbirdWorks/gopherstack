package connect

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	recActive    = "recording"
	recSuspended = "suspended"
	recStopped   = "stopped"
	recAgent     = "AGENT"
	recIVR       = "IVR"
	recScreen    = "SCREEN"
)

var snsTopicARN = regexp.MustCompile(`^arn:aws[a-z-]*:sns:[a-z0-9-]+:\d{12}:[A-Za-z0-9_-]{1,256}(\.fifo)?$`)

func conflictErr(format string, args ...any) error {
	return apiErr(statusConflict, "ConflictException", format, args...)
}

// PauseContactInput is the PauseContact and ResumeContact request.
type PauseContactInput struct {
	ContactID     string `json:"ContactId"`
	InstanceID    string `json:"InstanceId"`
	ContactFlowID string `json:"ContactFlowId"`
}

func (b *Backend) pausableLocked(in *PauseContactInput) (*contactRec, error) {
	c, err := b.activeContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if c.Contact.Channel != channelTask {
		return nil, invalidRequestErr("only task contacts can be paused or resumed")
	}

	if in.ContactFlowID != "" {
		if err = b.requireFlow(c.InstanceID, in.ContactFlowID); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// PauseContact pauses an ongoing task contact.
func (b *Backend) PauseContact(in *PauseContactInput) (*empty, error) {
	b.mu.Lock("PauseContact")
	defer b.mu.Unlock()

	c, err := b.pausableLocked(in)
	if err != nil {
		return nil, err
	}

	if c.PausedAt != nil {
		return nil, conflictErr("contact %q is already paused", in.ContactID)
	}

	t := awstime.NewEpochTime(b.now())
	c.PausedAt, c.Contact.LastPausedTimestamp = &t, &t
	c.Contact.TotalPauseCount++
	b.touch(c)

	return &empty{}, nil
}

// ResumeContact resumes a paused task contact.
func (b *Backend) ResumeContact(in *PauseContactInput) (*empty, error) {
	b.mu.Lock("ResumeContact")
	defer b.mu.Unlock()

	c, err := b.pausableLocked(in)
	if err != nil {
		return nil, err
	}

	if c.PausedAt == nil {
		return nil, conflictErr("contact %q is not paused", in.ContactID)
	}

	b.resumeLocked(c)

	return &empty{}, nil
}

func (b *Backend) resumeLocked(c *contactRec) {
	t := awstime.NewEpochTime(b.now())
	c.Contact.TotalPauseDurationInSeconds += int32(t.Sub(c.PausedAt.Time).Seconds())
	c.Contact.LastResumedTimestamp, c.PausedAt = &t, nil
	b.touch(c)
}

// RecordingInput is the request of the contact recording operations.
type RecordingInput struct {
	VoiceRecordingConfiguration *struct {
		IvrRecordingTrack   string
		VoiceRecordingTrack string
	}
	ContactID            string `json:"ContactId"`
	InitialContactID     string `json:"InitialContactId"`
	InstanceID           string `json:"InstanceId"`
	ContactRecordingType string
}

func (b *Backend) recordableLocked(in *RecordingInput) (*contactRec, error) {
	c, err := b.activeContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if c.Contact.Channel != channelVoice {
		return nil, invalidRequestErr("only voice recordings are supported")
	}

	if c.Contact.InitialContactID != in.InitialContactID {
		return nil, invalidRequestErr("InitialContactId does not match contact %q", in.ContactID)
	}

	return c, nil
}

func recordingTypes(in *RecordingInput) ([]string, error) {
	cfg := in.VoiceRecordingConfiguration
	if cfg == nil {
		return nil, invalidParamErr("VoiceRecordingConfiguration is required")
	}

	var types []string

	if cfg.VoiceRecordingTrack != "" {
		if err := oneOf("VoiceRecordingTrack", cfg.VoiceRecordingTrack, "FROM_AGENT", "TO_AGENT", "ALL"); err != nil {
			return nil, err
		}

		types = append(types, recAgent)
	}

	if cfg.IvrRecordingTrack != "" {
		if err := oneOf("IvrRecordingTrack", cfg.IvrRecordingTrack, "ALL"); err != nil {
			return nil, err
		}

		types = append(types, recIVR)
	}

	if len(types) == 0 {
		return nil, invalidParamErr("a VoiceRecordingTrack or IvrRecordingTrack is required")
	}

	return types, nil
}

func (b *Backend) recordingLocation(inst string) string {
	r, ok := b.instances.Get(inst)
	if !ok {
		return ""
	}

	for i := range r.Storage {
		if s := r.Storage[i]; s.ResourceType == "CALL_RECORDINGS" && s.S3Config != nil {
			return strings.Trim(s.S3Config.BucketName+"/"+strings.Trim(s.S3Config.BucketPrefix, "/"), "/")
		}
	}

	return ""
}

func trackParticipant(typ, track string) string {
	switch {
	case typ == recIVR:
		return "ALL"
	case track == "FROM_AGENT":
		return "AGENT"
	case track == "TO_AGENT":
		return "CUSTOMER"
	}

	return "ALL"
}

// StartContactRecording starts the requested recordings of a voice contact; each can be started once.
func (b *Backend) StartContactRecording(in *RecordingInput) (*empty, error) {
	types, err := recordingTypes(in)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("StartContactRecording")
	defer b.mu.Unlock()

	c, err := b.recordableLocked(in)
	if err != nil {
		return nil, err
	}

	for _, typ := range types {
		if c.Recording[typ] != nil {
			return nil, invalidRequestErr("the %s recording of contact %q was already started", typ, in.ContactID)
		}
	}

	loc := b.recordingLocation(c.InstanceID)
	t := awstime.NewEpochTime(b.now())

	if c.Recording == nil {
		c.Recording = map[string]*recState{}
	}

	for _, typ := range types {
		track := in.VoiceRecordingConfiguration.VoiceRecordingTrack
		info := RecordingInfo{
			StartTimestamp: &t, MediaStreamType: "AUDIO", ParticipantType: trackParticipant(typ, track), Status: "AVAILABLE",
		}

		if loc != "" {
			info.StorageType, info.Location = storageS3, loc
		}

		c.Contact.Recordings = append(c.Contact.Recordings, info)
		c.Recording[typ] = &recState{State: recActive, Index: len(c.Contact.Recordings) - 1}
	}

	b.touch(c)

	return &empty{}, nil
}

func (b *Backend) transitionRecording(op string, in *RecordingInput, from []string, to string) (*empty, error) {
	typ := in.ContactRecordingType
	if typ == "" {
		typ = recAgent
	}

	if err := oneOf("ContactRecordingType", typ, recAgent, recIVR, recScreen); err != nil {
		return nil, err
	}

	b.mu.Lock(op)
	defer b.mu.Unlock()

	c, err := b.recordableLocked(in)
	if err != nil {
		return nil, err
	}

	st := c.Recording[typ]
	if st == nil || !slices.Contains(from, st.State) {
		return nil, invalidRequestErr("the %s recording of contact %q is not in a state that allows %s", typ, in.ContactID, op)
	}

	st.State = to
	if to == recStopped {
		t := awstime.NewEpochTime(b.now())
		c.Contact.Recordings[st.Index].StopTimestamp = &t
	}

	b.touch(c)

	return &empty{}, nil
}

// StopContactRecording ends a recording for good.
func (b *Backend) StopContactRecording(in *RecordingInput) (*empty, error) {
	return b.transitionRecording("StopContactRecording", in, []string{recActive, recSuspended}, recStopped)
}

// SuspendContactRecording pauses an active recording.
func (b *Backend) SuspendContactRecording(in *RecordingInput) (*empty, error) {
	return b.transitionRecording("SuspendContactRecording", in, []string{recActive}, recSuspended)
}

// ResumeContactRecording resumes a suspended recording.
func (b *Backend) ResumeContactRecording(in *RecordingInput) (*empty, error) {
	return b.transitionRecording("ResumeContactRecording", in, []string{recSuspended}, recActive)
}

// MonitorContactInput is the MonitorContact request.
type MonitorContactInput struct {
	ContactID                  string `json:"ContactId"`
	InstanceID                 string `json:"InstanceId"`
	UserID                     string `json:"UserId"`
	ClientToken                string
	AllowedMonitorCapabilities []string
}

type monitorContactOut struct {
	ContactArn string `json:"ContactArn"`
	ContactID  string `json:"ContactId"`
}

// MonitorContact starts a silent-monitoring contact for a user on a connected voice or chat contact.
func (b *Backend) MonitorContact(ctx context.Context, in *MonitorContactInput) (*monitorContactOut, error) {
	if len(in.AllowedMonitorCapabilities) > 2 {
		return nil, invalidRequestErr("AllowedMonitorCapabilities holds at most 2 items")
	}

	for _, m := range in.AllowedMonitorCapabilities {
		if err := oneOf("AllowedMonitorCapabilities", m, "SILENT_MONITOR", "BARGE"); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("MonitorContact")
	defer b.mu.Unlock()

	target, err := b.activeContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if target.Contact.Channel != channelVoice && target.Contact.Channel != channelChat {
		return nil, invalidRequestErr("only voice and chat contacts can be monitored")
	}

	if target.Contact.AgentInfo == nil {
		return nil, invalidRequestErr("contact %q is not connected to an agent", in.ContactID)
	}

	uid := userRefID(in.UserID)
	if !b.users.has(target.InstanceID, uid) {
		return nil, notFoundErr("User", in.UserID)
	}

	m := b.newContact(ctx, target.InstanceID, target.Contact.Channel, "MONITOR")
	m.Contact.InitialContactID, m.Contact.RelatedContactID = target.Contact.InitialContactID, target.Contact.ID
	m.Contact.AgentInfo = &ContactAgentInfo{ID: uid, ConnectedToAgentTimestamp: m.Contact.InitiationTimestamp}
	m.MonitorCaps = slices.Clone(in.AllowedMonitorCapabilities)
	b.contacts.put(m)

	return &monitorContactOut{ContactArn: m.Contact.Arn, ContactID: m.Contact.ID}, nil
}

// StartContactStreamingInput is the StartContactStreaming request.
type StartContactStreamingInput struct {
	ChatStreamingConfiguration *struct{ StreamingEndpointArn string }
	ContactID                  string `json:"ContactId"`
	InstanceID                 string `json:"InstanceId"`
	ClientToken                string
}

type startStreamingOut struct {
	StreamingID string `json:"StreamingId"`
}

// StartContactStreaming starts message streaming of a chat contact to an SNS topic.
func (b *Backend) StartContactStreaming(in *StartContactStreamingInput) (*startStreamingOut, error) {
	cfg := in.ChatStreamingConfiguration
	if cfg == nil || !snsTopicARN.MatchString(cfg.StreamingEndpointArn) {
		return nil, invalidParamErr("ChatStreamingConfiguration.StreamingEndpointArn must be an SNS topic ARN")
	}

	b.mu.Lock("StartContactStreaming")
	defer b.mu.Unlock()

	c, err := b.activeContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if c.Contact.Channel != channelChat {
		return nil, invalidRequestErr("message streaming is only available for chat contacts")
	}

	if r, _ := b.instanceLocked(in.InstanceID); r.Attributes["MESSAGE_STREAMING"] != boolTrue {
		return nil, invalidRequestErr("the MESSAGE_STREAMING instance attribute is not enabled")
	}

	id := uuid.NewString()
	if c.Streams == nil {
		c.Streams = map[string]string{}
	}

	c.Streams[id] = cfg.StreamingEndpointArn
	b.touch(c)

	return &startStreamingOut{StreamingID: id}, nil
}

// StopContactStreamingInput is the StopContactStreaming request.
type StopContactStreamingInput struct {
	ContactID   string `json:"ContactId"`
	InstanceID  string `json:"InstanceId"`
	StreamingID string `json:"StreamingId"`
}

// StopContactStreaming ends one message streaming configuration of a chat contact.
func (b *Backend) StopContactStreaming(in *StopContactStreamingInput) (*empty, error) {
	b.mu.Lock("StopContactStreaming")
	defer b.mu.Unlock()

	c, err := b.resourceContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if _, ok := c.Streams[in.StreamingID]; !ok {
		return nil, notFoundErr("Streaming", in.StreamingID)
	}

	delete(c.Streams, in.StreamingID)
	b.touch(c)

	return &empty{}, nil
}

// StartContactMediaProcessingInput is the StartContactMediaProcessing request.
type StartContactMediaProcessingInput struct {
	ContactID    string `json:"ContactId"`
	InstanceID   string `json:"InstanceId"`
	ProcessorArn string
	FailureMode  string
}

// StartContactMediaProcessing enables Lambda message processing on a chat contact.
func (b *Backend) StartContactMediaProcessing(
	ctx context.Context,
	in *StartContactMediaProcessingInput,
) (*empty, error) {
	if !lambdaARNRegexp.MatchString(in.ProcessorArn) {
		return nil, invalidParamErr("ProcessorArn %q is not a Lambda function ARN", in.ProcessorArn)
	}

	if in.FailureMode != "" {
		if err := oneOf("FailureMode", in.FailureMode,
			"DELIVER_UNPROCESSED_MESSAGE", "DO_NOT_DELIVER_UNPROCESSED_MESSAGE"); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("StartContactMediaProcessing")
	defer b.mu.Unlock()

	c, err := b.activeContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if c.Contact.Channel != channelChat {
		return nil, invalidRequestErr("message processing is only available for chat contacts")
	}

	if c.MediaProcessor != nil {
		return nil, invalidRequestErr("message processing is already active on contact %q", in.ContactID)
	}

	if b.resolvers != nil && !b.resolvers.LambdaFunctionExists(ctx, in.ProcessorArn) {
		return nil, notFoundErr("Lambda function", in.ProcessorArn)
	}

	c.MediaProcessor = &mediaProcessor{ProcessorArn: in.ProcessorArn, FailureMode: in.FailureMode}
	b.touch(c)

	return &empty{}, nil
}

// StopContactMediaProcessingInput is the StopContactMediaProcessing request.
type StopContactMediaProcessingInput struct {
	ContactID  string `json:"ContactId"`
	InstanceID string `json:"InstanceId"`
}

// StopContactMediaProcessing disables message processing on a chat contact.
func (b *Backend) StopContactMediaProcessing(in *StopContactMediaProcessingInput) (*empty, error) {
	b.mu.Lock("StopContactMediaProcessing")
	defer b.mu.Unlock()

	c, err := b.resourceContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if c.MediaProcessor == nil {
		return nil, invalidRequestErr("message processing is not active on contact %q", in.ContactID)
	}

	c.MediaProcessor = nil
	b.touch(c)

	return &empty{}, nil
}

func (b *Backend) finishContactLocked(c *contactRec, t awstime.EpochTime) {
	if c.PausedAt != nil {
		b.resumeLocked(c)
	}

	for _, st := range c.Recording {
		if st.State != recStopped {
			st.State = recStopped
			c.Contact.Recordings[st.Index].StopTimestamp = &t
		}
	}

	if c.Contact.Channel == channelChat {
		ms := t.Sub(c.Contact.InitiationTimestamp.Time).Milliseconds()
		multi := len(c.Participants) > 0
		c.Contact.ChatMetrics = &ChatMetrics{ChatContactMetrics: &ChatContactMetrics{
			ConversationCloseTimeInMillis: &ms, MultiParty: &multi,
		}}
	}

	for _, m := range b.contacts.in(c.InstanceID) {
		if m.active() && m.Contact.InitiationMethod == "MONITOR" && m.Contact.RelatedContactID == c.Contact.ID {
			m.Contact.DisconnectTimestamp, m.Contact.DisconnectReason = &t, "API"
			b.touch(m)
		}
	}
}
