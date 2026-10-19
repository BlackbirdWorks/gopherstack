package connect

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	maxAttributeBytes   = 32768
	maxContactNameLen   = 512
	maxContactDescLen   = 4096
	maxRingTimeout      = 600
	maxScheduleAhead    = 6 * 24 * time.Hour
	minChatMinutes      = 60
	maxChatMinutes      = 10080
	maxInitialChatBytes = 1024
	tokenBytes          = 48
)

// StartOutboundVoiceContactInput is the StartOutboundVoiceContact request.
type StartOutboundVoiceContactInput struct {
	RingTimeoutInSeconds   *int32
	Attributes             map[string]string
	DestinationPhoneNumber string
	InstanceID             string `json:"InstanceId"`
	ContactFlowID          string `json:"ContactFlowId"`
	SourcePhoneNumber      string
	QueueID                string `json:"QueueId"`
	CampaignID             string `json:"CampaignId"`
	Name                   string
	Description            string
	RelatedContactID       string `json:"RelatedContactId"`
	ClientToken            string
	OutboundStrategy       json.RawMessage
	References             json.RawMessage
}

// ParticipantDetails names a chat participant.
type ParticipantDetails struct {
	DisplayName string `json:"DisplayName"`
}

// ChatMessage is the first message of a chat.
type ChatMessage struct {
	ContentType string `json:"ContentType"`
	Content     string `json:"Content"`
}

// PersistentChat continues an earlier chat.
type PersistentChat struct {
	RehydrationType string `json:"RehydrationType"`
	SourceContactID string `json:"SourceContactId"`
}

// StartChatContactInput is the StartChatContact request.
type StartChatContactInput struct {
	ParticipantDetails    *ParticipantDetails
	InitialMessage        *ChatMessage
	PersistentChat        *PersistentChat
	Attributes            map[string]string
	ChatDurationInMinutes *int32
	InstanceID            string `json:"InstanceId"`
	ContactFlowID         string `json:"ContactFlowId"`
	CustomerID            string `json:"CustomerId"`
	RelatedContactID      string `json:"RelatedContactId"`
	ClientToken           string
	SegmentAttributes     json.RawMessage
}

// StartTaskContactInput is the StartTaskContact request.
type StartTaskContactInput struct {
	ScheduledTime     *awstime.EpochTime
	Attributes        map[string]string
	PreviousContactID string `json:"PreviousContactId"`
	InstanceID        string `json:"InstanceId"`
	Name              string
	Description       string
	ContactFlowID     string `json:"ContactFlowId"`
	RelatedContactID  string `json:"RelatedContactId"`
	QuickConnectID    string `json:"QuickConnectId"`
	TaskTemplateID    string `json:"TaskTemplateId"`
	ClientToken       string
	SegmentAttributes json.RawMessage
	Attachments       json.RawMessage
	References        json.RawMessage
}

// ContactRefInput addresses one contact in body-addressed contact operations.
type ContactRefInput struct {
	UserInfo *struct {
		UserID string `json:"UserId"`
	}
	Description      *string
	Tags             map[string]string
	CustomerEndpoint *EndpointInfo
	SystemEndpoint   *EndpointInfo
	QueueInfo        *struct {
		ID string `json:"Id"`
	}
	DisconnectReason *struct {
		Code string `json:"Code"`
	}
	Name              *string
	Attributes        map[string]string
	ContactID         string `json:"ContactId"        path:"ContactId"`
	InstanceID        string `json:"InstanceId"       path:"InstanceId"`
	InitialContactID  string `json:"InitialContactId"`
	SegmentAttributes json.RawMessage
	References        json.RawMessage
}

// ContactPathInput addresses one contact by path.
type ContactPathInput struct {
	ContactID string `json:"-" path:"ContactId"`
	instPath
	TagKeys []string `json:"-"                  query:"TagKeys"`
}

func checkAttributes(attrs map[string]string) error {
	total := 0

	for k, v := range attrs {
		if k == "" || len(k) > maxAttributeBytes || len(v) > maxAttributeBytes {
			return invalidParamErr("attribute %q is not valid", k)
		}

		total += len(k) + len(v)
	}

	if total > maxAttributeBytes {
		return invalidParamErr("attributes exceed %d bytes", maxAttributeBytes)
	}

	return nil
}

func checkContactText(name, desc string) error {
	if len(name) > maxContactNameLen {
		return invalidParamErr("Name must be at most %d characters", maxContactNameLen)
	}

	if len(desc) > maxContactDescLen {
		return invalidParamErr("Description must be at most %d characters", maxContactDescLen)
	}

	return nil
}

func (b *Backend) requireFlow(inst, flowID string) error {
	f, err := b.flows.get(inst, flowID)
	if err != nil {
		return err
	}

	if f.Flow.Status != flowPublished {
		return invalidRequestErr("ContactFlow %q is not published", flowID)
	}

	return nil
}

func (b *Backend) newContact(ctx context.Context, inst string, channel, method string) *contactRec {
	id := uuid.NewString()
	now := awstime.NewEpochTime(b.now())

	return &contactRec{InstanceID: inst, Contact: Contact{
		InitiationTimestamp: &now, LastUpdateTimestamp: &now, Arn: b.buildARN(ctx, "instance/"+inst+"/contact/"+id),
		Channel: channel, ID: id, InitialContactID: id, InitiationMethod: method,
	}}
}

func (b *Backend) contactLocked(instRef, id string) (*contactRec, error) {
	r, err := b.instanceLocked(instRef)
	if err != nil {
		return nil, err
	}

	c, err := b.contacts.get(r.Instance.ID, id)
	if err != nil {
		return nil, contactNotFoundErr(id)
	}

	return c, nil
}

func (b *Backend) touch(c *contactRec) {
	t := awstime.NewEpochTime(b.now())
	c.Contact.LastUpdateTimestamp = &t
}

// StartOutboundVoiceContact places an outbound voice contact.
func (b *Backend) StartOutboundVoiceContact(
	ctx context.Context,
	in *StartOutboundVoiceContactInput,
) (*contactIDOut, error) {
	if !e164Pattern.MatchString(in.DestinationPhoneNumber) {
		return nil, invalidParamErr("DestinationPhoneNumber must be an E.164 number")
	}

	if in.SourcePhoneNumber != "" && !e164Pattern.MatchString(in.SourcePhoneNumber) {
		return nil, invalidParamErr("SourcePhoneNumber must be an E.164 number")
	}

	if in.RingTimeoutInSeconds != nil && (*in.RingTimeoutInSeconds < 0 || *in.RingTimeoutInSeconds > maxRingTimeout) {
		return nil, invalidParamErr("RingTimeoutInSeconds must be between 0 and %d", maxRingTimeout)
	}

	if err := firstError(
		checkAttributes(in.Attributes),
		checkContactText(in.Name, in.Description),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("StartOutboundVoiceContact")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	if !r.Instance.OutboundCallsEnabled {
		return nil, apiErr(
			statusForbidden,
			"OutboundContactNotPermittedException",
			"outbound calls are not enabled for the instance",
		)
	}

	if err = b.requireFlow(inst, in.ContactFlowID); err != nil {
		return nil, err
	}

	source, err := b.outboundSource(inst, in)
	if err != nil {
		return nil, err
	}

	c := b.newContact(ctx, inst, channelVoice, "OUTBOUND")
	c.FlowID, c.References = in.ContactFlowID, in.References
	c.Contact.Name, c.Contact.Description = in.Name, in.Description
	c.Contact.Attributes = cloneTags(in.Attributes)
	c.Contact.RelatedContactID = in.RelatedContactID
	c.Contact.OutboundStrategy = in.OutboundStrategy
	c.Contact.RingStartTimestamp = c.Contact.InitiationTimestamp
	c.Contact.CustomerEndpoint = &EndpointInfo{Address: in.DestinationPhoneNumber, Type: "TELEPHONE_NUMBER"}
	c.Contact.SystemEndpoint = &EndpointInfo{Address: source, Type: "TELEPHONE_NUMBER"}

	if in.CampaignID != "" {
		c.Contact.Campaign = &Campaign{CampaignID: in.CampaignID}
	}

	if in.QueueID != "" {
		c.Contact.QueueInfo = &ContactQueueInfo{ID: in.QueueID, EnqueueTimestamp: c.Contact.InitiationTimestamp}
	}

	b.contacts.put(c)

	return &contactIDOut{ContactID: c.Contact.ID}, nil
}

func (b *Backend) outboundSource(inst string, in *StartOutboundVoiceContactInput) (string, error) {
	var queue *queueRec

	if in.QueueID != "" {
		q, err := b.queues.get(inst, in.QueueID)
		if err != nil {
			return "", err
		}

		if q.Queue.Status != queueEnabled {
			return "", invalidRequestErr("Queue %q is disabled", in.QueueID)
		}

		queue = q
	}

	if in.SourcePhoneNumber != "" {
		for _, p := range b.phoneNumbers.All() {
			if p.InstanceID == inst && p.Phone.PhoneNumber == in.SourcePhoneNumber {
				return in.SourcePhoneNumber, nil
			}
		}

		return "", invalidParamErr("SourcePhoneNumber %q is not claimed by the instance", in.SourcePhoneNumber)
	}

	if queue != nil && queue.Queue.OutboundCallerConfig != nil &&
		queue.Queue.OutboundCallerConfig.OutboundCallerIDNumberID != "" {
		if p, ok := b.phoneNumbers.Get(queue.Queue.OutboundCallerConfig.OutboundCallerIDNumberID); ok {
			return p.Phone.PhoneNumber, nil
		}
	}

	return "", invalidParamErr("a SourcePhoneNumber or a queue with an outbound caller ID is required")
}

func newToken() string { return awsid.NewN("", tokenBytes*pairCount) }

// StartChatContact starts a chat contact and issues the participant token for the customer.
func (b *Backend) StartChatContact(ctx context.Context, in *StartChatContactInput) (*chatOut, error) {
	if in.ParticipantDetails == nil || strings.TrimSpace(in.ParticipantDetails.DisplayName) == "" {
		return nil, invalidParamErr("ParticipantDetails.DisplayName is required")
	}

	if in.ChatDurationInMinutes != nil &&
		(*in.ChatDurationInMinutes < minChatMinutes || *in.ChatDurationInMinutes > maxChatMinutes) {
		return nil, invalidParamErr("ChatDurationInMinutes must be between %d and %d", minChatMinutes, maxChatMinutes)
	}

	if m := in.InitialMessage; m != nil {
		if err := oneOf("InitialMessage.ContentType", m.ContentType, "text/plain", "text/markdown"); err != nil {
			return nil, err
		}

		if m.Content == "" || len(m.Content) > maxInitialChatBytes {
			return nil, invalidParamErr("InitialMessage.Content must be 1-%d characters", maxInitialChatBytes)
		}
	}

	if err := checkAttributes(in.Attributes); err != nil {
		return nil, err
	}

	b.mu.Lock("StartChatContact")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	if err = b.requireFlow(inst, in.ContactFlowID); err != nil {
		return nil, err
	}

	c := b.newContact(ctx, inst, channelChat, "API")
	c.FlowID, c.ParticipantID, c.ParticipantToken = in.ContactFlowID, uuid.NewString(), newToken()
	c.Contact.Attributes, c.Contact.CustomerID = cloneTags(in.Attributes), in.CustomerID
	c.Contact.RelatedContactID, c.Contact.SegmentAttributes = in.RelatedContactID, in.SegmentAttributes

	out := &chatOut{ContactID: c.Contact.ID, ParticipantID: c.ParticipantID, ParticipantToken: c.ParticipantToken}

	if p := in.PersistentChat; p != nil {
		src, perr := b.persistentSource(inst, p)
		if perr != nil {
			return nil, perr
		}

		c.Contact.PreviousContactID, c.Contact.InitialContactID = src.Contact.ID, src.Contact.InitialContactID
		out.ContinuedFromContactID = src.Contact.ID
	}

	b.contacts.put(c)

	return out, nil
}

func (b *Backend) persistentSource(inst string, p *PersistentChat) (*contactRec, error) {
	if err := oneOf(
		"PersistentChat.RehydrationType",
		p.RehydrationType,
		"ENTIRE_PAST_SESSION",
		"FROM_SEGMENT",
	); err != nil {
		return nil, err
	}

	src, err := b.contacts.get(inst, p.SourceContactID)
	if err != nil {
		return nil, contactNotFoundErr(p.SourceContactID)
	}

	if src.Contact.Channel != channelChat {
		return nil, invalidRequestErr("PersistentChat.SourceContactId must be a chat contact")
	}

	if src.Contact.DisconnectTimestamp == nil {
		return nil, apiErr(
			statusConflict,
			"ContactNotTerminatedException",
			"the source contact %q has not ended",
			p.SourceContactID,
		)
	}

	return src, nil
}

// StartTaskContact creates a task contact, optionally scheduled up to six days ahead.
func (b *Backend) StartTaskContact(ctx context.Context, in *StartTaskContactInput) (*contactIDOut, error) {
	if strings.TrimSpace(in.Name) == "" {
		return nil, invalidParamErr("Name is required")
	}

	if in.TaskTemplateID != "" {
		return nil, invalidRequestErr("task templates are not supported by this emulator")
	}

	if in.ContactFlowID == "" {
		return nil, invalidParamErr("ContactFlowId is required")
	}

	if in.ScheduledTime != nil &&
		(in.ScheduledTime.Before(b.now()) || in.ScheduledTime.After(b.now().Add(maxScheduleAhead))) {
		return nil, invalidParamErr("ScheduledTime must be in the future and within 6 days")
	}

	if err := firstError(
		checkAttributes(in.Attributes),
		checkContactText(in.Name, in.Description),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("StartTaskContact")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	if err = b.requireFlow(inst, in.ContactFlowID); err != nil {
		return nil, err
	}

	if in.QuickConnectID != "" && !b.quickConnects.has(inst, in.QuickConnectID) {
		return nil, notFoundErr("QuickConnect", in.QuickConnectID)
	}

	c := b.newContact(ctx, inst, channelTask, "API")
	c.FlowID, c.References = in.ContactFlowID, in.References
	c.Contact.Name, c.Contact.Description = in.Name, in.Description
	c.Contact.Attributes, c.Contact.SegmentAttributes = cloneTags(in.Attributes), in.SegmentAttributes
	c.Contact.RelatedContactID = in.RelatedContactID
	c.Contact.ScheduledTimestamp = in.ScheduledTime

	if in.PreviousContactID != "" {
		prev, perr := b.contacts.get(inst, in.PreviousContactID)
		if perr != nil {
			return nil, contactNotFoundErr(in.PreviousContactID)
		}

		c.Contact.PreviousContactID, c.Contact.InitialContactID = prev.Contact.ID, prev.Contact.InitialContactID
	}

	b.contacts.put(c)

	return &contactIDOut{ContactID: c.Contact.ID}, nil
}

// DescribeContact returns a contact.
func (b *Backend) DescribeContact(inst, id string) (*Contact, error) {
	b.mu.RLock("DescribeContact")
	defer b.mu.RUnlock()

	c, err := b.contactLocked(inst, id)
	if err != nil {
		return nil, err
	}

	out := c.Contact
	out.Attributes, out.Tags = cloneTags(out.Attributes), cloneTags(out.Tags)

	return &out, nil
}

// StopContact ends a contact that is still active.
func (b *Backend) StopContact(in *ContactRefInput) (*empty, error) {
	b.mu.Lock("StopContact")
	defer b.mu.Unlock()

	c, err := b.contactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if c.Contact.DisconnectTimestamp != nil {
		return nil, contactNotFoundErr(in.ContactID)
	}

	t := awstime.NewEpochTime(b.now())
	c.Contact.DisconnectTimestamp = &t
	c.Contact.DisconnectReason = "API"

	if in.DisconnectReason != nil && in.DisconnectReason.Code != "" {
		c.Contact.DisconnectReason = in.DisconnectReason.Code
	}

	b.touch(c)

	return &empty{}, nil
}

// UpdateContactAttributes merges attributes into a contact.
func (b *Backend) UpdateContactAttributes(in *ContactRefInput) (*empty, error) {
	if err := checkAttributes(in.Attributes); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateContactAttributes")
	defer b.mu.Unlock()

	c, err := b.contactLocked(in.InstanceID, in.InitialContactID)
	if err != nil {
		return nil, err
	}

	merged := cloneTags(c.Contact.Attributes)
	if merged == nil {
		merged = map[string]string{}
	}

	maps.Copy(merged, in.Attributes)

	if err = checkAttributes(merged); err != nil {
		return nil, err
	}

	c.Contact.Attributes = merged
	b.touch(c)

	return &empty{}, nil
}

// GetContactAttributes returns a contact's attributes.
func (b *Backend) GetContactAttributes(inst, id string) (*attributesOut, error) {
	b.mu.RLock("GetContactAttributes")
	defer b.mu.RUnlock()

	c, err := b.contactLocked(inst, id)
	if err != nil {
		return nil, err
	}

	attrs := cloneTags(c.Contact.Attributes)
	if attrs == nil {
		attrs = map[string]string{}
	}

	return &attributesOut{Attributes: attrs}, nil
}

// UpdateContact changes the supplied properties of a contact, including its queue and agent.
func (b *Backend) UpdateContact(in *ContactRefInput) (*empty, error) {
	if in.Name != nil || in.Description != nil {
		name, desc := "", ""
		if in.Name != nil {
			name = *in.Name
		}

		if in.Description != nil {
			desc = *in.Description
		}

		if err := checkContactText(name, desc); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("UpdateContact")
	defer b.mu.Unlock()

	c, err := b.contactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if c.Contact.DisconnectTimestamp != nil {
		return nil, invalidRequestErr("contact %q has ended", in.ContactID)
	}

	if err = b.applyContactUpdate(c, in); err != nil {
		return nil, err
	}

	b.touch(c)

	return &empty{}, nil
}

func (b *Backend) applyContactUpdate(c *contactRec, in *ContactRefInput) error {
	if in.UserInfo != nil {
		if !b.users.has(c.InstanceID, in.UserInfo.UserID) {
			return notFoundErr("User", in.UserInfo.UserID)
		}

		t := awstime.NewEpochTime(b.now())
		c.Contact.AgentInfo = &ContactAgentInfo{ID: in.UserInfo.UserID, ConnectedToAgentTimestamp: &t}
	}

	if in.QueueInfo != nil {
		if !b.queues.has(c.InstanceID, in.QueueInfo.ID) {
			return notFoundErr("Queue", in.QueueInfo.ID)
		}

		t := awstime.NewEpochTime(b.now())
		c.Contact.QueueInfo = &ContactQueueInfo{ID: in.QueueInfo.ID, EnqueueTimestamp: &t}
	}

	if in.Name != nil {
		c.Contact.Name = *in.Name
	}

	if in.Description != nil {
		c.Contact.Description = *in.Description
	}

	if in.CustomerEndpoint != nil {
		c.Contact.CustomerEndpoint = in.CustomerEndpoint
	}

	if in.SystemEndpoint != nil {
		c.Contact.SystemEndpoint = in.SystemEndpoint
	}

	if in.References != nil {
		c.References = in.References
	}

	if in.SegmentAttributes != nil {
		c.Contact.SegmentAttributes = in.SegmentAttributes
	}

	return nil
}

// TagContact adds tags to a contact.
func (b *Backend) TagContact(in *ContactRefInput) (*empty, error) {
	if err := checkTags(in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("TagContact")
	defer b.mu.Unlock()

	c, err := b.contactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	merged := cloneTags(c.Contact.Tags)
	if merged == nil {
		merged = map[string]string{}
	}

	maps.Copy(merged, in.Tags)

	if err = checkTags(merged); err != nil {
		return nil, err
	}

	c.Contact.Tags = merged

	return &empty{}, nil
}

// UntagContact removes tags from a contact.
func (b *Backend) UntagContact(in *ContactPathInput) (*empty, error) {
	b.mu.Lock("UntagContact")
	defer b.mu.Unlock()

	c, err := b.contactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	for _, k := range in.TagKeys {
		delete(c.Contact.Tags, k)
	}

	return &empty{}, nil
}

func (c *contactRec) active() bool { return c.Contact.DisconnectTimestamp == nil }

func (c *contactRec) agentID() string {
	if c.Contact.AgentInfo != nil {
		return c.Contact.AgentInfo.ID
	}

	return ""
}

func (c *contactRec) queueID() string {
	if c.Contact.QueueInfo != nil {
		return c.Contact.QueueInfo.ID
	}

	return ""
}

func sortContacts(cs []*contactRec, field, order string) {
	key := func(c *contactRec) string {
		switch field {
		case tsScheduled:
			return tsKey(c.Contact.ScheduledTimestamp)
		case tsDisconnect:
			return tsKey(c.Contact.DisconnectTimestamp)
		case tsConnected:
			if c.Contact.AgentInfo != nil {
				return tsKey(c.Contact.AgentInfo.ConnectedToAgentTimestamp)
			}

			return ""
		case "INITIATION_METHOD":
			return c.Contact.InitiationMethod
		case "CHANNEL":
			return c.Contact.Channel
		default:
			return tsKey(c.Contact.InitiationTimestamp)
		}
	}

	slices.SortStableFunc(cs, func(a, b *contactRec) int {
		r := strings.Compare(key(a), key(b))
		if order == "DESCENDING" {
			return -r
		}

		return r
	})
}

func tsKey(t *awstime.EpochTime) string {
	if t == nil {
		return ""
	}

	return t.UTC().Format("2006-01-02T15:04:05.000000000Z")
}
