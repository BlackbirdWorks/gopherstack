package connect

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	endpointPhoneARN       = "CONNECT_PHONENUMBER_ARN"
	maxSegmentAttrKeyLen   = 128
	maxOutboundChatMessage = 16384
	contentTextPlain       = "text/plain"
	maxOutboundFlowIDLen   = 500
)

func outboundChatContentTypes() []string {
	return []string{
		contentTextPlain, "text/markdown", "application/json",
		"application/vnd.amazonaws.connect.message.interactive",
		"application/vnd.amazonaws.connect.message.interactive.response",
	}
}

// StartOutboundChatContactInput is the StartOutboundChatContact request.
type StartOutboundChatContactInput struct {
	SourceEndpoint                 *EndpointInfo
	DestinationEndpoint            *EndpointInfo
	ParticipantDetails             *ParticipantDetails
	InitialSystemMessage           *ChatMessage
	InitialTemplatedSystemMessage  jsontext.Value
	ChatDurationInMinutes          *int32
	SegmentAttributes              map[string]jsontext.Value
	Attributes                     map[string]string
	InstanceID                     string `json:"InstanceId"`
	ContactFlowID                  string `json:"ContactFlowId"`
	RelatedContactID               string `json:"RelatedContactId"`
	ClientToken                    string
	SupportedMessagingContentTypes []string
}

func checkOutboundChat(in *StartOutboundChatContactInput) error {
	if in.SourceEndpoint == nil || in.SourceEndpoint.Type != endpointPhoneARN || in.SourceEndpoint.Address == "" {
		return invalidParamErr("SourceEndpoint must be a %s endpoint", endpointPhoneARN)
	}

	if in.DestinationEndpoint == nil || in.DestinationEndpoint.Type != endpointPhone ||
		!e164Pattern.MatchString(in.DestinationEndpoint.Address) {
		return invalidParamErr("DestinationEndpoint must be a %s endpoint with an E.164 number", endpointPhone)
	}

	return firstError(checkOutboundChatShape(in), checkOutboundChatMessages(in), checkAttributes(in.Attributes))
}

func checkOutboundChatShape(in *StartOutboundChatContactInput) error {
	if in.ContactFlowID == "" || len(in.ContactFlowID) > maxOutboundFlowIDLen {
		return invalidParamErr("ContactFlowId is required")
	}

	if len(in.SegmentAttributes) == 0 {
		return invalidParamErr("SegmentAttributes is required")
	}

	for k := range in.SegmentAttributes {
		if k == "" || len(k) > maxSegmentAttrKeyLen {
			return invalidParamErr("SegmentAttributes keys must be 1-%d characters", maxSegmentAttrKeyLen)
		}
	}

	if d := in.ChatDurationInMinutes; d != nil && (*d < minChatMinutes || *d > maxChatMinutes) {
		return invalidParamErr("ChatDurationInMinutes must be between %d and %d", minChatMinutes, maxChatMinutes)
	}

	return nil
}

func checkOutboundChatMessages(in *StartOutboundChatContactInput) error {
	if m := in.InitialSystemMessage; m != nil {
		if err := oneOf("InitialSystemMessage.ContentType", m.ContentType, outboundChatContentTypes()...); err != nil {
			return err
		}

		if m.Content == "" || len(m.Content) > maxOutboundChatMessage {
			return invalidParamErr("InitialSystemMessage.Content must be 1-%d bytes", maxOutboundChatMessage)
		}
	}

	if len(in.InitialTemplatedSystemMessage) > 0 && string(in.InitialTemplatedSystemMessage) != "null" {
		return invalidRequestErr("InitialTemplatedSystemMessage needs Wisdom message templates, which are not emulated")
	}

	if types := in.SupportedMessagingContentTypes; len(types) > 0 {
		if !slices.Contains(types, contentTextPlain) {
			return invalidParamErr("SupportedMessagingContentTypes must contain %s", contentTextPlain)
		}

		for _, t := range types {
			if err := oneOf("SupportedMessagingContentTypes", t, outboundChatContentTypes()...); err != nil {
				return err
			}
		}
	}

	if p := in.ParticipantDetails; p != nil && strings.TrimSpace(p.DisplayName) == "" {
		return invalidParamErr("ParticipantDetails.DisplayName is required")
	}

	return nil
}

func (b *Backend) claimedNumberByARN(inst, phoneARN string) (string, bool) {
	for _, p := range b.phoneNumbers.All() {
		if p.InstanceID == inst && p.Phone.PhoneNumberArn == phoneARN {
			return p.Phone.PhoneNumber, true
		}
	}

	return "", false
}

// StartOutboundChatContact starts an outbound SMS or WhatsApp chat contact; the message is not delivered.
func (b *Backend) StartOutboundChatContact(
	ctx context.Context,
	in *StartOutboundChatContactInput,
) (*ContactIDOut, error) {
	if err := checkOutboundChat(in); err != nil {
		return nil, err
	}

	segs, err := json.Marshal(in.SegmentAttributes, jsonx.V1())
	if err != nil {
		return nil, invalidParamErr("SegmentAttributes is not valid")
	}

	b.mu.Lock("StartOutboundChatContact")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	source, ok := b.claimedNumberByARN(inst, in.SourceEndpoint.Address)
	if !ok {
		return nil, notFoundErr("PhoneNumber", in.SourceEndpoint.Address)
	}

	if err = b.requireFlow(inst, in.ContactFlowID); err != nil {
		return nil, err
	}

	if in.RelatedContactID != "" && !b.contacts.has(inst, in.RelatedContactID) {
		return nil, notFoundErr("Contact", in.RelatedContactID)
	}

	c := b.newContact(ctx, inst, channelChat, methodOutbound)
	c.FlowID, c.Contact.Attributes = in.ContactFlowID, cloneTags(in.Attributes)
	c.Contact.RelatedContactID, c.Contact.SegmentAttributes = in.RelatedContactID, segs
	c.Contact.CustomerEndpoint = &EndpointInfo{Address: in.DestinationEndpoint.Address, Type: endpointPhone}
	c.Contact.SystemEndpoint = &EndpointInfo{Address: source, Type: endpointPhone}
	b.contacts.put(c)

	logger.Load(ctx).InfoContext(ctx, "connect: would send outbound chat message",
		"contact", c.Contact.ID, "to", in.DestinationEndpoint.Address)

	return &ContactIDOut{ContactID: c.Contact.ID}, nil
}
