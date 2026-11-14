package gcpeventarc

import (
	"context"
	"encoding/json/v2"
	"time"

	"cloud.google.com/go/eventarc/publishing/apiv1/publishingpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// publisherServer is the Eventarc Publishing API for message buses and channels.
type publisherServer struct {
	publishingpb.UnimplementedPublisherServer

	b *InMemoryBackend
}

// Publish routes a CloudEvent (JSON text or CloudEvent proto) through the enrollments of a message bus.
func (s *publisherServer) Publish(
	_ context.Context,
	in *publishingpb.PublishRequest,
) (*publishingpb.PublishResponse, error) {
	if in.GetMessageBus() == "" {
		return nil, errInvalid("message_bus is required.")
	}

	var event map[string]any

	switch f := in.GetFormat().(type) {
	case *publishingpb.PublishRequest_JsonMessage:
		if err := json.Unmarshal([]byte(f.JsonMessage), &event, jsonx.V1()); err != nil {
			return nil, errInvalid("json_message is not a JSON CloudEvent: %v", err)
		}
	case *publishingpb.PublishRequest_ProtoMessage_:
		event = protoEventMap(f.ProtoMessage_)
	default:
		return nil, status.Error(codes.Unimplemented, "Only json_message and proto_message are supported.")
	}

	if err := s.b.PublishToBus(in.GetMessageBus(), event); err != nil {
		return nil, err
	}

	return &publishingpb.PublishResponse{}, nil
}

func protoEventMap(ce *publishingpb.CloudEvent) map[string]any {
	out := map[string]any{
		"id": ce.GetId(), fieldSource: ce.GetSource(), fieldSpecVersion: ce.GetSpecVersion(), attrType: ce.GetType(),
	}

	for k, v := range ce.GetAttributes() {
		switch a := v.GetAttr().(type) {
		case *publishingpb.CloudEvent_CloudEventAttributeValue_CeString:
			out[k] = a.CeString
		case *publishingpb.CloudEvent_CloudEventAttributeValue_CeUri:
			out[k] = a.CeUri
		case *publishingpb.CloudEvent_CloudEventAttributeValue_CeUriRef:
			out[k] = a.CeUriRef
		case *publishingpb.CloudEvent_CloudEventAttributeValue_CeInteger:
			out[k] = a.CeInteger
		case *publishingpb.CloudEvent_CloudEventAttributeValue_CeBoolean:
			out[k] = a.CeBoolean
		case *publishingpb.CloudEvent_CloudEventAttributeValue_CeTimestamp:
			out[k] = a.CeTimestamp.AsTime().Format(time.RFC3339Nano)
		}
	}

	var raw []byte

	switch d := ce.GetData().(type) {
	case *publishingpb.CloudEvent_TextData:
		raw = []byte(d.TextData)
	case *publishingpb.CloudEvent_BinaryData:
		raw = d.BinaryData
	}

	if raw != nil {
		var decoded any
		if json.Unmarshal(raw, &decoded, jsonx.V1()) == nil {
			out["data"] = decoded
		} else {
			out["data"] = string(raw)
		}
	}

	return out
}

func publishedEvents(anys []*anypb.Any, texts []string) ([]map[string]any, error) {
	if len(anys) > 0 && len(texts) > 0 {
		return nil, errInvalid("events and text_events must not both be set.")
	}

	if len(anys) == 0 {
		return parseTextEvents(texts)
	}

	out := make([]map[string]any, 0, len(anys))

	for _, a := range anys {
		var ce publishingpb.CloudEvent
		if err := a.UnmarshalTo(&ce); err != nil {
			return nil, errInvalid("events must be CloudEvent messages: %v", err)
		}

		out = append(out, protoEventMap(&ce))
	}

	return out, nil
}

// PublishEvents delivers CloudEvents to the triggers of a channel.
func (s *publisherServer) PublishEvents(
	_ context.Context,
	in *publishingpb.PublishEventsRequest,
) (*publishingpb.PublishEventsResponse, error) {
	events, err := publishedEvents(in.GetEvents(), in.GetTextEvents())
	if err != nil {
		return nil, err
	}

	if err = s.b.PublishToChannel(in.GetChannel(), events); err != nil {
		return nil, err
	}

	return &publishingpb.PublishEventsResponse{}, nil
}

// PublishChannelConnectionEvents delivers CloudEvents published through a channel connection to its channel.
func (s *publisherServer) PublishChannelConnectionEvents(
	_ context.Context,
	in *publishingpb.PublishChannelConnectionEventsRequest,
) (*publishingpb.PublishChannelConnectionEventsResponse, error) {
	events, err := publishedEvents(in.GetEvents(), in.GetTextEvents())
	if err != nil {
		return nil, err
	}

	if err = s.b.PublishToChannelConnection(in.GetChannelConnection(), events); err != nil {
		return nil, err
	}

	return &publishingpb.PublishChannelConnectionEventsResponse{}, nil
}
