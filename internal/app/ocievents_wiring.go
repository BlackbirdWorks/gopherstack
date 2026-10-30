package app

import (
	"context"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocievents "github.com/blackbirdworks/gopherstack/services/ocievents"
	ocifunctions "github.com/blackbirdworks/gopherstack/services/ocifunctions"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ocinotifications "github.com/blackbirdworks/gopherstack/services/ocinotifications"
	ociobjectstorage "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
	ocistreaming "github.com/blackbirdworks/gopherstack/services/ocistreaming"
)

const eventSourceObjectStorage = "objectstorage"

const eventContentType = "application/json"

var errEventFunctionStatus = errors.New("function invocation failed")

// GetOCIEventsSettings returns OCI Events settings (ocievents.ConfigProvider).
func (c *CLI) GetOCIEventsSettings() ocievents.Settings { return c.OCIEvents }

type ociEventsNotifier struct {
	b *ocinotifications.InMemoryBackend
}

func (n ociEventsNotifier) TopicActive(id string) bool { return n.b.TopicActive(id) }

func (n ociEventsNotifier) PublishEvent(topicID, body string) error {
	_, err := n.b.PublishMessage(topicID, "JSON", ocinotifications.PublishIn{Body: &body})

	return err
}

type ociEventsStreamer struct {
	b *ocistreaming.InMemoryBackend
}

func (s ociEventsStreamer) StreamActive(id string) bool { return s.b.StreamActive(id) }

func (s ociEventsStreamer) AppendMessage(streamID string, key, value []byte) error {
	return s.b.AppendMessage(streamID, key, value)
}

type ociEventsFunctions struct {
	b *ocifunctions.InMemoryBackend
}

func (f ociEventsFunctions) FunctionActive(id string) bool { return f.b.FunctionActive(id) }

func (f ociEventsFunctions) InvokeFunction(ctx context.Context, functionID string, body []byte) error {
	out, err := f.b.Invoke(ctx, functionID, ocifunctions.InvokeIn{
		Header: http.Header{"Content-Type": {eventContentType}}, URL: "/", Body: body,
	})
	if err != nil {
		return err
	}

	if out.Status < http.StatusOK || out.Status >= http.StatusMultipleChoices {
		return errEventFunctionStatus
	}

	return nil
}

func objectEvent(e ociobjectstorage.ObjectEvent) ocievents.Event {
	return ocievents.Event{
		Time: e.Time, EventType: e.EventType, Source: eventSourceObjectStorage, CompartmentID: e.CompartmentID,
		ResourceName: e.ObjectName, AdditionalDetails: map[string]any{
			"eTag": e.ETag, "namespace": e.Namespace, "archivalState": nil, "bucketName": e.BucketName,
			"bucketId": e.BucketID,
		},
	}
}

func functionsEvent(e ocifunctions.ResourceEvent) ocievents.Event {
	return ocievents.Event{
		Time: e.Time, EventType: e.EventType, Source: "functions", CompartmentID: e.CompartmentID,
		ResourceName: e.ResourceName, ResourceID: e.ResourceID,
	}
}

// wireOCIEvents shares OCI Identity keys and compartments, connects the action targets (Notifications,
// Streaming, Functions) and hooks the event producers (Object Storage, Functions).
func wireOCIEvents(byName map[string]service.Registerable) {
	evH, ok := byName["OCIEvents"].(*ocievents.Handler)
	if !ok {
		return
	}

	if identityH, found := byName["OCIIdentity"].(*ociidentitybackend.Handler); found {
		evH.Backend.Auth.Link(identityH.Backend.Auth)
		evH.Backend.SetCompartmentChecker(identityH.Backend.CompartmentActive)
		evH.Backend.SetCompartmentTree(identityH.Backend.CompartmentWithin)
	}

	if h, found := byName["OCINotifications"].(*ocinotifications.Handler); found {
		evH.Backend.SetNotifier(ociEventsNotifier{b: h.Backend})
	}

	if h, found := byName["OCIStreaming"].(*ocistreaming.Handler); found {
		evH.Backend.SetStreamer(ociEventsStreamer{b: h.Backend})
	}

	if h, found := byName["OCIFunctions"].(*ocifunctions.Handler); found {
		evH.Backend.SetFunctionInvoker(ociEventsFunctions{b: h.Backend})
		h.Backend.SetEventSink(func(e ocifunctions.ResourceEvent) { evH.Backend.Emit(functionsEvent(e)) })
	}

	if h, found := byName["OCIObjectStorage"].(*ociobjectstorage.Handler); found {
		h.Backend.SetObjectEventSink(func(e ociobjectstorage.ObjectEvent) { evH.Backend.Emit(objectEvent(e)) })
	}
}
