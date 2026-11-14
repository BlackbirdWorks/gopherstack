package dashboard

import (
	"encoding/json/v2"
	"errors"
	"net/http"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	eventgridbackend "github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

const (
	eventGridMaxBody    = 128 << 10
	eventGridMaxData    = 64 << 10
	eventGridMaxSubject = 256
	eventGridMaxType    = 128
	eventGridTestSource = "/gopherstack/dashboard"
	eventGridVersion    = "1.0"
)

// eventGridTestEvent holds the only fields the page may set; the event itself is rebuilt here.
type eventGridTestEvent struct {
	Subject     string `json:"subject"`
	EventType   string `json:"eventType"`
	Data        string `json:"data"`
	DomainTopic string `json:"domainTopic"`
}

func (e eventGridTestEvent) validate(needTopic bool) string {
	switch {
	case e.EventType == "" || len(e.EventType) > eventGridMaxType:
		return "eventType is required (at most 128 characters)"
	case len(e.Subject) > eventGridMaxSubject:
		return "subject is at most 256 characters"
	case len(e.Data) > eventGridMaxData:
		return "data is at most 64 KiB"
	case needTopic && (e.DomainTopic == "" || len(e.DomainTopic) > eventGridMaxSubject):
		return "domainTopic is required"
	}

	return ""
}

func (e eventGridTestEvent) dataValue() any {
	var v any
	if json.Unmarshal([]byte(e.Data), &v, jsonx.V1()) == nil && e.Data != "" {
		return v
	}

	return e.Data
}

type eventGridSchemaEvent struct {
	Data        any    `json:"data"`
	ID          string `json:"id"`
	Subject     string `json:"subject"`
	EventType   string `json:"eventType"`
	EventTime   string `json:"eventTime"`
	DataVersion string `json:"dataVersion"`
	Topic       string `json:"topic,omitempty"`
}

type eventGridCloudEvent struct {
	Data        any    `json:"data"`
	SpecVersion string `json:"specversion"`
	ID          string `json:"id"`
	Source      string `json:"source"`
	Type        string `json:"type"`
	Subject     string `json:"subject"`
	Time        string `json:"time"`
}

type eventGridPublished struct {
	Published int `json:"published"`
}

// build returns the event in the given input schema; domainTopic routes it inside a domain.
func (e eventGridTestEvent) build(schema, domainTopic string, now time.Time) []byte {
	id, ts := uuid.NewString(), now.UTC().Format(time.RFC3339Nano)

	if e.Subject == "" {
		e.Subject = eventGridTestSource + "/test"
	}

	var ev any

	if schema == eventgridbackend.SchemaCloudEvent {
		source := eventGridTestSource
		if domainTopic != "" {
			source = domainTopic
		}

		ev = eventGridCloudEvent{
			SpecVersion: eventGridVersion, ID: id, Source: source, Type: e.EventType, Subject: e.Subject, Time: ts,
			Data: e.dataValue(),
		}
	} else {
		ev = eventGridSchemaEvent{
			ID: id, Subject: e.Subject, EventType: e.EventType, EventTime: ts, DataVersion: eventGridVersion,
			Topic: domainTopic, Data: e.dataValue(),
		}
	}

	raw, _ := json.Marshal(ev, jsonx.V1())

	return raw
}

func (a *azureEventGridAPI) decodeTest(c *echo.Context, needTopic bool) (eventGridTestEvent, bool) {
	var in eventGridTestEvent

	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, eventGridMaxBody)

	if !cloudJSONDecode(c, &in) {
		return in, false
	}

	if msg := in.validate(needTopic); msg != "" {
		_ = cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, msg)

		return in, false
	}

	return in, true
}

func (a *azureEventGridAPI) publishFail(c *echo.Context, err error) error {
	status := http.StatusBadRequest

	for _, nf := range []error{
		eventgridbackend.ErrTopicNotFound, eventgridbackend.ErrDomainNotFound,
		eventgridbackend.ErrNamespaceNotFound, eventgridbackend.ErrNamespaceTopicNotFound,
	} {
		if errors.Is(err, nf) {
			status = http.StatusNotFound
		}
	}

	code := cloudStatusBadArg
	if status == http.StatusNotFound {
		code = cloudStatusNotFnd
	}

	return cloudJSONFail(c, status, code, err.Error())
}

func (a *azureEventGridAPI) publishTopic(c *echo.Context) error {
	topic := c.Param("topic")

	info, err := a.b.Topic(topic)
	if err != nil {
		return a.publishFail(c, err)
	}

	in, ok := a.decodeTest(c, false)
	if !ok {
		return nil
	}

	events, err := eventgridbackend.ParseEvents(in.build(info.InputSchema, "", time.Now()), info.InputSchema)
	if err == nil {
		err = a.b.Publish(topic, events)
	}

	if err != nil {
		return a.publishFail(c, err)
	}

	return c.JSON(http.StatusOK, eventGridPublished{Published: len(events)})
}

func (a *azureEventGridAPI) publishDomain(c *echo.Context) error {
	domain := c.Param("domain")

	info, err := a.b.Domain(domain)
	if err != nil {
		return a.publishFail(c, err)
	}

	in, ok := a.decodeTest(c, true)
	if !ok {
		return nil
	}

	events, err := eventgridbackend.ParseEvents(
		in.build(info.InputSchema, in.DomainTopic, time.Now()), info.InputSchema,
	)
	if err == nil {
		err = a.b.PublishDomain(domain, events)
	}

	if err != nil {
		return a.publishFail(c, err)
	}

	return c.JSON(http.StatusOK, eventGridPublished{Published: len(events)})
}

func (a *azureEventGridAPI) publishNamespace(c *echo.Context) error {
	ns, topic := c.Param("ns"), c.Param("topic")

	if names, err := a.b.NamespaceTopicNames(ns); err != nil {
		return a.publishFail(c, err)
	} else if !slices.Contains(names, topic) {
		return a.publishFail(c, eventgridbackend.ErrNamespaceTopicNotFound)
	}

	in, ok := a.decodeTest(c, false)
	if !ok {
		return nil
	}

	events, err := eventgridbackend.ParseEvents(
		in.build(eventgridbackend.SchemaCloudEvent, "", time.Now()), eventgridbackend.SchemaCloudEvent,
	)
	if err == nil {
		err = a.b.PublishNamespace(ns, topic, events)
	}

	if err != nil {
		return a.publishFail(c, err)
	}

	return c.JSON(http.StatusOK, eventGridPublished{Published: len(events)})
}
