package dashboard

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
	eventhubsbackend "github.com/blackbirdworks/gopherstack/services/azureeventhubs"
)

const (
	azureEventHubsProxyPrefix = "/dashboard/api/v1/azure/eventhubs"
	eventHubsDefaultPeek      = 20
	eventHubsMaxPeek          = 100
	eventHubsMaxKeyLen        = 128
)

type eventHubsHubJSON struct {
	Name           string   `json:"name"`
	PartitionIDs   []string `json:"partitionIds"`
	ConsumerGroups []string `json:"consumerGroups"`
	RetentionHours float64  `json:"retentionHours"`
}

type eventHubsNamespaceJSON struct {
	Name string             `json:"name"`
	Hubs []eventHubsHubJSON `json:"hubs"`
}

type eventHubsPartitionJSON struct {
	LastEnqueuedTime time.Time `json:"lastEnqueuedTime"`
	ID               string    `json:"id"`
	BeginSequence    int64     `json:"beginSequence"`
	LastSequence     int64     `json:"lastSequence"`
	Empty            bool      `json:"empty"`
}

type eventHubsEventJSON struct {
	Enqueued     time.Time      `json:"enqueued"`
	Properties   map[string]any `json:"properties,omitempty"`
	PartitionKey string         `json:"partitionKey,omitempty"`
	ContentType  string         `json:"contentType,omitempty"`
	Body         string         `json:"body"`
	Sequence     int64          `json:"sequence"`
	Offset       int64          `json:"offset"`
}

type eventHubsSendJSON struct {
	Partition    *int   `json:"partition"`
	Body         string `json:"body"`
	PartitionKey string `json:"partitionKey"`
}

// registerAzureEventHubsRoutes serves the Event Hubs page from the backend in-process;
// namespaces and hubs are created through ARM, so the page only reads and sends.
func (h *DashboardHandler) registerAzureEventHubsRoutes() {
	ops := h.config.AzureEventHubsOps
	api := &azureEventHubsAPI{}

	if ops != nil {
		api.b = ops.Backend
	}

	r := h.SubRouter
	r.Add(http.MethodGet, azureEventHubsProxyPrefix+"/namespaces", api.guard(api.namespaces))
	r.Add(http.MethodGet, azureEventHubsProxyPrefix+"/namespaces/:ns/hubs/:hub/partitions", api.guard(api.partitions))
	r.Add(
		http.MethodGet, azureEventHubsProxyPrefix+"/namespaces/:ns/hubs/:hub/partitions/:pid/events",
		api.guard(api.events),
	)
	r.Add(http.MethodPost, azureEventHubsProxyPrefix+"/namespaces/:ns/hubs/:hub/events", api.guard(api.send))
}

type azureEventHubsAPI struct {
	b *eventhubsbackend.InMemoryBackend
}

func (a *azureEventHubsAPI) guard(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if a.b == nil {
			return cloudUnavailable(c, "Azure Event Hubs")
		}

		return next(c)
	}
}

func (a *azureEventHubsAPI) fail(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, eventhubsbackend.ErrNamespaceNotFound), errors.Is(err, eventhubsbackend.ErrHubNotFound),
		errors.Is(err, eventhubsbackend.ErrPartitionNotFound):
		return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, err.Error())
	default:
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, err.Error())
	}
}

func (a *azureEventHubsAPI) namespaces(c *echo.Context) error {
	out := []eventHubsNamespaceJSON{}

	for _, name := range a.b.NamespaceNames() {
		hubs, err := a.b.ListHubs(name)
		if err != nil {
			continue
		}

		ns := eventHubsNamespaceJSON{Name: name, Hubs: make([]eventHubsHubJSON, 0, len(hubs))}

		for _, hub := range hubs {
			ns.Hubs = append(ns.Hubs, a.hubJSON(name, hub))
		}

		out = append(out, ns)
	}

	return c.JSON(http.StatusOK, map[string]any{"namespaces": out})
}

func (a *azureEventHubsAPI) hubJSON(ns string, hub eventhubsbackend.HubInfo) eventHubsHubJSON {
	groups, _ := a.b.ListConsumerGroups(eventhubsbackend.HubRef{Namespace: ns, Hub: hub.Name})

	return eventHubsHubJSON{
		Name: hub.Name, PartitionIDs: hub.PartitionIDs, ConsumerGroups: groups,
		RetentionHours: hub.Config.Retention.Hours(),
	}
}

func (a *azureEventHubsAPI) ref(c *echo.Context) eventhubsbackend.HubRef {
	return eventhubsbackend.HubRef{Namespace: c.Param("ns"), Hub: c.Param("hub")}
}

func (a *azureEventHubsAPI) partitions(c *echo.Context) error {
	ref := a.ref(c)

	hub, err := a.b.Hub(ref)
	if err != nil {
		return a.fail(c, err)
	}

	out := make([]eventHubsPartitionJSON, 0, len(hub.PartitionIDs))

	for i := range hub.PartitionIDs {
		info, infoErr := a.b.PartitionInfo(ref, i)
		if infoErr != nil {
			return a.fail(c, infoErr)
		}

		out = append(out, eventHubsPartitionJSON{
			ID: info.Partition, BeginSequence: info.BeginSequence, LastSequence: info.LastSequence,
			LastEnqueuedTime: info.LastEnqueuedTime, Empty: info.Empty,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"partitions": out})
}

// events returns up to limit of the newest events of a partition without consuming anything.
func (a *azureEventHubsAPI) events(c *echo.Context) error {
	ref := a.ref(c)

	pid, err := strconv.Atoi(c.Param("pid"))
	if err != nil || pid < 0 {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "invalid partition id")
	}

	limit := eventHubsDefaultPeek
	if n, convErr := strconv.Atoi(c.QueryParam("limit")); convErr == nil && n > 0 {
		limit = min(n, eventHubsMaxPeek)
	}

	info, err := a.b.PartitionInfo(ref, pid)
	if err != nil {
		return a.fail(c, err)
	}

	out := []eventHubsEventJSON{}

	if !info.Empty {
		out, err = a.peek(c.Request().Context(), ref, pid, info, limit)
		if err != nil {
			return a.fail(c, err)
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"events": out})
}

func (a *azureEventHubsAPI) peek(
	ctx context.Context, ref eventhubsbackend.HubRef, pid int, info eventhubsbackend.PartitionInfo, limit int,
) ([]eventHubsEventJSON, error) {
	if limit > eventHubsMaxPeek {
		limit = eventHubsMaxPeek
	}

	start := max(info.BeginSequence, info.LastSequence-int64(limit)+1)
	out := []eventHubsEventJSON{}

	for seq := info.LastSequence; seq >= start; seq-- {
		ev, err := a.b.Next(ctx, ref, pid, seq, nil)
		if err != nil {
			return nil, err
		}

		out = append(out, decodeEventHubsEvent(ev.Raw, ev.Seq, ev.Offset, ev.Enqueued, ev.Key))
	}

	return out, nil
}

func decodeEventHubsEvent(raw []byte, seq, offset int64, enqueued time.Time, key string) eventHubsEventJSON {
	out := eventHubsEventJSON{Sequence: seq, Offset: offset, Enqueued: enqueued, PartitionKey: key}

	msg, err := amqp1.DecodeMessage(raw)
	if err != nil {
		out.Body = "(undecodable event)"

		return out
	}

	if msg.Properties != nil {
		out.ContentType = msg.Properties.ContentType
	}

	out.Body = eventHubsBody(msg)
	out.Properties = eventHubsProps(msg.ApplicationProperties)

	return out
}

func eventHubsBody(msg *amqp1.Message) string {
	if len(msg.Data) > 0 {
		d := msg.Data[0]
		if utf8.Valid(d) {
			return string(d)
		}

		return "(binary " + strconv.Itoa(len(d)) + " bytes)"
	}

	if msg.HasValue {
		if s, ok := msg.Value.(string); ok {
			return s
		}
	}

	return ""
}

func eventHubsProps(m amqp1.Map) map[string]any {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]any, len(m))

	for k, v := range m {
		out[fmt.Sprint(k)] = v
	}

	return out
}

func (a *azureEventHubsAPI) send(c *echo.Context) error {
	var in eventHubsSendJSON
	if !cloudJSONDecode(c, &in) {
		return nil
	}

	if len(in.PartitionKey) > eventHubsMaxKeyLen || len(in.Body) > eventhubsbackend.MaxEventBytes {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "event body or partition key is too large")
	}

	pid := -1
	if in.Partition != nil {
		pid = *in.Partition
	}

	ev, err := eventHubsNewEvent(in)
	if err != nil {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, err.Error())
	}

	used, err := a.b.Append(a.ref(c), pid, []eventhubsbackend.NewEvent{ev})
	if err != nil {
		return a.fail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]int{"partition": used})
}

func eventHubsNewEvent(in eventHubsSendJSON) (eventhubsbackend.NewEvent, error) {
	msg := &amqp1.Message{Data: [][]byte{[]byte(in.Body)}, Properties: &amqp1.Properties{ContentType: "text/plain"}}
	if in.PartitionKey != "" {
		msg.Annotations = amqp1.Map{amqp1.Symbol("x-opt-partition-key"): in.PartitionKey}
	}

	raw, err := msg.Encode()
	if err != nil {
		return eventhubsbackend.NewEvent{}, err
	}

	return eventhubsbackend.NewEvent{Key: in.PartitionKey, Raw: raw}, nil
}
