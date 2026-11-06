package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	eventgridbackend "github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

const azureEventGridProxyPrefix = "/dashboard/api/v1/azure/eventgrid"

// eventGridSubJSON is the secret-free view of an event subscription: the endpoint
// type only, never its URL, headers or delivery attribute values.
type eventGridSubJSON struct {
	Name                 string   `json:"name"`
	EndpointType         string   `json:"endpointType"`
	State                string   `json:"state"`
	Schema               string   `json:"deliverySchema"`
	SubjectBeginsWith    string   `json:"subjectBeginsWith,omitempty"`
	SubjectEndsWith      string   `json:"subjectEndsWith,omitempty"`
	DeadLetterType       string   `json:"deadLetterType,omitempty"`
	DeadLetterContainer  string   `json:"deadLetterContainer,omitempty"`
	IncludedEventTypes   []string `json:"includedEventTypes"`
	AdvancedFilterCount  int      `json:"advancedFilterCount"`
	MaxDeliveryAttempts  int      `json:"maxDeliveryAttempts"`
	EventTTLMinutes      int      `json:"eventTimeToLiveMinutes"`
	QueueLockSeconds     int      `json:"queueLockSeconds,omitempty"`
	QueueMaxDelivery     int      `json:"queueMaxDelivery,omitempty"`
	QueueEventTimeToLive string   `json:"queueEventTimeToLive,omitempty"`
}

type eventGridTopicJSON struct {
	Name          string             `json:"name"`
	InputSchema   string             `json:"inputSchema"`
	Subscriptions []eventGridSubJSON `json:"subscriptions"`
}

type eventGridDomainTopicJSON struct {
	Name          string             `json:"name"`
	Subscriptions []eventGridSubJSON `json:"subscriptions"`
}

type eventGridDomainJSON struct {
	Name          string                     `json:"name"`
	InputSchema   string                     `json:"inputSchema"`
	Subscriptions []eventGridSubJSON         `json:"subscriptions"`
	Topics        []eventGridDomainTopicJSON `json:"topics"`
	AutoCreate    bool                       `json:"autoCreateTopics"`
	AutoDelete    bool                       `json:"autoDeleteTopics"`
}

type eventGridNamespaceJSON struct {
	Name   string                     `json:"name"`
	Topics []eventGridDomainTopicJSON `json:"topics"`
}

type eventGridScopeJSON struct {
	ID            string             `json:"id"`
	Subscriptions []eventGridSubJSON `json:"subscriptions"`
}

// registerAzureEventGridRoutes serves the Event Grid page from the backend in-process.
// Topics, domains and subscriptions are created through ARM; the page lists, publishes
// test events and reveals access keys on an explicit POST.
func (h *DashboardHandler) registerAzureEventGridRoutes() {
	api := &azureEventGridAPI{}
	if h.config.AzureEventGridOps != nil {
		api.b = h.config.AzureEventGridOps.Backend
	}

	r := h.SubRouter
	p := azureEventGridProxyPrefix
	r.Add(http.MethodGet, p+"/topics", api.guard(api.topics))
	r.Add(http.MethodGet, p+"/overview", api.guard(api.overview))
	r.Add(http.MethodPost, p+"/topics/:topic/events", api.guard(api.publishTopic))
	r.Add(http.MethodPost, p+"/domains/:domain/events", api.guard(api.publishDomain))
	r.Add(http.MethodPost, p+"/namespaces/:ns/topics/:topic/events", api.guard(api.publishNamespace))
	r.Add(http.MethodPost, p+"/:kind/:name/keys/reveal", api.guard(api.revealKeys))
}

type azureEventGridAPI struct {
	b *eventgridbackend.InMemoryBackend
}

func (a *azureEventGridAPI) guard(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if a.b == nil {
			return cloudUnavailable(c, "Azure Event Grid")
		}

		return next(c)
	}
}

func (a *azureEventGridAPI) topics(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"topics": a.topicList()})
}

func (a *azureEventGridAPI) overview(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		"topics":     a.topicList(),
		"domains":    a.domainList(),
		"namespaces": a.namespaceList(),
		"scopes":     a.scopeList(),
	})
}

func (a *azureEventGridAPI) topicList() []eventGridTopicJSON {
	out := []eventGridTopicJSON{}

	for _, name := range a.b.TopicNames() {
		info, err := a.b.Topic(name)
		if err != nil {
			continue
		}

		out = append(out, eventGridTopicJSON{
			Name: name, InputSchema: info.InputSchema,
			Subscriptions: a.subscriptions(eventgridbackend.Scope{Topic: name}),
		})
	}

	return out
}

func (a *azureEventGridAPI) domainList() []eventGridDomainJSON {
	out := []eventGridDomainJSON{}

	for _, name := range a.b.DomainNames() {
		info, err := a.b.Domain(name)
		if err != nil {
			continue
		}

		d := eventGridDomainJSON{
			Name: name, InputSchema: info.InputSchema, AutoCreate: info.AutoCreate, AutoDelete: info.AutoDelete,
			Subscriptions: a.subscriptions(eventgridbackend.Scope{Domain: name}),
			Topics:        []eventGridDomainTopicJSON{},
		}

		topics, _ := a.b.DomainTopics(name)
		for _, t := range topics {
			d.Topics = append(d.Topics, eventGridDomainTopicJSON{
				Name:          t.Name,
				Subscriptions: a.subscriptions(eventgridbackend.Scope{Domain: name, Topic: t.Name}),
			})
		}

		out = append(out, d)
	}

	return out
}

func (a *azureEventGridAPI) namespaceList() []eventGridNamespaceJSON {
	out := []eventGridNamespaceJSON{}

	for _, ns := range a.b.NamespaceNames() {
		item := eventGridNamespaceJSON{Name: ns, Topics: []eventGridDomainTopicJSON{}}
		topics, _ := a.b.NamespaceTopicNames(ns)

		for _, t := range topics {
			item.Topics = append(item.Topics, eventGridDomainTopicJSON{
				Name: t, Subscriptions: a.pullSubscriptions(ns, t),
			})
		}

		out = append(out, item)
	}

	return out
}

func (a *azureEventGridAPI) scopeList() []eventGridScopeJSON {
	out := []eventGridScopeJSON{}

	for _, s := range a.b.ARMScopes() {
		out = append(out, eventGridScopeJSON{
			ID: s.ID, Subscriptions: a.subscriptions(eventgridbackend.Scope{Resource: s.ID}),
		})
	}

	return out
}

func (a *azureEventGridAPI) subscriptions(sc eventgridbackend.Scope) []eventGridSubJSON {
	names, _ := a.b.ScopedSubscriptionNames(sc)
	out := make([]eventGridSubJSON, 0, len(names))

	for _, name := range names {
		props, err := a.b.ScopedSubscription(sc, name)
		if err != nil {
			continue
		}

		out = append(out, subscriptionJSON(name, props))
	}

	return out
}

func (a *azureEventGridAPI) pullSubscriptions(ns, topic string) []eventGridSubJSON {
	names, _ := a.b.PullSubscriptionNames(ns, topic)
	out := make([]eventGridSubJSON, 0, len(names))

	for _, name := range names {
		props, _, err := a.b.PullSubscription(ns, topic, name)
		if err != nil {
			continue
		}

		out = append(out, pullSubscriptionJSON(name, props))
	}

	return out
}

func subMap(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)

	return v
}

func subNum(m map[string]any, key string) int {
	f, _ := m[key].(float64)

	return int(f)
}

func subStr(m map[string]any, key string) string {
	s, _ := m[key].(string)

	return s
}

func subStrings(m map[string]any, key string) []string {
	raw, _ := m[key].([]any)
	out := make([]string, 0, len(raw))

	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func subscriptionJSON(name string, props map[string]any) eventGridSubJSON {
	filter, retry := subMap(props, "filter"), subMap(props, "retryPolicy")
	adv, _ := filter["advancedFilters"].([]any)

	sub := eventGridSubJSON{
		Name: name, State: subStr(props, "provisioningState"), Schema: subStr(props, "eventDeliverySchema"),
		EndpointType:        subStr(subMap(props, "destination"), "endpointType"),
		SubjectBeginsWith:   subStr(filter, "subjectBeginsWith"),
		SubjectEndsWith:     subStr(filter, "subjectEndsWith"),
		IncludedEventTypes:  subStrings(filter, "includedEventTypes"),
		AdvancedFilterCount: len(adv),
		MaxDeliveryAttempts: subNum(retry, "maxDeliveryAttempts"),
		EventTTLMinutes:     subNum(retry, "eventTimeToLiveInMinutes"),
	}

	if dl := subMap(props, "deadLetterDestination"); dl != nil {
		sub.DeadLetterType = subStr(dl, "endpointType")
		sub.DeadLetterContainer = subStr(subMap(dl, "properties"), "blobContainerName")
	}

	return sub
}

func pullSubscriptionJSON(name string, props map[string]any) eventGridSubJSON {
	filters := subMap(props, "filtersConfiguration")
	queue := subMap(subMap(props, "deliveryConfiguration"), "queue")
	adv, _ := filters["filters"].([]any)

	return eventGridSubJSON{
		Name: name, State: subStr(props, "provisioningState"), Schema: subStr(props, "eventDeliverySchema"),
		EndpointType:         "Queue (pull)",
		IncludedEventTypes:   subStrings(filters, "includedEventTypes"),
		AdvancedFilterCount:  len(adv),
		QueueLockSeconds:     subNum(queue, "receiveLockDurationInSeconds"),
		QueueMaxDelivery:     subNum(queue, "maxDeliveryCount"),
		QueueEventTimeToLive: subStr(queue, "eventTimeToLive"),
	}
}

// revealKeys returns a topic, domain or namespace key pair; it is the only route that sends keys out.
func (a *azureEventGridAPI) revealKeys(c *echo.Context) error {
	var (
		keys eventgridbackend.Keys
		err  error
	)

	switch name := c.Param("name"); c.Param("kind") {
	case "topics":
		keys, err = a.b.TopicKeys(name)
	case "domains":
		keys, err = a.b.DomainKeys(name)
	case "namespaces":
		keys, err = a.b.NamespaceKeys(name)
	default:
		return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, "unknown resource kind")
	}

	if err != nil {
		return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, err.Error())
	}

	c.Response().Header().Set("Cache-Control", "no-store")

	return c.JSON(http.StatusOK, map[string]string{"key1": keys.Key1, "key2": keys.Key2})
}
