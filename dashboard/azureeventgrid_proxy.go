package dashboard

import (
	"errors"
	"io"
	"net/http"

	"github.com/labstack/echo/v5"

	eventgridbackend "github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

const azureEventGridProxyPrefix = "/dashboard/api/v1/azure/eventgrid"

type eventGridSubJSON struct {
	Name         string `json:"name"`
	EndpointType string `json:"endpointType"`
	State        string `json:"state"`
	Schema       string `json:"deliverySchema"`
}

type eventGridTopicJSON struct {
	Name          string             `json:"name"`
	InputSchema   string             `json:"inputSchema"`
	Subscriptions []eventGridSubJSON `json:"subscriptions"`
}

// registerAzureEventGridRoutes serves the Event Grid page from the backend in-process;
// topics and subscriptions are created through ARM, so the page lists and publishes.
func (h *DashboardHandler) registerAzureEventGridRoutes() {
	api := &azureEventGridAPI{}
	if h.config.AzureEventGridOps != nil {
		api.b = h.config.AzureEventGridOps.Backend
	}

	h.SubRouter.Add(http.MethodGet, azureEventGridProxyPrefix+"/topics", api.guard(api.topics))
	h.SubRouter.Add(http.MethodPost, azureEventGridProxyPrefix+"/topics/:topic/events", api.guard(api.publish))
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
	out := []eventGridTopicJSON{}

	for _, name := range a.b.TopicNames() {
		info, err := a.b.Topic(name)
		if err != nil {
			continue
		}

		out = append(out, eventGridTopicJSON{
			Name: name, InputSchema: info.InputSchema, Subscriptions: a.subscriptions(name),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"topics": out})
}

func (a *azureEventGridAPI) subscriptions(topic string) []eventGridSubJSON {
	names, _ := a.b.SubscriptionNames(topic)
	out := make([]eventGridSubJSON, 0, len(names))

	for _, name := range names {
		props, err := a.b.Subscription(topic, name)
		if err != nil {
			continue
		}

		sub := eventGridSubJSON{Name: name}
		sub.State, _ = props["provisioningState"].(string)
		sub.Schema, _ = props["eventDeliverySchema"].(string)

		if dest, ok := props["destination"].(map[string]any); ok {
			sub.EndpointType, _ = dest["endpointType"].(string)
		}

		out = append(out, sub)
	}

	return out
}

func (a *azureEventGridAPI) publish(c *echo.Context) error {
	topic := c.Param("topic")

	info, err := a.b.Topic(topic)
	if err != nil {
		return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, err.Error())
	}

	body, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, eventgridbackend.MaxRequestBytes))
	if err != nil {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "request body too large or unreadable")
	}

	events, err := eventgridbackend.ParseEvents(body, info.InputSchema)
	if err == nil {
		err = a.b.Publish(topic, events)
	}

	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, eventgridbackend.ErrTopicNotFound) {
			status = http.StatusNotFound
		}

		return cloudJSONFail(c, status, cloudStatusBadArg, err.Error())
	}

	return c.JSON(http.StatusOK, map[string]int{"published": len(events)})
}
