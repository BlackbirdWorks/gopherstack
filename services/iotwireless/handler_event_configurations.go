package iotwireless

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type eventConfigDocResponse struct {
	ConnectionStatus        map[string]any `json:"ConnectionStatus,omitempty"`
	DeviceRegistrationState map[string]any `json:"DeviceRegistrationState,omitempty"`
	Join                    map[string]any `json:"Join,omitempty"`
	MessageDeliveryStatus   map[string]any `json:"MessageDeliveryStatus,omitempty"`
	Proximity               map[string]any `json:"Proximity,omitempty"`
}

func eventConfigDocResponseFrom(doc *EventConfigDoc) eventConfigDocResponse {
	if doc == nil {
		return eventConfigDocResponse{}
	}

	return eventConfigDocResponse{
		ConnectionStatus:        doc.ConnectionStatus,
		DeviceRegistrationState: doc.DeviceRegistrationState,
		Join:                    doc.Join,
		MessageDeliveryStatus:   doc.MessageDeliveryStatus,
		Proximity:               doc.Proximity,
	}
}

type eventConfigurationItemResponse struct {
	Events         eventConfigDocResponse `json:"Events"`
	Identifier     string                 `json:"Identifier"`
	IdentifierType string                 `json:"IdentifierType"`
	PartnerType    string                 `json:"PartnerType,omitempty"`
}

func eventConfigurationItemResponseFrom(e *ResourceEventConfigEntry) eventConfigurationItemResponse {
	return eventConfigurationItemResponse{
		Identifier:     e.Identifier,
		IdentifierType: e.IdentifierType,
		PartnerType:    e.PartnerType,
		Events:         eventConfigDocResponseFrom(&e.Config),
	}
}

type listEventConfigurationsResponse struct {
	NextToken               string                           `json:"NextToken"`
	EventConfigurationsList []eventConfigurationItemResponse `json:"EventConfigurationsList"`
}

// eventConfigDocFromBody unmarshals an EventConfigDoc from a raw JSON request
// body, ignoring malformed input (matching the package's existing convention
// of treating unparsed bodies as empty).
func eventConfigDocFromBody(body []byte) *EventConfigDoc {
	var doc EventConfigDoc

	_ = json.Unmarshal(body, &doc)

	return &doc
}

func (h *Handler) getEventConfigurationByResourceTypes(c *echo.Context) error {
	doc := h.Backend.GetEventConfigurationByResourceTypes()

	return writeJSON(c, http.StatusOK, eventConfigDocResponseFrom(doc))
}

func (h *Handler) updateEventConfigurationByResourceTypes(c *echo.Context) error {
	body := readStubBody(c)
	h.Backend.UpdateEventConfigurationByResourceTypes(eventConfigDocFromBody(body))

	return stubNoContent(c)
}

func (h *Handler) listEventConfigurations(c *echo.Context) error {
	resourceType := c.QueryParam("resourceType")
	entries := h.Backend.ListEventConfigurations(resourceType)

	items := make([]eventConfigurationItemResponse, 0, len(entries))
	for _, e := range entries {
		items = append(items, eventConfigurationItemResponseFrom(e))
	}

	return writeJSON(c, http.StatusOK, listEventConfigurationsResponse{
		EventConfigurationsList: items,
	})
}

func (h *Handler) getResourceEventConfiguration(c *echo.Context, identifier string) error {
	entry, ok := h.Backend.GetResourceEventConfiguration(identifier)
	if !ok {
		return writeJSON(c, http.StatusOK, eventConfigDocResponse{})
	}

	return writeJSON(c, http.StatusOK, eventConfigDocResponseFrom(&entry.Config))
}

func (h *Handler) updateResourceEventConfiguration(c *echo.Context, identifier string) error {
	identifierType := c.QueryParam("identifierType")
	partnerType := c.QueryParam("partnerType")
	body := readStubBody(c)

	h.Backend.UpdateResourceEventConfiguration(identifier, identifierType, partnerType, eventConfigDocFromBody(body))

	return stubNoContent(c)
}
