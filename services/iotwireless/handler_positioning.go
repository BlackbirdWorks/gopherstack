package iotwireless

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

// accuracyResponse mirrors types.Accuracy: a struct of two float fields, not
// a bare scalar. Confirmed against GetPositionOutput.Accuracy's real type;
// this was previously modeled as a bare *float64, which would fail to
// deserialize in a real client (GetPositionOutput.Accuracy is
// *types.Accuracy{HorizontalAccuracy, VerticalAccuracy}).
type accuracyResponse struct {
	HorizontalAccuracy *float32 `json:"HorizontalAccuracy,omitempty"`
	VerticalAccuracy   *float32 `json:"VerticalAccuracy,omitempty"`
}

type getPositionResponse struct {
	Accuracy       *accuracyResponse `json:"Accuracy,omitempty"`
	SolverType     string            `json:"SolverType,omitempty"`
	SolverVersion  string            `json:"SolverVersion,omitempty"`
	SolverProvider string            `json:"SolverProvider,omitempty"`
	Timestamp      string            `json:"Timestamp,omitempty"`
	Position       []float64         `json:"Position"`
}

type getPositionConfigurationResponse struct {
	Solvers     map[string]any `json:"Solvers,omitempty"`
	Destination string         `json:"Destination,omitempty"`
}

type getPositionEstimateResponse struct {
	GeoJSONPayload []byte `json:"GeoJsonPayload"`
}

type getResourcePositionResponse struct {
	GeoJSONPayload []byte `json:"GeoJsonPayload"`
}

type positionConfigurationItemResponse struct {
	Solvers            map[string]any `json:"Solvers,omitempty"`
	ResourceIdentifier string         `json:"ResourceIdentifier,omitempty"`
	ResourceType       string         `json:"ResourceType,omitempty"`
	Destination        string         `json:"Destination,omitempty"`
}

func positionConfigurationItemResponseFrom(e *PositionConfigEntry) positionConfigurationItemResponse {
	return positionConfigurationItemResponse{
		ResourceIdentifier: e.ResourceIdentifier,
		ResourceType:       e.ResourceType,
		Destination:        e.Destination,
		Solvers:            e.Solvers,
	}
}

type listPositionConfigurationsResponse struct {
	NextToken                 string                              `json:"NextToken"`
	PositionConfigurationList []positionConfigurationItemResponse `json:"PositionConfigurationList"`
}

func positionCoords(pos map[string]any) []float64 {
	raw, ok := pos["Position"].([]any)
	if !ok {
		return nil
	}

	coords := make([]float64, 0, len(raw))

	for _, v := range raw {
		if f, isFloat := v.(float64); isFloat {
			coords = append(coords, f)
		}
	}

	return coords
}

func (h *Handler) getPosition(c *echo.Context, id string) error {
	pos := h.Backend.GetPosition(id)

	coords := positionCoords(pos)
	if coords == nil {
		// No position data has ever been submitted for this resource: return
		// the correct "no data available" shape (empty Position, no Accuracy).
		return writeJSON(c, http.StatusOK, getPositionResponse{Position: []float64{}})
	}

	// A value of 0.0 for both sub-fields indicates that position data is
	// available (see GetPositionOutput.Accuracy doc); this is a manual
	// override so no solver metadata is reported.
	var zero float32

	return writeJSON(c, http.StatusOK, getPositionResponse{
		Position: coords,
		Accuracy: &accuracyResponse{HorizontalAccuracy: &zero, VerticalAccuracy: &zero},
	})
}

func (h *Handler) updatePosition(c *echo.Context, id string) error {
	var req map[string]any

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)
	_ = h.Backend.UpdatePosition(id, req)

	return stubNoContent(c)
}

func (h *Handler) getPositionConfiguration(c *echo.Context, id string) error {
	entry, ok := h.Backend.GetPositionConfiguration(id)
	if !ok {
		return writeJSON(c, http.StatusOK, getPositionConfigurationResponse{})
	}

	return writeJSON(c, http.StatusOK, getPositionConfigurationResponse{
		Destination: entry.Destination,
		Solvers:     entry.Solvers,
	})
}

func (h *Handler) putPositionConfiguration(c *echo.Context, id string) error {
	resourceType := c.QueryParam("resourceType")

	var req struct {
		Solvers     map[string]any `json:"Solvers"`
		Destination string         `json:"Destination"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.PutPositionConfiguration(id, resourceType, req.Destination, req.Solvers); err != nil {
		return handleError(c, err)
	}

	return stubNoContent(c)
}

func (h *Handler) listPositionConfigurations(c *echo.Context) error {
	resourceType := c.QueryParam("resourceType")
	entries := h.Backend.ListPositionConfigurations(resourceType)
	pg, next := paginateQuery(c, entries)

	items := make([]positionConfigurationItemResponse, 0, len(pg))
	for _, e := range pg {
		items = append(items, positionConfigurationItemResponseFrom(e))
	}

	return writeJSON(c, http.StatusOK, listPositionConfigurationsResponse{
		NextToken:                 next,
		PositionConfigurationList: items,
	})
}

func (h *Handler) getPositionEstimate(c *echo.Context) error {
	geoJSON := geoJSONPointPayload([]float64{0, 0}, map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})

	return writeJSON(c, http.StatusOK, getPositionEstimateResponse{GeoJSONPayload: geoJSON})
}

// getResourcePosition echoes back the raw GeoJSON payload most recently
// submitted via UpdateResourcePosition for this resource.
func (h *Handler) getResourcePosition(c *echo.Context, id string) error {
	pos := h.Backend.GetPosition(id)

	raw, ok := pos["GeoJsonPayload"].(string)
	if !ok || raw == "" {
		return writeJSON(c, http.StatusOK, getResourcePositionResponse{})
	}

	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return writeJSON(c, http.StatusOK, getResourcePositionResponse{})
	}

	return writeJSON(c, http.StatusOK, getResourcePositionResponse{GeoJSONPayload: decoded})
}

// geoJSONPointPayload builds a GeoJSON Point Feature payload for the given
// coordinates, matching the wire shape AWS returns for GeoJsonPayload fields.
func geoJSONPointPayload(coords []float64, properties map[string]any) []byte {
	if properties == nil {
		properties = map[string]any{}
	}

	payload := map[string]any{
		"type": "Feature",
		"geometry": map[string]any{
			"type":        "Point",
			"coordinates": coords,
		},
		"properties": properties,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return []byte("{}")
	}

	return b
}
