package iotwireless

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"

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

type wifiAccessPointRequest struct {
	Rss        *int32 `json:"Rss"`
	MacAddress string `json:"MacAddress"`
}

type getPositionEstimateRequest struct {
	Gnss *struct {
		Payload string `json:"Payload"`
	} `json:"Gnss"`
	IP *struct {
		IPAddress string `json:"IpAddress"`
	} `json:"Ip"`
	WiFiAccessPoints []wifiAccessPointRequest `json:"WiFiAccessPoints"`
}

// getPositionEstimate validates the measurement inputs, then reports that no position can be resolved:
// real estimates come from third-party solvers (HERE, MaxMind, LoRa Cloud) that cannot run here, and the
// inputs carry no coordinates to derive one from, so no position is fabricated.
func (h *Handler) getPositionEstimate(c *echo.Context) error {
	var req getPositionEstimateRequest

	_ = json.Unmarshal(readStubBody(c), &req)

	if err := validatePositionEstimate(&req); err != nil {
		return handleError(c, err)
	}

	return handleError(c, ErrNoPositionSolver)
}

func validatePositionEstimate(req *getPositionEstimateRequest) error {
	if req.IP != nil {
		if _, err := netip.ParseAddr(req.IP.IPAddress); err != nil {
			return fmt.Errorf("%w: Ip.IpAddress %q is not a valid IP address", ErrValidation, req.IP.IPAddress)
		}
	}

	if req.Gnss != nil {
		if _, err := hex.DecodeString(req.Gnss.Payload); err != nil || req.Gnss.Payload == "" {
			return fmt.Errorf("%w: Gnss.Payload must be a hexadecimal NAV message", ErrValidation)
		}
	}

	for _, ap := range req.WiFiAccessPoints {
		if _, err := net.ParseMAC(ap.MacAddress); err != nil || ap.Rss == nil {
			return fmt.Errorf("%w: WiFiAccessPoints require a valid MacAddress and Rss", ErrValidation)
		}
	}

	return nil
}

// getResourcePosition echoes back the raw GeoJSON payload most recently
// submitted via UpdateResourcePosition for this resource. GeoJsonPayload is
// an httpPayload member (iotwireless@v1.59.4 deserializers.go:7941 assigns
// the whole response body to it), so the body itself must be the raw bytes,
// never a JSON envelope.
func (h *Handler) getResourcePosition(c *echo.Context, id string) error {
	pos := h.Backend.GetPosition(id)

	raw, ok := pos["GeoJsonPayload"].(string)
	if !ok || raw == "" {
		return c.Blob(http.StatusOK, "application/octet-stream", nil)
	}

	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return c.Blob(http.StatusOK, "application/octet-stream", nil)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", decoded)
}

// updateResourcePosition stores the raw GeoJSON payload for a resource.
// GeoJsonPayload is an httpPayload member (serializers.go:9182 streams
// input.GeoJsonPayload as the whole request body), so the request body is
// the payload itself, not a JSON-wrapped field.
func (h *Handler) updateResourcePosition(c *echo.Context, id string) error {
	body := readStubBody(c)

	_ = h.Backend.UpdatePosition(id, map[string]any{
		"GeoJsonPayload": base64.StdEncoding.EncodeToString(body),
	})

	return stubNoContent(c)
}
