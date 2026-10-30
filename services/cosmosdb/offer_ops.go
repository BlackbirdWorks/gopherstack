package cosmosdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	headerOfferThroughput = "X-Ms-Offer-Throughput"
	headerOfferAutopilot  = "X-Ms-Cosmos-Offer-Autopilot-Settings"
	headerOfferMinimum    = "X-Ms-Cosmos-Min-Throughput"
	headerOfferPending    = "X-Ms-Offer-Replace-Pending"
)

// offerWire is the throughput-relevant part of an offer request body.
type offerWire struct {
	Content struct {
		OfferThroughput        *int          `json:"offerThroughput"`
		OfferAutopilotSettings *autopilotRaw `json:"offerAutopilotSettings"`
	} `json:"content"`
}

// autopilotRaw is the autoscale settings object, in both the offer body and the autopilot header.
type autopilotRaw struct {
	AutoUpgradePolicy *struct {
		ThroughputPolicy *struct {
			IncrementPercent int `json:"incrementPercent"`
		} `json:"throughputPolicy"`
	} `json:"autoUpgradePolicy"`
	MaxThroughput int `json:"maxThroughput"`
}

func (a *autopilotRaw) spec() ThroughputSpec {
	s := ThroughputSpec{AutoscaleMax: a.MaxThroughput}
	if a.AutoUpgradePolicy != nil && a.AutoUpgradePolicy.ThroughputPolicy != nil {
		s.IncrementPercent = a.AutoUpgradePolicy.ThroughputPolicy.IncrementPercent
	}

	return s
}

func (o offerWire) spec() ThroughputSpec {
	switch {
	case o.Content.OfferAutopilotSettings != nil:
		return o.Content.OfferAutopilotSettings.spec()
	case o.Content.OfferThroughput != nil:
		return ThroughputSpec{Manual: *o.Content.OfferThroughput}
	}

	return ThroughputSpec{}
}

// specFromHeaders reads the throughput headers of a database or container create; has is false when absent.
func specFromHeaders(r *http.Request) (ThroughputSpec, bool, error) {
	manual, auto := r.Header.Get(headerOfferThroughput), r.Header.Get(headerOfferAutopilot)

	switch {
	case manual == "" && auto == "":
		return ThroughputSpec{}, false, nil
	case manual != "" && auto != "":
		return ThroughputSpec{}, true, fmt.Errorf(
			"%w: specify either x-ms-offer-throughput or autopilot settings, not both",
			ErrInvalidThroughput,
		)
	case manual != "":
		n, convErr := strconv.Atoi(manual)
		if convErr != nil {
			return ThroughputSpec{}, true, fmt.Errorf("%w: %w", ErrInvalidThroughput, convErr)
		}

		return ThroughputSpec{Manual: n}, true, nil
	}

	var raw autopilotRaw
	if jsonErr := json.Unmarshal([]byte(auto), &raw); jsonErr != nil {
		return ThroughputSpec{}, true, fmt.Errorf("%w: %w", ErrInvalidThroughput, jsonErr)
	}

	return raw.spec(), true, nil
}

// checkCreateThroughput validates create-time throughput headers before the resource is created; ok is false
// when an error response has been written.
func (h *Handler) checkCreateThroughput(c *echo.Context, containerLevel bool) (ThroughputSpec, bool, bool) {
	spec, has, err := specFromHeaders(c.Request())
	if err == nil && has {
		err = h.checkOffer(spec, containerLevel)
	}

	if err != nil {
		_ = h.writeThroughputError(c, err)

		return spec, has, false
	}

	return spec, has, true
}

func (h *Handler) checkOffer(spec ThroughputSpec, containerLevel bool) error {
	tb, ok := h.Backend.(ThroughputBackend)
	if !ok {
		return ErrThroughputNotFound
	}

	return tb.CheckThroughput(spec, containerLevel)
}

// provisionOffer stores the create-time offer once the resource exists.
func (h *Handler) provisionOffer(c *echo.Context, db, coll string, spec ThroughputSpec) error {
	tb, _ := h.Backend.(ThroughputBackend)
	if _, err := tb.SetThroughput(db, coll, spec); err != nil {
		return h.writeThroughputError(c, err)
	}

	return nil
}

func (h *Handler) writeThroughputError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrThroughputNotFound):
		return h.writeError(
			c,
			http.StatusNotFound,
			"NotFound",
			"Entity with the specified id does not exist in the system.",
		)
	case errors.Is(err, ErrInvalidThroughput), errors.Is(err, ErrServerless):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", err.Error())
	}

	return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
}

func offerContent(t ThroughputInfo) map[string]any {
	content := map[string]any{"offerIsRUPerMinuteThroughputEnabled": false}

	if t.AutoscaleMax == 0 {
		content["offerThroughput"] = t.Manual

		return content
	}

	auto := map[string]any{"maxThroughput": t.AutoscaleMax}
	if t.IncrementPercent > 0 {
		auto["autoUpgradePolicy"] = map[string]any{
			"throughputPolicy": map[string]any{"incrementPercent": t.IncrementPercent},
		}
	}

	content["offerAutopilotSettings"] = auto

	return content
}

func offerBody(t ThroughputInfo) map[string]any {
	return map[string]any{
		"id":              t.OfferRID,
		sysPropRID:        t.OfferRID,
		sysPropSelf:       offersSegment + "/" + t.OfferRID + "/",
		sysPropETag:       resourceETag(t.Modified, t.OfferRID),
		sysPropTS:         resourceTS(t.Modified),
		"offerVersion":    offerVersionV2,
		"offerType":       offerTypeInvalid,
		"resource":        t.ResourceLink,
		"offerResourceId": t.ResourceRID,
		"content":         offerContent(t),
	}
}

func (h *Handler) handleOffers(c *echo.Context) error {
	tb, ok := h.Backend.(ThroughputBackend)
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The backend does not support offers.")
	}

	bodies := make([]map[string]any, 0)
	for _, t := range tb.ListThroughputs() {
		bodies = append(bodies, offerBody(t))
	}

	switch c.Request().Method {
	case http.MethodPost:
		if isQueryRequest(c.Request()) {
			return h.queryResources(c, "Offers", bodies)
		}

		return h.writeError(c, http.StatusBadRequest, "BadRequest", "Offers cannot be created directly.")
	case http.MethodGet:
		setCharge(c, ruMetadata)

		return h.writeJSON(
			c,
			http.StatusOK,
			map[string]any{sysPropRID: "", "Offers": bodies, sysPropCount: len(bodies)},
		)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.")
	}
}

func (h *Handler) handleOfferItem(c *echo.Context, rid string) error {
	tb, ok := h.Backend.(ThroughputBackend)
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The backend does not support offers.")
	}

	switch c.Request().Method {
	case http.MethodGet:
		info, err := tb.GetThroughputByRID(rid)

		return h.writeOffer(c, http.StatusOK, info, err)
	case http.MethodPut:
		return h.replaceOffer(c, tb, rid)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.")
	}
}

func (h *Handler) replaceOffer(c *echo.Context, tb ThroughputBackend, rid string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var wire offerWire
	if jsonErr := json.Unmarshal(body, &wire); jsonErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
	}

	info, repErr := tb.ReplaceThroughputByRID(rid, wire.spec())

	return h.writeOffer(c, http.StatusOK, info, repErr)
}

func (h *Handler) writeOffer(c *echo.Context, status int, info ThroughputInfo, err error) error {
	if err != nil {
		return h.writeThroughputError(c, err)
	}

	hdr := c.Response().Header()
	hdr.Set(headerOfferMinimum, strconv.Itoa(info.Minimum))
	hdr.Set(headerOfferPending, strconv.FormatBool(info.Pending))
	hdr.Set("ETag", resourceETag(info.Modified, info.OfferRID))
	setCharge(c, ruMetadata)

	return h.writeJSON(c, status, offerBody(info))
}
