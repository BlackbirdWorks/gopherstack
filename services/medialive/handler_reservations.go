package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Offering handlers ---

// toOfferingOutput mirrors DescribeOfferingOutput/Offering exactly. Note
// the real shape has NO "name" and NO "tags" field (verified against the
// SDK deserializer) even though Reservation -- a purchased Offering --
// has both; gopherstack's Offering model never had Name/Tags fields
// either, so nothing to drop here.
func toOfferingOutput(o *Offering) map[string]any {
	return map[string]any{
		keyArn: o.Arn, "offeringId": o.OfferingID,
		"offeringDescription": o.OfferingDescription, "offeringType": o.OfferingType,
		"currencyCode": o.CurrencyCode, "fixedPrice": o.FixedPrice, "usagePrice": o.UsagePrice,
		"duration": o.Duration, "durationUnits": o.DurationUnits, "region": o.Region,
		"resourceSpecification": map[string]any{
			"resourceType":     o.ResourceSpecification.ResourceType,
			"videoQuality":     o.ResourceSpecification.VideoQuality,
			"resolution":       o.ResourceSpecification.Resolution,
			"maximumBitrate":   o.ResourceSpecification.MaximumBitrate,
			"maximumFramerate": o.ResourceSpecification.MaximumFramerate,
			"codec":            o.ResourceSpecification.Codec,
		},
	}
}

func (h *Handler) handleListOfferings(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListOfferings(0, "")
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, o := range items {
		out = append(out, toOfferingOutput(o))
	}
	resp := map[string]any{"offerings": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeOffering(c *echo.Context, offeringID string) error {
	o, err := h.Backend.DescribeOffering(offeringID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toOfferingOutput(o))
}

func (h *Handler) handlePurchaseOffering(
	c *echo.Context,
	offeringID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	var count int32 = 1
	if v, ok := body["count"].(float64); ok {
		count = int32(v)
	}
	tags := extractTags(body)
	r, err := h.Backend.PurchaseOffering(offeringID, name, count, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"reservation": toReservationOutput(r)})
}

// --- Reservation handlers ---

func toReservationOutput(r *Reservation) map[string]any {
	tags := r.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return map[string]any{
		keyArn: r.Arn, "reservationId": r.ReservationID, keyName: r.Name,
		"offeringId": r.OfferingID, "offeringDescription": r.OfferingDescription,
		"offeringType": r.OfferingType, "currencyCode": r.CurrencyCode,
		"fixedPrice": r.FixedPrice, "usagePrice": r.UsagePrice,
		"duration": r.Duration, "durationUnits": r.DurationUnits,
		"start": r.Start, "end": r.End, "region": r.Region, keyState: r.State,
		"count": r.Count,
		"resourceSpecification": map[string]any{
			"resourceType":     r.ResourceSpecification.ResourceType,
			"videoQuality":     r.ResourceSpecification.VideoQuality,
			"resolution":       r.ResourceSpecification.Resolution,
			"maximumBitrate":   r.ResourceSpecification.MaximumBitrate,
			"maximumFramerate": r.ResourceSpecification.MaximumFramerate,
			"codec":            r.ResourceSpecification.Codec,
		},
		keyTags: tags,
	}
}

func (h *Handler) handleListReservations(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListReservations(0, "")
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, r := range items {
		out = append(out, toReservationOutput(r))
	}
	resp := map[string]any{"reservations": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDescribeReservation(c *echo.Context, reservationID string) error {
	r, err := h.Backend.DescribeReservation(reservationID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toReservationOutput(r))
}

func (h *Handler) handleDeleteReservation(c *echo.Context, reservationID string) error {
	r, err := h.Backend.DeleteReservation(reservationID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toReservationOutput(r))
}

func (h *Handler) handleUpdateReservation(
	c *echo.Context,
	reservationID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	r, err := h.Backend.UpdateReservation(reservationID, name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"reservation": toReservationOutput(r)})
}
