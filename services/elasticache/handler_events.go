package elasticache

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"
)

func (h *Handler) describeEvents(ctx context.Context, c *echo.Context, form url.Values) error {
	sourceIdentifier := form.Get("SourceIdentifier")
	sourceType := form.Get("SourceType")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}

	var startTime, endTime time.Time

	if s := form.Get("StartTime"); s != "" {
		if t, parseErr := time.Parse(time.RFC3339, s); parseErr == nil {
			startTime = t
		}
	}

	if s := form.Get("EndTime"); s != "" {
		if t, parseErr := time.Parse(time.RFC3339, s); parseErr == nil {
			endTime = t
		}
	}

	duration := 0
	if s := form.Get("Duration"); s != "" {
		if n, parseErr := strconv.Atoi(s); parseErr == nil {
			duration = n
		}
	}

	p, err := h.Backend.DescribeEvents(
		ctx,
		sourceIdentifier,
		sourceType,
		marker,
		startTime,
		endTime,
		duration,
		maxRecords,
	)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type eventXML struct {
		Date             string `xml:"Date"`
		SourceIdentifier string `xml:"SourceIdentifier"`
		SourceType       string `xml:"SourceType"`
		Message          string `xml:"Message"`
	}
	type eventsList struct {
		Event []eventXML `xml:"Event"`
	}
	type result struct {
		XMLName xml.Name   `xml:"DescribeEventsResponse"`
		Xmlns   string     `xml:"xmlns,attr"`
		Marker  string     `xml:"DescribeEventsResult>Marker,omitempty"`
		Events  eventsList `xml:"DescribeEventsResult>Events"`
	}

	items := make([]eventXML, 0, len(p.Data))
	for _, e := range p.Data {
		items = append(items, eventXML{
			Date:             e.Date.UTC().Format(time.RFC3339),
			SourceIdentifier: e.SourceIdentifier,
			SourceType:       e.SourceType,
			Message:          e.Message,
		})
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:  elasticacheNS,
		Marker: p.Next,
		Events: eventsList{Event: items},
	})
}
