package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

type invalidationBatchXML struct {
	XMLName         xml.Name        `xml:"InvalidationBatch"`
	CallerReference string          `xml:"CallerReference"`
	Paths           invalidPathsXML `xml:"Paths"`
}

type invalidPathsXML struct {
	Items    []string `xml:"Items>Path"`
	Quantity int      `xml:"Quantity"`
}

func (h *Handler) handleCreateInvalidation(c *echo.Context, distID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(
			c,
			http.StatusInternalServerError,
			cfErrorXML("InternalFailure", err.Error()),
		)
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var batch invalidationBatchXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &batch); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", xmlErr.Error()))
		}
	}

	inv, backendErr := h.Backend.CreateInvalidation(
		distID,
		batch.CallerReference,
		batch.Paths.Items,
	)
	if backendErr != nil {
		return h.handleError(c, backendErr)
	}

	var pathsSB strings.Builder
	for _, p := range inv.Paths {
		fmt.Fprintf(&pathsSB, "<Path>%s</Path>", p)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Invalidation xmlns="%s">`+
		`<Id>%s</Id>`+
		`<Status>%s</Status>`+
		`<CreateTime>%s</CreateTime>`+
		`<InvalidationBatch>`+
		`<CallerReference>%s</CallerReference>`+
		`<Paths><Quantity>%d</Quantity><Items>%s</Items></Paths>`+
		`</InvalidationBatch>`+
		`</Invalidation>`,
		cfNS, inv.ID, inv.Status, inv.CreateTime.Format(time.RFC3339),
		batch.CallerReference, len(inv.Paths), pathsSB.String())

	c.Response().
		Header().
		Set("Location", cfPathPrefix+"distribution/"+distID+"/invalidation/"+inv.ID)

	return xmlResp(c, http.StatusCreated, resp)
}

func (h *Handler) handleListInvalidations(c *echo.Context, distID string) error {
	invs, err := h.Backend.ListInvalidations(distID)
	if err != nil {
		return h.handleError(c, err)
	}

	page, pageSize, isTruncated, nextMarker := paginateByMarkerID(
		c,
		invs,
		func(inv *Invalidation) string { return inv.ID },
	)

	var sb strings.Builder

	for _, inv := range page {
		fmt.Fprintf(
			&sb,
			`<InvalidationSummary>`+
				`<Id>%s</Id>`+
				`<Status>%s</Status>`+
				`<CreateTime>%s</CreateTime>`+
				`</InvalidationSummary>`,
			inv.ID, inv.Status, inv.CreateTime.Format(time.RFC3339),
		)
	}

	nextMarkerXML := ""
	if isTruncated {
		nextMarkerXML = fmt.Sprintf(`<NextMarker>%s</NextMarker>`, nextMarker)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<InvalidationList xmlns="%s">`+
		`<IsTruncated>%t</IsTruncated>`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`%s`+
		`<Items>%s</Items>`+
		`</InvalidationList>`,
		cfNS, isTruncated, pageSize, len(page), nextMarkerXML, sb.String())

	return xmlResp(c, http.StatusOK, resp)
}

// handleGetInvalidation returns a specific CloudFront invalidation by ID.
func (h *Handler) handleGetInvalidation(c *echo.Context, distID string) error {
	// Extract invalidation ID from the URL path after /invalidation/.
	path := c.Request().URL.Path
	invID := ""

	if _, after, ok := strings.Cut(path, "/invalidation/"); ok {
		invID = after
		// Trim any trailing slashes or sub-paths.
		if slash := strings.Index(invID, "/"); slash >= 0 {
			invID = invID[:slash]
		}
	}

	if invID == "" {
		return xmlResp(c, http.StatusBadRequest,
			cfErrorXML("InvalidArgument", "invalidation ID is required"))
	}

	inv, err := h.Backend.GetInvalidation(distID, invID)
	if err != nil {
		return h.handleError(c, err)
	}

	var pathsSB strings.Builder

	for _, p := range inv.Paths {
		pathsSB.WriteString(`<Path>` + p + `</Path>`)
	}

	resp := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<Invalidation xmlns="%s">`+
			`<Id>%s</Id>`+
			`<Status>%s</Status>`+
			`<CreateTime>%s</CreateTime>`+
			`<InvalidationBatch>`+
			`<CallerReference>%s</CallerReference>`+
			`<Paths>`+
			`<Quantity>%d</Quantity>`+
			`<Items>%s</Items>`+
			`</Paths>`+
			`</InvalidationBatch>`+
			`</Invalidation>`,
		cfNS,
		inv.ID,
		inv.Status,
		inv.CreateTime.Format(time.RFC3339),
		inv.CallerRef,
		len(inv.Paths),
		pathsSB.String(),
	)

	return xmlResp(c, http.StatusOK, resp)
}
