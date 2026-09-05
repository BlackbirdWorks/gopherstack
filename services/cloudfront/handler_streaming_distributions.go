package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

// streamingDistributionS3OriginXML models the S3Origin element of an incoming
// StreamingDistributionConfig.
type streamingDistributionS3OriginXML struct {
	DomainName           string `xml:"DomainName"`
	OriginAccessIdentity string `xml:"OriginAccessIdentity"`
}

// streamingDistributionTrustedSignersXML models the TrustedSigners element of an incoming
// StreamingDistributionConfig.
type streamingDistributionTrustedSignersXML struct {
	Items   []string `xml:"Items>AwsAccountNumber"`
	Enabled bool     `xml:"Enabled"`
}

// streamingDistributionConfigXML models an incoming StreamingDistributionConfig request body.
type streamingDistributionConfigXML struct {
	XMLName         xml.Name                         `xml:"StreamingDistributionConfig"`
	CallerReference string                           `xml:"CallerReference"`
	Comment         string                           `xml:"Comment"`
	PriceClass      string                           `xml:"PriceClass"`
	S3Origin        streamingDistributionS3OriginXML `xml:"S3Origin"`
	Aliases         struct {
		Items []string `xml:"Items>CNAME"`
	} `xml:"Aliases"`
	TrustedSigners streamingDistributionTrustedSignersXML `xml:"TrustedSigners"`
	Enabled        bool                                   `xml:"Enabled"`
}

// streamingDistributionConfigWithTagsXML models an incoming StreamingDistributionConfigWithTags body.
type streamingDistributionConfigWithTagsXML struct {
	XMLName xml.Name `xml:"StreamingDistributionConfigWithTags"`
	// Tags is *types.Tags on the wire: Items wraps the Tag list, not a bare
	// Tags>Tag path (cloudfront@v1.67.4 serializers.go
	// awsRestxml_serializeDocumentTags).
	Tags                        []tagXML                       `xml:"Tags>Items>Tag"`
	StreamingDistributionConfig streamingDistributionConfigXML `xml:"StreamingDistributionConfig"`
}

// streamingConfigFromXML converts a parsed streamingDistributionConfigXML into the backend's
// StreamingDistributionConfig representation.
func streamingConfigFromXML(x streamingDistributionConfigXML) StreamingDistributionConfig {
	return StreamingDistributionConfig{
		CallerReference: x.CallerReference,
		Comment:         x.Comment,
		PriceClass:      x.PriceClass,
		S3Origin: StreamingDistributionS3Origin{
			DomainName:           x.S3Origin.DomainName,
			OriginAccessIdentity: x.S3Origin.OriginAccessIdentity,
		},
		Aliases: x.Aliases.Items,
		TrustedSigners: StreamingDistributionTrustedSigners{
			Enabled: x.TrustedSigners.Enabled,
			Items:   x.TrustedSigners.Items,
		},
		Enabled: x.Enabled,
	}
}

// streamingDistributionXML builds the full StreamingDistribution XML response, embedding the
// raw StreamingDistributionConfig the caller last submitted (matching the Distribution pattern).
func streamingDistributionXML(sd *StreamingDistribution) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<StreamingDistribution xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Status>%s</Status>`+
		`<LastModifiedTime>%s</LastModifiedTime>`+
		`<DomainName>%s</DomainName>`+
		`%s`+
		`</StreamingDistribution>`,
		cfNS, sd.ID, sd.ARN, sd.Status, sd.LastModifiedTime, sd.DomainName, string(sd.RawConfig))
}

func (h *Handler) handleCreateStreamingDistribution(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var cfg streamingDistributionConfigXML
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid StreamingDistributionConfig XML"))
	}

	sd, createErr := h.Backend.CreateStreamingDistribution(streamingConfigFromXML(cfg), body)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"streaming-distribution/"+sd.ID)
	c.Response().Header().Set("ETag", sd.ETag)

	return xmlResp(c, http.StatusCreated, streamingDistributionXML(sd))
}

func (h *Handler) handleCreateStreamingDistributionWithTags(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req streamingDistributionConfigWithTagsXML
	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("MalformedXML", "invalid StreamingDistributionConfigWithTags XML"),
		)
	}

	rawConfig, marshalErr := xml.Marshal(req.StreamingDistributionConfig)
	if marshalErr != nil {
		rawConfig = body
	}

	sd, createErr := h.Backend.CreateStreamingDistribution(
		streamingConfigFromXML(req.StreamingDistributionConfig),
		rawConfig,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	if len(req.Tags) > 0 {
		tags := make(map[string]string, len(req.Tags))
		for _, tag := range req.Tags {
			tags[tag.Key] = tag.Value
		}
		if tagErr := h.Backend.TagResource(sd.ARN, tags); tagErr != nil {
			return h.handleError(c, tagErr)
		}
	}

	c.Response().Header().Set("Location", cfPathPrefix+"streaming-distribution/"+sd.ID)
	c.Response().Header().Set("ETag", sd.ETag)

	return xmlResp(c, http.StatusCreated, streamingDistributionXML(sd))
}

func (h *Handler) handleGetStreamingDistribution(c *echo.Context, id string) error {
	sd, err := h.Backend.GetStreamingDistribution(id)
	if err != nil {
		return h.handleError(c, err)
	}
	c.Response().Header().Set("ETag", sd.ETag)

	return xmlResp(c, http.StatusOK, streamingDistributionXML(sd))
}

func (h *Handler) handleGetStreamingDistributionConfig(c *echo.Context, id string) error {
	sd, err := h.Backend.GetStreamingDistribution(id)
	if err != nil {
		return h.handleError(c, err)
	}
	c.Response().Header().Set("ETag", sd.ETag)
	config := sd.RawConfig
	if len(config) == 0 {
		config = []byte(`<StreamingDistributionConfig xmlns="` + cfNS + `"/>`)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(config))
}

// streamingDistributionSummaryXML models one entry in a ListStreamingDistributions response.
type streamingDistributionSummaryXML struct {
	XMLName  xml.Name `xml:"StreamingDistributionSummary"`
	S3Origin struct {
		DomainName           string `xml:"DomainName"`
		OriginAccessIdentity string `xml:"OriginAccessIdentity"`
	} `xml:"S3Origin"`
	ID               string `xml:"Id"`
	ARN              string `xml:"ARN"`
	Status           string `xml:"Status"`
	LastModifiedTime string `xml:"LastModifiedTime"`
	DomainName       string `xml:"DomainName"`
	Comment          string `xml:"Comment"`
	PriceClass       string `xml:"PriceClass"`
	Aliases          struct {
		Items    []string `xml:"Items>CNAME"`
		Quantity int      `xml:"Quantity"`
	} `xml:"Aliases"`
	TrustedSigners struct {
		Items    []string `xml:"Items>AwsAccountNumber"`
		Quantity int      `xml:"Quantity"`
		Enabled  bool     `xml:"Enabled"`
	} `xml:"TrustedSigners"`
	Enabled bool `xml:"Enabled"`
}

// streamingDistributionSummary builds a list summary entry from a stored StreamingDistribution.
func streamingDistributionSummary(sd *StreamingDistribution) streamingDistributionSummaryXML {
	s := streamingDistributionSummaryXML{
		ID:               sd.ID,
		ARN:              sd.ARN,
		Status:           sd.Status,
		LastModifiedTime: sd.LastModifiedTime,
		DomainName:       sd.DomainName,
		Comment:          sd.Config.Comment,
		PriceClass:       sd.Config.PriceClass,
		Enabled:          sd.Config.Enabled,
	}
	s.S3Origin.DomainName = sd.Config.S3Origin.DomainName
	s.S3Origin.OriginAccessIdentity = sd.Config.S3Origin.OriginAccessIdentity
	s.Aliases.Quantity = len(sd.Config.Aliases)
	s.Aliases.Items = sd.Config.Aliases
	s.TrustedSigners.Enabled = sd.Config.TrustedSigners.Enabled
	s.TrustedSigners.Quantity = len(sd.Config.TrustedSigners.Items)
	s.TrustedSigners.Items = sd.Config.TrustedSigners.Items

	return s
}

// handleListStreamingDistributions paginates via Marker/MaxItems (both query-bound,
// cloudfront@v1.67.4 serializers.go).
func (h *Handler) handleListStreamingDistributions(c *echo.Context) error {
	items := h.Backend.ListStreamingDistributions()

	page, pageSize, isTruncated, nextMarker := paginateByMarkerID(
		c, items, func(sd *StreamingDistribution) string { return sd.ID },
	)

	type sdList struct {
		XMLName     xml.Name                          `xml:"StreamingDistributionList"`
		XMLNS       string                            `xml:"xmlns,attr"`
		NextMarker  string                            `xml:"NextMarker,omitempty"`
		Items       []streamingDistributionSummaryXML `xml:"Items>StreamingDistributionSummary"`
		MaxItems    int                               `xml:"MaxItems"`
		Quantity    int                               `xml:"Quantity"`
		IsTruncated bool                              `xml:"IsTruncated"`
	}
	summaries := make([]streamingDistributionSummaryXML, 0, len(page))
	for _, sd := range page {
		summaries = append(summaries, streamingDistributionSummary(sd))
	}
	list := sdList{
		XMLNS: cfNS, NextMarker: nextMarker, MaxItems: pageSize, Quantity: len(summaries),
		Items: summaries, IsTruncated: isTruncated,
	}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateStreamingDistribution(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var cfg streamingDistributionConfigXML
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid StreamingDistributionConfig XML"))
	}

	current, getErr := h.Backend.GetStreamingDistribution(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current streaming distribution config ETag",
			),
		)
	}

	sd, updateErr := h.Backend.UpdateStreamingDistribution(id, streamingConfigFromXML(cfg), body)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}
	c.Response().Header().Set("ETag", sd.ETag)

	return xmlResp(c, http.StatusOK, streamingDistributionXML(sd))
}

func (h *Handler) handleDeleteStreamingDistribution(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetStreamingDistribution(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current streaming distribution ETag"),
		)
	}

	if err := h.Backend.DeleteStreamingDistribution(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// MonitoringSubscription handlers
// ---------------------------------------------------------------------------
