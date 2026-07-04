package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// TrustStore handlers
// ---------------------------------------------------------------------------

type trustStoreCertificateBundleXML struct {
	S3Bucket                string `xml:"S3Bucket"`
	S3Key                   string `xml:"S3Key"`
	InlineCertificateBundle string `xml:"InlineCertificateBundle"`
}

type trustStoreConfigXML struct {
	XMLName                                xml.Name                       `xml:"TrustStoreConfig"`
	Name                                   string                         `xml:"Name"`
	Comment                                string                         `xml:"Comment"`
	CertificateAuthorityCertificatesBundle trustStoreCertificateBundleXML `xml:"CertificateAuthorityCertificatesBundle"`
}

func trustStoreBundleFromXML(x trustStoreCertificateBundleXML) TrustStoreCertificateBundle {
	return TrustStoreCertificateBundle(x)
}

func trustStoreXML(ns string, ts *TrustStore) string {
	bundle := ts.CertificateAuthorityCertificatesBundle

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<TrustStore xmlns="%s">`+
		`<Id>%s</Id><ARN>%s</ARN><Name>%s</Name><Comment>%s</Comment><Status>%s</Status>`+
		`<LastModifiedTime>%s</LastModifiedTime>`+
		`<CertificateAuthorityCertificatesBundle>`+
		`<S3Bucket>%s</S3Bucket><S3Key>%s</S3Key><InlineCertificateBundle>%s</InlineCertificateBundle>`+
		`</CertificateAuthorityCertificatesBundle>`+
		`</TrustStore>`,
		ns, ts.ID, ts.ARN, ts.Name, ts.Comment, ts.Status, ts.LastModifiedTime,
		bundle.S3Bucket, bundle.S3Key, bundle.InlineCertificateBundle)
}

func (h *Handler) handleCreateTrustStore(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}
	var req trustStoreConfigXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}
	ts, createErr := h.Backend.CreateTrustStore(
		req.Name, req.Comment, trustStoreBundleFromXML(req.CertificateAuthorityCertificatesBundle),
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}
	c.Response().Header().Set("ETag", ts.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"trust-store/"+ts.ID)

	return xmlResp(c, http.StatusCreated, trustStoreXML(cfNS, ts))
}

func (h *Handler) handleGetTrustStore(c *echo.Context, id string) error {
	ts, err := h.Backend.GetTrustStore(id)
	if err != nil {
		return h.handleError(c, err)
	}
	c.Response().Header().Set("ETag", ts.ETag)

	return xmlResp(c, http.StatusOK, trustStoreXML(cfNS, ts))
}

//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListTrustStores(c *echo.Context) error {
	items := h.Backend.ListTrustStores()

	type tsSummary struct {
		XMLName xml.Name `xml:"TrustStoreSummary"`
		ID      string   `xml:"Id"`
		ARN     string   `xml:"ARN"`
		Name    string   `xml:"Name"`
	}
	type tsList struct {
		XMLName  xml.Name    `xml:"TrustStoreList"`
		XMLNS    string      `xml:"xmlns,attr"`
		Items    []tsSummary `xml:"Items>TrustStoreSummary"`
		Quantity int         `xml:"Quantity"`
	}
	summaries := make([]tsSummary, 0, len(items))
	for _, ts := range items {
		summaries = append(summaries, tsSummary{ID: ts.ID, ARN: ts.ARN, Name: ts.Name})
	}
	list := tsList{XMLNS: cfNS, Quantity: len(summaries), Items: summaries}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateTrustStore(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetTrustStore(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current trust store ETag"),
		)
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}
	var req trustStoreConfigXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}
	ts, updateErr := h.Backend.UpdateTrustStore(
		id, req.Name, req.Comment, trustStoreBundleFromXML(req.CertificateAuthorityCertificatesBundle),
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}
	c.Response().Header().Set("ETag", ts.ETag)

	return xmlResp(c, http.StatusOK, trustStoreXML(cfNS, ts))
}

func (h *Handler) handleDeleteTrustStore(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetTrustStore(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current trust store ETag"),
		)
	}

	if err := h.Backend.DeleteTrustStore(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// StreamingDistribution handlers
// ---------------------------------------------------------------------------

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
	XMLName                     xml.Name                       `xml:"StreamingDistributionConfigWithTags"`
	Tags                        []tagXML                       `xml:"Tags>Tag"`
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

func (h *Handler) handleListStreamingDistributions(c *echo.Context) error {
	items := h.Backend.ListStreamingDistributions()

	type sdList struct {
		XMLName     xml.Name                          `xml:"StreamingDistributionList"`
		XMLNS       string                            `xml:"xmlns,attr"`
		Items       []streamingDistributionSummaryXML `xml:"Items>StreamingDistributionSummary"`
		MaxItems    int                               `xml:"MaxItems"`
		Quantity    int                               `xml:"Quantity"`
		IsTruncated bool                              `xml:"IsTruncated"`
	}
	summaries := make([]streamingDistributionSummaryXML, 0, len(items))
	for _, sd := range items {
		summaries = append(summaries, streamingDistributionSummary(sd))
	}
	list := sdList{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}
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

func monitoringSubscriptionXML(ns string, ms *MonitoringSubscription) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<MonitoringSubscription xmlns="%s">`+
		`<RealtimeMetricsSubscriptionConfig>`+
		`<RealtimeMetricsSubscriptionStatus>%s</RealtimeMetricsSubscriptionStatus>`+
		`</RealtimeMetricsSubscriptionConfig>`+
		`</MonitoringSubscription>`,
		ns, ms.RealtimeMetricsSubscriptionStatus)
}

func (h *Handler) handleCreateMonitoringSubscription(c *echo.Context, distributionID string) error {
	// Check if enabled from body.
	body, _ := readBody(c)
	enabled := true
	if len(body) > 0 {
		var req struct {
			XMLName xml.Name `xml:"MonitoringSubscription"`
			Status  string   `xml:"RealtimeMetricsSubscriptionConfig>RealtimeMetricsSubscriptionStatus"`
		}
		_ = xml.Unmarshal(body, &req)
		enabled = req.Status != metricDisabled
	}
	if err := h.Backend.CreateMonitoringSubscription(distributionID, enabled); err != nil {
		return h.handleError(c, err)
	}
	ms, _ := h.Backend.GetMonitoringSubscription(distributionID)

	return xmlResp(c, http.StatusOK, monitoringSubscriptionXML(cfNS, ms))
}

func (h *Handler) handleGetMonitoringSubscription(c *echo.Context, distributionID string) error {
	ms, err := h.Backend.GetMonitoringSubscription(distributionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return xmlResp(c, http.StatusOK, monitoringSubscriptionXML(cfNS, ms))
}

func (h *Handler) handleDeleteMonitoringSubscription(c *echo.Context, distributionID string) error {
	if err := h.Backend.DeleteMonitoringSubscription(distributionID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// ResourcePolicy handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleGetResourcePolicy(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("arn")
	policy, err := h.Backend.GetResourcePolicy(resourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	return xmlResp(c, http.StatusOK, fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ResourcePolicy xmlns="%s"><Policy>%s</Policy></ResourcePolicy>`,
		cfNS, policy,
	))
}

func (h *Handler) handlePutResourcePolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}
	var req struct {
		XMLName     xml.Name `xml:"ResourcePolicy"`
		Policy      string   `xml:"Policy"`
		ResourceARN string   `xml:"ResourceArn"`
	}
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}
	resourceARN := req.ResourceARN
	if resourceARN == "" {
		resourceARN = c.Request().URL.Query().Get("arn")
	}
	if putErr := h.Backend.PutResourcePolicy(resourceARN, req.Policy); putErr != nil {
		return h.handleError(c, putErr)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("arn")
	if err := h.Backend.DeleteResourcePolicy(resourceARN); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// ConnectionGroup extra handlers (Get/List/Update/Delete)
// ---------------------------------------------------------------------------

// connectionGroupPreconditionFailedXML is the shared If-Match error body for connection group
// mutations, mirroring the trust store / streaming distribution PreconditionFailed pattern.
func connectionGroupPreconditionFailedXML() string {
	return cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current connection group ETag")
}

func connectionGroupXML(cg *ConnectionGroup) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ConnectionGroup xmlns="%s">`+
		`<Id>%s</Id><Name>%s</Name><ARN>%s</ARN><Comment>%s</Comment>`+
		`<CreatedTime>%s</CreatedTime><LastModifiedTime>%s</LastModifiedTime>`+
		`<Status>%s</Status><Enabled>%v</Enabled><Ipv6Enabled>%v</Ipv6Enabled>`+
		`<IsDefault>%v</IsDefault><RoutingEndpoint>%s</RoutingEndpoint>`+
		`<AnycastIpListId>%s</AnycastIpListId>`+
		`</ConnectionGroup>`,
		cfNS, cg.ID, cg.Name, cg.ARN, cg.Comment,
		cg.CreatedTime, cg.LastModifiedTime,
		cg.Status, cg.Enabled, cg.IPv6Enabled,
		cg.IsDefault, cg.RoutingEndpoint,
		cg.AnycastIPListID)
}

func (h *Handler) handleGetConnectionGroup(c *echo.Context, id string) error {
	cg, err := h.Backend.GetConnectionGroup(id)
	if err != nil {
		return h.handleError(c, err)
	}
	c.Response().Header().Set("ETag", cg.ETag)

	return xmlResp(c, http.StatusOK, connectionGroupXML(cg))
}

func (h *Handler) handleGetConnectionGroupByRoutingEndpoint(c *echo.Context, endpoint string) error {
	cg, err := h.Backend.GetConnectionGroupByRoutingEndpoint(endpoint)
	if err != nil {
		return h.handleError(c, err)
	}
	c.Response().Header().Set("ETag", cg.ETag)

	return xmlResp(c, http.StatusOK, connectionGroupXML(cg))
}

func (h *Handler) handleListConnectionGroups(c *echo.Context) error {
	items := h.Backend.ListConnectionGroups()

	type cgSummary struct {
		XMLName         xml.Name `xml:"ConnectionGroupSummary"`
		ID              string   `xml:"Id"`
		Name            string   `xml:"Name"`
		ARN             string   `xml:"ARN"`
		ETag            string   `xml:"ETag"`
		RoutingEndpoint string   `xml:"RoutingEndpoint"`
		Status          string   `xml:"Status"`
	}
	type cgList struct {
		XMLName  xml.Name    `xml:"ConnectionGroupList"`
		XMLNS    string      `xml:"xmlns,attr"`
		Items    []cgSummary `xml:"Items>ConnectionGroupSummary"`
		Quantity int         `xml:"Quantity"`
	}
	summaries := make([]cgSummary, 0, len(items))
	for _, cg := range items {
		summaries = append(summaries, cgSummary{
			ID: cg.ID, Name: cg.Name, ARN: cg.ARN, ETag: cg.ETag,
			RoutingEndpoint: cg.RoutingEndpoint, Status: cg.Status,
		})
	}
	list := cgList{XMLNS: cfNS, Quantity: len(summaries), Items: summaries}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateConnectionGroup(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetConnectionGroup(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, connectionGroupPreconditionFailedXML())
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}
	var req updateConnectionGroupRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid UpdateConnectionGroupRequest XML"),
			)
		}
	}

	var anycastIPListID *string
	if req.AnycastIPListID != "" {
		anycastIPListID = &req.AnycastIPListID
	}

	cg, updateErr := h.Backend.UpdateConnectionGroup(id, req.Comment, anycastIPListID, req.Ipv6Enabled, req.Enabled)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}
	c.Response().Header().Set("ETag", cg.ETag)

	return xmlResp(c, http.StatusOK, connectionGroupXML(cg))
}

func (h *Handler) handleDeleteConnectionGroup(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetConnectionGroup(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, connectionGroupPreconditionFailedXML())
	}

	if err := h.Backend.DeleteConnectionGroup(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// ConnectionFunction extra handlers (Get/Describe/List/Update/Delete/Publish/Test)
// ---------------------------------------------------------------------------

// connectionFunctionPreconditionFailedXML is the shared If-Match error body for connection
// function mutations.
func connectionFunctionPreconditionFailedXML() string {
	return cfErrorXML(
		"PreconditionFailed",
		"If-Match ETag did not match the current connection function ETag",
	)
}

// connectionFunctionSummaryXML builds the ConnectionFunctionSummary XML representation used by
// DescribeConnectionFunction, Publish, and List responses.
func connectionFunctionSummaryXML(fn *ConnectionFunction) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ConnectionFunctionSummary xmlns="%s">`+
		`<Id>%s</Id><ConnectionFunctionArn>%s</ConnectionFunctionArn><Name>%s</Name>`+
		`<ConnectionFunctionConfig><Comment>%s</Comment><Runtime>%s</Runtime></ConnectionFunctionConfig>`+
		`<Stage>%s</Stage><Status>%s</Status>`+
		`<CreatedTime>%s</CreatedTime><LastModifiedTime>%s</LastModifiedTime>`+
		`</ConnectionFunctionSummary>`,
		cfNS, fn.ID, fn.ARN, fn.Name, fn.Comment, fn.Runtime, fn.Stage, fn.Status,
		fn.CreatedTime, fn.LastModifiedTime)
}

// handleGetConnectionFunction returns the connection function's code and content type, mirroring
// GetConnectionFunctionOutput (ConnectionFunctionCode + ContentType), unlike
// DescribeConnectionFunction which returns metadata only.
func (h *Handler) handleGetConnectionFunction(c *echo.Context, id string) error {
	fn, err := h.Backend.GetConnectionFunction(id)
	if err != nil {
		return h.handleError(c, err)
	}
	c.Response().Header().Set("ETag", fn.ETag)

	return c.Blob(http.StatusOK, "application/octet-stream", fn.FunctionCode)
}

func (h *Handler) handleDescribeConnectionFunction(c *echo.Context, id string) error {
	fn, err := h.Backend.GetConnectionFunction(id)
	if err != nil {
		return h.handleError(c, err)
	}
	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusOK, connectionFunctionSummaryXML(fn))
}

func (h *Handler) handleListConnectionFunctions(c *echo.Context) error {
	items := h.Backend.ListConnectionFunctions()

	type cfnConfig struct {
		Comment string `xml:"Comment"`
		Runtime string `xml:"Runtime"`
	}
	type cfnSummary struct {
		XMLName xml.Name  `xml:"ConnectionFunctionSummary"`
		ID      string    `xml:"Id"`
		ARN     string    `xml:"ConnectionFunctionArn"`
		Name    string    `xml:"Name"`
		Config  cfnConfig `xml:"ConnectionFunctionConfig"`
		Stage   string    `xml:"Stage"`
		Status  string    `xml:"Status"`
	}
	type cfnList struct {
		XMLName  xml.Name     `xml:"ConnectionFunctionList"`
		XMLNS    string       `xml:"xmlns,attr"`
		Items    []cfnSummary `xml:"Items>ConnectionFunctionSummary"`
		Quantity int          `xml:"Quantity"`
	}
	summaries := make([]cfnSummary, 0, len(items))
	for _, fn := range items {
		summaries = append(summaries, cfnSummary{
			ID: fn.ID, ARN: fn.ARN, Name: fn.Name, Stage: fn.Stage, Status: fn.Status,
			Config: cfnConfig{Comment: fn.Comment, Runtime: fn.Runtime},
		})
	}
	list := cfnList{XMLNS: cfNS, Quantity: len(summaries), Items: summaries}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateConnectionFunction(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetConnectionFunction(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, connectionFunctionPreconditionFailedXML())
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}
	var req updateConnectionFunctionRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid UpdateConnectionFunctionRequest XML"),
			)
		}
	}

	fn, updateErr := h.Backend.UpdateConnectionFunction(
		id, req.ConnectionFunctionConfig.Comment, req.ConnectionFunctionConfig.Runtime,
		decodeConnectionFunctionCode(req.ConnectionFunctionCode),
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}
	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusOK, connectionFunctionSummaryXML(fn))
}

func (h *Handler) handleDeleteConnectionFunction(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetConnectionFunction(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, connectionFunctionPreconditionFailedXML())
	}

	if err := h.Backend.DeleteConnectionFunction(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handlePublishConnectionFunction(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetConnectionFunction(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, connectionFunctionPreconditionFailedXML())
	}

	fn, err := h.Backend.PublishConnectionFunction(id)
	if err != nil {
		return h.handleError(c, err)
	}
	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusOK, connectionFunctionSummaryXML(fn))
}

// testConnectionFunctionRequestXML models the TestConnectionFunction request body: the
// base64-encoded connection object to run the function against.
type testConnectionFunctionRequestXML struct {
	XMLName          xml.Name `xml:"TestConnectionFunctionRequest"`
	ConnectionObject string   `xml:"ConnectionObject"`
}

func (h *Handler) handleTestConnectionFunction(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetConnectionFunction(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, connectionFunctionPreconditionFailedXML())
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}
	var req testConnectionFunctionRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	result, testErr := h.Backend.TestConnectionFunction(id, decodeConnectionFunctionCode(req.ConnectionObject))
	if testErr != nil {
		return h.handleError(c, testErr)
	}

	var logsXML strings.Builder
	for _, l := range result.ExecutionLogs {
		fmt.Fprintf(&logsXML, "<member>%s</member>", l)
	}

	return xmlResp(c, http.StatusOK, fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<TestResult xmlns="%s">`+
			`<ConnectionFunctionSummary><Id>%s</Id><Name>%s</Name><Stage>%s</Stage></ConnectionFunctionSummary>`+
			`<ConnectionFunctionExecutionLogs>%s</ConnectionFunctionExecutionLogs>`+
			`<ConnectionFunctionErrorMessage></ConnectionFunctionErrorMessage>`+
			`<ConnectionFunctionOutput>%s</ConnectionFunctionOutput>`+
			`<ComputeUtilization>%s</ComputeUtilization>`+
			`</TestResult>`,
		cfNS, current.ID, current.Name, current.Stage,
		logsXML.String(), result.FunctionOutput, result.ComputeUtilization,
	))
}

// ---------------------------------------------------------------------------
// AnycastIPList extra handlers (Get/List/Update/Delete)
// ---------------------------------------------------------------------------

func anycastIPListXML(ns string, list *AnycastIPList) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<AnycastIpList xmlns="%s">`+
		`<Id>%s</Id><ARN>%s</ARN><Name>%s</Name><Status>%s</Status><IpCount>%d</IpCount>`+
		`</AnycastIpList>`,
		ns, list.ID, list.ARN, list.Name, list.Status, list.IPCount)
}

func (h *Handler) handleGetAnycastIPList(c *echo.Context, id string) error {
	list, err := h.Backend.GetAnycastIPList(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return xmlResp(c, http.StatusOK, anycastIPListXML(cfNS, list))
}

//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListAnycastIPLists(c *echo.Context) error {
	items := h.Backend.ListAnycastIPLists()

	type ailSummary struct {
		XMLName xml.Name `xml:"AnycastIpListSummary"`
		ID      string   `xml:"Id"`
		Name    string   `xml:"Name"`
		Status  string   `xml:"Status"`
	}
	type ailList struct {
		XMLName  xml.Name     `xml:"AnycastIpLists"`
		XMLNS    string       `xml:"xmlns,attr"`
		Items    []ailSummary `xml:"Items>AnycastIpListSummary"`
		Quantity int          `xml:"Quantity"`
	}
	summaries := make([]ailSummary, 0, len(items))
	for _, ail := range items {
		summaries = append(summaries, ailSummary{ID: ail.ID, Name: ail.Name, Status: ail.Status})
	}
	list := ailList{XMLNS: cfNS, Quantity: len(summaries), Items: summaries}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateAnycastIPList(c *echo.Context, id string) error {
	var req struct {
		XMLName xml.Name `xml:"AnycastIpListConfig"`
		IPCount int32    `xml:"IpCount"`
	}
	body, _ := readBody(c)
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}
	list, updateErr := h.Backend.UpdateAnycastIPList(id, req.IPCount)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	return xmlResp(c, http.StatusOK, anycastIPListXML(cfNS, list))
}

func (h *Handler) handleDeleteAnycastIPList(c *echo.Context, id string) error {
	if err := h.Backend.DeleteAnycastIPList(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// ListDistributionsBy* handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleListDistributionsByWebACLID(c *echo.Context, webACLID string) error {
	dists := h.Backend.ListDistributionsByWebACLID(webACLID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByCachePolicyID(c *echo.Context, policyID string) error {
	dists := h.Backend.ListDistributionsByCachePolicyID(policyID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByOriginRequestPolicyID(c *echo.Context, policyID string) error {
	dists := h.Backend.ListDistributionsByOriginRequestPolicyID(policyID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByResponseHeadersPolicyID(c *echo.Context, policyID string) error {
	dists := h.Backend.ListDistributionsByResponseHeadersPolicyID(policyID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByRealtimeLogConfig(c *echo.Context, arn string) error {
	dists := h.Backend.ListDistributionsByRealtimeLogConfigARN(arn)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) marshalDistributionList(c *echo.Context, dists []*Distribution) error {
	type distSummary struct {
		XMLName    xml.Name `xml:"DistributionSummary"`
		ID         string   `xml:"Id"`
		ARN        string   `xml:"ARN"`
		Status     string   `xml:"Status"`
		DomainName string   `xml:"DomainName"`
	}
	type distList struct {
		XMLName     xml.Name      `xml:"DistributionList"`
		XMLNS       string        `xml:"xmlns,attr"`
		Items       []distSummary `xml:"Items>DistributionSummary"`
		MaxItems    int           `xml:"MaxItems"`
		Quantity    int           `xml:"Quantity"`
		IsTruncated bool          `xml:"IsTruncated"`
	}
	summaries := make([]distSummary, 0, len(dists))
	for _, d := range dists {
		summaries = append(summaries, distSummary{
			ID: d.ID, ARN: d.ARN, Status: d.Status, DomainName: d.DomainName,
		})
	}
	list := distList{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}
