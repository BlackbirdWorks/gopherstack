package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

type continuousDeploymentSessionStickinessConfigXML struct {
	IdleTTL    int32 `xml:"IdleTTL"`
	MaximumTTL int32 `xml:"MaximumTTL"`
}

type continuousDeploymentSingleWeightConfigXML struct {
	SessionStickinessConfig *continuousDeploymentSessionStickinessConfigXML `xml:"SessionStickinessConfig"`
	Weight                  float64                                         `xml:"Weight"`
}

type continuousDeploymentSingleHeaderConfigXML struct {
	Header string `xml:"Header"`
	Value  string `xml:"Value"`
}

type continuousDeploymentTrafficConfigXML struct {
	SingleWeightConfig *continuousDeploymentSingleWeightConfigXML `xml:"SingleWeightConfig"`
	SingleHeaderConfig *continuousDeploymentSingleHeaderConfigXML `xml:"SingleHeaderConfig"`
	Type               string                                     `xml:"Type"`
}

type continuousDeploymentPolicyConfigXML struct {
	XMLName                     xml.Name                              `xml:"ContinuousDeploymentPolicyConfig"`
	TrafficConfig               *continuousDeploymentTrafficConfigXML `xml:"TrafficConfig"`
	StagingDistributionDNS      string                                `xml:"StagingDistributionDnsNames>DnsName,omitempty"`
	StagingDistributionDNSNames []string                              `xml:"StagingDistributionDnsNames>Items>DnsName"`
	Enabled                     bool                                  `xml:"Enabled"`
}

// dnsNames returns the effective list of staging distribution DNS names from a parsed
// ContinuousDeploymentPolicyConfig request, preferring the real AWS Items>DnsName list shape
// and falling back to the legacy bare DnsName element for backward compatibility.
func (req continuousDeploymentPolicyConfigXML) dnsNames() []string {
	if len(req.StagingDistributionDNSNames) > 0 {
		return req.StagingDistributionDNSNames
	}

	return dnsNamesFromSingle(req.StagingDistributionDNS)
}

// trafficConfig converts a parsed TrafficConfig XML element into the backend model, returning
// the zero value if the request did not include one.
func (req continuousDeploymentPolicyConfigXML) trafficConfig() ContinuousDeploymentTrafficConfig {
	if req.TrafficConfig == nil {
		return ContinuousDeploymentTrafficConfig{}
	}

	out := ContinuousDeploymentTrafficConfig{Type: req.TrafficConfig.Type}
	if swc := req.TrafficConfig.SingleWeightConfig; swc != nil {
		out.SingleWeightConfig = &ContinuousDeploymentSingleWeightConfig{Weight: swc.Weight}
		if ssc := swc.SessionStickinessConfig; ssc != nil {
			out.SingleWeightConfig.SessionStickinessConfig = &ContinuousDeploymentSessionStickinessConfig{
				IdleTTL:    ssc.IdleTTL,
				MaximumTTL: ssc.MaximumTTL,
			}
		}
	}
	if shc := req.TrafficConfig.SingleHeaderConfig; shc != nil {
		out.SingleHeaderConfig = &ContinuousDeploymentSingleHeaderConfig{Header: shc.Header, Value: shc.Value}
	}

	return out
}

func (h *Handler) handleCreateContinuousDeploymentPolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req continuousDeploymentPolicyConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid ContinuousDeploymentPolicyConfig XML"))
		}
	}

	policy, createErr := h.Backend.CreateContinuousDeploymentPolicyWithConfig(
		req.Enabled, req.dnsNames(), req.trafficConfig(),
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	resp := continuousDeploymentPolicyXML(cfNS, policy)
	c.Response().Header().Set("Location", cfPathPrefix+"continuous-deployment-policy/"+policy.ID)
	c.Response().Header().Set("ETag", policy.ETag)

	return xmlResp(c, http.StatusCreated, resp)
}

// continuousDeploymentPolicyBodyXML renders the child elements of a ContinuousDeploymentPolicy
// (everything a real ContinuousDeploymentPolicy element contains, without the element itself),
// shared by the singular ContinuousDeploymentPolicy root and the nested
// ContinuousDeploymentPolicySummary>ContinuousDeploymentPolicy used by ListContinuousDeploymentPolicies.
func continuousDeploymentPolicyBodyXML(policy *ContinuousDeploymentPolicy) string {
	var dnsNames strings.Builder
	for _, dns := range policy.StagingDistributionDNSNames {
		fmt.Fprintf(&dnsNames, `<DnsName>%s</DnsName>`, dns)
	}

	return fmt.Sprintf(
		`<Id>%s</Id>`+
			`<ARN>%s</ARN>`+
			`<LastModifiedTime>%s</LastModifiedTime>`+
			`<ContinuousDeploymentPolicyConfig>`+
			`<StagingDistributionDnsNames><Quantity>%d</Quantity><Items>%s</Items></StagingDistributionDnsNames>`+
			`<Enabled>%v</Enabled>`+
			`<TrafficConfig><Type>%s</Type></TrafficConfig>`+
			`</ContinuousDeploymentPolicyConfig>`,
		policy.ID, policy.ARN, policy.LastModifiedTime,
		len(policy.StagingDistributionDNSNames), dnsNames.String(),
		policy.Enabled, policy.TrafficConfig.Type)
}

func continuousDeploymentPolicyXML(ns string, policy *ContinuousDeploymentPolicy) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ContinuousDeploymentPolicy xmlns="%s">%s</ContinuousDeploymentPolicy>`,
		ns, continuousDeploymentPolicyBodyXML(policy))
}

func (h *Handler) handleGetContinuousDeploymentPolicy(c *echo.Context, id string) error {
	policy, err := h.Backend.GetContinuousDeploymentPolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", policy.ETag)

	return xmlResp(c, http.StatusOK, continuousDeploymentPolicyXML(cfNS, policy))
}

func (h *Handler) handleUpdateContinuousDeploymentPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetContinuousDeploymentPolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, cfErrorXML(
			"PreconditionFailed", "If-Match ETag did not match the current continuous deployment policy ETag",
		))
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req continuousDeploymentPolicyConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid ContinuousDeploymentPolicyConfig XML"))
		}
	}

	policy, updateErr := h.Backend.UpdateContinuousDeploymentPolicyWithConfig(
		id, req.Enabled, req.dnsNames(), req.trafficConfig(),
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", policy.ETag)

	return xmlResp(c, http.StatusOK, continuousDeploymentPolicyXML(cfNS, policy))
}

func (h *Handler) handleDeleteContinuousDeploymentPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetContinuousDeploymentPolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, cfErrorXML(
			"PreconditionFailed", "If-Match ETag did not match the current continuous deployment policy ETag",
		))
	}

	if err := h.Backend.DeleteContinuousDeploymentPolicy(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// handleListContinuousDeploymentPolicies paginates via Marker/MaxItems (both query-bound,
// cloudfront@v1.67.4 serializers.go). Real ContinuousDeploymentPolicyList has no IsTruncated
// field -- NextMarker's presence alone signals truncation (types/types.go:1435-1455).
func (h *Handler) handleListContinuousDeploymentPolicies(c *echo.Context) error {
	policies := h.Backend.ListContinuousDeploymentPolicies()

	page, pageSize, isTruncated, nextMarker := paginateByMarkerID(
		c, policies, func(p *ContinuousDeploymentPolicy) string { return p.ID },
	)

	var sb strings.Builder

	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	sb.WriteString(`<ContinuousDeploymentPolicyList xmlns="`)
	sb.WriteString(cfNS)
	sb.WriteString(`">`)
	sb.WriteString(`<MaxItems>`)
	sb.WriteString(strconv.Itoa(pageSize))
	sb.WriteString(`</MaxItems>`)
	sb.WriteString(`<Quantity>`)
	sb.WriteString(strconv.Itoa(len(page)))
	sb.WriteString(`</Quantity>`)
	sb.WriteString(`<Items>`)

	// A ContinuousDeploymentPolicySummary wraps a single nested <ContinuousDeploymentPolicy>
	// child (awsRestxml_deserializeDocumentContinuousDeploymentPolicySummary case
	// "ContinuousDeploymentPolicy"), not Id/Enabled flattened directly onto the summary -- a
	// real client decodes ContinuousDeploymentPolicySummary.ContinuousDeploymentPolicy as nil
	// for every item against the flattened shape, giving the right item count with entirely
	// blank content.
	for _, p := range page {
		sb.WriteString(`<ContinuousDeploymentPolicySummary><ContinuousDeploymentPolicy>`)
		sb.WriteString(continuousDeploymentPolicyBodyXML(p))
		sb.WriteString(`</ContinuousDeploymentPolicy></ContinuousDeploymentPolicySummary>`)
	}

	sb.WriteString(`</Items>`)
	if isTruncated {
		sb.WriteString(`<NextMarker>` + nextMarker + `</NextMarker>`)
	}
	sb.WriteString(`</ContinuousDeploymentPolicyList>`)

	return xmlResp(c, http.StatusOK, sb.String())
}

// --- Function association handlers ---
