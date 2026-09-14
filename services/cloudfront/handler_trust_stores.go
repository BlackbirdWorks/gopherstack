package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// caCertificatesBundleS3LocationXML matches CaCertificatesBundleS3Location (cloudfront@v1.67.4
// types/types.go:301-317: Bucket/Key/Region, all required).
type caCertificatesBundleS3LocationXML struct {
	Bucket string `xml:"Bucket"`
	Key    string `xml:"Key"`
	Region string `xml:"Region"`
}

func (loc caCertificatesBundleS3LocationXML) bundle() TrustStoreCACertificatesBundleSource {
	return TrustStoreCACertificatesBundleSource{S3Bucket: loc.Bucket, S3Key: loc.Key, S3Region: loc.Region}
}

// updateTrustStoreRequestXML matches UpdateTrustStoreInput's body (cloudfront@v1.67.4
// api_op_UpdateTrustStore.go:28-46; serializers.go: awsRestxml_serializeOpUpdateTrustStore).
// Its root element is CaCertificatesBundleSource, with CaCertificatesBundleS3Location as its
// only child -- UpdateTrustStoreInput has no Name or Comment member at all, so real AWS cannot
// change either through this operation; Id/IfMatch travel as URI/header, and unlike
// CreateTrustStore, UseClientCertificateOCSPEndpoint travels as the
// Useclientcertificateocspendpoint header, not the body
// (awsRestxml_serializeOpHttpBindingsUpdateTrustStoreInput).
type updateTrustStoreRequestXML struct {
	XMLName                        xml.Name                          `xml:"CaCertificatesBundleSource"`
	CaCertificatesBundleS3Location caCertificatesBundleS3LocationXML `xml:"CaCertificatesBundleS3Location"`
}

func (req updateTrustStoreRequestXML) bundle() TrustStoreCACertificatesBundleSource {
	return req.CaCertificatesBundleS3Location.bundle()
}

// createTrustStoreRequestXML matches CreateTrustStoreInput (cloudfront@v1.67.4
// api_op_CreateTrustStore.go:28-50; serializers.go:
// awsRestxml_serializeOpDocumentCreateTrustStoreInput). The real root is
// <CreateTrustStoreRequest>; the XMLName field is intentionally omitted so Unmarshal does not
// reject the request based on the root element's name. AWS has no Comment member here.
type createTrustStoreRequestXML struct {
	CaCertificatesBundleSource struct {
		S3Location caCertificatesBundleS3LocationXML `xml:"CaCertificatesBundleS3Location"`
	} `xml:"CaCertificatesBundleSource"`
	Name                             string   `xml:"Name"`
	Tags                             []tagXML `xml:"Tags>Items>Tag"`
	UseClientCertificateOCSPEndpoint bool     `xml:"UseClientCertificateOCSPEndpoint"`
}

func (req createTrustStoreRequestXML) bundle() TrustStoreCACertificatesBundleSource {
	return req.CaCertificatesBundleSource.S3Location.bundle()
}

// trustStoreXML renders types.TrustStore (cloudfront@v1.67.4 types/types.go:6633-6661): Arn,
// Id, LastModifiedTime, Name, NumberOfCaCertificates, Reason, Status,
// UseClientCertificateOCSPEndpoint. AWS never echoes the CA bundle's content or location.
func trustStoreXML(ns string, ts *TrustStore) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<TrustStore xmlns="%s">`+
		`<Id>%s</Id><Arn>%s</Arn><Name>%s</Name><Status>%s</Status><Reason>%s</Reason>`+
		`<NumberOfCaCertificates>%d</NumberOfCaCertificates>`+
		`<UseClientCertificateOCSPEndpoint>%t</UseClientCertificateOCSPEndpoint>`+
		`<LastModifiedTime>%s</LastModifiedTime>`+
		`</TrustStore>`,
		ns, ts.ID, ts.ARN, ts.Name, ts.Status, ts.Reason,
		ts.NumberOfCaCertificates, ts.UseClientCertificateOCSPEndpoint, ts.LastModifiedTime)
}

func (h *Handler) handleCreateTrustStore(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}
	var req createTrustStoreRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid CreateTrustStoreRequest XML"))
		}
	}
	tags := make(map[string]string, len(req.Tags))
	for _, tag := range req.Tags {
		tags[tag.Key] = tag.Value
	}

	ts, createErr := h.Backend.CreateTrustStore(req.Name, req.bundle(), req.UseClientCertificateOCSPEndpoint, tags)
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

// listTrustStoresRequestXML matches ListTrustStoresInput: Marker/MaxItems travel in the XML
// request body, not the query string (cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOpDocumentListTrustStoresInput; ListTrustStores is a POST).
type listTrustStoresRequestXML struct {
	Marker   string `xml:"Marker"`
	MaxItems int    `xml:"MaxItems"`
}

// handleListTrustStores paginates via body-bound Marker/MaxItems.
func (h *Handler) handleListTrustStores(c *echo.Context) error {
	items := h.Backend.ListTrustStores()

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req listTrustStoresRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid ListTrustStoresRequest XML"))
		}
	}

	page, _, isTruncated := paginateByMarkerValue(
		items,
		func(ts *TrustStore) string { return ts.ID },
		req.Marker,
		req.MaxItems,
	)

	nextMarker := ""
	if isTruncated && len(page) > 0 {
		nextMarker = page[len(page)-1].ID
	}

	// ARN is tagged "Arn" (not "ARN") to match the real deserializer's exact-case
	// literal (awsRestxml_deserializeDocumentTrustStoreSummary) -- a case-only
	// mismatch that decoded correctly today only because the XML decoder folds
	// case, per gopherstack-21my. Fields mirror types.TrustStoreSummary
	// (cloudfront@v1.67.4 types/types.go:6681-6719).
	type tsSummary struct {
		XMLName                xml.Name `xml:"TrustStoreSummary"`
		ID                     string   `xml:"Id"`
		ARN                    string   `xml:"Arn"`
		Name                   string   `xml:"Name"`
		Status                 string   `xml:"Status"`
		Reason                 string   `xml:"Reason,omitempty"`
		ETag                   string   `xml:"ETag"`
		LastModifiedTime       string   `xml:"LastModifiedTime"`
		NumberOfCaCertificates int32    `xml:"NumberOfCaCertificates"`
	}
	// The real deserializer (awsRestxml_deserializeDocumentTrustStoreList) expects each
	// TrustStoreSummary directly as a child of TrustStoreList, with no <Items> wrapper.
	type tsList struct {
		XMLName  xml.Name    `xml:"TrustStoreList"`
		Items    []tsSummary `xml:"TrustStoreSummary"`
		Quantity int         `xml:"Quantity"`
	}
	// ListTrustStoresOutput has no httpPayload member (it carries both TrustStoreList and
	// NextMarker), so the real deserializer
	// (awsRestxml_deserializeOpDocumentListTrustStoresOutput) reads TrustStoreList as a CHILD
	// of the response root, not as the root itself. NextMarker is a sibling of TrustStoreList,
	// not a field on it.
	type tsListResult struct {
		XMLName        xml.Name `xml:"ListTrustStoresResult"`
		XMLNS          string   `xml:"xmlns,attr"`
		NextMarker     string   `xml:"NextMarker,omitempty"`
		TrustStoreList tsList   `xml:"TrustStoreList"`
	}
	summaries := make([]tsSummary, 0, len(page))
	for _, ts := range page {
		summaries = append(summaries, tsSummary{
			ID: ts.ID, ARN: ts.ARN, Name: ts.Name, Status: ts.Status, Reason: ts.Reason,
			ETag: ts.ETag, LastModifiedTime: ts.LastModifiedTime,
			NumberOfCaCertificates: ts.NumberOfCaCertificates,
		})
	}
	result := tsListResult{
		XMLNS: cfNS, NextMarker: nextMarker, TrustStoreList: tsList{Quantity: len(summaries), Items: summaries},
	}
	out, xmlErr := xml.Marshal(result)
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

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}
	var req updateTrustStoreRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CaCertificatesBundleSource XML"),
			)
		}
	}
	var useClientCertificateOCSPEndpoint *bool
	if raw := c.Request().Header.Get("Useclientcertificateocspendpoint"); raw != "" {
		parsed, parseErr := strconv.ParseBool(raw)
		if parseErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("InvalidArgument", "invalid Useclientcertificateocspendpoint header"),
			)
		}
		useClientCertificateOCSPEndpoint = &parsed
	}

	ts, updateErr := h.Backend.UpdateTrustStore(id, req.bundle(), useClientCertificateOCSPEndpoint)
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
