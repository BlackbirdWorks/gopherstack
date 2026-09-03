package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

type trustStoreCertificateBundleXML struct {
	S3Bucket                string `xml:"S3Bucket"`
	S3Key                   string `xml:"S3Key"`
	InlineCertificateBundle string `xml:"InlineCertificateBundle"`
}

// updateTrustStoreRequestXML matches UpdateTrustStoreInput (cloudfront@v1.67.4
// serializers.go: awsRestxml_serializeOpUpdateTrustStore). Its root element is
// CaCertificatesBundleSource, with CaCertificatesBundleS3Location as its only
// child (types.go: CaCertificatesBundleSourceMemberCaCertificatesBundleS3Location)
// -- UpdateTrustStoreInput has no Name or Comment member at all, so real AWS
// cannot change either through this operation; Id/IfMatch travel as URI/header.
// The previous root here was TrustStoreConfig, which never matched a real
// client's root element, so xml.Unmarshal errored on the whole body (err
// discarded) and every real UpdateTrustStore call silently no-opped.
type updateTrustStoreRequestXML struct {
	XMLName                        xml.Name `xml:"CaCertificatesBundleSource"`
	CaCertificatesBundleS3Location struct {
		Bucket string `xml:"Bucket"`
		Key    string `xml:"Key"`
	} `xml:"CaCertificatesBundleS3Location"`
	// CertificateAuthorityCertificatesBundle is not part of the real
	// UpdateTrustStore request shape, but accepted here too for backward
	// compatibility with callers that send the old shape.
	CertificateAuthorityCertificatesBundle trustStoreCertificateBundleXML `xml:"CertificateAuthorityCertificatesBundle"`
}

// bundle resolves the CA certificate bundle from whichever shape was populated,
// preferring the real SDK's CaCertificatesBundleSource>CaCertificatesBundleS3Location
// shape.
func (req updateTrustStoreRequestXML) bundle() TrustStoreCertificateBundle {
	if req.CaCertificatesBundleS3Location.Bucket != "" || req.CaCertificatesBundleS3Location.Key != "" {
		return TrustStoreCertificateBundle{
			S3Bucket: req.CaCertificatesBundleS3Location.Bucket,
			S3Key:    req.CaCertificatesBundleS3Location.Key,
		}
	}

	return trustStoreBundleFromXML(req.CertificateAuthorityCertificatesBundle)
}

// createTrustStoreRequestXML models the real CreateTrustStore wire request. The real SDK
// (aws-sdk-go-v2/service/cloudfront) sends a root element <CreateTrustStoreRequest> containing
// <CaCertificatesBundleSource><CaCertificatesBundleS3Location><Bucket>/<Key>/... and <Name> as
// direct children (see serializers.go: awsRestxml_serializeOpDocumentCreateTrustStoreInput /
// awsRestxml_serializeDocumentCaCertificatesBundleSource). The XMLName field is intentionally
// omitted so Unmarshal does not reject the request based on the root element's name.
type createTrustStoreRequestXML struct {
	Name                       string `xml:"Name"`
	CaCertificatesBundleSource struct {
		S3Location struct {
			Bucket string `xml:"Bucket"`
			Key    string `xml:"Key"`
		} `xml:"CaCertificatesBundleS3Location"`
	} `xml:"CaCertificatesBundleSource"`
	// CertificateAuthorityCertificatesBundle is not part of the real CreateTrustStore request
	// shape, but is accepted here too for backward compatibility with callers that send it.
	CertificateAuthorityCertificatesBundle trustStoreCertificateBundleXML `xml:"CertificateAuthorityCertificatesBundle"`
	Comment                                string                         `xml:"Comment"`
	// Tags is *types.Tags on the wire: Items wraps the Tag list, not a bare
	// Tags>Tag path (cloudfront@v1.67.4 serializers.go awsRestxml_serializeDocumentTags).
	Tags []tagXML `xml:"Tags>Items>Tag"`
}

// bundle resolves the CA certificate bundle from whichever shape was populated, preferring the
// real SDK's CaCertificatesBundleSource>CaCertificatesBundleS3Location shape.
func (req createTrustStoreRequestXML) bundle() TrustStoreCertificateBundle {
	if req.CaCertificatesBundleSource.S3Location.Bucket != "" || req.CaCertificatesBundleSource.S3Location.Key != "" {
		return TrustStoreCertificateBundle{
			S3Bucket: req.CaCertificatesBundleSource.S3Location.Bucket,
			S3Key:    req.CaCertificatesBundleSource.S3Location.Key,
		}
	}

	return trustStoreBundleFromXML(req.CertificateAuthorityCertificatesBundle)
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

	ts, createErr := h.Backend.CreateTrustStore(req.Name, req.Comment, req.bundle(), tags)
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
	// case, per gopherstack-21my.
	type tsSummary struct {
		XMLName          xml.Name `xml:"TrustStoreSummary"`
		ID               string   `xml:"Id"`
		ARN              string   `xml:"Arn"`
		Name             string   `xml:"Name"`
		Status           string   `xml:"Status"`
		ETag             string   `xml:"ETag"`
		LastModifiedTime string   `xml:"LastModifiedTime"`
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
			ID: ts.ID, ARN: ts.ARN, Name: ts.Name,
			Status: ts.Status, ETag: ts.ETag, LastModifiedTime: ts.LastModifiedTime,
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
	ts, updateErr := h.Backend.UpdateTrustStore(id, "", "", req.bundle())
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
