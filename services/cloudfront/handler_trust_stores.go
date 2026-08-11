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

type trustStoreConfigXML struct {
	XMLName                                xml.Name                       `xml:"TrustStoreConfig"`
	Name                                   string                         `xml:"Name"`
	Comment                                string                         `xml:"Comment"`
	CertificateAuthorityCertificatesBundle trustStoreCertificateBundleXML `xml:"CertificateAuthorityCertificatesBundle"`
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

func (h *Handler) handleListTrustStores(c *echo.Context) error {
	items := h.Backend.ListTrustStores()

	type tsSummary struct {
		XMLName xml.Name `xml:"TrustStoreSummary"`
		ID      string   `xml:"Id"`
		ARN     string   `xml:"ARN"`
		Name    string   `xml:"Name"`
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
	// of the response root, not as the root itself.
	type tsListResult struct {
		XMLName        xml.Name `xml:"ListTrustStoresResult"`
		XMLNS          string   `xml:"xmlns,attr"`
		TrustStoreList tsList   `xml:"TrustStoreList"`
	}
	summaries := make([]tsSummary, 0, len(items))
	for _, ts := range items {
		summaries = append(summaries, tsSummary{ID: ts.ID, ARN: ts.ARN, Name: ts.Name})
	}
	result := tsListResult{XMLNS: cfNS, TrustStoreList: tsList{Quantity: len(summaries), Items: summaries}}
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
