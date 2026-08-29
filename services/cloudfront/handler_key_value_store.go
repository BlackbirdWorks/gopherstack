package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func kvsResponseXML(kvs *KeyValueStore) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<KeyValueStore xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<Status>%s</Status>`+
		`<LastModifiedTime>%s</LastModifiedTime>`+
		`</KeyValueStore>`,
		cfNS, kvs.ID, kvs.ARN, xmlEscape(kvs.Name), xmlEscape(kvs.Comment),
		kvs.Status, kvs.LastModifiedTime)
}

// keyValueStoreRequestFields is shared by Create and Update, whose real
// request shapes carry identical fields but different root element names
// (CreateKeyValueStoreRequest vs UpdateKeyValueStoreRequest; cloudfront@v1.67.4
// serializers.go). A prior single struct fixed the root to
// "KeyValueStoreRequest", which matched neither real root name, so
// xml.Unmarshal silently failed on every field (Name, Comment, Tags all
// dropped) for any real client -- masked because the existing tests
// hand-crafted request bodies using that same wrong name.
type keyValueStoreRequestFields struct {
	Name    string  `xml:"Name"`
	Comment string  `xml:"Comment"`
	Tags    tagsXML `xml:"Tags"`
}

type createKeyValueStoreRequestXML struct {
	XMLName xml.Name `xml:"CreateKeyValueStoreRequest"`
	keyValueStoreRequestFields
}

type updateKeyValueStoreRequestXML struct {
	XMLName xml.Name `xml:"UpdateKeyValueStoreRequest"`
	keyValueStoreRequestFields
}

func (h *Handler) handleCreateKeyValueStore(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req createKeyValueStoreRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CreateKeyValueStoreRequest XML"),
			)
		}
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	kvs, createErr := h.Backend.CreateKeyValueStore(req.Name, req.Comment, tagsXMLToMap(req.Tags))
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", kvs.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"key-value-store/"+kvs.ID)

	return xmlResp(c, http.StatusCreated, kvsResponseXML(kvs))
}

func (h *Handler) handleGetKeyValueStore(c *echo.Context, id string) error {
	kvs, err := h.Backend.GetKeyValueStore(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", kvs.ETag)

	return xmlResp(c, http.StatusOK, kvsResponseXML(kvs))
}

func (h *Handler) handleListKeyValueStores(c *echo.Context) error {
	items := h.Backend.ListKeyValueStores()

	// Status is a real query-bound filter (cloudfront@v1.67.4 serializers.go:
	// awsRestxml_serializeOpHttpBindingsListKeyValueStoresInput), not just display metadata.
	if status := c.QueryParam("Status"); status != "" {
		items = filterSlice(items, func(kvs *KeyValueStore) bool { return kvs.Status == status })
	}

	page, pageSize, isTruncated, nextMarker := paginateByMarkerID(
		c,
		items,
		func(kvs *KeyValueStore) string { return kvs.Name },
	)

	type kvsSummaryXML struct {
		XMLName          xml.Name `xml:"KeyValueStore"`
		ID               string   `xml:"Id"`
		ARN              string   `xml:"ARN"`
		Name             string   `xml:"Name"`
		Comment          string   `xml:"Comment"`
		Status           string   `xml:"Status"`
		LastModifiedTime string   `xml:"LastModifiedTime"`
	}

	type kvsListXML struct {
		XMLName    xml.Name        `xml:"KeyValueStoreList"`
		XMLNS      string          `xml:"xmlns,attr"`
		NextMarker string          `xml:"NextMarker,omitempty"`
		Items      []kvsSummaryXML `xml:"Items>KeyValueStore"`
		MaxItems   int             `xml:"MaxItems"`
		Quantity   int             `xml:"Quantity"`
	}

	summaries := make([]kvsSummaryXML, 0, len(page))
	for _, kvs := range page {
		summaries = append(summaries, kvsSummaryXML{
			ID:               kvs.ID,
			ARN:              kvs.ARN,
			Name:             kvs.Name,
			Comment:          kvs.Comment,
			Status:           kvs.Status,
			LastModifiedTime: kvs.LastModifiedTime,
		})
	}

	list := kvsListXML{XMLNS: cfNS, MaxItems: pageSize, Quantity: len(summaries), Items: summaries}
	if isTruncated {
		list.NextMarker = nextMarker
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleDeleteKeyValueStore(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetKeyValueStore(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current KeyValueStore ETag"))
	}

	if err := h.Backend.DeleteKeyValueStore(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- VPC Origin handlers ---

func (h *Handler) handleUpdateKeyValueStore(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req updateKeyValueStoreRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid UpdateKeyValueStoreRequest XML"),
			)
		}
	}

	current, getErr := h.Backend.GetKeyValueStore(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current KeyValueStore ETag"))
	}

	kvs, updateErr := h.Backend.UpdateKeyValueStore(id, req.Comment)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", kvs.ETag)

	return xmlResp(c, http.StatusOK, kvsResponseXML(kvs))
}
