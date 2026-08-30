package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func publicKeyResponseXML(pk *PublicKey) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<PublicKey xmlns="%s">`+
		`<Id>%s</Id>`+
		`<PublicKeyConfig>`+
		`<CallerReference>%s</CallerReference>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<EncodedKey>%s</EncodedKey>`+
		`</PublicKeyConfig>`+
		`</PublicKey>`,
		cfNS, pk.ID, pk.CallerReference, pk.Name, pk.Comment, pk.EncodedKey)
}

type publicKeyConfigXML struct {
	XMLName         xml.Name `xml:"PublicKeyConfig"`
	CallerReference string   `xml:"CallerReference"`
	Name            string   `xml:"Name"`
	Comment         string   `xml:"Comment"`
	EncodedKey      string   `xml:"EncodedKey"`
}

func (h *Handler) handleCreatePublicKey(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req publicKeyConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid PublicKeyConfig XML"))
		}
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	pk, createErr := h.Backend.CreatePublicKey(req.CallerReference, req.Name, req.Comment, req.EncodedKey)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", pk.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"public-key/"+pk.ID)

	return xmlResp(c, http.StatusCreated, publicKeyResponseXML(pk))
}

func (h *Handler) handleGetPublicKey(c *echo.Context, id string) error {
	pk, err := h.Backend.GetPublicKey(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", pk.ETag)

	return xmlResp(c, http.StatusOK, publicKeyResponseXML(pk))
}

// handleListPublicKeys paginates via Marker/MaxItems (both query-bound, cloudfront@v1.67.4
// serializers.go). Real PublicKeyList has no IsTruncated field -- NextMarker's presence
// alone signals truncation (types/types.go:5126-5146).
//
//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListPublicKeys(c *echo.Context) error {
	items := h.Backend.ListPublicKeys()

	page, pageSize, _, nextMarker := paginateByMarkerID(c, items, func(pk *PublicKey) string { return pk.ID })

	type pkSummaryXML struct {
		XMLName xml.Name `xml:"PublicKeySummary"`
		ID      string   `xml:"Id"`
		Name    string   `xml:"Name"`
		Comment string   `xml:"Comment"`
	}

	type pkListXML struct {
		XMLName    xml.Name       `xml:"PublicKeyList"`
		XMLNS      string         `xml:"xmlns,attr"`
		NextMarker string         `xml:"NextMarker,omitempty"`
		Items      []pkSummaryXML `xml:"Items>PublicKeySummary"`
		MaxItems   int            `xml:"MaxItems"`
		Quantity   int            `xml:"Quantity"`
	}

	summaries := make([]pkSummaryXML, 0, len(page))
	for _, pk := range page {
		summaries = append(summaries, pkSummaryXML{ID: pk.ID, Name: pk.Name, Comment: pk.Comment})
	}

	list := pkListXML{
		XMLNS:      cfNS,
		NextMarker: nextMarker,
		MaxItems:   pageSize,
		Quantity:   len(summaries),
		Items:      summaries,
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdatePublicKey(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req publicKeyConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid PublicKeyConfig XML"))
		}
	}

	pk, updateErr := h.Backend.UpdatePublicKey(id, req.Comment)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", pk.ETag)

	return xmlResp(c, http.StatusOK, publicKeyResponseXML(pk))
}

func (h *Handler) handleDeletePublicKey(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetPublicKey(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current PublicKey ETag"))
	}

	if err := h.Backend.DeletePublicKey(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Key Group handlers ---

func keyGroupResponseXML(kg *KeyGroup) string {
	// Items entries are <PublicKey>, not <Key> (awsRestxml_deserializeDocumentPublicKeyIdList,
	// cloudfront@v1.67.4 deserializers.go): a real client decodes KeyGroupConfig.Items as
	// always-empty against the wrong tag, matching keyGroupConfigResponseXML below.
	var sb strings.Builder
	for _, item := range kg.Items {
		sb.WriteString("<PublicKey>")
		sb.WriteString(xmlEscape(item))
		sb.WriteString("</PublicKey>")
	}
	itemsXML := sb.String()

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<KeyGroup xmlns="%s">`+
		`<Id>%s</Id>`+
		`<KeyGroupConfig>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<Items>%s</Items>`+
		`</KeyGroupConfig>`+
		`</KeyGroup>`,
		cfNS, kg.ID, kg.Name, kg.Comment, itemsXML)
}

type keyGroupConfigXML struct {
	XMLName xml.Name `xml:"KeyGroupConfig"`
	Name    string   `xml:"Name"`
	Comment string   `xml:"Comment"`
	Items   []string `xml:"Items>PublicKey"`
}

func (h *Handler) handleCreateKeyGroup(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req keyGroupConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid KeyGroupConfig XML"))
		}
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	kg, createErr := h.Backend.CreateKeyGroup(req.Name, req.Comment, req.Items)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", kg.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"key-group/"+kg.ID)

	return xmlResp(c, http.StatusCreated, keyGroupResponseXML(kg))
}

func (h *Handler) handleGetKeyGroup(c *echo.Context, id string) error {
	kg, err := h.Backend.GetKeyGroup(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", kg.ETag)

	return xmlResp(c, http.StatusOK, keyGroupResponseXML(kg))
}

// kgConfigXML is types.KeyGroupConfig on the wire (awsRestxml_deserializeDocumentKeyGroupConfig).
type kgConfigXML struct {
	Name    string   `xml:"Name"`
	Comment string   `xml:"Comment,omitempty"`
	Items   []string `xml:"Items>PublicKey"`
}

// kgXML is types.KeyGroup on the wire (awsRestxml_deserializeDocumentKeyGroup).
type kgXML struct {
	ID     string      `xml:"Id"`
	Config kgConfigXML `xml:"KeyGroupConfig"`
}

// kgSummaryXML is types.KeyGroupSummary: a KeyGroupSummary element wraps a single nested
// <KeyGroup> child (awsRestxml_deserializeDocumentKeyGroupSummary case "KeyGroup"), not the
// KeyGroup's fields flattened directly onto KeyGroupSummary -- a real client decodes
// KeyGroupSummary.KeyGroup as nil for every item against the flattened shape, giving the right
// item count with entirely blank content.
type kgSummaryXML struct {
	XMLName  xml.Name `xml:"KeyGroupSummary"`
	KeyGroup kgXML    `xml:"KeyGroup"`
}

// handleListKeyGroups paginates via Marker/MaxItems (both query-bound, cloudfront@v1.67.4
// serializers.go). Real KeyGroupList has no IsTruncated field -- NextMarker's presence
// alone signals truncation (types/types.go:3823-3843).
func (h *Handler) handleListKeyGroups(c *echo.Context) error {
	items := h.Backend.ListKeyGroups()

	page, pageSize, _, nextMarker := paginateByMarkerID(c, items, func(kg *KeyGroup) string { return kg.ID })

	type kgListXML struct {
		XMLName    xml.Name       `xml:"KeyGroupList"`
		XMLNS      string         `xml:"xmlns,attr"`
		NextMarker string         `xml:"NextMarker,omitempty"`
		Items      []kgSummaryXML `xml:"Items>KeyGroupSummary"`
		MaxItems   int            `xml:"MaxItems"`
		Quantity   int            `xml:"Quantity"`
	}

	summaries := make([]kgSummaryXML, 0, len(page))
	for _, kg := range page {
		summaries = append(summaries, kgSummaryXML{
			KeyGroup: kgXML{
				ID:     kg.ID,
				Config: kgConfigXML{Name: kg.Name, Comment: kg.Comment, Items: kg.Items},
			},
		})
	}

	list := kgListXML{
		XMLNS:      cfNS,
		NextMarker: nextMarker,
		MaxItems:   pageSize,
		Quantity:   len(summaries),
		Items:      summaries,
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateKeyGroup(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req keyGroupConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid KeyGroupConfig XML"))
		}
	}

	current, getErr := h.Backend.GetKeyGroup(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if req.Name == "" {
		req.Name = current.Name
	}

	kg, updateErr := h.Backend.UpdateKeyGroup(id, req.Name, req.Comment, req.Items)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", kg.ETag)

	return xmlResp(c, http.StatusOK, keyGroupResponseXML(kg))
}

func (h *Handler) handleDeleteKeyGroup(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetKeyGroup(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current KeyGroup ETag"))
	}

	if err := h.Backend.DeleteKeyGroup(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Realtime Log Config handlers ---

// publicKeyConfigResponseXML renders the config-only PublicKeyConfig root.
func publicKeyConfigResponseXML(pk *PublicKey) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<PublicKeyConfig xmlns="%s">`+
		`<CallerReference>%s</CallerReference>`+
		`<Name>%s</Name>`+
		`<EncodedKey>%s</EncodedKey>`+
		`<Comment>%s</Comment>`+
		`</PublicKeyConfig>`,
		cfNS, xmlEscape(pk.CallerReference), xmlEscape(pk.Name),
		xmlEscape(pk.EncodedKey), xmlEscape(pk.Comment))
}

func (h *Handler) handleGetPublicKeyConfig(c *echo.Context, id string) error {
	pk, err := h.Backend.GetPublicKey(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", pk.ETag)

	return xmlResp(c, http.StatusOK, publicKeyConfigResponseXML(pk))
}

// keyGroupConfigResponseXML renders the config-only KeyGroupConfig root.
func keyGroupConfigResponseXML(kg *KeyGroup) string {
	var items strings.Builder
	for _, id := range kg.Items {
		items.WriteString(`<PublicKey>`)
		items.WriteString(xmlEscape(id))
		items.WriteString(`</PublicKey>`)
	}

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<KeyGroupConfig xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<Items>%s</Items>`+
		`</KeyGroupConfig>`,
		cfNS, xmlEscape(kg.Name), xmlEscape(kg.Comment), items.String())
}

func (h *Handler) handleGetKeyGroupConfig(c *echo.Context, id string) error {
	kg, err := h.Backend.GetKeyGroup(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", kg.ETag)

	return xmlResp(c, http.StatusOK, keyGroupConfigResponseXML(kg))
}
