package cloudfront

import (
	"encoding/json"
	"encoding/xml"
	"errors"
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
		XMLName     xml.Name        `xml:"KeyValueStoreList"`
		XMLNS       string          `xml:"xmlns,attr"`
		Items       []kvsSummaryXML `xml:"Items>KeyValueStore"`
		MaxItems    int             `xml:"MaxItems"`
		Quantity    int             `xml:"Quantity"`
		IsTruncated bool            `xml:"IsTruncated"`
	}

	summaries := make([]kvsSummaryXML, 0, len(items))
	for _, kvs := range items {
		summaries = append(summaries, kvsSummaryXML{
			ID:               kvs.ID,
			ARN:              kvs.ARN,
			Name:             kvs.Name,
			Comment:          kvs.Comment,
			Status:           kvs.Status,
			LastModifiedTime: kvs.LastModifiedTime,
		})
	}

	list := kvsListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}

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

type kvsKeyValueJSON struct {
	Value string `json:"value"`
}

type kvsKeyItemJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type kvsListKeysResponseJSON struct {
	ETag     string            `json:"eTag"`
	Items    []*kvsKeyItemJSON `json:"items"`
	Quantity int               `json:"quantity"`
}

type kvsUpdateKeysRequestJSON struct {
	Puts    []*kvsKeyItemJSON `json:"puts"`
	Deletes []string          `json:"deletes"`
}

type kvsUpdateKeysResponseJSON struct {
	ETag      string `json:"eTag"`
	ItemCount int    `json:"itemCount"`
}

func (h *Handler) handleGetKVSKey(c *echo.Context, kvsID, key string) error {
	val, etag, err := h.Backend.GetKVSValue(kvsID, key)
	if err != nil {
		return kvsHandleErr(c, err)
	}

	c.Response().Header().Set("ETag", etag)
	c.Response().Header().Set("Content-Type", "application/json")

	return jsonResp(c, http.StatusOK, kvsKeyValueJSON{Value: val})
}

func (h *Handler) handlePutKVSKey(c *echo.Context, kvsID, key string) error {
	body, err := readBody(c)
	if err != nil {
		return jsonErrResp(c, http.StatusBadRequest, "MalformedBody", "failed to read request body")
	}

	var req kvsKeyValueJSON
	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return jsonErrResp(c, http.StatusBadRequest, "MalformedBody", "invalid JSON body")
		}
	}

	ifMatch := c.Request().Header.Get("If-Match")
	newETag, putErr := h.Backend.PutKVSValue(kvsID, key, req.Value, ifMatch)
	if putErr != nil {
		return kvsHandleErr(c, putErr)
	}

	c.Response().Header().Set("ETag", newETag)
	c.Response().Header().Set("Content-Type", "application/json")

	return jsonResp(c, http.StatusOK, kvsKeyValueJSON{Value: req.Value})
}

func (h *Handler) handleDeleteKVSKey(c *echo.Context, kvsID, key string) error {
	ifMatch := c.Request().Header.Get("If-Match")
	newETag, delErr := h.Backend.DeleteKVSValue(kvsID, key, ifMatch)
	if delErr != nil {
		return kvsHandleErr(c, delErr)
	}

	c.Response().Header().Set("ETag", newETag)

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListKVSKeys(c *echo.Context, kvsID string) error {
	items, etag, err := h.Backend.ListKVSValues(kvsID)
	if err != nil {
		return kvsHandleErr(c, err)
	}

	out := make([]*kvsKeyItemJSON, 0, len(items))
	for _, item := range items {
		out = append(out, &kvsKeyItemJSON{Key: item.Key, Value: item.Value})
	}

	c.Response().Header().Set("ETag", etag)
	c.Response().Header().Set("Content-Type", "application/json")

	return jsonResp(c, http.StatusOK, kvsListKeysResponseJSON{
		Items:    out,
		Quantity: len(out),
		ETag:     etag,
	})
}

func (h *Handler) handleUpdateKVSKeys(c *echo.Context, kvsID string) error {
	body, err := readBody(c)
	if err != nil {
		return jsonErrResp(c, http.StatusBadRequest, "MalformedBody", "failed to read request body")
	}

	var req kvsUpdateKeysRequestJSON
	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return jsonErrResp(c, http.StatusBadRequest, "MalformedBody", "invalid JSON body")
		}
	}

	puts := make([]*KVSItem, 0, len(req.Puts))
	for _, p := range req.Puts {
		puts = append(puts, &KVSItem{Key: p.Key, Value: p.Value})
	}

	ifMatch := c.Request().Header.Get("If-Match")
	newETag, updateErr := h.Backend.UpdateKVSValues(kvsID, ifMatch, puts, req.Deletes)
	if updateErr != nil {
		return kvsHandleErr(c, updateErr)
	}

	items, _, _ := h.Backend.ListKVSValues(kvsID)

	c.Response().Header().Set("ETag", newETag)
	c.Response().Header().Set("Content-Type", "application/json")

	return jsonResp(c, http.StatusOK, kvsUpdateKeysResponseJSON{
		ETag:      newETag,
		ItemCount: len(items),
	})
}

// jsonResp encodes v as JSON and writes it with the given status code.
func jsonResp(c *echo.Context, code int, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.Blob(code, "application/json", b)
}

type kvsErrJSON struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// jsonErrResp writes a JSON error response.
func jsonErrResp(c *echo.Context, code int, errType, msg string) error {
	return jsonResp(c, code, kvsErrJSON{Type: errType, Message: msg})
}

// handleError for KVS ops converts ErrPreconditionFailed to 412.
// This overrides the package-level handleError for KVS-specific error cases.
func kvsHandleErr(c *echo.Context, err error) error {
	if errors.Is(err, ErrPreconditionFailed) {
		return jsonErrResp(c, http.StatusPreconditionFailed, "PreconditionFailed", err.Error())
	}
	if errors.Is(err, ErrKeyValueStoreNotFound) {
		return jsonErrResp(c, http.StatusNotFound, "EntityNotFound", err.Error())
	}
	if errors.Is(err, ErrNotFound) {
		return jsonErrResp(c, http.StatusNotFound, "NotFound", err.Error())
	}

	return jsonErrResp(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}
