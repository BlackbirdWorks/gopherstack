package cloudfront

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	echo "github.com/labstack/echo/v5"
)

// --- KVS Data Plane handlers ---

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
