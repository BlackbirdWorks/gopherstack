package azureappconfig

import (
	"errors"
	"net/http"
	"strings"
	"time"
)

type kvBody struct {
	Value       *string           `json:"value"`
	ContentType *string           `json:"content_type"`
	Tags        map[string]string `json:"tags"`
}

// keyAndLabel returns the key from the path tail (slashes allowed) and the label parameter.
func (req *request) keyAndLabel() (string, string, error) {
	key := strings.Join(req.segs[1:], "/")
	if key == "" {
		return "", "", ErrInvalidArgument
	}

	label := req.q.Get("label")
	if label == `\0` || label == "\x00" {
		label = ""
	}

	return key, label, nil
}

func (h *Handler) routeKV(req *request) error {
	if len(req.segs) == 1 {
		if !isRead(req.r.Method) {
			return methodNotAllowed()
		}

		return h.listKeyValues(req)
	}

	switch req.r.Method {
	case http.MethodGet, http.MethodHead:
		return h.getKeyValue(req)
	case http.MethodPut:
		return h.putKeyValue(req)
	case http.MethodDelete:
		return h.deleteKeyValue(req)
	}

	return methodNotAllowed()
}

func (h *Handler) respondKV(req *request, kv KeyValue, sel fieldSet) error {
	req.c.Response().Header().Set("ETag", quoted(kv.ETag))

	return h.respond(req, http.StatusOK, mediaKV, kvWire(kv, sel))
}

func (h *Handler) getKeyValue(req *request) error {
	key, label, err := req.keyAndLabel()
	if err != nil {
		return err
	}

	sel, err := parseSelect(req.q, kvFields())
	if err != nil {
		return err
	}

	at, err := req.pointInTime()
	if err != nil {
		return err
	}

	kv, err := h.Backend.GetKeyValue(req.store, key, label, at)
	if err != nil {
		return err
	}

	pre := req.precondition()

	switch {
	case pre.IfMatch != "" && pre.IfMatch != "*" && pre.IfMatch != kv.ETag:
		return ErrPrecondition
	case pre.IfNoneMatch != "" && (pre.IfNoneMatch == "*" || pre.IfNoneMatch == kv.ETag):
		req.c.Response().Header().Set("ETag", quoted(kv.ETag))

		return h.respondEmpty(req, http.StatusNotModified)
	}

	if !at.IsZero() {
		req.c.Response().Header().Set("Memento-Datetime", at.UTC().Format(http.TimeFormat))
	}

	return h.respondKV(req, kv, sel)
}

func (h *Handler) putKeyValue(req *request) error {
	key, label, err := req.keyAndLabel()
	if err != nil {
		return err
	}

	var in kvBody
	if err = decodeBody(req.body, &in); err != nil {
		return err
	}

	kv, err := h.Backend.SetKeyValue(req.store, key, label,
		KVInput(in), req.precondition())
	if err != nil {
		return err
	}

	return h.respondKV(req, kv, nil)
}

func (h *Handler) deleteKeyValue(req *request) error {
	key, label, err := req.keyAndLabel()
	if err != nil {
		return err
	}

	kv, err := h.Backend.DeleteKeyValue(req.store, key, label, req.precondition())
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return h.respondEmpty(req, http.StatusNoContent)
		}

		return err
	}

	return h.respondKV(req, kv, nil)
}

func (req *request) selector() (Selector, error) {
	return NewSelector(req.q.Get(fieldKey), req.q.Get("label"), req.q["tags"])
}

func (h *Handler) listKeyValues(req *request) error {
	sel, err := req.selector()
	if err != nil {
		return err
	}

	fields, err := parseSelect(req.q, kvFields())
	if err != nil {
		return err
	}

	at, err := req.pointInTime()
	if err != nil {
		return err
	}

	var kvs []KeyValue

	if snap := req.q.Get("snapshot"); snap != "" {
		kvs, err = h.Backend.SnapshotKeyValues(req.store, snap, sel)
		at = time.Time{}
	} else {
		kvs, err = h.Backend.ListKeyValues(req.store, sel, at)
	}

	if err != nil {
		return err
	}

	return h.respondKVs(req, kvs, fields, at)
}

func (h *Handler) respondKVs(req *request, kvs []KeyValue, fields fieldSet, at time.Time) error {
	p := pageOf(req, kvs)
	items := make([]map[string]any, 0, len(p.Data))
	etags := make([]string, 0, len(p.Data))

	for _, kv := range p.Data {
		items = append(items, kvWire(kv, fields))
		etags = append(etags, kv.ETag)
	}

	return h.respondList(req, mediaKVSet, items, etags, p.Next, at)
}

func (h *Handler) routeRevisions(req *request) error {
	if !isRead(req.r.Method) || len(req.segs) != 1 {
		return methodNotAllowed()
	}

	sel, err := req.selector()
	if err != nil {
		return err
	}

	fields, err := parseSelect(req.q, kvFields())
	if err != nil {
		return err
	}

	at, err := req.pointInTime()
	if err != nil {
		return err
	}

	kvs, err := h.Backend.ListRevisions(req.store, sel, at)
	if err != nil {
		return err
	}

	return h.respondKVs(req, kvs, fields, at)
}

func (h *Handler) routeLocks(req *request) error {
	key, label, err := req.keyAndLabel()
	if err != nil {
		return err
	}

	var locked bool

	switch req.r.Method {
	case http.MethodPut:
		locked = true
	case http.MethodDelete:
	default:
		return methodNotAllowed()
	}

	kv, err := h.Backend.SetLock(req.store, key, label, locked, req.precondition())
	if err != nil {
		return err
	}

	return h.respondKV(req, kv, nil)
}

func (h *Handler) routeKeys(req *request) error {
	return h.listNames(req, func(name string, at time.Time) ([]string, error) {
		return h.Backend.ListKeyNames(req.store, name, at)
	})
}

func (h *Handler) routeLabels(req *request) error {
	return h.listNames(req, func(name string, at time.Time) ([]string, error) {
		return h.Backend.ListLabelNames(req.store, name, at)
	})
}

func (h *Handler) listNames(req *request, fetch func(string, time.Time) ([]string, error)) error {
	if !isRead(req.r.Method) || len(req.segs) != 1 {
		return methodNotAllowed()
	}

	at, err := req.pointInTime()
	if err != nil {
		return err
	}

	names, err := fetch(req.q.Get(fieldName), at)
	if err != nil {
		return err
	}

	p := pageOf(req, names)
	items := make([]map[string]any, 0, len(p.Data))

	for _, n := range p.Data {
		items = append(items, map[string]any{fieldName: n})
	}

	media := mediaKeySet
	if strings.EqualFold(req.segs[0], "labels") {
		media = mediaLabelSet
	}

	return h.respondList(req, media, items, p.Data, p.Next, at)
}
