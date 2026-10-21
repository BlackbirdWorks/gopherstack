package azureappconfig

import (
	"net/http"
	"strings"
	"time"
)

type snapshotBody struct {
	Tags        map[string]string `json:"tags"`
	Composition string            `json:"composition_type"`
	Status      string            `json:"status"`
	Filters     []struct {
		Key   string `json:"key"`
		Label string `json:"label"`
	} `json:"filters"`
	Retention int64 `json:"retention_period"`
}

func (h *Handler) routeSnapshots(req *request) error {
	if len(req.segs) == 1 {
		if !isRead(req.r.Method) {
			return methodNotAllowed()
		}

		return h.listSnapshots(req)
	}

	name := strings.Join(req.segs[1:], "/")

	switch req.r.Method {
	case http.MethodGet, http.MethodHead:
		return h.getSnapshot(req, name)
	case http.MethodPut:
		return h.createSnapshot(req, name)
	case http.MethodPatch:
		return h.updateSnapshot(req, name)
	}

	return methodNotAllowed()
}

func (h *Handler) respondSnapshot(req *request, status int, s Snapshot, sel fieldSet) error {
	hdr := req.c.Response().Header()
	hdr.Set("ETag", quoted(s.ETag))
	hdr.Set("Link", "</kv?snapshot="+s.Name+`>; rel="items"`)

	return h.respond(req, status, mediaSnapshot, snapshotWire(s, sel))
}

func (h *Handler) getSnapshot(req *request, name string) error {
	sel, err := parseSelect(req.q, snapshotFields())
	if err != nil {
		return err
	}

	s, err := h.Backend.GetSnapshot(req.store, name)
	if err != nil {
		return err
	}

	return h.respondSnapshot(req, http.StatusOK, s, sel)
}

func (h *Handler) createSnapshot(req *request, name string) error {
	var in snapshotBody
	if err := decodeBody(req.body, &in); err != nil {
		return err
	}

	spec := SnapshotSpec{Tags: in.Tags, Composition: in.Composition, Retention: in.Retention}
	for _, f := range in.Filters {
		spec.Filters = append(spec.Filters, Filter{Key: f.Key, Label: f.Label})
	}

	s, err := h.Backend.CreateSnapshot(req.store, name, spec)
	if err != nil {
		return err
	}

	op := baseURL(req.r) + "/operations?snapshot=" + name + "&api-version=" + req.q.Get("api-version")
	req.c.Response().Header().Set("Operation-Location", op)

	return h.respondSnapshot(req, http.StatusCreated, s, nil)
}

func (h *Handler) updateSnapshot(req *request, name string) error {
	var in snapshotBody
	if err := decodeBody(req.body, &in); err != nil {
		return err
	}

	s, err := h.Backend.SetSnapshotStatus(req.store, name, in.Status, req.precondition())
	if err != nil {
		return err
	}

	return h.respondSnapshot(req, http.StatusOK, s, nil)
}

func (h *Handler) listSnapshots(req *request) error {
	sel, err := parseSelect(req.q, snapshotFields())
	if err != nil {
		return err
	}

	var statuses []string

	for _, v := range req.q[fieldStatus] {
		statuses = append(statuses, strings.Split(v, ",")...)
	}

	snaps, err := h.Backend.ListSnapshots(req.store, req.q.Get(fieldName), statuses)
	if err != nil {
		return err
	}

	p := pageOf(req, snaps)
	items := make([]map[string]any, 0, len(p.Data))
	etags := make([]string, 0, len(p.Data))

	for _, s := range p.Data {
		items = append(items, snapshotWire(s, sel))
		etags = append(etags, s.ETag)
	}

	return h.respondList(req, mediaSnapshotSet, items, etags, p.Next, time.Time{})
}

func (h *Handler) routeOperations(req *request) error {
	if !isRead(req.r.Method) || len(req.segs) != 1 {
		return methodNotAllowed()
	}

	name := req.q.Get("snapshot")

	if _, err := h.Backend.GetSnapshot(req.store, name); err != nil {
		return err
	}

	return h.respond(req, http.StatusOK, mediaJSON, map[string]any{"id": name, "status": "Succeeded"})
}

func baseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	return scheme + "://" + r.Host
}
