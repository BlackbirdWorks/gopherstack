package azuregraph

import (
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	segDeletedItems = "deleteditems"
	segRestore      = "restore"
	graphTypePrefix = "microsoft.graph."
)

func deletedMap(d DeletedObject) map[string]any {
	m := typed(d.Kind, d.Entity)
	m["deletedDateTime"] = d.DeletedDateTime.UTC().Format("2006-01-02T15:04:05Z")

	return m
}

// handleDirectory serves /directory/deletedItems: list by type cast, get, restore and permanentDelete.
func (h *Handler) handleDirectory(c *echo.Context, segs []string) error {
	if len(segs) >= pathWithID {
		if name, key := splitKey(segs[1]); name == collAdminUnits {
			return h.handleCollection(c, h.colls[collAdminUnits], key, segs[1:], nil)
		}
	}

	if len(segs) < pathWithID || strings.ToLower(segs[1]) != segDeletedItems {
		return h.writeErr(c, segmentErr(segs[len(segs)-1]))
	}

	r := c.Request()

	switch {
	case len(segs) == pathWithID && r.Method == http.MethodGet:
		return h.listDeleted(c, "", "DirectoryObject", nil)
	case len(segs) == pathWithID+1:
		return h.deletedItem(c, segs[2])
	case len(segs) == pathWithID+2 && strings.EqualFold(segs[3], segRestore) && r.Method == http.MethodPost:
		o, err := h.Backend.RestoreDeleted(segs[2])
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusOK, "directoryObjects", typed(o.Kind, o.Entity))
	}

	return h.methodNotAllowed(c)
}

// deletedItem serves GET and DELETE on /directory/deletedItems/{id or type cast}.
func (h *Handler) deletedItem(c *echo.Context, seg string) error {
	switch c.Request().Method {
	case http.MethodGet:
		if kind, ok := strings.CutPrefix(seg, graphTypePrefix); ok {
			for _, coll := range h.colls {
				if strings.EqualFold(coll.kind, kind) {
					return h.listDeleted(c, coll.kind, coll.resource, coll.fields)
				}
			}

			return h.writeErr(c, segmentErr(seg))
		}

		d, err := h.Backend.GetDeleted(seg)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusOK, "directoryObjects", deletedMap(d))
	case http.MethodDelete:
		return h.noContent(c, h.Backend.PermanentDelete(seg))
	}

	return h.methodNotAllowed(c)
}

func (h *Handler) listDeleted(c *echo.Context, kind, resource string, fields map[string]bool) error {
	deleted := h.Backend.ListDeleted(kind)
	items := make([]map[string]any, 0, len(deleted))

	for _, d := range deleted {
		items = append(items, deletedMap(d))
	}

	if fields != nil {
		fields = maps.Clone(fields)
		fields["deletedDateTime"] = true
	}

	return h.list(c, "directoryObjects", resource, fields, items)
}
