package azureeventhubs

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	atomNS    = "http://www.w3.org/2005/Atom"
	sbNS      = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"
	xmlHeader = `<?xml version="1.0" encoding="utf-8" standalone="yes"?>`
	maxAtom   = 64 * 1024
	day       = 24 * time.Hour
)

type hubDescription struct {
	RetentionDays int `xml:"MessageRetentionInDays"`
	PartitionCnt  int `xml:"PartitionCount"`
}

type atomEntry struct {
	XMLName xml.Name `xml:"entry"`
	Content struct {
		Hub hubDescription `xml:"EventHubDescription"`
	} `xml:"content"`
}

func xmlEsc(s string) string {
	var b bytes.Buffer

	_ = xml.EscapeText(&b, []byte(s))

	return b.String()
}

func (h *Handler) handleAtom(c *echo.Context, req request) error {
	m := c.Request().Method

	switch {
	case req.kind == segHubList && m == http.MethodGet:
		return h.listHubs(c)
	case req.kind == segGroupList && m == http.MethodGet:
		return h.listGroups(c, req)
	case req.kind == segHub:
		return h.hubOp(c, req)
	case req.kind == segGroup:
		return h.groupOp(c, req)
	}

	return writeError(c, http.StatusMethodNotAllowed, "The resource doesn't support the specified HTTP verb.")
}

func atomXML(c *echo.Context, status int, body string) error {
	return c.Blob(status, "application/atom+xml;type=entry;charset=utf-8", []byte(xmlHeader+body))
}

func hubEntry(info HubInfo) string {
	var ids strings.Builder
	for _, id := range info.PartitionIDs {
		ids.WriteString("<string>" + id + "</string>")
	}

	return fmt.Sprintf(`<entry xmlns="%s"><id>%s</id><title type="text">%s</title><content type="application/xml">`+
		`<EventHubDescription xmlns="%s"><MessageRetentionInDays>%d</MessageRetentionInDays><Status>Active</Status>`+
		`<CreatedAt>%s</CreatedAt><PartitionCount>%d</PartitionCount><PartitionIds>%s</PartitionIds>`+
		`</EventHubDescription></content></entry>`,
		atomNS, xmlEsc(info.Name), xmlEsc(info.Name), sbNS, int(info.Config.Retention/day),
		info.CreatedAt.Format(time.RFC3339Nano), len(info.PartitionIDs), ids.String())
}

func groupEntry(hub, name string, created time.Time) string {
	return fmt.Sprintf(`<entry xmlns="%s"><id>%s/consumergroups/%s</id><title type="text">%s</title>`+
		`<content type="application/xml"><ConsumerGroupDescription xmlns="%s"><CreatedAt>%s</CreatedAt>`+
		`<EventHubPath>%s</EventHubPath></ConsumerGroupDescription></content></entry>`,
		atomNS, xmlEsc(hub), xmlEsc(name), xmlEsc(name), sbNS, created.Format(time.RFC3339Nano), xmlEsc(hub))
}

func feed(entries []string) string {
	return `<feed xmlns="` + atomNS + `">` + strings.Join(entries, "") + `</feed>`
}

func (h *Handler) listHubs(c *echo.Context) error {
	var entries []string

	for _, ns := range h.namespaceNames() {
		infos, err := h.Backend.ListHubs(ns)
		if err != nil {
			continue
		}

		for _, info := range infos {
			entries = append(entries, hubEntry(info))
		}
	}

	return atomXML(c, http.StatusOK, feed(entries))
}

func (h *Handler) namespaceNames() []string {
	h.Backend.mu.RLock("namespaceNames")
	defer h.Backend.mu.RUnlock()

	out := make([]string, 0, len(h.Backend.namespaces))
	for n := range h.Backend.namespaces {
		out = append(out, n)
	}

	return out
}

func (h *Handler) listGroups(c *echo.Context, req request) error {
	ref, ok := h.Backend.ResolveHub(req.hub)
	if !ok {
		return notFoundResp(c, req.hub)
	}

	names, err := h.Backend.ListConsumerGroups(ref)
	if err != nil {
		return notFoundResp(c, req.hub)
	}

	entries := make([]string, 0, len(names))

	for _, n := range names {
		created, _ := h.Backend.ConsumerGroup(ref, n)
		entries = append(entries, groupEntry(req.hub, n, created))
	}

	return atomXML(c, http.StatusOK, feed(entries))
}

func notFoundResp(c *echo.Context, name string) error {
	return writeError(c, http.StatusNotFound, "The messaging entity '"+name+"' could not be found.")
}

func (h *Handler) hubOp(c *echo.Context, req request) error {
	switch c.Request().Method {
	case http.MethodPut:
		return h.putHub(c, req)
	case http.MethodGet:
		ref, ok := h.Backend.ResolveHub(req.hub)
		if !ok {
			return notFoundResp(c, req.hub)
		}

		info, err := h.Backend.Hub(ref)
		if err != nil {
			return notFoundResp(c, req.hub)
		}

		return atomXML(c, http.StatusOK, hubEntry(info))
	case http.MethodDelete:
		ref, ok := h.Backend.ResolveHub(req.hub)
		if !ok || h.Backend.DeleteHub(ref) != nil {
			return notFoundResp(c, req.hub)
		}

		return c.NoContent(http.StatusOK)
	}

	return writeError(c, http.StatusMethodNotAllowed, "The resource doesn't support the specified HTTP verb.")
}

func (h *Handler) putHub(c *echo.Context, req request) error {
	body, err := io.ReadAll(io.LimitReader(c.Request().Body, maxAtom))
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error())
	}

	var entry atomEntry
	if len(bytes.TrimSpace(body)) > 0 {
		if xmlErr := xml.Unmarshal(body, &entry); xmlErr != nil {
			return writeError(c, http.StatusBadRequest, "The request body is not a valid Atom entry.")
		}
	}

	ref, existed := h.Backend.ResolveHub(req.hub)
	if !existed {
		ref = HubRef{Namespace: DefaultNamespace, Hub: req.hub}
		if nsErr := h.Backend.EnsureNamespace(DefaultNamespace); nsErr != nil {
			return writeError(c, http.StatusBadRequest, nsErr.Error())
		}
	}

	cfg := HubConfig{
		PartitionCount: entry.Content.Hub.PartitionCnt,
		Retention:      time.Duration(entry.Content.Hub.RetentionDays) * day,
	}

	info, err := h.Backend.CreateHub(ref, cfg)
	if err != nil {
		return writeError(c, http.StatusBadRequest, err.Error())
	}

	status := http.StatusCreated
	if existed {
		status = http.StatusOK
	}

	return atomXML(c, status, hubEntry(info))
}

func (h *Handler) groupOp(c *echo.Context, req request) error {
	ref, ok := h.Backend.ResolveHub(req.hub)
	if !ok {
		return notFoundResp(c, req.hub)
	}

	switch c.Request().Method {
	case http.MethodPut:
		created, err := h.Backend.CreateConsumerGroup(ref, req.group)
		if err != nil {
			return writeError(c, http.StatusBadRequest, err.Error())
		}

		return atomXML(c, http.StatusCreated, groupEntry(req.hub, req.group, created))
	case http.MethodGet:
		created, err := h.Backend.ConsumerGroup(ref, req.group)
		if err != nil {
			return notFoundResp(c, req.group)
		}

		return atomXML(c, http.StatusOK, groupEntry(req.hub, req.group, created))
	case http.MethodDelete:
		err := h.Backend.DeleteConsumerGroup(ref, req.group)

		switch {
		case err == nil:
			return c.NoContent(http.StatusOK)
		case errors.Is(err, ErrProtectedEntity):
			return writeError(c, http.StatusBadRequest, err.Error())
		}

		return notFoundResp(c, req.group)
	}

	return writeError(c, http.StatusMethodNotAllowed, "The resource doesn't support the specified HTTP verb.")
}
