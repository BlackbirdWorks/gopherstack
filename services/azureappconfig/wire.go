package azureappconfig

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	fieldName   = "name"
	fieldStatus = "status"
	fieldETag   = "etag"
	fieldKey    = "key"
	fieldLabel  = "label"
	fieldTags   = "tags"
)

func kvFields() []string {
	return []string{fieldKey, fieldLabel, "content_type", "value", "last_modified", fieldTags, "locked", fieldETag}
}

func snapshotFields() []string {
	return []string{
		fieldName, fieldStatus, "filters", "composition_type", "created", "expires", "size", "items_count",
		fieldTags, fieldETag, "retention_period",
	}
}

type fieldSet map[string]bool

func parseSelect(q url.Values, allowed []string) (fieldSet, error) {
	raw := q["$select"]
	if len(raw) == 0 {
		return fieldSet{}, nil
	}

	sel := fieldSet{}

	for _, group := range raw {
		for f := range strings.SplitSeq(group, ",") {
			f = strings.ToLower(strings.TrimSpace(f))
			if f == "" {
				continue
			}

			if !contains(allowed, f) {
				return nil, fmt.Errorf("%w: unknown $select field %q", ErrInvalidArgument, f)
			}

			sel[f] = true
		}
	}

	return sel, nil
}

func contains(list []string, v string) bool {
	return slices.Contains(list, v)
}

func nullable(s string) any {
	if s == "" {
		return nil
	}

	return s
}

func timeText(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func kvWire(kv KeyValue, sel fieldSet) map[string]any {
	tags := kv.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	full := map[string]any{
		fieldETag: kv.ETag, fieldKey: kv.Key, fieldLabel: nullable(kv.Label), "content_type": kv.ContentType,
		"value": kv.Value, "tags": tags, "locked": kv.Locked, "last_modified": timeText(kv.LastModified),
	}

	return only(full, sel)
}

func only(full map[string]any, sel fieldSet) map[string]any {
	if len(sel) == 0 {
		return full
	}

	out := make(map[string]any, len(sel))

	for k, v := range full {
		if sel[k] {
			out[k] = v
		}
	}

	return out
}

func snapshotWire(s Snapshot, sel fieldSet) map[string]any {
	filters := make([]map[string]any, 0, len(s.Filters))
	for _, f := range s.Filters {
		filters = append(filters, map[string]any{fieldKey: nullable(f.Key), fieldLabel: nullable(f.Label)})
	}

	tags := s.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	full := map[string]any{
		fieldETag:          s.ETag,
		fieldName:          s.Name,
		fieldStatus:        s.Status,
		"filters":          filters,
		"composition_type": s.Composition,
		"created":          timeText(s.Created),
		"size":             s.Size,
		"items_count":      len(s.Items),
		"tags":             tags,
		"retention_period": s.Retention,
	}

	if s.Expires != nil {
		full["expires"] = timeText(*s.Expires)
	}

	return only(full, sel)
}

// listETag derives a collection ETag from the member ETags.
func listETag(parts []string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, ",")))

	return base64.RawURLEncoding.EncodeToString(sum[:12])
}

func quoted(etag string) string { return `"` + etag + `"` }

// nextLink rebuilds the request URL with the continuation token, keeping any point-in-time.
func (req *request) nextLink(token string, at time.Time) string {
	vals := req.r.URL.Query()

	for k := range vals {
		if strings.EqualFold(k, "after") {
			delete(vals, k)
		}
	}

	vals.Set("after", token)

	if !at.IsZero() && vals.Get("at") == "" {
		vals.Set("at", at.UTC().Format(time.RFC3339Nano))
	}

	return req.r.URL.EscapedPath() + "?" + vals.Encode()
}

// pageToken extracts the continuation token; point-in-time "after" values are not tokens.
func (req *request) pageToken() string {
	after := req.q.Get("after")
	if _, err := parseTime(after); err == nil {
		return ""
	}

	return after
}

func (h *Handler) respondList(
	req *request, media string, items []map[string]any, etags []string, next string, at time.Time,
) error {
	body := map[string]any{"items": items}
	hdr := req.c.Response().Header()
	hdr.Set("ETag", quoted(listETag(etags)))

	if next != "" {
		link := req.nextLink(next, at)
		body["@nextLink"] = link
		hdr.Set("Link", "<"+link+`>; rel="next"`)
	}

	if !at.IsZero() {
		hdr.Set("Memento-Datetime", at.UTC().Format(time.RFC1123))
	}

	return h.respond(req, http.StatusOK, media, body)
}

func pageOf[T any](req *request, all []T) page.Page[T] {
	return page.New(all, req.pageToken(), pageSize, pageSize)
}
