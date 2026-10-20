package azurequeue

import (
	"encoding/xml"
	"errors"
	"maps"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	metaHeaderPrefix = "X-Ms-Meta-"
	compMetadata     = "metadata"

	minQueueNameLen = 3
	maxQueueNameLen = 63
)

var (
	queueNameRe    = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	metadataNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// validateQueueName returns the Azure error code and message for an invalid
// queue name; code is empty when the name is valid. Length violations are
// OutOfRangeInput, character violations InvalidResourceName.
func validateQueueName(name string) (string, string) {
	if len(name) < minQueueNameLen || len(name) > maxQueueNameLen {
		return "OutOfRangeInput", "The specified resource name length is not within the permissible limits."
	}

	if !queueNameRe.MatchString(name) {
		return "InvalidResourceName", "The specifed resource name contains invalid characters."
	}

	return "", ""
}

func cloneMetadata(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}

	return maps.Clone(m)
}

// parseMetadata collects x-ms-meta-* request headers (names lowercased).
// ok is false when a name is not a valid identifier.
func parseMetadata(r *http.Request) (map[string]string, bool) {
	var md map[string]string

	for name, vals := range r.Header {
		if len(name) <= len(metaHeaderPrefix) || !strings.EqualFold(name[:len(metaHeaderPrefix)], metaHeaderPrefix) {
			continue
		}

		key := name[len(metaHeaderPrefix):]
		if !metadataNameRe.MatchString(key) {
			return nil, false
		}

		if md == nil {
			md = make(map[string]string)
		}

		md[strings.ToLower(key)] = strings.Join(vals, ",")
	}

	return md, true
}

func (h *Handler) writeInvalidMetadata(c *echo.Context) error {
	return h.writeError(c, http.StatusBadRequest, "InvalidMetadata",
		"The metadata specified is invalid. It has characters that are not permitted.")
}

// setQueueMetadata serves PUT /<account>/<queue>?comp=metadata.
func (h *Handler) setQueueMetadata(c *echo.Context, queue string) error {
	md, ok := parseMetadata(c.Request())
	if !ok {
		return h.writeInvalidMetadata(c)
	}

	if err := h.Backend.SetQueueMetadata(queue, md); err != nil {
		return h.writeQueueNotFoundError(c)
	}

	return c.NoContent(http.StatusNoContent)
}

// getQueueMetadata serves GET/HEAD /<account>/<queue>?comp=metadata.
func (h *Handler) getQueueMetadata(c *echo.Context, queue string) error {
	props, err := h.Backend.GetQueueProperties(queue)
	if err != nil {
		return h.writeQueueNotFoundError(c)
	}

	hdr := c.Response().Header()
	for k, v := range props.Metadata {
		hdr[metaHeaderPrefix+k] = []string{v}
	}

	hdr.Set("X-Ms-Approximate-Messages-Count", strconv.Itoa(props.ApproximateMessageCount))

	return c.NoContent(http.StatusOK)
}

func metadataToXML(m map[string]string) *metadataXML {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := &metadataXML{Items: make([]metadataItem, 0, len(keys))}
	for _, k := range keys {
		out.Items = append(out.Items, metadataItem{XMLName: xmlName(k), Value: m[k]})
	}

	return out
}

// SetQueueMetadata replaces a queue's metadata.
func (b *InMemoryBackend) SetQueueMetadata(name string, metadata map[string]string) error {
	b.mu.Lock("SetQueueMetadata")
	defer b.mu.Unlock()

	q, ok := b.queues[name]
	if !ok {
		return ErrQueueNotFound
	}

	q.Metadata = cloneMetadata(metadata)

	return nil
}

// GetQueueProperties returns a queue's metadata and approximate message count
// (visible and in-flight, excluding expired).
func (b *InMemoryBackend) GetQueueProperties(name string) (QueueProperties, error) {
	b.mu.RLock("GetQueueProperties")
	defer b.mu.RUnlock()

	q, ok := b.queues[name]
	if !ok {
		return QueueProperties{}, ErrQueueNotFound
	}

	now := b.now()
	count := 0

	for _, msg := range q.Messages {
		if !msg.isExpired(now) {
			count++
		}
	}

	return QueueProperties{Metadata: cloneMetadata(q.Metadata), ApproximateMessageCount: count}, nil
}

// writeCreateQueueError maps CreateQueue backend errors.
func (h *Handler) writeCreateQueueError(c *echo.Context, err error) error {
	if errors.Is(err, ErrQueueAlreadyExists) {
		return h.writeError(c, http.StatusConflict, "QueueAlreadyExists",
			"The specified queue already exists.")
	}

	return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
}

func xmlName(local string) xml.Name { return xml.Name{Local: local} }
