package cosmosdb

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	headerAIM         = "A-IM"
	headerPKRangeID   = "X-Ms-Documentdb-Partitionkeyrangeid"
	headerIfNoneMatch = "If-None-Match"
	headerIfModified  = "If-Modified-Since"

	incrementalFeed = "incremental feed"
	// onlyPKRangeID is the single physical partition range the emulator reports.
	onlyPKRangeID = "0"
	// pkRangeMax is the exclusive upper bound of the hash space of the single range.
	pkRangeMax  = "FF"
	pkRangeETag = `"1"`
)

func isChangeFeedRequest(r *http.Request) bool {
	return strings.EqualFold(strings.TrimSpace(r.Header.Get(headerAIM)), incrementalFeed)
}

type changeFeedRequest struct {
	Since        time.Time
	PartitionKey string
	AfterSeq     int64
	Limit        int
	FromNow      bool
}

type changeFeedPage struct {
	RID  string
	Docs []DocumentInfo
	Seq  int64
}

func etagForSeq(seq int64) string { return `"` + strconv.FormatInt(seq, 10) + `"` }

func parseChangeFeedRequest(r *http.Request) (changeFeedRequest, error) {
	var req changeFeedRequest

	if id := r.Header.Get(headerPKRangeID); id != "" && id != onlyPKRangeID {
		return req, fmt.Errorf("%w: %q", ErrInvalidPKRange, id)
	}

	scope, err := queryPartitionScope(r)
	if err != nil {
		return req, err
	}

	req.PartitionKey = scope

	_, req.Limit, _ = pageRequest(r)

	return req, applyChangeFeedStart(&req, r)
}

// applyChangeFeedStart reads the start position: a continuation (If-None-Match), "now" (*), or a start time.
func applyChangeFeedStart(req *changeFeedRequest, r *http.Request) error {
	inm := strings.TrimSpace(r.Header.Get(headerIfNoneMatch))

	switch {
	case inm == "*":
		req.FromNow = true
	case inm != "":
		seq, err := strconv.ParseInt(strings.Trim(inm, `"`), 10, 64)
		if err != nil || seq < 0 {
			return ErrInvalidContinuation
		}

		req.AfterSeq = seq
	case r.Header.Get(headerIfModified) != "":
		since, err := parseHTTPDate(r.Header.Get(headerIfModified))
		if err != nil {
			return fmt.Errorf("%w: invalid %s header", ErrInvalidDocument, headerIfModified)
		}

		req.Since = since
	}

	return nil
}

func parseHTTPDate(v string) (time.Time, error) {
	if t, err := time.Parse(time.RFC1123, v); err == nil {
		return t, nil
	}

	return http.ParseTime(v)
}

// readChangeFeed returns the latest version of every document changed after the request's start position, in
// modification order. Deletes are not part of the latest-version feed.
func (b *InMemoryBackend) readChangeFeed(dbID, collID string, req changeFeedRequest) (changeFeedPage, error) {
	b.mu.RLock("readChangeFeed")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return changeFeedPage{}, err
	}

	rid := b.containerInfo(dbID, c).RID
	if req.FromNow {
		return changeFeedPage{RID: rid, Seq: c.ChangeSeq}, nil
	}

	docs := changedDocs(c.liveDocs(b.now()), req)
	sort.Slice(docs, func(i, j int) bool { return docs[i].Seq < docs[j].Seq })

	seq := c.ChangeSeq
	if req.Limit > 0 && len(docs) > req.Limit {
		docs = docs[:req.Limit]
		seq = docs[len(docs)-1].Seq
	}

	infos := make([]DocumentInfo, 0, len(docs))
	for _, d := range docs {
		infos = append(infos, b.documentInfo(dbID, collID, d))
	}

	return changeFeedPage{RID: rid, Docs: infos, Seq: seq}, nil
}

func changedDocs(live []*storedDocument, req changeFeedRequest) []*storedDocument {
	out := make([]*storedDocument, 0, len(live))

	for _, d := range live {
		if d.Seq <= req.AfterSeq {
			continue
		}

		if !req.Since.IsZero() && d.Timestamp.Unix() < req.Since.Unix() {
			continue
		}

		if req.PartitionKey != "" && !partitionKeyMatches(d.PartitionKeyJSON, req.PartitionKey) {
			continue
		}

		out = append(out, d)
	}

	return out
}

type changeFeedReader interface {
	readChangeFeed(dbID, collID string, req changeFeedRequest) (changeFeedPage, error)
}

func (h *Handler) readChangeFeed(c *echo.Context, dbID, collID string) error {
	req, reqErr := parseChangeFeedRequest(c.Request())
	if reqErr != nil {
		if errors.Is(reqErr, ErrInvalidContinuation) {
			return h.writeContinuationError(c)
		}

		return h.writeDocumentError(c, reqErr)
	}

	reader, ok := h.Backend.(changeFeedReader)
	if !ok {
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed", "The backend has no change feed.")
	}

	pg, err := reader.readChangeFeed(dbID, collID, req)
	if err != nil {
		return h.writeDocumentError(c, err)
	}

	c.Response().Header().Set("ETag", etagForSeq(pg.Seq))

	if len(pg.Docs) == 0 {
		setCharge(c, ruReadBase)

		return c.NoContent(http.StatusNotModified)
	}

	values := make([]map[string]any, 0, len(pg.Docs))

	for _, info := range pg.Docs {
		m := h.encodeDocument(info)
		m["_lsn"] = info.Seq
		values = append(values, m)
	}

	setPageHeaders(c, len(values), "")

	return h.writeJSONCharged(c, http.StatusOK,
		map[string]any{sysPropRID: pg.RID, listDocumentsKey: values, sysPropCount: len(values)}, readCharge)
}

func (h *Handler) handlePKRanges(c *echo.Context, dbID, collID string) error {
	if c.Request().Method != http.MethodGet {
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.")
	}

	info, err := h.Backend.GetContainer(dbID, collID)
	if err != nil {
		return h.writeDocumentError(c, err)
	}

	hdr := c.Response().Header()
	hdr.Set("ETag", pkRangeETag)

	if strings.TrimSpace(c.Request().Header.Get(headerIfNoneMatch)) == pkRangeETag {
		return c.NoContent(http.StatusNotModified)
	}

	setCharge(c, ruMetadata)

	rng := map[string]any{
		"id": onlyPKRangeID, sysPropRID: info.RID + "AAAA", sysPropETag: pkRangeETag, "minInclusive": "",
		"maxExclusive": pkRangeMax, "ridPrefix": 0, sysPropSelf: "dbs/" + dbID + "/colls/" + collID + "/pkranges/0/",
		"throughputFraction": 1, "status": "online", "parents": []string{}, sysPropTS: time.Now().Unix(), "lsn": 1,
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{
		sysPropRID: info.RID, "PartitionKeyRanges": []map[string]any{rng}, sysPropCount: 1,
	})
}
