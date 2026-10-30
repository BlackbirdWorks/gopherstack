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
)

func isChangeFeedRequest(r *http.Request) bool {
	return strings.EqualFold(strings.TrimSpace(r.Header.Get(headerAIM)), incrementalFeed)
}

type changeFeedRequest struct {
	Since        time.Time
	PartitionKey string
	RangeID      string
	hashes       hashRange
	AfterSeq     int64
	Limit        int
	FromNow      bool
	// AllVersions selects the all-versions-and-deletes mode; Started is set once the request names a start.
	AllVersions bool
	Started     bool
}

type changeFeedPage struct {
	RID      string
	Docs     []DocumentInfo
	Versions []ChangeVersion
	Seq      int64
}

func etagForSeq(seq int64) string { return `"` + strconv.FormatInt(seq, 10) + `"` }

func parseChangeFeedRequest(r *http.Request) (changeFeedRequest, error) {
	var req changeFeedRequest

	req.RangeID = r.Header.Get(headerPKRangeID)
	req.AllVersions = r.Header.Get(headerWireFormat) != ""

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
		req.FromNow, req.Started = true, true
	case inm != "":
		seq, err := strconv.ParseInt(strings.Trim(inm, `"`), 10, 64)
		if err != nil || seq < 0 {
			return ErrInvalidContinuation
		}

		req.AfterSeq, req.Started = seq, true
	case r.Header.Get(headerIfModified) != "":
		since, err := parseHTTPDate(r.Header.Get(headerIfModified))
		if err != nil {
			return fmt.Errorf("%w: invalid %s header", ErrInvalidDocument, headerIfModified)
		}

		req.Since, req.Started = since, true
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

	c, err := b.findDataContainer(dbID, collID, true)
	if err != nil {
		return changeFeedPage{}, err
	}

	rid := b.containerInfo(dbID, c).RID

	if err = b.resolveRange(dbID, collID, &req); err != nil {
		return changeFeedPage{}, err
	}

	if req.AllVersions && b.changeRetention() < 0 {
		return changeFeedPage{}, ErrChangeFeedBackup
	}

	if req.FromNow {
		return changeFeedPage{RID: rid, Seq: c.ChangeSeq}, nil
	}

	if req.AllVersions {
		return b.readVersionFeed(dbID, collID, c, rid, req)
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

// readVersionFeed serves the all-versions-and-deletes mode: every create, replace and delete (TTL expiry
// included) after the start, which must be "now", a continuation or a start time.
func (b *InMemoryBackend) readVersionFeed(
	dbID, collID string, c *storedContainer, rid string, req changeFeedRequest,
) (changeFeedPage, error) {
	if !req.Started || !req.Since.IsZero() {
		return changeFeedPage{}, ErrChangeFeedStart
	}

	if req.AfterSeq < c.ChangeFloor {
		return changeFeedPage{}, ErrChangeFeedRetention
	}

	recs := c.changedVersions(req)
	seq := c.ChangeSeq

	if req.Limit > 0 && len(recs) > req.Limit {
		recs = recs[:req.Limit]
		seq = recs[len(recs)-1].Seq
	}

	versions := make([]ChangeVersion, 0, len(recs))
	for _, r := range recs {
		versions = append(versions, b.changeVersion(dbID, collID, c, r))
	}

	return changeFeedPage{RID: rid, Versions: versions, Seq: seq}, nil
}

// resolveRange validates the requested partition key range id against the container's current ranges; the lock
// must be held.
func (b *InMemoryBackend) resolveRange(dbID, collID string, req *changeFeedRequest) error {
	if req.RangeID == "" {
		return nil
	}

	l := b.layoutView(b.databases[dbID], b.databases[dbID].Containers[collID])

	id, err := strconv.Atoi(req.RangeID)
	if err != nil || id < 0 {
		return fmt.Errorf("%w: %q", ErrInvalidPKRange, req.RangeID)
	}

	idx, active := l.byID(id)
	if !active {
		if l.gone(id) {
			return fmt.Errorf("%w: %q", ErrPKRangeGone, req.RangeID)
		}

		return fmt.Errorf("%w: %q", ErrInvalidPKRange, req.RangeID)
	}

	req.hashes = l.rangeOf(idx)

	return nil
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

		if !req.hashes.contains(d.PartitionKeyJSON) {
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

	reader, ok := h.be(c).(changeFeedReader)
	if !ok {
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed", "The backend has no change feed.")
	}

	pg, err := reader.readChangeFeed(dbID, collID, req)
	if err != nil {
		return h.writeDocumentError(c, err)
	}

	c.Response().Header().Set("ETag", etagForSeq(pg.Seq))

	if len(pg.Docs) == 0 && len(pg.Versions) == 0 {
		setCharge(c, ruReadBase)

		return c.NoContent(http.StatusNotModified)
	}

	if req.AllVersions {
		return h.writeVersionFeed(c, pg)
	}

	values := make([]map[string]any, 0, len(pg.Docs))

	for _, info := range pg.Docs {
		m := h.encodeDocument(info)
		m["_lsn"] = info.Seq
		values = append(values, m)
	}

	setPageHeaders(c, len(values), "")

	return h.writeJSONCharged(
		c,
		http.StatusOK,
		map[string]any{
			sysPropRID:       pg.RID,
			listDocumentsKey: values,
			sysPropCount:     len(values),
		},
		h.feedCharger(c, values),
	)
}

// writeVersionFeed renders the all-versions-and-deletes page: {current, previous, metadata} per entry.
func (h *Handler) writeVersionFeed(c *echo.Context, pg changeFeedPage) error {
	values := make([]map[string]any, 0, len(pg.Versions))

	for _, v := range pg.Versions {
		values = append(values, h.encodeVersion(v))
	}

	setPageHeaders(c, len(values), "")

	return h.writeJSONCharged(
		c,
		http.StatusOK,
		map[string]any{
			sysPropRID:       pg.RID,
			listDocumentsKey: values,
			sysPropCount:     len(values),
		},
		h.feedCharger(c, values),
	)
}

func (h *Handler) encodeVersion(v ChangeVersion) map[string]any {
	meta := map[string]any{
		"lsn": v.LSN, "crts": v.CRTS, "operationType": v.Op, "id": v.ID, "partitionKey": v.PartitionKey,
	}

	if v.PreviousLSN > 0 {
		meta["previousImageLSN"] = v.PreviousLSN
	}

	out := map[string]any{"current": map[string]any{}, "metadata": meta}

	if v.Current != nil {
		cur := h.encodeDocument(*v.Current)
		cur["_lsn"] = v.Current.Seq
		out["current"] = cur
	}

	if v.Op == opDelete {
		meta["timeToLiveExpired"] = v.TTLExpired
	}

	return out
}

func (h *Handler) handlePKRanges(c *echo.Context, dbID, collID string) error {
	if c.Request().Method != http.MethodGet {
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.")
	}

	pr, err := h.partitionRanges(dbID, collID)
	if err != nil {
		return h.writeDocumentError(c, err)
	}

	etag := pr.Layout.etag()
	c.Response().Header().Set("ETag", etag)

	if strings.TrimSpace(c.Request().Header.Get(headerIfNoneMatch)) == etag {
		return c.NoContent(http.StatusNotModified)
	}

	setCharge(c, ruMetadata)

	n := len(pr.Layout.Spans)
	ranges := make([]map[string]any, n)

	for i := range ranges {
		ranges[i] = pr.Layout.pkRangeBody(pr.RID, dbID, collID, i)
		ranges[i][sysPropTS] = time.Now().Unix()
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{
		sysPropRID: pr.RID, "PartitionKeyRanges": ranges, sysPropCount: n,
	})
}

// partitionRanges reads the container's current partition layout.
func (h *Handler) partitionRanges(dbID, collID string) (PartitionKeyRanges, error) {
	if pb, ok := h.Backend.(interface {
		partitionRanges(db, coll string) (PartitionKeyRanges, error)
	}); ok {
		return pb.partitionRanges(dbID, collID)
	}

	info, err := h.Backend.GetContainer(dbID, collID)
	if err != nil {
		return PartitionKeyRanges{}, err
	}

	return PartitionKeyRanges{Layout: evenLayout(1), RID: info.RID}, nil
}
