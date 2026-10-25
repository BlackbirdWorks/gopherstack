package ocistreaming

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	defaultGroupTimeoutMs = 60000
	maxGroupTimeoutMs     = 300000

	cursorTrimHorizon = "TRIM_HORIZON"
	cursorLatest      = "LATEST"
	cursorAtTime      = "AT_TIME"
	cursorAtOffset    = "AT_OFFSET"
	cursorAfterOffset = "AFTER_OFFSET"
)

// cursorTok is the decoded form of an opaque cursor value.
type cursorTok struct {
	Pending  map[int]int64 `json:"pe,omitempty"`
	Stream   string        `json:"s"`
	Group    string        `json:"g,omitempty"`
	Instance string        `json:"i,omitempty"`
	Offset   int64         `json:"o"`
	Part     int           `json:"p"`
	Timeout  int           `json:"t,omitempty"`
	Commit   bool          `json:"c,omitempty"`
}

func (c cursorTok) encode() string {
	raw, _ := json.Marshal(c) //nolint:errchkjson // static shape

	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(streamID, v string) (cursorTok, error) {
	var c cursorTok

	raw, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil || json.Unmarshal(raw, &c) != nil || c.Stream != streamID {
		return c, errParam("The cursor is invalid or does not belong to this stream")
	}

	return c, nil
}

// CreateCursorIn is the CreateCursor request body.
type CreateCursorIn struct {
	Offset    *int64     `json:"offset"`
	Time      *time.Time `json:"time"`
	Partition string     `json:"partition"`
	Type      string     `json:"type"`
}

// CreateGroupCursorIn is the CreateGroupCursor request body.
type CreateGroupCursorIn struct {
	Time         *time.Time `json:"time"`
	CommitOnGet  *bool      `json:"commitOnGet"`
	TimeoutInMs  *int       `json:"timeoutInMs"`
	Type         string     `json:"type"`
	GroupName    string     `json:"groupName"`
	InstanceName string     `json:"instanceName"`
}

// UpdateGroupIn is the UpdateGroup request body.
type UpdateGroupIn struct {
	Time *time.Time `json:"time"`
	Type string     `json:"type"`
}

type cursorWire struct {
	Value string `json:"value"`
}

type reservationWire struct {
	TimeReservedUntil *time.Time `json:"timeReservedUntil,omitempty"`
	CommittedOffset   *int64     `json:"committedOffset,omitempty"`
	Partition         string     `json:"partition"`
	ReservedInstance  *string    `json:"reservedInstance,omitempty"`
}

type groupWire struct {
	StreamID     string            `json:"streamId"`
	GroupName    string            `json:"groupName"`
	Reservations []reservationWire `json:"reservations"`
}

func (p *Partition) startFor(kind string, offset *int64, at *time.Time) (int64, *APIError) {
	switch strings.ToUpper(kind) {
	case cursorTrimHorizon:
		return p.Base, nil
	case cursorLatest:
		return p.end(), nil
	case cursorAtTime:
		if at == nil {
			return 0, errMissing("time is required for an AT_TIME cursor")
		}

		for i, m := range p.Messages {
			if !m.Time.Before(*at) {
				return p.Base + int64(i), nil
			}
		}

		return p.end(), nil
	case cursorAtOffset, cursorAfterOffset:
		return p.offsetStart(strings.ToUpper(kind) == cursorAfterOffset, offset)
	}

	return 0, errParam("type must be one of AFTER_OFFSET, AT_OFFSET, AT_TIME, LATEST or TRIM_HORIZON")
}

func (p *Partition) offsetStart(after bool, offset *int64) (int64, *APIError) {
	if offset == nil {
		return 0, errMissing("offset is required for an offset cursor")
	}

	pos := *offset
	if after {
		pos++
	}

	if pos < p.Base || pos > p.end() {
		return 0, errParam("offset is outside the retained range of the partition")
	}

	return pos, nil
}

func (s *Stream) partIndex(v string) (int, *APIError) {
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 || n >= len(s.Parts) {
		return 0, errParam("partition " + v + " does not exist in the stream")
	}

	return n, nil
}

// CreateCursor creates a single-partition cursor.
func (b *InMemoryBackend) CreateCursor(streamID string, in CreateCursorIn) (cursorWire, error) {
	if in.Type == "" || in.Partition == "" {
		return cursorWire{}, errMissing("partition and type are required")
	}

	b.mu.RLock("CreateCursor")
	defer b.mu.RUnlock()

	s, aerr := b.liveStream(streamID)
	if aerr != nil {
		return cursorWire{}, aerr
	}

	part, aerr := s.partIndex(in.Partition)
	if aerr != nil {
		return cursorWire{}, aerr
	}

	off, aerr := s.Parts[part].startFor(in.Type, in.Offset, in.Time)
	if aerr != nil {
		return cursorWire{}, aerr
	}

	return cursorWire{Value: cursorTok{Stream: streamID, Part: part, Offset: off}.encode()}, nil
}

// GetMessages reads from a plain or group cursor and returns the cursor for the next call.
func (b *InMemoryBackend) GetMessages(streamID, cursor string, limit int) ([]messageWire, string, error) {
	if limit < 0 || limit > maxGetLimit {
		return nil, "", errParam("limit must be between 1 and 10000")
	}

	if limit == 0 {
		limit = maxGetLimit
	}

	b.mu.Lock("GetMessages")
	defer b.mu.Unlock()

	s, aerr := b.liveStream(streamID)
	if aerr != nil {
		return nil, "", aerr
	}

	cur, err := decodeCursor(streamID, cursor)
	if err != nil {
		return nil, "", err
	}

	if cur.Group != "" {
		return b.readGroup(s, cur, limit)
	}

	if cur.Part >= len(s.Parts) {
		return nil, "", errParam("The cursor partition does not exist")
	}

	msgs := s.read(cur.Part, cur.Offset, limit)
	cur.Offset = max(cur.Offset, s.Parts[cur.Part].Base)

	if n := len(msgs); n > 0 {
		cur.Offset = msgs[n-1].Offset + 1
	}

	return nonNil(msgs), cur.encode(), nil
}

func nonNil(m []messageWire) []messageWire {
	if m == nil {
		return []messageWire{}
	}

	return m
}

func groupKey(streamID, name string) string { return streamID + "/" + name }

func (g *Group) setPositions(s *Stream, kind string, at *time.Time) *APIError {
	next := make([]int64, len(s.Parts))

	for i, p := range s.Parts {
		off, aerr := p.startFor(kind, nil, at)
		if aerr != nil {
			return aerr
		}

		next[i] = off
	}

	g.Next = next

	return nil
}

func validGroupType(kind string) bool {
	switch strings.ToUpper(kind) {
	case cursorTrimHorizon, cursorLatest, cursorAtTime:
		return true
	}

	return false
}

// claim registers the instance and rebalances reservations to its fair share.
func (g *Group) claim(s *Stream, inst string, timeout time.Duration, now time.Time) {
	for name, exp := range g.Instances {
		if !exp.After(now) {
			delete(g.Instances, name)
			delete(g.Rotation, name)
		}
	}

	for i, r := range g.Reserved {
		if _, live := g.Instances[r.Instance]; r.Instance != "" && (!live || !r.Until.After(now)) && r.Instance != inst {
			g.Reserved[i] = Reservation{}
		}
	}

	g.Instances[inst] = now.Add(timeout)
	fair := (len(s.Parts) + len(g.Instances) - 1) / len(g.Instances)
	owned := 0

	for i, r := range g.Reserved {
		if r.Instance != inst {
			continue
		}

		if owned >= fair {
			g.Reserved[i] = Reservation{}

			continue
		}

		owned++
	}

	for i := range g.Reserved {
		if owned < fair && g.Reserved[i].Instance == "" {
			g.Reserved[i] = Reservation{Instance: inst}
			owned++
		}
	}

	for i := range g.Reserved {
		if g.Reserved[i].Instance == inst {
			g.Reserved[i].Until = now.Add(timeout)
		}
	}
}

func (g *Group) ownedBy(inst string) []int {
	var owned []int

	for i, r := range g.Reserved {
		if r.Instance == inst {
			owned = append(owned, i)
		}
	}

	return owned
}

// CreateGroupCursor joins an instance to a consumer group, creating the group on first use.
func (b *InMemoryBackend) CreateGroupCursor(streamID string, in CreateGroupCursorIn) (cursorWire, error) {
	if in.GroupName == "" || in.Type == "" {
		return cursorWire{}, errMissing("groupName and type are required")
	}

	if !validGroupType(in.Type) {
		return cursorWire{}, errParam("type must be one of AT_TIME, LATEST or TRIM_HORIZON")
	}

	timeout := defaultGroupTimeoutMs
	if in.TimeoutInMs != nil {
		timeout = *in.TimeoutInMs
	}

	if timeout < 1 || timeout > maxGroupTimeoutMs {
		return cursorWire{}, errParam("timeoutInMs must be between 1 and 300000")
	}

	b.mu.Lock("CreateGroupCursor")
	defer b.mu.Unlock()

	s, aerr := b.activeStream(streamID)
	if aerr != nil {
		return cursorWire{}, aerr
	}

	g, ok := b.groups[groupKey(streamID, in.GroupName)]
	if !ok {
		g = &Group{
			StreamID: streamID, Name: in.GroupName, Instances: map[string]time.Time{},
			Rotation: map[string]int{}, Reserved: make([]Reservation, len(s.Parts)),
		}

		if aerr = g.setPositions(s, in.Type, in.Time); aerr != nil {
			return cursorWire{}, aerr
		}

		b.groups[groupKey(streamID, in.GroupName)] = g
	}

	inst := in.InstanceName
	if inst == "" {
		inst = uuid.NewString()
	}

	g.claim(s, inst, time.Duration(timeout)*time.Millisecond, b.nowUTC())

	return cursorWire{Value: cursorTok{
		Stream: streamID, Group: in.GroupName, Instance: inst, Timeout: timeout,
		Commit: in.CommitOnGet == nil || *in.CommitOnGet,
	}.encode()}, nil
}

func (b *InMemoryBackend) liveGroup(s *Stream, cur cursorTok) (*Group, *APIError) {
	g, ok := b.groups[groupKey(s.ID, cur.Group)]
	if !ok {
		return nil, errNotFound("group " + cur.Group)
	}

	return g, nil
}

func (b *InMemoryBackend) readGroup(s *Stream, cur cursorTok, limit int) ([]messageWire, string, error) {
	g, aerr := b.liveGroup(s, cur)
	if aerr != nil {
		return nil, "", aerr
	}

	g.claim(s, cur.Instance, time.Duration(cur.Timeout)*time.Millisecond, b.nowUTC())

	owned := g.ownedBy(cur.Instance)
	for step := range owned {
		idx := (g.Rotation[cur.Instance] + step) % len(owned)
		part := owned[idx]
		from := max(g.Next[part], cur.Pending[part])

		msgs := s.read(part, from, limit)
		if len(msgs) == 0 {
			continue
		}

		g.Rotation[cur.Instance] = idx + 1
		end := msgs[len(msgs)-1].Offset + 1

		if cur.Commit {
			g.Next[part] = max(g.Next[part], end)
		} else {
			pend := map[int]int64{}
			for k, v := range cur.Pending {
				pend[k] = v
			}

			pend[part], cur.Pending = end, pend
		}

		return msgs, cur.encode(), nil
	}

	return []messageWire{}, cur.encode(), nil
}

// ConsumerCommit commits the offsets a group cursor accumulated.
func (b *InMemoryBackend) ConsumerCommit(streamID, cursor string) (cursorWire, error) {
	cur, err := decodeCursor(streamID, cursor)
	if err != nil {
		return cursorWire{}, err
	}

	b.mu.Lock("ConsumerCommit")
	defer b.mu.Unlock()

	s, g, aerr := b.groupFor(streamID, cur)
	if aerr != nil {
		return cursorWire{}, aerr
	}

	owned := map[int]bool{}
	for _, p := range g.ownedBy(cur.Instance) {
		owned[p] = true
	}

	for part, off := range cur.Pending {
		if !owned[part] {
			return cursorWire{}, errState("The consumer instance no longer holds the reservation for partition " +
				strconv.Itoa(part))
		}

		g.Next[part] = max(g.Next[part], min(off, s.Parts[part].end()))
	}

	cur.Pending = nil
	g.claim(s, cur.Instance, time.Duration(cur.Timeout)*time.Millisecond, b.nowUTC())

	return cursorWire{Value: cur.encode()}, nil
}

// ConsumerHeartbeat extends the instance's reservations.
func (b *InMemoryBackend) ConsumerHeartbeat(streamID, cursor string) (cursorWire, error) {
	cur, err := decodeCursor(streamID, cursor)
	if err != nil {
		return cursorWire{}, err
	}

	b.mu.Lock("ConsumerHeartbeat")
	defer b.mu.Unlock()

	s, g, aerr := b.groupFor(streamID, cur)
	if aerr != nil {
		return cursorWire{}, aerr
	}

	g.claim(s, cur.Instance, time.Duration(cur.Timeout)*time.Millisecond, b.nowUTC())

	return cursorWire{Value: cur.encode()}, nil
}

func (b *InMemoryBackend) groupFor(streamID string, cur cursorTok) (*Stream, *Group, *APIError) {
	if cur.Group == "" {
		return nil, nil, errParam("The cursor is not a group cursor")
	}

	s, aerr := b.liveStream(streamID)
	if aerr != nil {
		return nil, nil, aerr
	}

	g, aerr := b.liveGroup(s, cur)
	if aerr != nil {
		return nil, nil, aerr
	}

	return s, g, nil
}

// GetGroup returns the committed offsets and reservations of a consumer group.
func (b *InMemoryBackend) GetGroup(streamID, name string) (groupWire, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	if _, aerr := b.liveStream(streamID); aerr != nil {
		return groupWire{}, aerr
	}

	g, ok := b.groups[groupKey(streamID, name)]
	if !ok {
		return groupWire{}, errNotFound("group " + name)
	}

	now := b.nowUTC()
	out := groupWire{StreamID: streamID, GroupName: name, Reservations: make([]reservationWire, len(g.Next))}

	for i, next := range g.Next {
		rw := reservationWire{Partition: strconv.Itoa(i)}
		if next > 0 {
			rw.CommittedOffset = new(next - 1)
		}

		if r := g.Reserved[i]; r.Instance != "" && r.Until.After(now) {
			rw.ReservedInstance, rw.TimeReservedUntil = new(r.Instance), new(r.Until)
		}

		out.Reservations[i] = rw
	}

	return out, nil
}

// UpdateGroup resets the committed offsets of every partition.
func (b *InMemoryBackend) UpdateGroup(streamID, name string, in UpdateGroupIn) error {
	if !validGroupType(in.Type) {
		return errParam("type must be one of AT_TIME, LATEST or TRIM_HORIZON")
	}

	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	s, aerr := b.liveStream(streamID)
	if aerr != nil {
		return aerr
	}

	g, ok := b.groups[groupKey(streamID, name)]
	if !ok {
		return errNotFound("group " + name)
	}

	return nilErr(g.setPositions(s, in.Type, in.Time))
}

func nilErr(aerr *APIError) error {
	if aerr == nil {
		return nil
	}

	return aerr
}

func (h *Handler) putMessages(q *req) error {
	var in putMessagesIn
	if err := q.decode(&in); err != nil {
		return err
	}

	res, err := h.Backend.PutMessages(q.param("streamId"), in)

	return reply(q, res, "", err)
}

func (h *Handler) getMessages(q *req) error {
	cursor := q.query("cursor")
	if cursor == "" {
		return errMissing("cursor is required")
	}

	limit := 0
	if v := q.query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return errParam("limit must be between 1 and 10000")
		}

		limit = n
	}

	msgs, next, err := h.Backend.GetMessages(q.param("streamId"), cursor, limit)
	if err != nil {
		return err
	}

	q.header("opc-next-cursor", next)

	return q.json(http.StatusOK, msgs, "")
}

func (h *Handler) createCursor(q *req) error {
	var in CreateCursorIn
	if err := q.decode(&in); err != nil {
		return err
	}

	c, err := h.Backend.CreateCursor(q.param("streamId"), in)

	return reply(q, c, "", err)
}

func (h *Handler) createGroupCursor(q *req) error {
	var in CreateGroupCursorIn
	if err := q.decode(&in); err != nil {
		return err
	}

	c, err := h.Backend.CreateGroupCursor(q.param("streamId"), in)

	return reply(q, c, "", err)
}

func (h *Handler) getGroup(q *req) error {
	g, err := h.Backend.GetGroup(q.param("streamId"), q.param("groupName"))

	return reply(q, g, "", err)
}

func (h *Handler) updateGroup(q *req) error {
	var in UpdateGroupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	if err := h.Backend.UpdateGroup(q.param("streamId"), q.param("groupName"), in); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusOK)

	return nil
}

func (h *Handler) consumerCommit(q *req) error {
	c, err := h.Backend.ConsumerCommit(q.param("streamId"), q.query("cursor"))

	return reply(q, c, "", err)
}

func (h *Handler) consumerHeartbeat(q *req) error {
	c, err := h.Backend.ConsumerHeartbeat(q.param("streamId"), q.query("cursor"))

	return reply(q, c, "", err)
}
