package bedrockagentcore

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	maxPayloadItems  = 100
	maxMetadataItems = 15
	defaultPageSize  = 100
	maxPageSize      = 100
	eventIDRandLen   = 8
)

func (b *InMemoryBackend) memoryOf(sc Scope, id string) (MemoryInfo, error) {
	c := b.ctl()
	if c == nil {
		return MemoryInfo{}, internal("the control plane is not wired")
	}

	m, ok := c.Memory(sc, id)
	if !ok {
		return MemoryInfo{}, notFound("memory %q not found", id)
	}

	return m, nil
}

func (e *Event) expired(m MemoryInfo, now time.Time) bool {
	return m.ExpiryDays > 0 && now.After(e.Timestamp.AddDate(0, 0, m.ExpiryDays))
}

func validatePayload(p []any) error {
	if len(p) == 0 || len(p) > maxPayloadItems {
		return invalid("payload must contain between 1 and %d items", maxPayloadItems)
	}

	for _, item := range p {
		m, _ := item.(map[string]any)

		key, ok := exactlyOne(m, "conversational", "blob", "json")
		if !ok {
			return invalid("each payload item must set exactly one of conversational, blob or json")
		}

		if key == "conversational" {
			if err := validateConversational(objOf(m, key)); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateConversational(c doc) error {
	if !slices.Contains([]string{"USER", "ASSISTANT", "TOOL", "OTHER"}, strOf(c, "role")) {
		return invalid("conversational role must be one of USER, ASSISTANT, TOOL or OTHER")
	}

	if _, ok := objOf(c, "content")[keyText].(string); !ok {
		return invalid("conversational content must set text")
	}

	return nil
}

func validateEventMetadata(m doc) error {
	if len(m) > maxMetadataItems {
		return invalid("metadata may hold at most %d entries", maxMetadataItems)
	}

	for k, v := range m {
		if _, ok := objOf(map[string]any{k: v}, k)["stringValue"].(string); !ok {
			return invalid("metadata value %q must set stringValue", k)
		}
	}

	return nil
}

func (b *InMemoryBackend) validateBranch(sc Scope, memory, session string, branch doc) error {
	if branch == nil {
		return nil
	}

	if strOf(branch, "name") == "" {
		return invalid("branch.name is required")
	}

	root := strOf(branch, "rootEventId")
	if root == "" {
		return nil
	}

	ev, ok := b.events.Get(eventKey(sc.Account, sc.Region, memory, root))
	if !ok || ev.SessionID != session {
		return notFound("branch root event %q not found in session %q", root, session)
	}

	return nil
}

func (b *InMemoryBackend) createEvent(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeDoc(req)
	if err != nil {
		return nil, 0, err
	}

	sc, memory := b.scopeOf(ctx), req.Params["memoryId"]

	if _, err = b.memoryOf(sc, memory); err != nil {
		return nil, 0, err
	}

	ev, err := b.newEvent(sc, memory, in)
	if err != nil {
		return nil, 0, err
	}

	b.mu.Lock("CreateEvent")
	defer b.mu.Unlock()

	if prior := b.eventByToken(ev); prior != nil {
		return doc{keyEvent: eventView(prior, true)}, 0, nil
	}

	if err = b.validateBranch(sc, memory, ev.SessionID, ev.Branch); err != nil {
		return nil, 0, err
	}

	b.seq++
	ev.Seq = b.seq
	b.events.Put(ev)

	return doc{keyEvent: eventView(ev, true)}, 0, nil
}

func (b *InMemoryBackend) newEvent(sc Scope, memory string, in doc) (*Event, error) {
	actor, session := strOf(in, keyActorID), strOf(in, "sessionId")
	if actor == "" || session == "" {
		return nil, invalid("actorId and sessionId are required")
	}

	ts, ok := fromEpoch(in["eventTimestamp"])
	if !ok {
		return nil, invalid("eventTimestamp is required")
	}

	payload := listOf(in, "payload")
	if err := validatePayload(payload); err != nil {
		return nil, err
	}

	if err := validateEventMetadata(objOf(in, "metadata")); err != nil {
		return nil, err
	}

	return &Event{
		Account: sc.Account, Region: sc.Region, MemoryID: memory, ActorID: actor, SessionID: session,
		EventID:   fmt.Sprintf("%019d#%s", ts.UnixMilli(), awsid.NewN("", eventIDRandLen)),
		Timestamp: ts, Payload: payload, Metadata: objOf(in, "metadata"), Branch: objOf(in, "branch"),
		ClientToken: strOf(in, "clientToken"),
	}, nil
}

func (b *InMemoryBackend) eventByToken(ev *Event) *Event {
	if ev.ClientToken == "" {
		return nil
	}

	for _, e := range b.events.All() {
		if e.Account == ev.Account && e.Region == ev.Region && e.MemoryID == ev.MemoryID &&
			e.ClientToken == ev.ClientToken {
			return e
		}
	}

	return nil
}

func eventView(e *Event, payload bool) doc {
	out := doc{
		"eventId": e.EventID, "memoryId": e.MemoryID, keyActorID: e.ActorID, keySessionID: e.SessionID,
		"eventTimestamp": epoch(e.Timestamp),
	}

	if payload {
		out["payload"] = e.Payload
	} else {
		out["payload"] = []any{}
	}

	if e.Branch != nil {
		out["branch"] = e.Branch
	}

	if len(e.Metadata) > 0 {
		out["metadata"] = e.Metadata
	}

	return out
}

func (b *InMemoryBackend) findEvent(sc Scope, p map[string]string) (*Event, error) {
	m, err := b.memoryOf(sc, p["memoryId"])
	if err != nil {
		return nil, err
	}

	e, ok := b.events.Get(eventKey(sc.Account, sc.Region, p["memoryId"], p["eventId"]))
	if !ok || e.ActorID != p[keyActorID] || e.SessionID != p["sessionId"] || e.expired(m, b.nowT()) {
		return nil, notFound("event %q not found", p["eventId"])
	}

	return e, nil
}

func (b *InMemoryBackend) getEvent(ctx context.Context, req *restroute.Request) (any, int, error) {
	b.mu.RLock("GetEvent")
	defer b.mu.RUnlock()

	e, err := b.findEvent(b.scopeOf(ctx), req.Params)
	if err != nil {
		return nil, 0, err
	}

	return doc{keyEvent: eventView(e, true)}, 0, nil
}

func (b *InMemoryBackend) deleteEvent(ctx context.Context, req *restroute.Request) (any, int, error) {
	b.mu.Lock("DeleteEvent")
	defer b.mu.Unlock()

	sc := b.scopeOf(ctx)

	e, err := b.findEvent(sc, req.Params)
	if err != nil {
		return nil, 0, err
	}

	b.events.Delete(eventKey(sc.Account, sc.Region, e.MemoryID, e.EventID))

	return doc{"eventId": e.EventID}, 0, nil
}

func (b *InMemoryBackend) listEvents(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeDoc(req)
	if err != nil {
		return nil, 0, err
	}

	b.mu.RLock("ListEvents")
	defer b.mu.RUnlock()

	sc, p := b.scopeOf(ctx), req.Params

	m, err := b.memoryOf(sc, p["memoryId"])
	if err != nil {
		return nil, 0, err
	}

	filter, err := parseEventFilter(objOf(in, "filter"))
	if err != nil {
		return nil, 0, err
	}

	var session []*Event

	for _, e := range b.events.All() {
		if e.Account == sc.Account && e.Region == sc.Region && e.MemoryID == p["memoryId"] &&
			e.ActorID == p[keyActorID] && e.SessionID == p["sessionId"] && !e.expired(m, b.nowT()) {
			session = append(session, e)
		}
	}

	filter = filter.withParents(session)
	all := slices.DeleteFunc(slices.Clone(session), func(e *Event) bool { return !filter.match(e) })

	slices.SortFunc(all, func(x, y *Event) int {
		return cmp.Or(x.Timestamp.Compare(y.Timestamp), cmp.Compare(x.Seq, y.Seq))
	})

	pg, err := restroute.Paginate(all, strOf(in, "nextToken"), intOf(in, "maxResults"), defaultPageSize, maxPageSize)
	if err != nil {
		return nil, 0, err
	}

	withPayload := boolOf(in, "includePayloads", true)
	views := make([]doc, 0, len(pg.Data))

	for _, e := range pg.Data {
		views = append(views, eventView(e, withPayload))
	}

	return pageBody("events", views, pg.Next), 0, nil
}

func pageBody(key string, items any, next string) doc {
	out := doc{key: items}
	if next != "" {
		out["nextToken"] = next
	}

	return out
}

func (b *InMemoryBackend) listSessions(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeDoc(req)
	if err != nil {
		return nil, 0, err
	}

	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	sc, p := b.scopeOf(ctx), req.Params

	m, err := b.memoryOf(sc, p["memoryId"])
	if err != nil {
		return nil, 0, err
	}

	window := objOf(objOf(in, "filter"), "eventFilter")
	first := map[string]time.Time{}

	for _, e := range b.events.All() {
		if e.Account != sc.Account || e.Region != sc.Region || e.MemoryID != p["memoryId"] ||
			e.ActorID != p[keyActorID] || e.expired(m, b.nowT()) || !inWindow(e.Timestamp, window) {
			continue
		}

		if t, seen := first[e.SessionID]; !seen || e.Timestamp.Before(t) {
			first[e.SessionID] = e.Timestamp
		}
	}

	ids := sortedKeys(first)

	pg, err := restroute.Paginate(ids, strOf(in, "nextToken"), intOf(in, "maxResults"), defaultPageSize, maxPageSize)
	if err != nil {
		return nil, 0, err
	}

	out := make([]doc, 0, len(pg.Data))
	for _, id := range pg.Data {
		out = append(out, doc{keyActorID: p[keyActorID], keySessionID: id, keyCreatedAt: epoch(first[id])})
	}

	return pageBody("sessionSummaries", out, pg.Next), 0, nil
}

func inWindow(t time.Time, w doc) bool {
	if s, ok := fromEpoch(w["startTime"]); ok && t.Before(s) {
		return false
	}

	if e, ok := fromEpoch(w["endTime"]); ok && t.After(e) {
		return false
	}

	return true
}

func (b *InMemoryBackend) listActors(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeDoc(req)
	if err != nil {
		return nil, 0, err
	}

	b.mu.RLock("ListActors")
	defer b.mu.RUnlock()

	sc := b.scopeOf(ctx)

	m, err := b.memoryOf(sc, req.Params["memoryId"])
	if err != nil {
		return nil, 0, err
	}

	seen := map[string]bool{}

	for _, e := range b.events.All() {
		if e.Account == sc.Account && e.Region == sc.Region && e.MemoryID == req.Params["memoryId"] &&
			!e.expired(m, b.nowT()) {
			seen[e.ActorID] = true
		}
	}

	pg, err := restroute.Paginate(
		sortedKeys(seen), strOf(in, "nextToken"), intOf(in, "maxResults"), defaultPageSize, maxPageSize,
	)
	if err != nil {
		return nil, 0, err
	}

	out := make([]doc, 0, len(pg.Data))
	for _, id := range pg.Data {
		out = append(out, doc{keyActorID: id})
	}

	return pageBody("actorSummaries", out, pg.Next), 0, nil
}
