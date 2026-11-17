package ocistreaming

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	maxKafkaGroups        = 1000
	maxKafkaMembers       = 100
	minSessionTimeout     = 6 * time.Second
	maxSessionTimeout     = 30 * time.Minute
	maxRebalanceTimeout   = 5 * time.Minute
	maxMemberProtocols    = 16
	maxMemberMetadataSize = 64 << 10
	maxAssignmentSize     = 1 << 20
	maxMemberIDLen        = 256
)

const (
	gsPreparing = iota
	gsAwaitingSync
	gsStable
)

type kmember struct {
	last      time.Time
	assign    []byte
	protocols []kmsg.JoinGroupRequestProtocol
	session   time.Duration
	rebalance time.Duration
}

type kgroup struct {
	members  map[string]*kmember
	joined   map[string]bool
	joinDone chan struct{}
	syncDone chan struct{}
	leader   string
	protoTyp string
	protocol string
	gen      int32
	state    int
}

// coordinator holds the consumer-group membership of one Kafka endpoint; offsets live in the backend.
type coordinator struct {
	mu     *lockmetrics.RWMutex
	now    func() time.Time
	groups map[string]*kgroup
}

func newCoordinator(now func() time.Time) *coordinator {
	return &coordinator{mu: lockmetrics.New("ocistreaming.kafka.groups"), now: now, groups: map[string]*kgroup{}}
}

func groupMapKey(poolID, group string) string {
	return itoa(len(poolID)) + ":" + poolID + group
}

func joinErr(code int16) *kmsg.JoinGroupResponse {
	return &kmsg.JoinGroupResponse{ErrorCode: code, Generation: -1}
}

func (g *kgroup) startRound() {
	g.state, g.joined, g.joinDone = gsPreparing, map[string]bool{}, make(chan struct{})
}

func (g *kgroup) allJoined() bool {
	for id := range g.members {
		if !g.joined[id] {
			return false
		}
	}

	return true
}

// completeJoin drops members that did not rejoin and moves the group to the next generation.
func (c *coordinator) completeJoin(key string, g *kgroup) {
	for id := range g.members {
		if !g.joined[id] {
			delete(g.members, id)
		}
	}

	if len(g.members) == 0 {
		delete(c.groups, key)
		close(g.joinDone)

		return
	}

	g.gen++
	g.protocol = commonProtocol(g.members)

	if _, ok := g.members[g.leader]; !ok {
		for id := range g.members {
			if g.leader == "" || id < g.leader {
				g.leader = id
			}
		}
	}

	for _, m := range g.members {
		m.assign = nil
	}

	g.state, g.syncDone = gsAwaitingSync, make(chan struct{})
	close(g.joinDone)
}

func commonProtocol(members map[string]*kmember) string {
	var first *kmember

	for _, m := range members {
		if first == nil || len(m.protocols) < len(first.protocols) {
			first = m
		}
	}

	for _, p := range first.protocols {
		if supportedByAll(members, p.Name) {
			return p.Name
		}
	}

	return ""
}

func supportedByAll(members map[string]*kmember, name string) bool {
	for _, m := range members {
		found := false

		for _, p := range m.protocols {
			if p.Name == name {
				found = true

				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func metadataFor(m *kmember, proto string) []byte {
	for _, p := range m.protocols {
		if p.Name == proto {
			return p.Metadata
		}
	}

	return nil
}

func clampDur(ms int32, lo, hi time.Duration) time.Duration {
	return min(max(time.Duration(ms)*time.Millisecond, lo), hi)
}

func validJoin(r *kmsg.JoinGroupRequest) int16 {
	if r.Group == "" || len(r.Group) > kafkaMaxGroupName {
		return kerrInvalidGroupID
	}

	if len(r.MemberID) > maxMemberIDLen || len(r.Protocols) == 0 || len(r.Protocols) > maxMemberProtocols ||
		r.ProtocolType == "" {
		return kerrInconsistentProto
	}

	if t := time.Duration(r.SessionTimeoutMillis) * time.Millisecond; t < minSessionTimeout || t > maxSessionTimeout {
		return kerrInvalidSession
	}

	for _, p := range r.Protocols {
		if len(p.Metadata) > maxMemberMetadataSize {
			return kerrInconsistentProto
		}
	}

	return kerrNone
}

func (c *coordinator) join(ctx context.Context, key string, r *kmsg.JoinGroupRequest) *kmsg.JoinGroupResponse {
	if code := validJoin(r); code != kerrNone {
		return joinErr(code)
	}

	id, g, wait, rebalance, code := c.admit(key, r)
	if code != kerrNone {
		return joinErr(code)
	}

	timer := time.NewTimer(rebalance)
	defer timer.Stop()

	select {
	case <-wait:
	case <-ctx.Done():
	case <-timer.C:
		c.mu.Lock("joinTimeout")
		if cur, ok := c.groups[key]; ok && cur == g && g.joinDone == wait && g.state == gsPreparing {
			c.completeJoin(key, g)
		}
		c.mu.Unlock()
	}

	c.mu.RLock("join")
	defer c.mu.RUnlock()

	m, ok := g.members[id]
	if !ok || c.groups[key] != g {
		return joinErr(kerrUnknownMember)
	}

	resp := &kmsg.JoinGroupResponse{
		Generation: g.gen, ProtocolType: &g.protoTyp, Protocol: &g.protocol, LeaderID: g.leader, MemberID: id,
	}

	if id == g.leader {
		for mid, mm := range g.members {
			resp.Members = append(
				resp.Members,
				kmsg.JoinGroupResponseMember{MemberID: mid, ProtocolMetadata: metadataFor(mm, g.protocol)},
			)
		}
	}

	_ = m

	return resp
}

func (c *coordinator) admit(
	key string, r *kmsg.JoinGroupRequest,
) (string, *kgroup, chan struct{}, time.Duration, int16) {
	c.mu.Lock("join")
	defer c.mu.Unlock()

	g, ok := c.groups[key]
	if !ok {
		if len(c.groups) >= maxKafkaGroups {
			return "", nil, nil, 0, kerrCoordinatorNA
		}

		g = &kgroup{members: map[string]*kmember{}, protoTyp: r.ProtocolType}
		g.startRound()
		c.groups[key] = g
	}

	if g.protoTyp != r.ProtocolType {
		return "", nil, nil, 0, kerrInconsistentProto
	}

	id := r.MemberID
	if _, known := g.members[id]; id != "" && !known {
		return "", nil, nil, 0, kerrUnknownMember
	}

	if id == "" {
		if len(g.members) >= maxKafkaMembers {
			return "", nil, nil, 0, kerrGroupMaxSize
		}

		id = "member-" + uuid.NewString()
	}

	session := clampDur(r.SessionTimeoutMillis, minSessionTimeout, maxSessionTimeout)
	rebalance := clampDur(r.RebalanceTimeoutMillis, session, maxRebalanceTimeout)

	if r.Version == 0 {
		rebalance = session
	}

	g.members[id] = &kmember{
		last:      c.now(),
		session:   session,
		rebalance: rebalance,
		protocols: append([]kmsg.JoinGroupRequestProtocol(nil), r.Protocols...),
	}

	if g.state != gsPreparing {
		g.startRound()
	}

	g.joined[id] = true
	if g.allJoined() {
		c.completeJoin(key, g)
	}

	return id, g, g.joinDone, rebalance, kerrNone
}

func (c *coordinator) sync(ctx context.Context, key string, r *kmsg.SyncGroupRequest) *kmsg.SyncGroupResponse {
	c.mu.Lock("sync")

	g, m, code := c.member(key, r.MemberID, r.Generation)
	if code != kerrNone {
		c.mu.Unlock()

		return &kmsg.SyncGroupResponse{ErrorCode: code}
	}

	if r.MemberID == g.leader && g.state == gsAwaitingSync {
		for _, a := range r.GroupAssignment {
			if mm, ok := g.members[a.MemberID]; ok && len(a.MemberAssignment) <= maxAssignmentSize {
				mm.assign = a.MemberAssignment
			}
		}

		g.state = gsStable
		close(g.syncDone)
	}

	wait, rebalance := g.syncDone, m.rebalance
	c.mu.Unlock()

	timer := time.NewTimer(rebalance)
	defer timer.Stop()

	select {
	case <-wait:
	case <-ctx.Done():
	case <-timer.C:
	}

	c.mu.RLock("sync")
	defer c.mu.RUnlock()

	g, m, code = c.member(key, r.MemberID, r.Generation)
	if code != kerrNone {
		return &kmsg.SyncGroupResponse{ErrorCode: code}
	}

	if g.state != gsStable {
		return &kmsg.SyncGroupResponse{ErrorCode: kerrRebalanceInProgress}
	}

	return &kmsg.SyncGroupResponse{MemberAssignment: m.assign}
}

func (c *coordinator) member(key, id string, gen int32) (*kgroup, *kmember, int16) {
	g, ok := c.groups[key]
	if !ok {
		return nil, nil, kerrUnknownMember
	}

	m, ok := g.members[id]

	switch {
	case !ok:
		return nil, nil, kerrUnknownMember
	case g.state == gsPreparing:
		return g, m, kerrRebalanceInProgress
	case gen != g.gen:
		return g, m, kerrIllegalGeneration
	}

	return g, m, kerrNone
}

func (c *coordinator) heartbeat(key, id string, gen int32) int16 {
	c.mu.Lock("heartbeat")
	defer c.mu.Unlock()

	g, m, code := c.member(key, id, gen)
	if m != nil && (code == kerrNone || code == kerrRebalanceInProgress) {
		m.last = c.now()
	}

	_ = g

	return code
}

func (c *coordinator) leave(key string, ids []string) []int16 {
	c.mu.Lock("leave")
	defer c.mu.Unlock()

	codes := make([]int16, len(ids))
	g, ok := c.groups[key]

	for i, id := range ids {
		if !ok {
			codes[i] = kerrUnknownMember

			continue
		}

		if _, found := g.members[id]; !found {
			codes[i] = kerrUnknownMember

			continue
		}

		delete(g.members, id)
		delete(g.joined, id)
	}

	if ok {
		c.afterRemoval(key, g)
	}

	return codes
}

// afterRemoval re-balances the survivors or drops the empty group.
func (c *coordinator) afterRemoval(key string, g *kgroup) {
	switch {
	case len(g.members) == 0:
		delete(c.groups, key)
		g.closeWaiters()
	case g.state == gsPreparing && g.allJoined():
		c.completeJoin(key, g)
	case g.state != gsPreparing:
		g.startRound()
	}
}

func (g *kgroup) closeWaiters() {
	select {
	case <-g.joinDone:
	default:
		close(g.joinDone)
	}
}

// commitAllowed validates the membership of a group commit; generation -1 is a standalone consumer.
func (c *coordinator) commitAllowed(key, id string, gen int32) int16 {
	if gen < 0 {
		return kerrNone
	}

	c.mu.RLock("commit")
	defer c.mu.RUnlock()

	_, _, code := c.member(key, id, gen)

	return code
}

// sweep evicts members whose session timed out on the backend clock.
func (c *coordinator) sweep() {
	c.mu.Lock("sweep")
	defer c.mu.Unlock()

	now := c.now()

	for key, g := range c.groups {
		dropped := false

		for id, m := range g.members {
			if now.Sub(m.last) > m.session {
				delete(g.members, id)
				delete(g.joined, id)

				dropped = true
			}
		}

		if dropped {
			c.afterRemoval(key, g)
		}
	}
}
