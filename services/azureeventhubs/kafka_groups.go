package azureeventhubs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	maxKafkaGroups        = 10000
	maxMembersPerGroup    = 1000
	maxPendingMembers     = 1000
	pendingMemberLifetime = 5 * time.Minute
	minSessionTimeout     = 6 * time.Second
	maxSessionTimeout     = 30 * time.Minute
	memberIDBytes         = 8

	joinProtocolTypeVersion     int16 = 7
	joinMemberIDRequiredVersion int16 = 4
	leaveMembersVersion         int16 = 3
)

type groupState int

const (
	groupEmpty groupState = iota
	groupPreparing
	groupCompleting
	groupStable
)

type groupMember struct {
	lastBeat         time.Time
	protocols        []kmsg.JoinGroupRequestProtocol
	assignment       []byte
	sessionTimeout   time.Duration
	rebalanceTimeout time.Duration
	joined           bool
}

type kafkaGroup struct {
	members      map[string]*groupMember
	pending      map[string]time.Time
	joinDone     chan struct{}
	syncDone     chan struct{}
	protocolType string
	protocol     string
	leader       string
	generation   int32
	state        groupState
}

// groupCoordinator runs the classic consumer group protocol: join, sync, heartbeat and leave, with
// session expiry driven by sweep.
type groupCoordinator struct {
	mu     *lockmetrics.RWMutex
	groups map[string]*kafkaGroup
	now    func() time.Time
	after  func(time.Duration) <-chan time.Time
}

func newGroupCoordinator(now func() time.Time) *groupCoordinator {
	return &groupCoordinator{
		mu: lockmetrics.New("azureeventhubs.kafka.groups"), groups: map[string]*kafkaGroup{}, now: now,
		after: time.After,
	}
}

func groupKey(ns, group string) string { return ns + "\x00" + group }

func newMemberID() string {
	buf := make([]byte, memberIDBytes)
	_, _ = rand.Read(buf)

	return "member-" + hex.EncodeToString(buf)
}

func (gc *groupCoordinator) groupLocked(key string, create bool) *kafkaGroup {
	g := gc.groups[key]
	if g == nil && create && len(gc.groups) < maxKafkaGroups {
		g = &kafkaGroup{members: map[string]*groupMember{}, pending: map[string]time.Time{}}
		gc.groups[key] = g
	}

	return g
}

func sortedMemberIDs(g *kafkaGroup) []string {
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}

func (g *kafkaGroup) startRebalance() {
	g.state = groupPreparing
	g.joinDone = make(chan struct{})

	if g.syncDone != nil {
		close(g.syncDone)
		g.syncDone = nil
	}

	for _, m := range g.members {
		m.joined = false
	}
}

func (g *kafkaGroup) allJoined() bool {
	for _, m := range g.members {
		if !m.joined {
			return false
		}
	}

	return true
}

func (g *kafkaGroup) rebalanceTimeout() time.Duration {
	var d time.Duration

	for _, m := range g.members {
		d = max(d, m.rebalanceTimeout)
	}

	return d
}

// commonProtocol picks the protocol every member supports that the most members rank first.
func (g *kafkaGroup) commonProtocol() (string, bool) {
	var common map[string]bool

	for _, m := range g.members {
		names := map[string]bool{}
		for _, p := range m.protocols {
			names[p.Name] = true
		}

		if common == nil {
			common = names

			continue
		}

		for n := range common {
			if !names[n] {
				delete(common, n)
			}
		}
	}

	votes := map[string]int{}

	for _, m := range g.members {
		for _, p := range m.protocols {
			if common[p.Name] {
				votes[p.Name]++

				break
			}
		}
	}

	best, bestVotes := "", -1

	for _, n := range sortedKeys(votes) {
		if votes[n] > bestVotes {
			best, bestVotes = n, votes[n]
		}
	}

	return best, best != ""
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// completeJoin ends the join phase: members that did not rejoin are dropped and a new generation begins.
func (g *kafkaGroup) completeJoin(now time.Time) {
	for id, m := range g.members {
		if !m.joined {
			delete(g.members, id)
		}
	}

	g.generation++

	if len(g.members) == 0 {
		g.state, g.leader, g.protocol = groupEmpty, "", ""
		close(g.joinDone)

		return
	}

	g.protocol, _ = g.commonProtocol()

	if _, ok := g.members[g.leader]; !ok {
		g.leader = sortedMemberIDs(g)[0]
	}

	for _, m := range g.members {
		m.assignment, m.lastBeat = nil, now
	}

	g.state = groupCompleting
	g.syncDone = make(chan struct{})
	close(g.joinDone)
}

func joinError(code int16) *kmsg.JoinGroupResponse {
	return &kmsg.JoinGroupResponse{ErrorCode: code, Generation: -1}
}

func (gc *groupCoordinator) join(ctx context.Context, key string, req *kmsg.JoinGroupRequest) *kmsg.JoinGroupResponse {
	ch, timeout, memberID, early := gc.admit(key, req)
	if early != nil {
		return early
	}

	select {
	case <-ch:
	case <-gc.after(timeout):
		gc.expireJoin(key, ch)
		<-ch
	case <-ctx.Done():
		return joinError(kerrCoordinatorUnavailable)
	}

	gc.mu.Lock("join")
	defer gc.mu.Unlock()

	g := gc.groupLocked(key, false)
	if g == nil || g.members[memberID] == nil {
		return joinError(kerrUnknownMember)
	}

	return gc.joinResponse(g, memberID, req.Version)
}

func (gc *groupCoordinator) joinResponse(g *kafkaGroup, memberID string, version int16) *kmsg.JoinGroupResponse {
	proto, ptype := g.protocol, g.protocolType
	resp := &kmsg.JoinGroupResponse{Generation: g.generation, Protocol: &proto, LeaderID: g.leader, MemberID: memberID}

	if version >= joinProtocolTypeVersion {
		resp.ProtocolType = &ptype
	}

	if memberID != g.leader {
		return resp
	}

	for _, id := range sortedMemberIDs(g) {
		meta := []byte(nil)

		for _, p := range g.members[id].protocols {
			if p.Name == g.protocol {
				meta = p.Metadata
			}
		}

		resp.Members = append(resp.Members, kmsg.JoinGroupResponseMember{MemberID: id, ProtocolMetadata: meta})
	}

	return resp
}

// admit registers the join and returns the channel that closes when the join phase ends.
func (gc *groupCoordinator) admit(
	key string, req *kmsg.JoinGroupRequest,
) (chan struct{}, time.Duration, string, *kmsg.JoinGroupResponse) {
	gc.mu.Lock("admit")
	defer gc.mu.Unlock()

	session := time.Duration(req.SessionTimeoutMillis) * time.Millisecond
	if session < minSessionTimeout || session > maxSessionTimeout {
		return nil, 0, "", joinError(kerrInvalidSessionTimeout)
	}

	if req.ProtocolType == "" || len(req.Protocols) == 0 {
		return nil, 0, "", joinError(kerrInconsistentProtocol)
	}

	g := gc.groupLocked(key, true)
	if g == nil {
		return nil, 0, "", joinError(kerrCoordinatorUnavailable)
	}

	memberID, early := gc.resolveMember(g, req)
	if early != nil {
		return nil, 0, "", early
	}

	if len(g.members) > 0 && g.protocolType != req.ProtocolType {
		return nil, 0, "", joinError(kerrInconsistentProtocol)
	}

	rebalance := time.Duration(req.RebalanceTimeoutMillis) * time.Millisecond
	if req.Version == 0 || rebalance <= 0 {
		rebalance = session
	}

	now := gc.now()
	prev := g.members[memberID]
	g.members[memberID] = &groupMember{
		protocols: req.Protocols, sessionTimeout: session, rebalanceTimeout: rebalance, lastBeat: now, joined: true,
	}

	if _, ok := g.commonProtocol(); !ok {
		if prev != nil {
			g.members[memberID] = prev
		} else {
			delete(g.members, memberID)
		}

		return nil, 0, "", joinError(kerrInconsistentProtocol)
	}

	g.protocolType = req.ProtocolType

	if g.state != groupPreparing {
		g.startRebalance()
		g.members[memberID].joined = true
	}

	ch := g.joinDone
	if g.allJoined() {
		g.completeJoin(now)
	}

	return ch, g.rebalanceTimeoutOr(rebalance), memberID, nil
}

func (g *kafkaGroup) rebalanceTimeoutOr(d time.Duration) time.Duration {
	return max(g.rebalanceTimeout(), d)
}

func (gc *groupCoordinator) resolveMember(g *kafkaGroup, req *kmsg.JoinGroupRequest) (string, *kmsg.JoinGroupResponse) {
	if req.MemberID != "" {
		if _, ok := g.members[req.MemberID]; ok {
			return req.MemberID, nil
		}

		if _, ok := g.pending[req.MemberID]; ok {
			delete(g.pending, req.MemberID)

			return req.MemberID, gc.capMembers(g)
		}

		return "", joinError(kerrUnknownMember)
	}

	id := newMemberID()

	if req.Version >= joinMemberIDRequiredVersion {
		if len(g.pending) >= maxPendingMembers {
			return "", joinError(kerrCoordinatorUnavailable)
		}

		g.pending[id] = gc.now()
		resp := joinError(kerrMemberIDRequired)
		resp.MemberID = id

		return "", resp
	}

	return id, gc.capMembers(g)
}

func (gc *groupCoordinator) capMembers(g *kafkaGroup) *kmsg.JoinGroupResponse {
	if len(g.members) >= maxMembersPerGroup {
		return joinError(kerrCoordinatorUnavailable)
	}

	return nil
}

// expireJoin ends a join phase whose timeout passed: members that never rejoined are removed.
func (gc *groupCoordinator) expireJoin(key string, ch chan struct{}) {
	gc.mu.Lock("expireJoin")
	defer gc.mu.Unlock()

	if g := gc.groupLocked(key, false); g != nil && g.joinDone == ch && g.state == groupPreparing {
		g.completeJoin(gc.now())
	}
}

func (gc *groupCoordinator) sync(ctx context.Context, key string, req *kmsg.SyncGroupRequest) *kmsg.SyncGroupResponse {
	gc.mu.Lock("sync")

	g := gc.groupLocked(key, false)
	if code := gc.checkMember(g, req.MemberID, req.Generation); code != kerrNone {
		gc.mu.Unlock()

		return &kmsg.SyncGroupResponse{ErrorCode: code}
	}

	if g.state == groupCompleting && req.MemberID == g.leader {
		for _, a := range req.GroupAssignment {
			if m, ok := g.members[a.MemberID]; ok {
				m.assignment = a.MemberAssignment
			}
		}

		g.state = groupStable
		close(g.syncDone)
		g.syncDone = nil
	}

	wait := g.syncDone
	gc.mu.Unlock()

	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return &kmsg.SyncGroupResponse{ErrorCode: kerrCoordinatorUnavailable}
		}
	}

	gc.mu.Lock("sync")
	defer gc.mu.Unlock()

	g = gc.groupLocked(key, false)
	if code := gc.checkMember(g, req.MemberID, req.Generation); code != kerrNone {
		return &kmsg.SyncGroupResponse{ErrorCode: code}
	}

	if g.state != groupStable {
		return &kmsg.SyncGroupResponse{ErrorCode: kerrRebalanceInProgress}
	}

	m := g.members[req.MemberID]
	m.lastBeat = gc.now()

	proto, ptype := g.protocol, g.protocolType

	return &kmsg.SyncGroupResponse{MemberAssignment: m.assignment, Protocol: &proto, ProtocolType: &ptype}
}

// checkMember validates a member and generation for sync, heartbeat and commits.
func (gc *groupCoordinator) checkMember(g *kafkaGroup, member string, generation int32) int16 {
	switch {
	case g == nil || g.members[member] == nil:
		return kerrUnknownMember
	case generation != g.generation:
		return kerrIllegalGeneration
	case g.state == groupPreparing:
		return kerrRebalanceInProgress
	}

	return kerrNone
}

func (gc *groupCoordinator) heartbeat(key, member string, generation int32) int16 {
	gc.mu.Lock("heartbeat")
	defer gc.mu.Unlock()

	g := gc.groupLocked(key, false)
	if g == nil || g.members[member] == nil {
		return kerrUnknownMember
	}

	if generation != g.generation {
		return kerrIllegalGeneration
	}

	g.members[member].lastBeat = gc.now()

	if g.state == groupPreparing {
		return kerrRebalanceInProgress
	}

	return kerrNone
}

func (gc *groupCoordinator) leave(key string, members []string) []int16 {
	gc.mu.Lock("leave")
	defer gc.mu.Unlock()

	g := gc.groupLocked(key, false)
	codes := make([]int16, len(members))

	for i, id := range members {
		if g == nil || g.members[id] == nil {
			codes[i] = kerrUnknownMember

			continue
		}

		delete(g.members, id)
	}

	if g != nil {
		gc.afterRemoval(g)
	}

	return codes
}

// afterRemoval re-forms the group after members left or expired.
func (gc *groupCoordinator) afterRemoval(g *kafkaGroup) {
	switch {
	case len(g.members) == 0 && g.state == groupPreparing:
		g.completeJoin(gc.now())
	case len(g.members) == 0:
		g.state, g.leader, g.protocol = groupEmpty, "", ""
		g.generation++

		if g.syncDone != nil {
			close(g.syncDone)
			g.syncDone = nil
		}
	case g.state == groupPreparing:
		if g.allJoined() {
			g.completeJoin(gc.now())
		}
	default:
		g.startRebalance()
	}
}

// commitAllowed validates an OffsetCommit against group membership.
func (gc *groupCoordinator) commitAllowed(key, member string, generation int32) int16 {
	gc.mu.Lock("commitAllowed")
	defer gc.mu.Unlock()

	g := gc.groupLocked(key, false)

	if generation < 0 && member == "" {
		if g != nil && len(g.members) > 0 {
			return kerrUnknownMember
		}

		return kerrNone
	}

	if g == nil || g.members[member] == nil {
		return kerrUnknownMember
	}

	if generation != g.generation {
		return kerrIllegalGeneration
	}

	if g.state == groupPreparing {
		return kerrRebalanceInProgress
	}

	return kerrNone
}

// sweep removes members whose session expired and forgets stale pending member IDs and empty groups.
func (gc *groupCoordinator) sweep() {
	gc.mu.Lock("sweep")
	defer gc.mu.Unlock()

	now := gc.now()

	for key, g := range gc.groups {
		expired := false

		for id, m := range g.members {
			if g.state != groupPreparing && now.Sub(m.lastBeat) > m.sessionTimeout {
				delete(g.members, id)

				expired = true
			}
		}

		for id, at := range g.pending {
			if now.Sub(at) > pendingMemberLifetime {
				delete(g.pending, id)
			}
		}

		if expired {
			gc.afterRemoval(g)
		}

		if len(g.members) == 0 && len(g.pending) == 0 && g.state == groupEmpty {
			delete(gc.groups, key)
		}
	}
}

func (gc *groupCoordinator) memberIDs(key string) []string {
	gc.mu.RLock("memberIDs")
	defer gc.mu.RUnlock()

	g := gc.groupLocked(key, false)
	if g == nil {
		return nil
	}

	return slices.Clone(sortedMemberIDs(g))
}
