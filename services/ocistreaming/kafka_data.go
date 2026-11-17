package ocistreaming

import (
	"strconv"
	"time"
)

const (
	kerrNone                int16 = 0
	kerrOffsetOutOfRange    int16 = 1
	kerrCorrupt             int16 = 2
	kerrUnknownTopic        int16 = 3
	kerrLeaderNotAvailable  int16 = 5
	kerrMessageTooLarge     int16 = 10
	kerrCoordinatorNA       int16 = 15
	kerrIllegalGeneration   int16 = 22
	kerrInconsistentProto   int16 = 23
	kerrInvalidGroupID      int16 = 24
	kerrUnknownMember       int16 = 25
	kerrInvalidSession      int16 = 26
	kerrRebalanceInProgress int16 = 27
	kerrTopicAuthFailed     int16 = 29
	kerrUnsupportedSASL     int16 = 33
	kerrIllegalSASLState    int16 = 34
	kerrUnsupportedVersion  int16 = 35
	kerrInvalidRequest      int16 = 42
	kerrInvalidConfig       int16 = 40
	kerrTopicExists         int16 = 36
	kerrInvalidPartitions   int16 = 37
	kerrUnsupportedCompress int16 = 76
	kerrInvalidRecord       int16 = 87
	kerrSASLAuthFailed      int16 = 58
	kerrThrottled           int16 = 89
	kerrGroupMaxSize        int16 = 81
	kerrInvalidCommitOffset int16 = 28

	kafkaMaxGroupName  = 255
	kafkaFetchMaxBytes = 4 << 20
)

type kafkaTopic struct {
	Name  string
	Parts int
	Code  int16
}

type kafkaRead struct {
	Msgs  []Message
	First int64
	Start int64
	End   int64
	Code  int16
}

func (b *InMemoryBackend) signalLocked() {
	close(b.sig)
	b.sig = make(chan struct{})
}

// appendSignal returns a channel closed by the next append to any stream.
func (b *InMemoryBackend) appendSignal() <-chan struct{} {
	b.mu.RLock("appendSignal")
	defer b.mu.RUnlock()

	return b.sig
}

// SetTokenVerifier wires the Kafka SASL password check (an OCI Identity auth token for the named user).
func (b *InMemoryBackend) SetTokenVerifier(fn func(user, token string) bool) {
	b.mu.Lock("SetTokenVerifier")
	b.tokenVerifier = fn
	b.mu.Unlock()
}

func (b *InMemoryBackend) setKafkaPort(port int) {
	b.mu.Lock("setKafkaPort")
	b.kafkaPort = port
	b.mu.Unlock()
}

func (b *InMemoryBackend) kafkaStreamLocked(poolID, topic string) (*Stream, int16) {
	for _, s := range b.streams {
		if s.PoolID != poolID || s.Name != topic || s.State == stateDeleted || s.State == stateDeleting {
			continue
		}

		if s.State != stateActive && s.State != stateUpdating {
			return nil, kerrLeaderNotAvailable
		}

		return s, kerrNone
	}

	return nil, kerrUnknownTopic
}

// kafkaAuth checks the SASL PLAIN credentials and returns the pool ID the connection is bound to.
func (b *InMemoryBackend) kafkaAuth(user, token string, enforce bool) (string, bool) {
	parts := splitUser(user)
	if len(parts) < 3 || token == "" {
		return "", false
	}

	poolID, name := parts[len(parts)-1], parts[len(parts)-2]

	b.mu.RLock("kafkaAuth")
	verify := b.tokenVerifier
	p, ok := b.pools[poolID]
	live := ok && p.State != stateDeleted && p.State != stateDeleting
	b.mu.RUnlock()

	if !live || (verify == nil && enforce) || (verify != nil && !verify(name, token)) {
		return "", false
	}

	return poolID, true
}

func (b *InMemoryBackend) kafkaTopics(poolID string, names []string, all bool) []kafkaTopic {
	b.mu.RLock("kafkaTopics")
	defer b.mu.RUnlock()

	var out []kafkaTopic

	if all {
		for _, s := range b.streams {
			if s.PoolID == poolID && s.State != stateDeleted && s.State != stateDeleting {
				out = append(out, kafkaTopic{Name: s.Name, Parts: len(s.Parts)})
			}
		}

		return out
	}

	for _, n := range names {
		s, code := b.kafkaStreamLocked(poolID, n)
		t := kafkaTopic{Name: n, Code: code}

		if s != nil {
			t.Parts = len(s.Parts)
		}

		out = append(out, t)
	}

	return out
}

// kafkaAutoCreateIfMissing creates the topic when it does not exist and the pool allows it.
func (b *InMemoryBackend) kafkaAutoCreateIfMissing(poolID, name string) {
	b.mu.RLock("kafkaAutoCreateIfMissing")
	_, code := b.kafkaStreamLocked(poolID, name)
	b.mu.RUnlock()

	if code == kerrUnknownTopic {
		b.kafkaAutoCreate(poolID, name)
	}
}

// kafkaAutoCreate creates the topic as a stream when the pool allows it; it reports whether it did.
func (b *InMemoryBackend) kafkaAutoCreate(poolID, name string) bool {
	b.mu.RLock("kafkaAutoCreate")
	p := b.pools[poolID]
	ok := p != nil && p.Kafka.AutoCreateTopicsEnable != nil && *p.Kafka.AutoCreateTopicsEnable
	in := CreateStreamIn{Name: name, PoolID: poolID}

	if ok {
		in.Partitions, in.RetentionInHours = p.Kafka.NumPartitions, p.Kafka.LogRetentionHours
	}

	b.mu.RUnlock()

	if !ok || validateName(name) != nil {
		return false
	}

	_, _, _, err := b.CreateStream(in, "")

	return err == nil
}

func (b *InMemoryBackend) kafkaAppend(poolID, topic string, part int, msgs []putEntryIn) (int64, int16) {
	total := 0

	for _, m := range msgs {
		if len(m.Key) > maxKeyBytes {
			return -1, kerrInvalidRecord
		}

		total += len(m.Key) + len(m.Value)
	}

	if total > maxRequestBytes {
		return -1, kerrMessageTooLarge
	}

	b.mu.Lock("kafkaAppend")
	defer b.mu.Unlock()

	s, code := b.kafkaStreamLocked(poolID, topic)
	if code != kerrNone {
		return -1, code
	}

	if part < 0 || part >= len(s.Parts) {
		return -1, kerrUnknownTopic
	}

	now := b.nowUTC()
	if s.Parts[part].used(now)+total > maxPartitionBytes {
		return -1, kerrThrottled
	}

	base := s.Parts[part].end()
	targets := make([]int, len(msgs))

	for i := range targets {
		targets[i] = part
	}

	appendEntries(s, msgs, targets, now)

	if len(msgs) > 0 {
		b.signalLocked()
	}

	return base, kerrNone
}

func (b *InMemoryBackend) kafkaFetch(poolID, topic string, part int, off int64, maxBytes int) kafkaRead {
	b.mu.Lock("kafkaFetch")
	defer b.mu.Unlock()

	s, code := b.kafkaStreamLocked(poolID, topic)
	if code != kerrNone {
		return kafkaRead{Code: code, Start: -1, End: -1}
	}

	if part < 0 || part >= len(s.Parts) {
		return kafkaRead{Code: kerrUnknownTopic, Start: -1, End: -1}
	}

	p := s.Parts[part]
	res := kafkaRead{Start: p.Base, End: p.end(), First: off}

	if off < p.Base || off > p.end() {
		res.Code = kerrOffsetOutOfRange

		return res
	}

	size := 0

	for i := off; i < p.end(); i++ {
		m := p.Messages[i-p.Base]
		if len(res.Msgs) > 0 && size+len(m.Key)+len(m.Value) > maxBytes {
			break
		}

		size += len(m.Key) + len(m.Value)
		res.Msgs = append(res.Msgs, m)
	}

	if len(res.Msgs) > 0 {
		mw := []messageWire{{Key: make([]byte, 0), Value: make([]byte, size)}}
		if aerr := b.chargeRead(s.ID, part, mw, b.nowUTC()); aerr != nil {
			return kafkaRead{Code: kerrThrottled, Start: res.Start, End: res.End}
		}
	}

	return res
}

// kafkaOffsetAt resolves a ListOffsets timestamp: -1 latest, -2 earliest, otherwise the first message at or after it.
func (b *InMemoryBackend) kafkaOffsetAt(poolID, topic string, part int, ts int64) (int64, int64, int16) {
	b.mu.RLock("kafkaOffsetAt")
	defer b.mu.RUnlock()

	s, code := b.kafkaStreamLocked(poolID, topic)
	if code != kerrNone {
		return -1, -1, code
	}

	if part < 0 || part >= len(s.Parts) {
		return -1, -1, kerrUnknownTopic
	}

	p := s.Parts[part]

	switch ts {
	case -2:
		return p.Base, -1, kerrNone
	case -1:
		return p.end(), -1, kerrNone
	}

	for i, m := range p.Messages {
		if m.Time.UnixMilli() >= ts {
			return p.Base + int64(i), m.Time.UnixMilli(), kerrNone
		}
	}

	return p.end(), -1, kerrNone
}

func (b *InMemoryBackend) kafkaGroupLocked(s *Stream, name string, create bool) (*Group, int16) {
	if name == "" || len(name) > kafkaMaxGroupName {
		return nil, kerrInvalidGroupID
	}

	if g, ok := b.groups[groupKey(s.ID, name)]; ok {
		return g, kerrNone
	}

	if !create {
		return nil, kerrNone
	}

	if b.groupCount(s.ID) >= maxGroupsPerStream {
		return nil, kerrGroupMaxSize
	}

	g := &Group{
		StreamID: s.ID, Name: name, Instances: map[string]time.Time{}, Rotation: map[string]int{},
		Reserved: make([]Reservation, len(s.Parts)),
	}
	if aerr := g.setPositions(s, cursorTrimHorizon, nil); aerr != nil {
		return nil, kerrInvalidRequest
	}

	b.groups[groupKey(s.ID, name)] = g

	return g, kerrNone
}

func (b *InMemoryBackend) kafkaCommit(poolID, topic, group string, part int, off int64) int16 {
	b.mu.Lock("kafkaCommit")
	defer b.mu.Unlock()

	s, code := b.kafkaStreamLocked(poolID, topic)
	if code != kerrNone {
		return code
	}

	if part < 0 || part >= len(s.Parts) {
		return kerrUnknownTopic
	}

	if off < 0 || off > s.Parts[part].end() {
		return kerrInvalidCommitOffset
	}

	g, code := b.kafkaGroupLocked(s, group, true)
	if code != kerrNone {
		return code
	}

	g.Next[part] = off

	return kerrNone
}

// kafkaCommitted returns the committed offset or -1.
func (b *InMemoryBackend) kafkaCommitted(poolID, topic, group string, part int) (int64, int16) {
	b.mu.RLock("kafkaCommitted")
	defer b.mu.RUnlock()

	s, code := b.kafkaStreamLocked(poolID, topic)
	if code != kerrNone {
		return -1, code
	}

	if part < 0 || part >= len(s.Parts) {
		return -1, kerrUnknownTopic
	}

	g, ok := b.groups[groupKey(s.ID, group)]
	if !ok || part >= len(g.Next) {
		return -1, kerrNone
	}

	return g.Next[part], kerrNone
}

func splitUser(u string) []string {
	var out []string

	start := 0

	for i := 0; i <= len(u); i++ {
		if i == len(u) || u[i] == '/' {
			out = append(out, u[start:i])
			start = i + 1
		}
	}

	return out
}

func itoa(n int) string { return strconv.Itoa(n) }

func (b *InMemoryBackend) kafkaCreateTopic(in CreateStreamIn, validateOnly bool) int16 {
	b.mu.RLock("kafkaCreateTopic")
	p := b.pools[in.PoolID]

	if p != nil {
		if in.Partitions == nil {
			in.Partitions = p.Kafka.NumPartitions
		}

		if in.RetentionInHours == nil {
			in.RetentionInHours = p.Kafka.LogRetentionHours
		}
	}

	_, existing := b.kafkaStreamLocked(in.PoolID, in.Name)
	b.mu.RUnlock()

	if existing != kerrUnknownTopic {
		return kerrTopicExists
	}

	if _, _, aerr := validateStreamShape(in); aerr != nil {
		return kerrInvalidPartitions
	}

	if validateOnly {
		return kerrNone
	}

	if _, _, _, err := b.CreateStream(in, ""); err != nil {
		return kerrInvalidRequest
	}

	return kerrNone
}

func (b *InMemoryBackend) kafkaDeleteTopic(poolID, name string) int16 {
	b.mu.RLock("kafkaDeleteTopic")
	s, code := b.kafkaStreamLocked(poolID, name)

	var id string
	if s != nil {
		id = s.ID
	}

	b.mu.RUnlock()

	if code != kerrNone {
		return code
	}

	if _, err := b.DeleteStream(id, ""); err != nil {
		return kerrUnknownTopic
	}

	return kerrNone
}
