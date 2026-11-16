package azureeventhubs

import (
	"crypto/hmac"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

const maxKafkaOffsetGroupsPerHub = 1000

// logRecord is a copy of a stored event handed to the Kafka plane.
type logRecord struct {
	Enqueued time.Time
	Key      string
	Raw      []byte
	Seq      int64
}

// logSlice is a read of one partition.
type logSlice struct {
	notify  <-chan struct{}
	records []logRecord
	begin   int64
	end     int64
}

// readLog returns up to maxBytes of events from sequence from on, plus the log bounds. The first event is
// always returned when one exists, so an oversized event cannot stall a consumer.
func (b *InMemoryBackend) readLog(ref HubRef, pid int, from int64, maxBytes int) (logSlice, error) {
	b.mu.Lock("readLog")
	defer b.mu.Unlock()

	h, p, err := b.partLocked(ref, pid)
	if err != nil {
		return logSlice{}, err
	}

	b.trimLocked(h, b.now())

	out := logSlice{begin: max(p.begin(), 0), end: p.NextSeq, notify: p.notify}
	if len(p.Events) == 0 || from >= p.NextSeq || from < out.begin {
		return out, nil
	}

	size := 0

	for _, ev := range p.Events[from-p.Events[0].Seq:] {
		if len(out.records) > 0 && size+len(ev.Raw) > maxBytes {
			break
		}

		size += len(ev.Raw)
		out.records = append(out.records, logRecord{Seq: ev.Seq, Enqueued: ev.Enqueued, Key: ev.Key, Raw: ev.Raw})
	}

	return out, nil
}

// offsetAt resolves a ListOffsets timestamp: -2 earliest, -1 latest, otherwise the first event enqueued at or
// after ts (milliseconds), or -1 when none is.
func (b *InMemoryBackend) offsetAt(ref HubRef, pid int, ts int64) (int64, time.Time, error) {
	b.mu.Lock("offsetAt")
	defer b.mu.Unlock()

	h, p, err := b.partLocked(ref, pid)
	if err != nil {
		return 0, time.Time{}, err
	}

	b.trimLocked(h, b.now())

	switch ts {
	case -2:
		return max(p.begin(), 0), time.Time{}, nil
	case -1:
		return p.NextSeq, time.Time{}, nil
	}

	for _, ev := range p.Events {
		if ev.Enqueued.UnixMilli() >= ts {
			return ev.Seq, ev.Enqueued, nil
		}
	}

	return -1, time.Time{}, nil
}

// CommitKafkaOffset stores a consumer group's committed offset for one partition.
func (b *InMemoryBackend) CommitKafkaOffset(ref HubRef, group string, pid int, offset int64) error {
	b.mu.Lock("CommitKafkaOffset")
	defer b.mu.Unlock()

	_, h, err := b.hubLocked(ref)
	if err != nil {
		return err
	}

	if pid < 0 || pid >= len(h.Partitions) {
		return ErrPartitionNotFound
	}

	if h.KafkaOffsets == nil {
		h.KafkaOffsets = map[string]map[string]int64{}
	}

	if _, ok := h.KafkaOffsets[group]; !ok && len(h.KafkaOffsets) >= maxKafkaOffsetGroupsPerHub {
		return ErrTooManyKafkaGroups
	}

	if h.KafkaOffsets[group] == nil {
		h.KafkaOffsets[group] = map[string]int64{}
	}

	h.KafkaOffsets[group][strconv.Itoa(pid)] = offset

	return nil
}

// KafkaOffset returns a group's committed offset for a partition, or -1 when none was committed.
func (b *InMemoryBackend) KafkaOffset(ref HubRef, group string, pid int) (int64, error) {
	b.mu.RLock("KafkaOffset")
	defer b.mu.RUnlock()

	_, h, err := b.hubLocked(ref)
	if err != nil {
		return 0, err
	}

	if pid < 0 || pid >= len(h.Partitions) {
		return 0, ErrPartitionNotFound
	}

	if off, ok := h.KafkaOffsets[group][strconv.Itoa(pid)]; ok {
		return off, nil
	}

	return -1, nil
}

// KafkaCommitted lists the partitions of a hub with a committed offset for the group.
func (b *InMemoryBackend) KafkaCommitted(ref HubRef, group string) (map[int]int64, error) {
	b.mu.RLock("KafkaCommitted")
	defer b.mu.RUnlock()

	_, h, err := b.hubLocked(ref)
	if err != nil {
		return nil, err
	}

	out := map[int]int64{}

	for k, v := range h.KafkaOffsets[group] {
		if pid, convErr := strconv.Atoi(k); convErr == nil {
			out[pid] = v
		}
	}

	return out, nil
}

// HubNames lists the hubs a Kafka client can see: those of ns, or of every namespace (first match wins) when
// ns is empty.
func (b *InMemoryBackend) HubNames(ns string) []string {
	b.mu.RLock("HubNames")
	defer b.mu.RUnlock()

	seen := map[string]bool{}

	add := func(n *namespace) {
		for name := range n.Hubs {
			seen[name] = true
		}
	}

	if n, ok := b.namespaces[ns]; ok {
		add(n)
	} else if ns == "" {
		for _, n := range b.namespaces {
			add(n)
		}
	}

	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}

	sort.Strings(names)

	return names
}

// RuleRights validates a shared-access key pair and returns the rights and hub scope of the rule it belongs
// to. The scope is empty for a namespace-level rule.
func (b *InMemoryBackend) RuleRights(name, key string) ([]string, string, bool) {
	b.mu.RLock("RuleRights")
	defer b.mu.RUnlock()

	for _, ns := range b.namespaces {
		for _, r := range ns.Rules {
			if r.Name == name && r.hasKey(key) {
				return slices.Clone(r.Rights), r.Hub, true
			}
		}
	}

	if name == DefaultKeyName && constantEqual(DefaultKeyValue, key) {
		return []string{RightListen, RightSend, RightManage}, "", true
	}

	return nil, "", false
}

func (r *rule) hasKey(key string) bool {
	return constantEqual(r.PrimaryKey, key) || constantEqual(r.SecondaryKey, key)
}

// ParseConnectionString extracts the endpoint namespace, key name and key from an Event Hubs connection string.
func ParseConnectionString(cs string) (string, string, string) {
	var ns, keyName, key string

	for part := range strings.SplitSeq(cs, ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}

		switch strings.ToLower(k) {
		case "endpoint":
			ns = namespaceOfEndpoint(v)
		case "sharedaccesskeyname":
			keyName = v
		case "sharedaccesskey":
			key = v
		}
	}

	return ns, keyName, key
}

func namespaceOfEndpoint(v string) string {
	v = strings.TrimPrefix(strings.TrimPrefix(v, "sb://"), "https://")
	host, _, _ := strings.Cut(v, "/")
	host, _, _ = strings.Cut(host, ":")
	first, _, _ := strings.Cut(host, ".")

	return first
}

func constantEqual(a, b string) bool { return hmac.Equal([]byte(a), []byte(b)) }

// appendAt stores events in an explicit partition and returns the sequence number of the first one.
func (b *InMemoryBackend) appendAt(ref HubRef, pid int, evs []NewEvent) (int64, error) {
	_, first, err := b.appendEvents(ref, pid, evs)

	return first, err
}
