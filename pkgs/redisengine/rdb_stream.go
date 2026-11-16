package redisengine

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	rdbTypeStreamV1 = 15
	rdbTypeStreamV2 = 19
	rdbTypeStreamV3 = 21

	streamIDLen          = 16
	streamFlagDeleted    = 1
	streamFlagSameFields = 2
	streamEntryFixed     = 3
	idParts              = 2
	streamNoTime         = -1
)

type streamGroupDump struct {
	Name      string
	LastID    string
	Consumers []streamConsumerDump
	Pending   []streamPendingDump
}

type streamConsumerDump struct {
	Name     string
	SeenMS   int64
	ActiveMS int64
}

type streamPendingDump struct {
	ID         string
	Consumer   string
	DeliveryMS int64
	Count      int
}

type streamID struct{ ms, seq uint64 }

func (s streamID) String() string {
	return strconv.FormatUint(s.ms, 10) + "-" + strconv.FormatUint(s.seq, 10)
}

func (s streamID) raw() []byte {
	var b [streamIDLen]byte

	binary.BigEndian.PutUint64(b[:size64], s.ms)
	binary.BigEndian.PutUint64(b[size64:], s.seq)

	return b[:]
}

func rawStreamID(b []byte) streamID {
	return streamID{ms: binary.BigEndian.Uint64(b[:size64]), seq: binary.BigEndian.Uint64(b[size64:])}
}

func parseStreamID(s string) (streamID, bool) {
	ms, seq, ok := strings.Cut(s, "-")
	if !ok {
		return streamID{}, false
	}

	m, err := strconv.ParseUint(ms, 10, 64)
	if err != nil {
		return streamID{}, false
	}

	q, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return streamID{}, false
	}

	return streamID{ms: m, seq: q}, true
}

func (r *rdbReader) plainLength() (uint64, error) {
	n, enc, err := r.length()
	if err != nil || enc >= 0 {
		return 0, r.fail("bad length")
	}

	return n, nil
}

func (r *rdbReader) streamIDPair() (streamID, error) {
	ms, err := r.plainLength()
	if err != nil {
		return streamID{}, err
	}

	seq, err := r.plainLength()

	return streamID{ms: ms, seq: seq}, err
}

func (r *rdbReader) millis() (int64, error) {
	b, err := r.take(size64)
	if err != nil {
		return 0, err
	}

	return int64(binary.LittleEndian.Uint64(b)), nil //nolint:gosec // reinterpreting the stored signed value
}

func (r *rdbReader) rawStreamID() (streamID, error) {
	b, err := r.take(streamIDLen)
	if err != nil {
		return streamID{}, err
	}

	return rawStreamID(b), nil
}

// stream reads RDB_TYPE_STREAM_LISTPACKS, _2 and _3 (rdb.c rdbLoadObject).
func (r *rdbReader) stream(kd *keyDump, typ byte) error {
	kd.Type = typeStream

	nodes, err := r.count()
	if err != nil {
		return err
	}

	for range nodes {
		if err = r.streamNode(kd); err != nil {
			return err
		}
	}

	if _, err = r.plainLength(); err != nil {
		return err
	}

	last, err := r.streamIDPair()
	if err != nil {
		return err
	}

	kd.StreamLastID = last.String()

	if typ >= rdbTypeStreamV2 {
		if err = r.skipStreamV2Meta(); err != nil {
			return err
		}
	}

	return r.streamGroups(kd, typ)
}

func (r *rdbReader) skipStreamV2Meta() error {
	for range 2 {
		if _, err := r.streamIDPair(); err != nil {
			return err
		}
	}

	_, err := r.plainLength()

	return err
}

func (r *rdbReader) streamNode(kd *keyDump) error {
	idStr, err := r.str()
	if err != nil || len(idStr) != streamIDLen {
		return r.fail("bad stream node key")
	}

	blob, err := r.str()
	if err != nil {
		return err
	}

	items, err := decodeListpack([]byte(blob))
	if err != nil {
		return err
	}

	entries, err := parseStreamNode(rawStreamID([]byte(idStr)), items)
	if err != nil {
		return err
	}

	kd.Stream = append(kd.Stream, entries...)

	return nil
}

type lpCursor struct {
	items []string
	pos   int
}

func (c *lpCursor) next() (string, error) {
	if c.pos >= len(c.items) {
		return "", badPacked("truncated stream listpack")
	}

	s := c.items[c.pos]
	c.pos++

	return s, nil
}

func (c *lpCursor) int() (int64, error) {
	s, err := c.next()
	if err != nil {
		return 0, err
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, badPacked("stream listpack integer expected")
	}

	return n, nil
}

func (c *lpCursor) strs(n int) ([]string, error) {
	if n < 0 || n > len(c.items)-c.pos {
		return nil, badPacked("truncated stream listpack")
	}

	out := c.items[c.pos : c.pos+n]
	c.pos += n

	return out, nil
}

func parseStreamNode(master streamID, items []string) ([]streamEntry, error) {
	c := &lpCursor{items: items}

	count, err := c.int()
	if err != nil {
		return nil, err
	}

	deleted, err := c.int()
	if err != nil {
		return nil, err
	}

	nf, err := c.int()
	if err != nil {
		return nil, err
	}

	fields, err := c.strs(int(min(nf, math.MaxInt32)))
	if err != nil {
		return nil, err
	}

	if _, err = c.next(); err != nil {
		return nil, err
	}

	var out []streamEntry

	for range min(count+deleted, int64(len(items))) {
		en, deletedEntry, eErr := parseStreamEntry(c, master, fields)
		if eErr != nil {
			return nil, eErr
		}

		if !deletedEntry {
			out = append(out, en)
		}
	}

	return out, nil
}

func parseStreamEntry(c *lpCursor, master streamID, fields []string) (streamEntry, bool, error) {
	flags, err := c.int()
	if err != nil {
		return streamEntry{}, false, err
	}

	msd, err := c.int()
	if err != nil {
		return streamEntry{}, false, err
	}

	seqd, err := c.int()
	if err != nil {
		return streamEntry{}, false, err
	}

	var values []string

	if flags&streamFlagSameFields != 0 {
		values, err = sameFieldValues(c, fields)
	} else {
		values, err = freeFieldValues(c)
	}

	if err != nil {
		return streamEntry{}, false, err
	}

	if _, err = c.int(); err != nil {
		return streamEntry{}, false, err
	}

	id := streamID{ms: addDiff(master.ms, msd), seq: addDiff(master.seq, seqd)}

	return streamEntry{ID: id.String(), Values: values}, flags&streamFlagDeleted != 0, nil
}

func sameFieldValues(c *lpCursor, fields []string) ([]string, error) {
	vals, err := c.strs(len(fields))
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(fields)*idParts)
	for i, f := range fields {
		out = append(out, f, vals[i])
	}

	return out, nil
}

func freeFieldValues(c *lpCursor) ([]string, error) {
	n, err := c.int()
	if err != nil {
		return nil, err
	}

	flat, err := c.strs(int(min(n, math.MaxInt32)) * idParts)
	if err != nil {
		return nil, err
	}

	return append([]string(nil), flat...), nil
}

func (r *rdbReader) streamGroups(kd *keyDump, typ byte) error {
	n, err := r.count()
	if err != nil {
		return err
	}

	for range n {
		g, gErr := r.streamGroup(typ)
		if gErr != nil {
			return gErr
		}

		kd.Groups = append(kd.Groups, g)
	}

	return nil
}

func (r *rdbReader) streamGroup(typ byte) (streamGroupDump, error) {
	var g streamGroupDump

	name, err := r.str()
	if err != nil {
		return g, err
	}

	last, err := r.streamIDPair()
	if err != nil {
		return g, err
	}

	g.Name, g.LastID = name, last.String()

	if typ >= rdbTypeStreamV2 {
		if _, err = r.plainLength(); err != nil {
			return g, err
		}
	}

	pel, err := r.groupPEL()
	if err != nil {
		return g, err
	}

	consumers, owners, err := r.groupConsumers(typ)
	if err != nil {
		return g, err
	}

	for i := range pel {
		pel[i].Consumer = owners[pel[i].ID]
	}

	g.Pending, g.Consumers = pel, consumers

	return g, nil
}

func (r *rdbReader) groupPEL() ([]streamPendingDump, error) {
	n, err := r.count()
	if err != nil {
		return nil, err
	}

	out := make([]streamPendingDump, 0, n)

	for range n {
		id, idErr := r.rawStreamID()
		if idErr != nil {
			return nil, idErr
		}

		ms, msErr := r.millis()
		if msErr != nil {
			return nil, msErr
		}

		cnt, cErr := r.plainLength()
		if cErr != nil {
			return nil, cErr
		}

		out = append(out, streamPendingDump{ID: id.String(), DeliveryMS: ms, Count: int(min(cnt, math.MaxInt32))})
	}

	return out, nil
}

func (r *rdbReader) groupConsumers(typ byte) ([]streamConsumerDump, map[string]string, error) {
	n, err := r.count()
	if err != nil {
		return nil, nil, err
	}

	owners := map[string]string{}
	out := make([]streamConsumerDump, 0, n)

	for range n {
		c, ids, cErr := r.streamConsumer(typ)
		if cErr != nil {
			return nil, nil, cErr
		}

		for _, id := range ids {
			owners[id] = c.Name
		}

		out = append(out, c)
	}

	return out, owners, nil
}

func (r *rdbReader) streamConsumer(typ byte) (streamConsumerDump, []string, error) {
	var c streamConsumerDump

	name, err := r.str()
	if err != nil {
		return c, nil, err
	}

	seen, err := r.millis()
	if err != nil {
		return c, nil, err
	}

	active := seen

	if typ >= rdbTypeStreamV3 {
		if active, err = r.millis(); err != nil {
			return c, nil, err
		}
	}

	n, err := r.count()
	if err != nil {
		return c, nil, err
	}

	ids := make([]string, 0, n)

	for range n {
		id, idErr := r.rawStreamID()
		if idErr != nil {
			return c, nil, idErr
		}

		ids = append(ids, id.String())
	}

	return streamConsumerDump{Name: name, SeenMS: seen, ActiveMS: active}, ids, nil
}

func ensureConsistentGroups(kd *keyDump) error {
	for _, g := range kd.Groups {
		names := map[string]struct{}{}
		for _, c := range g.Consumers {
			names[c.Name] = struct{}{}
		}

		for _, p := range g.Pending {
			if _, ok := names[p.Consumer]; !ok {
				return fmt.Errorf("%w: pending entry %s of group %q has no consumer", ErrBadRDB, p.ID, g.Name)
			}
		}
	}

	return nil
}

// addDiff applies a signed listpack diff; the two's complement wrap makes negative diffs subtract.
func addDiff(base uint64, diff int64) uint64 {
	return base + uint64(diff) //nolint:gosec // wrapping add of a signed diff
}
