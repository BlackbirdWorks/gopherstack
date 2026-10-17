package redisengine

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/alicebob/miniredis/v2"
)

const (
	keyspaceDBs = 16
	// maxRestoreBytes caps the decompressed size of a snapshot blob.
	maxRestoreBytes = 256 << 20

	typeString = "string"
	typeHash   = "hash"
	typeList   = "list"
	typeSet    = "set"
	typeZSet   = "zset"
	typeStream = "stream"
)

// ErrBadSnapshot is returned when snapshot data cannot be decoded.
var ErrBadSnapshot = errors.New("invalid engine snapshot data")

var (
	errEmptyStream = errors.New("empty stream")
	errUnknownType = errors.New("unknown key type")
)

type zMember struct {
	Member string
	Score  float64
}

type streamEntry struct {
	ID     string
	Values []string
}

type keyDump struct {
	Hash      map[string]string
	HashTTL   map[string]time.Duration
	Key       string
	Type      string
	Str       string
	List      []string
	Set       []string
	ZSet      []zMember
	Stream    []streamEntry
	TTL       time.Duration
	DB        int
	HasExpiry bool
}

type keyspaceDump struct {
	Keys []keyDump
}

// Snapshot serialises the keyspace (strings, hashes, lists, sets, sorted sets,
// streams, key and hash-field TTLs) into a gzip'd opaque blob. Returns nil
// when the engine holds no keys. HyperLogLog keys and stream consumer groups
// are not captured.
func (e *Engine) Snapshot() ([]byte, error) {
	if e == nil {
		return nil, nil
	}

	return DumpKeyspace(e.Mini)
}

// DumpKeyspace is Snapshot for a bare miniredis server. A nil server yields nil.
func DumpKeyspace(mr *miniredis.Miniredis) ([]byte, error) {
	if mr == nil {
		return nil, nil
	}

	var dump keyspaceDump

	for i := range keyspaceDBs {
		db := mr.DB(i)

		for _, k := range db.Keys() {
			kd, ok := dumpKey(db, i, k)
			if ok {
				dump.Keys = append(dump.Keys, kd)
			}
		}
	}

	if len(dump.Keys) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	if err := gob.NewEncoder(zw).Encode(&dump); err != nil {
		return nil, fmt.Errorf("encode engine snapshot: %w", err)
	}

	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("compress engine snapshot: %w", err)
	}

	return buf.Bytes(), nil
}

func dumpKey(db *miniredis.RedisDB, idx int, k string) (keyDump, bool) {
	kd := keyDump{Key: k, DB: idx, Type: db.Type(k)}

	read := readerFor(kd.Type)
	if read == nil || read(db, k, &kd) != nil {
		return kd, false
	}

	if ttl := db.TTL(k); ttl > 0 {
		kd.TTL = ttl
		kd.HasExpiry = true
	}

	return kd, true
}

type keyReader func(db *miniredis.RedisDB, k string, kd *keyDump) error

func readerFor(typ string) keyReader {
	switch typ {
	case typeString:
		return readString
	case typeHash:
		return readHash
	case typeList:
		return readList
	case typeSet:
		return readSet
	case typeZSet:
		return readZSet
	case typeStream:
		return readStream
	default:
		return nil
	}
}

func readString(db *miniredis.RedisDB, k string, kd *keyDump) error {
	v, err := db.Get(k)
	kd.Str = v

	return err
}

func readHash(db *miniredis.RedisDB, k string, kd *keyDump) error {
	fields, err := db.HKeys(k)
	if err != nil {
		return err
	}

	kd.Hash = make(map[string]string, len(fields))

	for _, f := range fields {
		kd.Hash[f] = db.HGet(k, f)

		if ttl := db.HTTL(k, f); ttl > 0 {
			if kd.HashTTL == nil {
				kd.HashTTL = map[string]time.Duration{}
			}

			kd.HashTTL[f] = ttl
		}
	}

	return nil
}

func readList(db *miniredis.RedisDB, k string, kd *keyDump) error {
	l, err := db.List(k)
	kd.List = l

	return err
}

func readSet(db *miniredis.RedisDB, k string, kd *keyDump) error {
	m, err := db.Members(k)
	kd.Set = m

	return err
}

func readZSet(db *miniredis.RedisDB, k string, kd *keyDump) error {
	m, err := db.SortedSet(k)
	if err != nil {
		return err
	}

	for member, score := range m {
		kd.ZSet = append(kd.ZSet, zMember{Member: member, Score: score})
	}

	return nil
}

func readStream(db *miniredis.RedisDB, k string, kd *keyDump) error {
	entries, err := db.Stream(k)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return errEmptyStream
	}

	for _, en := range entries {
		kd.Stream = append(kd.Stream, streamEntry{ID: en.ID, Values: slices.Clone(en.Values)})
	}

	return nil
}

// Restore loads a blob produced by Snapshot into the engine, overwriting
// existing keys of the same name. Empty data is a no-op.
func (e *Engine) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if e == nil {
		return fmt.Errorf("%w: engine not running", ErrBadSnapshot)
	}

	return LoadKeyspace(e.Mini, data)
}

// LoadKeyspace is Restore for a bare miniredis server.
func LoadKeyspace(mr *miniredis.Miniredis, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if mr == nil {
		return fmt.Errorf("%w: engine not running", ErrBadSnapshot)
	}

	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadSnapshot, err)
	}

	defer zr.Close()

	var dump keyspaceDump
	if err = gob.NewDecoder(io.LimitReader(zr, maxRestoreBytes)).Decode(&dump); err != nil {
		return fmt.Errorf("%w: %w", ErrBadSnapshot, err)
	}

	for i := range dump.Keys {
		kd := &dump.Keys[i]
		if kd.DB < 0 || kd.DB >= keyspaceDBs {
			return fmt.Errorf("%w: database index %d", ErrBadSnapshot, kd.DB)
		}

		if err = loadKey(mr.DB(kd.DB), kd); err != nil {
			return fmt.Errorf("%w: key %q: %w", ErrBadSnapshot, kd.Key, err)
		}
	}

	return nil
}

func loadKey(db *miniredis.RedisDB, kd *keyDump) error {
	db.Del(kd.Key)

	if err := writeKey(db, kd); err != nil {
		return err
	}

	if kd.HasExpiry {
		db.SetTTL(kd.Key, kd.TTL)
	}

	return nil
}

func writeKey(db *miniredis.RedisDB, kd *keyDump) error {
	var err error

	switch kd.Type {
	case typeString:
		err = db.Set(kd.Key, kd.Str)
	case typeHash:
		for f, v := range kd.Hash {
			db.HSet(kd.Key, f, v)
		}

		for f, ttl := range kd.HashTTL {
			db.HExpire(kd.Key, f, ttl)
		}
	case typeList:
		_, err = db.Push(kd.Key, kd.List...)
	case typeSet:
		_, err = db.SetAdd(kd.Key, kd.Set...)
	case typeZSet:
		err = writeZSet(db, kd)
	case typeStream:
		err = writeStream(db, kd)
	default:
		err = fmt.Errorf("%w: %q", errUnknownType, kd.Type)
	}

	return err
}

func writeZSet(db *miniredis.RedisDB, kd *keyDump) error {
	for _, m := range kd.ZSet {
		if _, err := db.ZAdd(kd.Key, m.Score, m.Member); err != nil {
			return err
		}
	}

	return nil
}

func writeStream(db *miniredis.RedisDB, kd *keyDump) error {
	for _, en := range kd.Stream {
		if _, err := db.XAdd(kd.Key, en.ID, en.Values); err != nil {
			return err
		}
	}

	return nil
}
