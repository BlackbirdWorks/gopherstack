package redisengine

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

const (
	rdbTypeHashMetaPreGA   = 22
	rdbTypeHashLPExPreGA   = 23
	rdbTypeHashMeta        = 24
	rdbTypeHashLPEx        = 25
	rdbVersionStreams      = 11
	rdbVersionHashFieldTTL = 12
	hashTriple             = 3
)

// extendedValue reads the value types added after listpack: streams and hashes with field expiration.
func (r *rdbReader) extendedValue(typ byte, kd *keyDump) error {
	switch typ {
	case rdbTypeStreamV1, rdbTypeStreamV2, rdbTypeStreamV3:
		if err := r.stream(kd, typ); err != nil {
			return err
		}

		return ensureConsistentGroups(kd)
	case rdbTypeHashMetaPreGA, rdbTypeHashMeta:
		kd.Type = typeHash

		return r.hashMetadata(kd, typ == rdbTypeHashMeta)
	case rdbTypeHashLPExPreGA, rdbTypeHashLPEx:
		kd.Type = typeHash

		return r.hashListpackEx(kd, typ == rdbTypeHashLPEx)
	default:
		return fmt.Errorf("%w: encoded value type %d for key %q", ErrUnsupportedRDB, typ, kd.Key)
	}
}

// hashMetadata reads RDB_TYPE_HASH_METADATA: [min expire] count then per field its TTL (0 none, else relative to
// min expire plus one; absolute for the pre-GA type), field and value (rdb.c rdbLoadObject).
func (r *rdbReader) hashMetadata(kd *keyDump, relative bool) error {
	var minExpire int64

	if relative {
		m, err := r.millis()
		if err != nil {
			return err
		}

		minExpire = m
	}

	n, err := r.count()
	if err != nil {
		return err
	}

	kd.Hash = make(map[string]string, n)
	now := time.Now()

	for range n {
		ttl, tErr := r.plainLength()
		if tErr != nil {
			return tErr
		}

		f, fErr := r.str()
		if fErr != nil {
			return fErr
		}

		v, vErr := r.str()
		if vErr != nil {
			return vErr
		}

		at := int64(ttl) //nolint:gosec // an out-of-range value only yields an already-expired field
		if relative && ttl != 0 {
			at = at + minExpire - 1
		}

		setHashField(kd, f, v, at, now)
	}

	return nil
}

// hashListpackEx reads RDB_TYPE_HASH_LISTPACK_EX: [min expire] then a listpack of field, value, absolute expiry
// milliseconds (0 none) triplets.
func (r *rdbReader) hashListpackEx(kd *keyDump, hasMin bool) error {
	if hasMin {
		if _, err := r.millis(); err != nil {
			return err
		}
	}

	items, err := r.packedBlob(decodeListpack)
	if err != nil {
		return err
	}

	if len(items)%hashTriple != 0 {
		return r.fail("hash listpack holds a partial field triplet")
	}

	kd.Hash = make(map[string]string, len(items)/hashTriple)
	now := time.Now()

	for i := 0; i < len(items); i += hashTriple {
		at, convErr := strconv.ParseInt(items[i+2], 10, 64)
		if convErr != nil || at < 0 {
			return r.fail("bad hash field expiry")
		}

		setHashField(kd, items[i], items[i+1], at, now)
	}

	return nil
}

// setHashField stores a field; a field whose expiry (unix ms, 0 for none) has passed is dropped as Redis does on load.
func setHashField(kd *keyDump, field, value string, atMS int64, now time.Time) {
	if atMS == 0 {
		kd.Hash[field] = value

		return
	}

	left := time.UnixMilli(atMS).Sub(now)
	if left <= 0 {
		return
	}

	kd.Hash[field] = value

	if kd.HashTTL == nil {
		kd.HashTTL = map[string]time.Duration{}
	}

	kd.HashTTL[field] = left
}

// putHash writes a hash; one with field TTLs uses RDB_TYPE_HASH_METADATA, which needs RDB version 12.
func (en *rdbEncoder) putHash(w *bytes.Buffer, kd *keyDump) {
	if len(kd.HashTTL) == 0 {
		w.WriteByte(rdbTypeHash)
		putString(w, kd.Key)
		putLen(w, uint64(len(kd.Hash)))

		for f, v := range kd.Hash {
			putString(w, f)
			putString(w, v)
		}

		return
	}

	en.need(rdbVersionHashFieldTTL)

	abs := make(map[string]int64, len(kd.HashTTL))
	minExpire := int64(0)

	for f, ttl := range kd.HashTTL {
		at := en.now.Add(ttl).UnixMilli()
		abs[f] = at

		if minExpire == 0 || at < minExpire {
			minExpire = at
		}
	}

	w.WriteByte(rdbTypeHashMeta)
	putString(w, kd.Key)
	putMillis(w, minExpire)
	putLen(w, uint64(len(kd.Hash)))

	for f, v := range kd.Hash {
		if at, ok := abs[f]; ok {
			putLen(w, uint64(at-minExpire+1)) //nolint:gosec // at >= minExpire
		} else {
			putLen(w, 0)
		}

		putString(w, f)
		putString(w, v)
	}
}
