package redisengine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/alicebob/miniredis/v2"
)

const (
	rdbMagic    = "REDIS"
	rdbVersion  = 9
	rdbMaxVer   = 12
	rdbVerLen   = 4
	rdbEOF      = 0xFF
	rdbSelectDB = 0xFE
	rdbResizeDB = 0xFB
	rdbExpireMS = 0xFC
	rdbExpireS  = 0xFD
	rdbAux      = 0xFA
	rdbFreq     = 0xF9
	rdbIdle     = 0xF8

	rdbTypeString = 0
	rdbTypeList   = 1
	rdbTypeSet    = 2
	rdbTypeZSet   = 3
	rdbTypeHash   = 4
	rdbTypeZSet2  = 5

	rdbMaxBytes = 256 << 20
	crc64Poly   = 0xad93d23594c935a9
	lenTag14    = 0x40
	lenTag32    = 0x80
	lenTag64    = 0x81
	lzfOffMask  = 0x1f
	size32      = 4
	size64      = 8
	typeBits    = 6
	typeShort   = 1
	typeMedium  = 2
	byteMask    = 0xff
	bitsPerByte = 8
	lenMask6    = 0x3f
	lenMask14   = 0x3fff
	lenMax6     = 1 << 6
	lenMax14    = 1 << 14
	enc8        = 0
	enc16       = 1
	enc32       = 2
	encLZF      = 3
	lzfLiteral  = 32
	lzfLongRun  = 7
	lzfMinRun   = 2
	lzfOffShift = 5
)

// ErrUnsupportedRDB is returned for RDB content the engine cannot represent.
var ErrUnsupportedRDB = errors.New("unsupported RDB content")

// ErrBadRDB is returned when RDB data is malformed.
var ErrBadRDB = errors.New("invalid RDB data")

// crc64Table is the reflected CRC-64/Jones table Redis checksums RDB files with.
var crc64Table = func() [256]uint64 { //nolint:gochecknoglobals // immutable lookup table
	var t [256]uint64

	for i := range t {
		c := uint64(i)

		for range bitsPerByte {
			if c&1 == 1 {
				c = c>>1 ^ reflect64(crc64Poly)
			} else {
				c >>= 1
			}
		}

		t[i] = c
	}

	return t
}()

func reflect64(v uint64) uint64 {
	var r uint64

	for range 64 {
		r = r<<1 | v&1
		v >>= 1
	}

	return r
}

func crc64Jones(b []byte) uint64 {
	var crc uint64

	for _, x := range b {
		crc = crc64Table[byte(crc)^x] ^ crc>>bitsPerByte
	}

	return crc
}

// ExportRDB serialises the keyspace as a Redis RDB file (version 9). Streams are not representable and fail the
// export; per-field hash TTLs are dropped because RDB 9 cannot carry them.
func (e *Engine) ExportRDB() ([]byte, error) {
	if e == nil || e.Mini == nil {
		return nil, fmt.Errorf("%w: engine not running", ErrBadRDB)
	}

	return EncodeRDB(e.Mini)
}

// EncodeRDB is ExportRDB for a bare miniredis server.
func EncodeRDB(mr *miniredis.Miniredis) ([]byte, error) {
	var w bytes.Buffer

	w.WriteString(rdbMagic + fmt.Sprintf("%04d", rdbVersion))
	w.WriteByte(rdbAux)
	putString(&w, "redis-ver")
	putString(&w, "7.0.0")

	for i := range keyspaceDBs {
		if err := encodeDB(&w, mr, i); err != nil {
			return nil, err
		}
	}

	w.WriteByte(rdbEOF)

	var sum [8]byte

	binary.LittleEndian.PutUint64(sum[:], crc64Jones(w.Bytes()))
	w.Write(sum[:])

	return w.Bytes(), nil
}

func encodeDB(w *bytes.Buffer, mr *miniredis.Miniredis, idx int) error {
	db := mr.DB(idx)
	keys := db.Keys()

	if len(keys) == 0 {
		return nil
	}

	w.WriteByte(rdbSelectDB)
	putLen(w, uint64(idx)) //nolint:gosec // idx is a database index below 16
	w.WriteByte(rdbResizeDB)
	putLen(w, uint64(len(keys)))

	withTTL := 0

	for _, k := range keys {
		if db.TTL(k) > 0 {
			withTTL++
		}
	}

	putLen(w, uint64(withTTL))

	for _, k := range keys {
		if err := encodeKey(w, db, idx, k); err != nil {
			return err
		}
	}

	return nil
}

func encodeKey(w *bytes.Buffer, db *miniredis.RedisDB, idx int, k string) error {
	kd, ok := dumpKey(db, idx, k)
	if !ok {
		if db.Type(k) == typeStream {
			return fmt.Errorf("%w: stream key %q", ErrUnsupportedRDB, k)
		}

		return nil
	}

	if kd.HasExpiry {
		var ms [8]byte

		binary.LittleEndian.PutUint64(ms[:], uint64(time.Now().Add(kd.TTL).UnixMilli()))

		w.WriteByte(rdbExpireMS)
		w.Write(ms[:])
	}

	return encodeValue(w, &kd)
}

func encodeValue(w *bytes.Buffer, kd *keyDump) error {
	switch kd.Type {
	case typeString:
		w.WriteByte(rdbTypeString)
		putString(w, kd.Key)
		putString(w, kd.Str)
	case typeList:
		w.WriteByte(rdbTypeList)
		putString(w, kd.Key)
		putStrings(w, kd.List)
	case typeSet:
		w.WriteByte(rdbTypeSet)
		putString(w, kd.Key)
		putStrings(w, kd.Set)
	case typeHash:
		w.WriteByte(rdbTypeHash)
		putString(w, kd.Key)
		putLen(w, uint64(len(kd.Hash)))

		for f, v := range kd.Hash {
			putString(w, f)
			putString(w, v)
		}
	case typeZSet:
		w.WriteByte(rdbTypeZSet2)
		putString(w, kd.Key)
		putLen(w, uint64(len(kd.ZSet)))

		for _, m := range kd.ZSet {
			putString(w, m.Member)

			var f [8]byte

			binary.LittleEndian.PutUint64(f[:], math.Float64bits(m.Score))
			w.Write(f[:])
		}
	default:
		return fmt.Errorf("%w: %s key %q", ErrUnsupportedRDB, kd.Type, kd.Key)
	}

	return nil
}

func putStrings(w *bytes.Buffer, vs []string) {
	putLen(w, uint64(len(vs)))

	for _, v := range vs {
		putString(w, v)
	}
}

func putString(w *bytes.Buffer, s string) {
	putLen(w, uint64(len(s)))
	w.WriteString(s)
}

func putLen(w *bytes.Buffer, n uint64) {
	switch {
	case n < lenMax6:
		w.WriteByte(byte(n))
	case n < lenMax14:
		w.WriteByte(byte(n>>bitsPerByte) | lenTag14)
		w.WriteByte(byte(n & byteMask))
	case n <= math.MaxUint32:
		w.WriteByte(lenTag32)

		var b [4]byte

		binary.BigEndian.PutUint32(b[:], uint32(n))
		w.Write(b[:])
	default:
		w.WriteByte(lenTag64)

		var b [8]byte

		binary.BigEndian.PutUint64(b[:], n)
		w.Write(b[:])
	}
}

// ImportRDB replaces the keyspace with the contents of an RDB file. Strings, lists, sets, sorted sets and hashes
// in their plain encodings are supported (with LZF and integer string encodings); compact ziplist, listpack and
// intset encodings, streams and modules fail with ErrUnsupportedRDB. The keyspace is left untouched on error.
func (e *Engine) ImportRDB(data []byte) error {
	if e == nil || e.Mini == nil {
		return fmt.Errorf("%w: engine not running", ErrBadRDB)
	}

	return DecodeRDB(e.Mini, data)
}

type rdbEntry struct {
	expire time.Time
	kd     keyDump
}

// DecodeRDB is ImportRDB for a bare miniredis server.
func DecodeRDB(mr *miniredis.Miniredis, data []byte) error {
	entries, err := parseRDB(data)
	if err != nil {
		return err
	}

	mr.FlushAll()

	now := time.Now()

	for i := range entries {
		en := &entries[i]
		if !en.expire.IsZero() && !en.expire.After(now) {
			continue
		}

		if err = loadKey(mr.DB(en.kd.DB), &en.kd); err != nil {
			mr.FlushAll()

			return fmt.Errorf("%w: key %q: %w", ErrBadRDB, en.kd.Key, err)
		}

		if !en.expire.IsZero() {
			mr.DB(en.kd.DB).SetTTL(en.kd.Key, en.expire.Sub(now))
		}
	}

	return nil
}

type rdbReader struct {
	b   []byte
	pos int
}

func (r *rdbReader) fail(what string) error { return fmt.Errorf("%w: %s", ErrBadRDB, what) }

func (r *rdbReader) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.b)-r.pos {
		return nil, r.fail("truncated")
	}

	out := r.b[r.pos : r.pos+n]
	r.pos += n

	return out, nil
}

func (r *rdbReader) byte() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}

	return b[0], nil
}

// length reads a length; enc is the special-encoding selector when the length is really an encoding marker.
func (r *rdbReader) length() (uint64, int, error) {
	first, err := r.byte()
	if err != nil {
		return 0, -1, err
	}

	switch first >> typeBits {
	case 0:
		return uint64(first & lenMask6), -1, nil
	case typeShort:
		second, bErr := r.byte()
		if bErr != nil {
			return 0, -1, bErr
		}

		return uint64(first&lenMask6)<<bitsPerByte | uint64(second), -1, nil
	case typeMedium:
		return r.wideLength(first)
	default:
		return 0, int(first & lenMask6), nil
	}
}

func (r *rdbReader) wideLength(first byte) (uint64, int, error) {
	switch first {
	case lenTag32:
		b, err := r.take(size32)
		if err != nil {
			return 0, -1, err
		}

		return uint64(binary.BigEndian.Uint32(b)), -1, nil
	case lenTag64:
		b, err := r.take(size64)
		if err != nil {
			return 0, -1, err
		}

		return binary.BigEndian.Uint64(b), -1, nil
	default:
		return 0, -1, r.fail("bad length encoding")
	}
}

func (r *rdbReader) count() (int, error) {
	n, enc, err := r.length()
	if err != nil {
		return 0, err
	}

	if enc >= 0 || n > uint64(len(r.b)-r.pos) { //nolint:gosec // remaining is non-negative
		return 0, r.fail("bad element count")
	}

	return int(n), nil //nolint:gosec // bounded by the remaining input above
}

func (r *rdbReader) str() (string, error) {
	n, enc, err := r.length()
	if err != nil {
		return "", err
	}

	if enc >= 0 {
		return r.encodedString(enc)
	}

	if n > uint64(len(r.b)-r.pos) { //nolint:gosec // remaining is non-negative
		return "", r.fail("truncated string")
	}

	b, err := r.take(int(n)) //nolint:gosec // bounded by the remaining input above

	return string(b), err
}

func (r *rdbReader) encodedString(enc int) (string, error) {
	switch enc {
	case enc8, enc16, enc32:
		size := 1 << enc

		b, err := r.take(size)
		if err != nil {
			return "", err
		}

		return strconv.FormatInt(signedLE(b), 10), nil
	case encLZF:
		return r.lzfString()
	default:
		return "", fmt.Errorf("%w: string encoding %d", ErrUnsupportedRDB, enc)
	}
}

func signedLE(b []byte) int64 {
	switch len(b) {
	case 1:
		return int64(int8(b[0])) //nolint:gosec // reinterpreting as signed is the encoding
	case 2: //nolint:mnd // int16
		return int64(int16(binary.LittleEndian.Uint16(b))) //nolint:gosec // as above
	default:
		return int64(int32(binary.LittleEndian.Uint32(b))) //nolint:gosec // as above
	}
}

func (r *rdbReader) lzfString() (string, error) {
	clen, enc, err := r.length()
	if err != nil || enc >= 0 {
		return "", r.fail("bad compressed length")
	}

	ulen, enc, err := r.length()
	if err != nil || enc >= 0 || ulen > rdbMaxBytes {
		return "", r.fail("bad uncompressed length")
	}

	if clen > uint64(len(r.b)-r.pos) { //nolint:gosec // remaining is non-negative
		return "", r.fail("truncated compressed string")
	}

	in, err := r.take(int(clen)) //nolint:gosec // bounded by the remaining input above
	if err != nil {
		return "", err
	}

	out, err := lzfDecompress(in, int(ulen))
	if err != nil {
		return "", err
	}

	return string(out), nil
}

func lzfDecompress(in []byte, ulen int) ([]byte, error) {
	out := make([]byte, 0, ulen)
	bad := fmt.Errorf("%w: corrupt LZF data", ErrBadRDB)

	for ip := 0; ip < len(in); {
		ctrl := int(in[ip])
		ip++

		if ctrl < lzfLiteral {
			run := ctrl + 1
			if ip+run > len(in) || len(out)+run > ulen {
				return nil, bad
			}

			out = append(out, in[ip:ip+run]...)
			ip += run

			continue
		}

		run := ctrl >> lzfOffShift
		if run == lzfLongRun {
			if ip >= len(in) {
				return nil, bad
			}

			run += int(in[ip])
			ip++
		}

		if ip >= len(in) {
			return nil, bad
		}

		ref := len(out) - (ctrl&lzfOffMask)<<bitsPerByte - 1 - int(in[ip])
		ip++
		run += lzfMinRun

		if ref < 0 || len(out)+run > ulen {
			return nil, bad
		}

		for range run {
			out = append(out, out[ref])
			ref++
		}
	}

	if len(out) != ulen {
		return nil, bad
	}

	return out, nil
}

func parseRDB(data []byte) ([]rdbEntry, error) {
	if len(data) > rdbMaxBytes {
		return nil, fmt.Errorf("%w: file exceeds %d bytes", ErrBadRDB, rdbMaxBytes)
	}

	r := &rdbReader{b: data}

	head, err := r.take(len(rdbMagic) + rdbVerLen)
	if err != nil || string(head[:len(rdbMagic)]) != rdbMagic {
		return nil, fmt.Errorf("%w: missing REDIS header", ErrBadRDB)
	}

	if ver, convErr := strconv.Atoi(string(head[len(rdbMagic):])); convErr != nil || ver < 1 || ver > rdbMaxVer {
		return nil, fmt.Errorf("%w: unsupported RDB version %q", ErrUnsupportedRDB, head[len(rdbMagic):])
	}

	return r.entries()
}

func (r *rdbReader) entries() ([]rdbEntry, error) {
	var (
		out    []rdbEntry
		db     int
		expire time.Time
	)

	for {
		op, err := r.byte()
		if err != nil {
			return nil, err
		}

		switch op {
		case rdbEOF:
			return out, nil
		case rdbSelectDB:
			if db, err = r.selectDB(); err != nil {
				return nil, err
			}
		case rdbExpireMS, rdbExpireS:
			if expire, err = r.expiry(op); err != nil {
				return nil, err
			}
		case rdbAux:
			err = r.skipAux()
		case rdbResizeDB:
			err = r.skipSizes()
		case rdbFreq:
			_, err = r.byte()
		case rdbIdle:
			_, _, err = r.length()
		default:
			var en rdbEntry

			en, err = r.entry(op, db, expire)
			expire = time.Time{}

			if err == nil {
				out = append(out, en)
			}
		}

		if err != nil {
			return nil, err
		}
	}
}

func (r *rdbReader) selectDB() (int, error) {
	n, enc, err := r.length()
	if err != nil || enc >= 0 || n >= keyspaceDBs {
		return 0, r.fail("bad database index")
	}

	return int(n), nil
}

func (r *rdbReader) expiry(op byte) (time.Time, error) {
	if op == rdbExpireS {
		b, err := r.take(size32)
		if err != nil {
			return time.Time{}, err
		}

		return time.Unix(int64(binary.LittleEndian.Uint32(b)), 0), nil
	}

	b, err := r.take(size64)
	if err != nil {
		return time.Time{}, err
	}

	return time.UnixMilli(int64(binary.LittleEndian.Uint64(b))), nil //nolint:gosec // epoch millis fit int64
}

func (r *rdbReader) skipAux() error {
	for range 2 {
		if _, err := r.str(); err != nil {
			return err
		}
	}

	return nil
}

func (r *rdbReader) skipSizes() error {
	for range 2 {
		if _, _, err := r.length(); err != nil {
			return err
		}
	}

	return nil
}

func (r *rdbReader) entry(typ byte, db int, expire time.Time) (rdbEntry, error) {
	key, err := r.str()
	if err != nil {
		return rdbEntry{}, err
	}

	kd := keyDump{Key: key, DB: db}

	if err = r.value(typ, &kd); err != nil {
		return rdbEntry{}, err
	}

	return rdbEntry{kd: kd, expire: expire}, nil
}

func (r *rdbReader) value(typ byte, kd *keyDump) error {
	switch typ {
	case rdbTypeString:
		kd.Type = typeString

		return r.into(&kd.Str)
	case rdbTypeList:
		kd.Type = typeList

		return r.strings(&kd.List)
	case rdbTypeSet:
		kd.Type = typeSet

		return r.strings(&kd.Set)
	case rdbTypeHash:
		kd.Type = typeHash

		return r.hash(kd)
	case rdbTypeZSet, rdbTypeZSet2:
		kd.Type = typeZSet

		return r.zset(kd, typ == rdbTypeZSet2)
	default:
		return fmt.Errorf("%w: encoded value type %d for key %q", ErrUnsupportedRDB, typ, kd.Key)
	}
}

func (r *rdbReader) into(dst *string) error {
	s, err := r.str()
	*dst = s

	return err
}

func (r *rdbReader) strings(dst *[]string) error {
	n, err := r.count()
	if err != nil {
		return err
	}

	*dst = make([]string, 0, n)

	for range n {
		s, sErr := r.str()
		if sErr != nil {
			return sErr
		}

		*dst = append(*dst, s)
	}

	return nil
}

func (r *rdbReader) hash(kd *keyDump) error {
	n, err := r.count()
	if err != nil {
		return err
	}

	kd.Hash = make(map[string]string, n)

	for range n {
		f, fErr := r.str()
		if fErr != nil {
			return fErr
		}

		v, vErr := r.str()
		if vErr != nil {
			return vErr
		}

		kd.Hash[f] = v
	}

	return nil
}

func (r *rdbReader) zset(kd *keyDump, binary2 bool) error {
	n, err := r.count()
	if err != nil {
		return err
	}

	kd.ZSet = make([]zMember, 0, n)

	for range n {
		m, mErr := r.str()
		if mErr != nil {
			return mErr
		}

		score, sErr := r.score(binary2)
		if sErr != nil {
			return sErr
		}

		kd.ZSet = append(kd.ZSet, zMember{Member: m, Score: score})
	}

	return nil
}

func (r *rdbReader) score(binary2 bool) (float64, error) {
	if binary2 {
		b, err := r.take(size64)
		if err != nil {
			return 0, err
		}

		return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
	}

	l, err := r.byte()
	if err != nil {
		return 0, err
	}

	switch l {
	case 253: //nolint:mnd // RDB NaN marker
		return math.NaN(), nil
	case 254: //nolint:mnd // RDB +Inf marker
		return math.Inf(1), nil
	case 255: //nolint:mnd // RDB -Inf marker
		return math.Inf(-1), nil
	}

	b, err := r.take(int(l))
	if err != nil {
		return 0, err
	}

	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, r.fail("bad sorted-set score")
	}

	return f, nil
}
