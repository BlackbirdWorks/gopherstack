package redisdocker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

const (
	rdbMagic     = "REDIS"
	bundleMagic  = "GSRDBS1\n"
	maxRDBBytes  = 1 << 30
	syncDeadline = 2 * time.Minute
)

// ErrBadSnapshot is returned when snapshot data cannot be read.
var ErrBadSnapshot = errors.New("invalid redis snapshot data")

// ErrShape is returned when snapshot data cannot be spread over the requested shards.
var ErrShape = errors.New("snapshot shards do not match the cluster")

// Snapshot reads each shard primary's dataset as an RDB file.
func (g *Group) Snapshot(ctx context.Context) ([][]byte, error) {
	out := make([][]byte, len(g.spec.Shards))

	for i := range g.spec.Shards {
		primary := g.shardNodes(i)[0]

		rdb, err := g.engine.cfg.FetchRDB(ctx, g.target(primary))
		if err != nil {
			return nil, fmt.Errorf("snapshot shard %d: %w", i, err)
		}

		out[i] = rdb
	}

	return out, nil
}

// fetchRDB performs a SYNC handshake, which makes the server hand over its dataset as one RDB payload.
func fetchRDB(ctx context.Context, t Target) ([]byte, error) {
	dialer := &net.Dialer{Timeout: syncDeadline}

	conn, err := dialer.DialContext(ctx, "tcp", t.Addr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	if t.TLS != nil {
		tc := tls.Client(conn, t.TLS)
		if err = tc.HandshakeContext(ctx); err != nil {
			_ = conn.Close()

			return nil, fmt.Errorf("tls: %w", err)
		}

		conn = tc
	}

	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(syncDeadline))

	r := bufio.NewReader(conn)

	if err = command(conn, r, "AUTH", t.Username, t.Password); err != nil {
		return nil, fmt.Errorf("auth: %w", redactErr(err, []any{">" + t.Password}))
	}

	return syncPayload(conn, r)
}

func command(w io.Writer, r *bufio.Reader, args ...string) error {
	var b strings.Builder

	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")

	for _, a := range args {
		b.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}

	if _, err := io.WriteString(w, b.String()); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	line, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	if !strings.HasPrefix(line, "+") {
		return fmt.Errorf("%w: %s", ErrBadSnapshot, strings.TrimSpace(line))
	}

	return nil
}

func syncPayload(w io.Writer, r *bufio.Reader) ([]byte, error) {
	if _, err := io.WriteString(w, "SYNC\r\n"); err != nil {
		return nil, fmt.Errorf("write sync: %w", err)
	}

	for {
		line, rerr := r.ReadString('\n')
		if rerr != nil {
			return nil, fmt.Errorf("read sync: %w", rerr)
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if !strings.HasPrefix(line, "$") {
			return nil, fmt.Errorf("%w: %s", ErrBadSnapshot, line)
		}

		size, perr := strconv.Atoi(line[1:])
		if perr != nil || size < 0 || size > maxRDBBytes {
			return nil, fmt.Errorf("%w: payload size %q", ErrBadSnapshot, line)
		}

		rdb := make([]byte, size)
		if _, err := io.ReadFull(r, rdb); err != nil {
			return nil, fmt.Errorf("read rdb: %w", err)
		}

		return rdb, nil
	}
}

// EncodeBundle packs one RDB per shard into a single blob.
func EncodeBundle(shards [][]byte) []byte {
	out := []byte(bundleMagic)
	out = binary.AppendUvarint(out, uint64(len(shards)))

	for _, s := range shards {
		out = binary.AppendUvarint(out, uint64(len(s)))
		out = append(out, s...)
	}

	return out
}

// DecodeBundle unpacks EncodeBundle output; ok is false when blob is not a bundle.
func DecodeBundle(blob []byte) ([][]byte, bool) {
	rest, found := bytes.CutPrefix(blob, []byte(bundleMagic))
	if !found {
		return nil, false
	}

	count, n := binary.Uvarint(rest)
	if n <= 0 || count > uint64(len(rest)) {
		return nil, false
	}

	rest = rest[n:]

	var shards [][]byte

	for range count {
		size, m := binary.Uvarint(rest)
		if m <= 0 || size > uint64(len(rest)-m) { //nolint:gosec // len(rest) > m is checked by the loop invariant
			return nil, false
		}

		end := m + int(size) //nolint:gosec // size <= len(rest)
		shards = append(shards, rest[m:end])
		rest = rest[end:]
	}

	return shards, true
}

// IsRDB reports whether blob is a raw RDB file.
func IsRDB(blob []byte) bool { return bytes.HasPrefix(blob, []byte(rdbMagic)) }

// SeedFromBlob converts any snapshot payload this module produces (a raw RDB or an embedded keyspace dump) to an RDB.
func SeedFromBlob(blob []byte) ([]byte, error) {
	if len(blob) == 0 || IsRDB(blob) {
		return blob, nil
	}

	mr := miniredis.NewMiniRedis()
	if err := redisengine.LoadKeyspace(mr, blob); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadSnapshot, err)
	}

	return redisengine.EncodeRDB(mr)
}

// SeedsFor turns a snapshot blob (bundle, raw RDB or embedded dump) into per-shard RDB seeds for shards shards.
func SeedsFor(blob []byte, shards int) ([][]byte, error) {
	if len(blob) == 0 {
		return make([][]byte, shards), nil
	}

	parts, isBundle := DecodeBundle(blob)
	if !isBundle {
		parts = [][]byte{blob}
	}

	if len(parts) != shards {
		return nil, fmt.Errorf("%w: snapshot holds %d shard(s), cluster has %d", ErrShape, len(parts), shards)
	}

	out := make([][]byte, len(parts))

	for i, p := range parts {
		seed, err := SeedFromBlob(p)
		if err != nil {
			return nil, err
		}

		out[i] = seed
	}

	return out, nil
}

// LoadEmbedded loads a snapshot blob produced by either engine mode into an embedded server.
func LoadEmbedded(mr *miniredis.Miniredis, blob []byte) error {
	if mr == nil && len(blob) > 0 {
		return fmt.Errorf("%w: engine not running", ErrBadSnapshot)
	}

	if parts, ok := DecodeBundle(blob); ok {
		if len(parts) != 1 {
			return fmt.Errorf("%w: snapshot holds %d shards", ErrShape, len(parts))
		}

		blob = parts[0]
	}

	if IsRDB(blob) {
		if err := redisengine.DecodeRDB(mr, blob); err != nil {
			return fmt.Errorf("%w: %w", ErrBadSnapshot, err)
		}

		return nil
	}

	return redisengine.LoadKeyspace(mr, blob)
}
