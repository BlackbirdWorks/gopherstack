package redisengine

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
)

// internalPeerName cannot be set by a real client: CLIENT SETNAME rejects names containing spaces.
const internalPeerName = "redisengine internal"

var errNoServer = errors.New("redis server not running")

// localClient runs commands against a server in-process, on a peer the tuning hook leaves alone.
type localClient struct {
	srv  *server.Server
	peer *server.Peer
	rd   *bufio.Reader
	wr   *bufio.Writer
	buf  bytes.Buffer
	db   int
}

func newLocalClient(mr *miniredis.Miniredis, auth string) (*localClient, error) {
	if mr == nil || mr.Server() == nil {
		return nil, errNoServer
	}

	c := &localClient{srv: mr.Server()}
	c.wr = bufio.NewWriter(&c.buf)
	c.rd = bufio.NewReader(&c.buf)
	c.peer = server.NewPeer(c.wr)
	c.peer.ClientName = internalPeerName

	if auth != "" {
		if _, err := c.do("AUTH", auth); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *localClient) do(args ...string) (any, error) {
	c.buf.Reset()
	c.rd.Reset(&c.buf)
	c.srv.Dispatch(c.peer, args)
	c.peer.Flush()

	v, err := server.ParseReply(c.rd)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", args[0], err)
	}

	return v, nil
}

func (c *localClient) selectDB(db int) error {
	if c.db == db {
		return nil
	}

	if _, err := c.do("SELECT", strconv.Itoa(db)); err != nil {
		return err
	}

	c.db = db

	return nil
}

func replyStrings(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))

	for _, it := range items {
		out = append(out, replyString(it))
	}

	return out
}

func replyString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case server.Simple:
		return string(x)
	case int:
		return strconv.Itoa(x)
	default:
		return ""
	}
}

func replyInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case string:
		n, _ := strconv.Atoi(x)

		return n
	default:
		return 0
	}
}

func replyMap(v any) map[string]any {
	items, _ := v.([]any)
	out := make(map[string]any, len(items)/pairSize)

	for i := 0; i+1 < len(items); i += pairSize {
		out[replyString(items[i])] = items[i+1]
	}

	return out
}
