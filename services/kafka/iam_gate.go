package kafka

import (
	"encoding/binary"
	"net"
	"sync"

	"github.com/twmb/franz-go/pkg/kmsg"
)

const gateMaxFrame uint32 = 128 << 20

// gate relays an authenticated AWS_MSK_IAM connection while evaluating the per-API kafka-cluster
// actions of each request. Rejections are answered locally, in order with upstream responses.
type gate struct {
	client  net.Conn
	up      net.Conn
	authz   iamAuthorizer
	cluster string
	queue   []gateSlot
	mu      sync.Mutex
}

// gateSlot is one response owed to the client: a locally built payload, or nil when upstream answers.
type gateSlot struct {
	local []byte
}

func newGate(client, up net.Conn, authz iamAuthorizer, cluster string) *gate {
	return &gate{client: client, up: up, authz: authz, cluster: cluster}
}

func (g *gate) run() {
	done := make(chan struct{}, spliceDirections)

	go func() { g.clientToUpstream(); done <- struct{}{} }()
	go func() { g.upstreamToClient(); done <- struct{}{} }()

	<-done
	_ = g.client.Close()
	_ = g.up.Close()
	<-done
}

func (g *gate) clientToUpstream() {
	for {
		frame, err := readFrameMax(g.client, gateMaxFrame)
		if err != nil || len(frame) < frameHeaderMin {
			return
		}

		if !g.handle(frame) {
			return
		}
	}
}

// handle forwards or answers one request frame; false ends the connection.
func (g *gate) handle(frame []byte) bool {
	key := int16(binary.BigEndian.Uint16(frame[0:2]) & maxInt16)
	version := int16(binary.BigEndian.Uint16(frame[2:4]) & maxInt16)
	corr := binary.BigEndian.Uint32(frame[4:8])

	req := kmsg.RequestForKey(key)
	if req == nil {
		return g.forward(frame, false)
	}

	if _, gated := ruleFor(req); !gated {
		return g.forward(frame, false)
	}

	req.SetVersion(version)

	if version < 0 || version > req.MaxVersion() || readRequestBody(frame, req) != nil {
		return false
	}

	r, _ := ruleFor(req)
	if r.allowed(g.authz, g.cluster) {
		return g.forward(frame, r.silent)
	}

	return g.reject(r, corr, version)
}

// forward sends frame upstream; silent requests (acks=0 produce) get no response.
func (g *gate) forward(frame []byte, silent bool) bool {
	if !silent {
		g.mu.Lock()
		g.queue = append(g.queue, gateSlot{})
		g.mu.Unlock()
	}

	return writeFrame(g.up, frame) == nil
}

func (g *gate) reject(r rule, corr uint32, version int16) bool {
	if r.deny == nil {
		return false
	}

	if r.silent {
		return true
	}

	resp := r.deny()
	resp.SetVersion(version)

	g.mu.Lock()
	defer g.mu.Unlock()

	g.queue = append(g.queue, gateSlot{local: responsePayload(corr, resp)})

	return g.flushLocalsLocked()
}

func (g *gate) upstreamToClient() {
	for {
		frame, err := readFrameMax(g.up, gateMaxFrame)
		if err != nil || !g.deliver(frame) {
			return
		}
	}
}

// deliver writes an upstream response after any local replies queued ahead of it.
func (g *gate) deliver(frame []byte) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.flushLocalsLocked() {
		return false
	}

	if len(g.queue) > 0 {
		g.queue = g.queue[1:]
	}

	if writeFrame(g.client, frame) != nil {
		return false
	}

	return g.flushLocalsLocked()
}

func (g *gate) flushLocalsLocked() bool {
	for len(g.queue) > 0 && g.queue[0].local != nil {
		payload := g.queue[0].local
		g.queue = g.queue[1:]

		if writeFrame(g.client, payload) != nil {
			return false
		}
	}

	return true
}
