package ocistreaming

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	kafkaMaxFrame     = 8 << 20
	kafkaMaxAuthFrame = 16 << 10
	kafkaAuthDeadline = 30 * time.Second
	kafkaIdleDeadline = 10 * time.Minute
	kafkaMechanism    = "PLAIN"
	maxAPIKey         = int16(40)
	kafkaNodeID       = 0
	kafkaMaxConns     = 500
	kafkaSweepEvery   = time.Second
	kafkaMaxWait      = 30 * time.Second
	kafkaClusterID    = "gopherstack-streaming"
	minRequestHeader  = 10
	frameSizeBytes    = 4
	kafkaTopicsPerReq = 1000
	kafkaWriteTimeout = 30 * time.Second

	apiProduce          int16 = 0
	apiFetch            int16 = 1
	apiListOffsets      int16 = 2
	apiMetadata         int16 = 3
	apiOffsetCommit     int16 = 8
	apiOffsetFetch      int16 = 9
	apiFindCoordinator  int16 = 10
	apiJoinGroup        int16 = 11
	apiHeartbeat        int16 = 12
	apiLeaveGroup       int16 = 13
	apiSyncGroup        int16 = 14
	apiCreateTopics     int16 = 19
	apiDeleteTopics     int16 = 20
	apiSaslHandshake    int16 = 17
	apiVersions         int16 = 18
	apiSaslAuthenticate int16 = 36
)

type apiRange struct{ min, max int16 }

// kafkaAPIs lists what the endpoint serves; versions stay below the flexible and topic-ID variants.
func kafkaAPIs() map[int16]apiRange {
	return map[int16]apiRange{
		apiProduce: {3, 8}, apiFetch: {4, 11}, apiListOffsets: {1, 5}, apiMetadata: {1, 8},
		apiOffsetCommit: {2, 7}, apiOffsetFetch: {1, 5}, apiFindCoordinator: {0, 2}, apiJoinGroup: {1, 5},
		apiHeartbeat: {0, 3}, apiLeaveGroup: {0, 3}, apiSyncGroup: {0, 3}, apiSaslHandshake: {1, 1},
		apiVersions: {0, 2}, apiCreateTopics: {2, 4}, apiDeleteTopics: {1, 3}, apiSaslAuthenticate: {0, 1},
	}
}

var errKafkaFrame = errors.New("kafka: malformed request frame")

type kafkaServer struct {
	ctx     context.Context //nolint:containedctx // owns the lifetime of blocked long polls
	ln      net.Listener
	h       *Handler
	groups  *coordinator
	conns   map[net.Conn]struct{}
	mu      *lockmetrics.RWMutex
	cancel  context.CancelFunc
	certPEM []byte
	wg      sync.WaitGroup
}

func (h *Handler) startKafka(ctx context.Context) error {
	if h.KafkaPort <= 0 {
		return nil
	}

	raw, port, err := portalloc.ListenTCP(ctx, h.KafkaPort)
	if err != nil {
		return fmt.Errorf("ocistreaming: bind kafka port %d: %w", h.KafkaPort, err)
	}

	h.KafkaPort = port

	if err = h.ServeKafka(ctx, raw); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "ocistreaming: starting Kafka listener", "port", port)

	return nil
}

// ServeKafka serves the Kafka protocol (SASL PLAIN over TLS 1.2+) on raw until Shutdown.
func (h *Handler) ServeKafka(ctx context.Context, raw net.Listener) error {
	cert, err := devtls.GenerateSelfSignedCert()
	if err != nil {
		_ = raw.Close()

		return fmt.Errorf("ocistreaming: kafka certificate: %w", err)
	}

	if addr, ok := raw.Addr().(*net.TCPAddr); ok {
		h.Backend.setKafkaPort(addr.Port)
	}

	ln := tls.NewListener(raw, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	k := &kafkaServer{
		h: h, ln: ln, conns: map[net.Conn]struct{}{}, mu: lockmetrics.New("ocistreaming.kafka"),
		groups: newCoordinator(h.Backend.nowUTC), ctx: sctx, cancel: cancel,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}),
	}

	h.srvMu.Lock("kafka")
	h.kafka = k
	h.srvMu.Unlock()

	k.wg.Go(k.accept)
	k.wg.Go(k.sweep)

	return nil
}

// KafkaCertificatePEM returns the self-signed certificate the Kafka listener presents, or nil when it is off.
func (h *Handler) KafkaCertificatePEM() []byte {
	h.srvMu.RLock("kafka")
	defer h.srvMu.RUnlock()

	if h.kafka == nil {
		return nil
	}

	return h.kafka.certPEM
}

func (h *Handler) stopKafka() {
	h.srvMu.Lock("kafka")
	k := h.kafka
	h.kafka = nil
	h.srvMu.Unlock()

	if k == nil {
		return
	}

	k.cancel()
	_ = k.ln.Close()

	k.mu.Lock("stop")

	for c := range k.conns {
		_ = c.Close()
	}

	k.mu.Unlock()
	k.wg.Wait()
}

func (k *kafkaServer) sweep() {
	t := time.NewTicker(kafkaSweepEvery)
	defer t.Stop()

	for {
		select {
		case <-k.ctx.Done():
			return
		case <-t.C:
			k.groups.sweep()
		}
	}
}

func (k *kafkaServer) accept() {
	for {
		conn, err := k.ln.Accept()
		if err != nil {
			return
		}

		if !k.track(conn) {
			_ = conn.Close()

			continue
		}

		k.wg.Go(func() {
			defer logger.Recover(k.ctx, "ocistreaming.kafka", nil)
			defer k.untrack(conn)

			(&kconn{k: k, conn: conn}).serve()
		})
	}
}

func (k *kafkaServer) track(c net.Conn) bool {
	k.mu.Lock("track")
	defer k.mu.Unlock()

	if len(k.conns) >= kafkaMaxConns || k.ctx.Err() != nil {
		return false
	}

	k.conns[c] = struct{}{}

	return true
}

func (k *kafkaServer) untrack(c net.Conn) {
	_ = c.Close()

	k.mu.Lock("untrack")
	delete(k.conns, c)
	k.mu.Unlock()
}

type kconn struct {
	conn       net.Conn
	k          *kafkaServer
	poolID     string
	mech       bool
	closeAfter bool
	authed     bool
}

type reqHeader struct {
	clientID string
	key      int16
	version  int16
	corr     int32
}

func (c *kconn) serve() {
	_ = c.conn.SetDeadline(time.Now().Add(kafkaAuthDeadline))

	for {
		limit := uint32(kafkaMaxAuthFrame)
		if c.authed {
			limit = kafkaMaxFrame
		}

		frame, err := readKafkaFrame(c.conn, limit)
		if err != nil {
			return
		}

		if !c.step(frame) {
			return
		}

		if c.authed {
			_ = c.conn.SetReadDeadline(time.Now().Add(kafkaIdleDeadline))
		}
	}
}

func readKafkaFrame(r io.Reader, limit uint32) ([]byte, error) {
	var sz [4]byte
	if _, err := io.ReadFull(r, sz[:]); err != nil {
		return nil, fmt.Errorf("kafka frame size: %w", err)
	}

	n := binary.BigEndian.Uint32(sz[:])
	if n == 0 || n > limit {
		return nil, errKafkaFrame
	}

	var buf bytes.Buffer

	if _, err := io.CopyN(&buf, r, int64(n)); err != nil {
		return nil, fmt.Errorf("kafka frame body: %w", err)
	}

	return buf.Bytes(), nil
}

func parseHeader(frame []byte) (reqHeader, []byte, error) {
	if len(frame) < minRequestHeader {
		return reqHeader{}, nil, errKafkaFrame
	}

	h := reqHeader{
		key:     int16(binary.BigEndian.Uint16(frame[0:2])), //nolint:gosec // protocol field
		version: int16(binary.BigEndian.Uint16(frame[2:4])), //nolint:gosec // protocol field
		corr:    int32(binary.BigEndian.Uint32(frame[4:8])), //nolint:gosec // protocol field
	}

	idLen := int(int16(binary.BigEndian.Uint16(frame[8:10]))) //nolint:gosec // protocol field
	rest := frame[10:]

	if idLen > 0 {
		if idLen > len(rest) {
			return reqHeader{}, nil, errKafkaFrame
		}

		h.clientID, rest = string(rest[:idLen]), rest[idLen:]
	}

	return h, rest, nil
}

// step handles one request frame; false closes the connection.
func (c *kconn) step(frame []byte) bool {
	hdr, body, err := parseHeader(frame)
	if err != nil {
		return false
	}

	if !supportedVersion(hdr) {
		return hdr.key == apiVersions && c.write(
			reqHeader{key: hdr.key, corr: hdr.corr},
			&kmsg.ApiVersionsResponse{ErrorCode: kerrUnsupportedVersion, ApiKeys: kafkaVersionList()},
		)
	}

	if !c.authed && !preAuthAPI(hdr.key) {
		return false
	}

	req := kmsg.RequestForKey(hdr.key)
	req.SetVersion(hdr.version)

	if req.IsFlexible() || req.ReadFrom(body) != nil {
		return false
	}

	if c.authed {
		c.k.h.Backend.Sweep()
	}

	resp := c.dispatch(req)
	if resp == nil {
		return false
	}

	if pr, ok := req.(*kmsg.ProduceRequest); ok && pr.Acks == 0 {
		return true
	}

	return c.write(hdr, resp) && !c.closeAfter
}

func supportedVersion(hdr reqHeader) bool {
	rng, known := kafkaAPIs()[hdr.key]

	return known && hdr.version >= rng.min && hdr.version <= rng.max
}

func preAuthAPI(key int16) bool {
	return key == apiVersions || key == apiSaslHandshake || key == apiSaslAuthenticate
}

func (c *kconn) write(hdr reqHeader, resp kmsg.Response) bool {
	resp.SetVersion(hdr.version)

	out := binary.BigEndian.AppendUint32(nil, 0)
	out = binary.BigEndian.AppendUint32(out, uint32(hdr.corr)) //nolint:gosec // protocol field
	out = resp.AppendTo(out)
	binary.BigEndian.PutUint32(out[:frameSizeBytes], uint32(len(out)-frameSizeBytes)) //nolint:gosec // bounded

	_ = c.conn.SetWriteDeadline(time.Now().Add(kafkaWriteTimeout))
	_, err := c.conn.Write(out)

	return err == nil
}

func kafkaVersionList() []kmsg.ApiVersionsResponseApiKey {
	apis := kafkaAPIs()
	keys := make([]kmsg.ApiVersionsResponseApiKey, 0, len(apis))

	for k := range maxAPIKey {
		if r, ok := apis[k]; ok {
			keys = append(keys, kmsg.ApiVersionsResponseApiKey{ApiKey: k, MinVersion: r.min, MaxVersion: r.max})
		}
	}

	return keys
}

// advertised returns the host and port the client reached, which is where it is told to reconnect.
func (c *kconn) advertised() (string, int32) {
	host, port, err := net.SplitHostPort(c.conn.LocalAddr().String())
	if err != nil {
		return "localhost", 0
	}

	p, _ := strconv.ParseInt(port, 10, 32)

	return host, int32(p)
}
