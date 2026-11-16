package azureeventhubs

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	// DefaultKafkaPort is the port of the Kafka endpoint, as on the real service.
	DefaultKafkaPort = 9093

	kafkaMaxFrame     = 16 << 20
	kafkaAuthDeadline = 30 * time.Second
	kafkaConnUser     = "$ConnectionString"
	kafkaMechanism    = "PLAIN"
	kafkaNodeID       = 0
	kafkaMaxConns     = 1000
	kafkaSweepEvery   = time.Second
	minRequestHeader  = 10
	frameSizeBytes    = 4

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
	apiSaslHandshake    int16 = 17
	apiVersions         int16 = 18
	apiCreateTopics     int16 = 19
	apiDeleteTopics     int16 = 20
	apiInitProducerID   int16 = 22
	apiSaslAuthenticate int16 = 36
)

type apiRange struct{ min, max int16 }

// kafkaAPIs lists what the endpoint serves. Versions stay below the topic-ID variants of each API.
func kafkaAPIs() map[int16]apiRange {
	return map[int16]apiRange{
		apiProduce: {3, 9}, apiFetch: {4, 12}, apiListOffsets: {1, 7}, apiMetadata: {1, 9},
		apiOffsetCommit: {2, 9}, apiOffsetFetch: {1, 7}, apiFindCoordinator: {0, 3}, apiJoinGroup: {2, 7},
		apiHeartbeat: {0, 4}, apiLeaveGroup: {0, 4}, apiSyncGroup: {0, 5}, apiSaslHandshake: {1, 1},
		apiVersions: {0, 3}, apiCreateTopics: {2, 5}, apiDeleteTopics: {1, 5}, apiInitProducerID: {0, 4},
		apiSaslAuthenticate: {0, 2},
	}
}

var (
	errKafkaFrame = errors.New("kafka: malformed request frame")
	errKafkaAuth  = errors.New("kafka: request before authentication")
)

// kafkaServer is the Kafka endpoint of the emulator. Topics are event hubs, partitions are event hub
// partitions and the log offset of an event is its sequence number.
type kafkaServer struct {
	ctx    context.Context //nolint:containedctx // owns the lifetime of blocked group requests
	ln     net.Listener
	h      *Handler
	groups *groupCoordinator
	prod   *producerTable
	conns  map[net.Conn]struct{}
	mu     *lockmetrics.RWMutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (h *Handler) startKafka(ctx context.Context) error {
	if h.KafkaPort <= 0 {
		return nil
	}

	var lc net.ListenConfig

	raw, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.KafkaPort))
	if err != nil {
		return fmt.Errorf("azureeventhubs: bind kafka port %d: %w", h.KafkaPort, err)
	}

	if err = h.ServeKafka(ctx, raw); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "azureeventhubs: starting Kafka listener", "port", h.KafkaPort)

	return nil
}

// ServeKafka serves the Kafka protocol over TLS on ln until Shutdown.
func (h *Handler) ServeKafka(ctx context.Context, raw net.Listener) error {
	cert, err := h.amqpCertificate()
	if err != nil {
		_ = raw.Close()

		return err
	}

	ln := tls.NewListener(raw, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	k := &kafkaServer{
		h: h, ln: ln, conns: map[net.Conn]struct{}{}, mu: lockmetrics.New("azureeventhubs.kafka"),
		groups: newGroupCoordinator(h.Backend.now), prod: newProducerTable(),
		ctx: sctx, cancel: cancel,
	}

	h.kafkaMu.Lock("kafka")
	h.kafka = k
	h.kafkaMu.Unlock()

	k.wg.Go(k.accept)
	k.wg.Go(k.sweep)

	return nil
}

func (h *Handler) stopKafka() {
	h.kafkaMu.Lock("kafka")
	k := h.kafka
	h.kafka = nil
	h.kafkaMu.Unlock()

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
			defer logger.Recover(k.ctx, "azureeventhubs.kafka", nil)
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

// kconn is one client connection.
type kconn struct {
	conn       net.Conn
	k          *kafkaServer
	hubScope   string
	ns         string
	user       string
	rights     []string
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
		frame, err := readKafkaFrame(c.conn)
		if err != nil {
			return
		}

		if !c.step(frame) {
			return
		}

		if c.authed {
			_ = c.conn.SetDeadline(time.Time{})
		}
	}
}

func readKafkaFrame(r io.Reader) ([]byte, error) {
	var sz [4]byte
	if _, err := io.ReadFull(r, sz[:]); err != nil {
		return nil, fmt.Errorf("kafka frame size: %w", err)
	}

	n := binary.BigEndian.Uint32(sz[:])
	if n == 0 || n > kafkaMaxFrame {
		return nil, errKafkaFrame
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("kafka frame body: %w", err)
	}

	return buf, nil
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

func skipTaggedFields(b []byte) ([]byte, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, errKafkaFrame
	}

	b = b[n:]

	for range count {
		_, tn := binary.Uvarint(b)
		if tn <= 0 {
			return nil, errKafkaFrame
		}

		size, sn := binary.Uvarint(b[tn:])
		if sn <= 0 || uint64(len(b[tn+sn:])) < size {
			return nil, errKafkaFrame
		}

		b = b[tn+sn+int(size):] //nolint:gosec // bounded by the frame length
	}

	return b, nil
}

// step handles one request frame; false closes the connection.
func (c *kconn) step(frame []byte) bool {
	hdr, body, err := parseHeader(frame)
	if err != nil {
		return false
	}

	rng, known := kafkaAPIs()[hdr.key]
	if !known || hdr.version < rng.min || hdr.version > rng.max {
		if hdr.key == apiVersions {
			hdr.version = 0

			resp := &kmsg.ApiVersionsResponse{ErrorCode: kerrUnsupportedVersion, ApiKeys: kafkaVersionList()}

			return c.write(hdr, resp)
		}

		return false
	}

	req := kmsg.RequestForKey(hdr.key)
	req.SetVersion(hdr.version)

	if req.IsFlexible() {
		if body, err = skipTaggedFields(body); err != nil {
			return false
		}
	}

	if err = req.ReadFrom(body); err != nil {
		return false
	}

	if !c.authed && hdr.key != apiVersions && hdr.key != apiSaslHandshake && hdr.key != apiSaslAuthenticate {
		logger.Load(c.k.ctx).DebugContext(c.k.ctx, "azureeventhubs: kafka request before sasl", "error", errKafkaAuth)

		return false
	}

	resp := c.dispatch(hdr, req)
	if resp == nil {
		return false
	}

	return c.write(hdr, resp) && !c.closeAfter
}

func (c *kconn) write(hdr reqHeader, resp kmsg.Response) bool {
	resp.SetVersion(hdr.version)

	out := binary.BigEndian.AppendUint32(nil, 0)
	out = binary.BigEndian.AppendUint32(out, uint32(hdr.corr)) //nolint:gosec // protocol field

	if resp.IsFlexible() && hdr.key != apiVersions {
		out = append(out, 0)
	}

	out = resp.AppendTo(out)
	size := uint32(len(out) - frameSizeBytes) //nolint:gosec // bounded by the response size
	binary.BigEndian.PutUint32(out[:frameSizeBytes], size)

	_, err := c.conn.Write(out)

	return err == nil
}

func kafkaVersionList() []kmsg.ApiVersionsResponseApiKey {
	apis := kafkaAPIs()
	keys := make([]kmsg.ApiVersionsResponseApiKey, 0, len(apis))

	for k, r := range apis {
		keys = append(keys, kmsg.ApiVersionsResponseApiKey{ApiKey: k, MinVersion: r.min, MaxVersion: r.max})
	}

	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1].ApiKey > keys[j].ApiKey; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}

	return keys
}

// advertised returns the host and port the client reached, which is where it is told to reconnect.
func (c *kconn) advertised() (string, int32) {
	host, port, err := net.SplitHostPort(c.conn.LocalAddr().String())
	if err != nil {
		return "localhost", DefaultKafkaPort
	}

	var p int32

	_, _ = fmt.Sscanf(port, "%d", &p)

	return strings.Trim(host, "[]"), p
}
