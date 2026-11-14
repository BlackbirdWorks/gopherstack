package gcpeventarc

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"time"

	"cel.dev/cel-go/cel"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	deliveryTimeout = 30 * time.Second
	etagLen         = 8
)

type triggerRec struct {
	pb         *eventarcpb.Trigger
	policy     *iampb.Policy
	ownedTopic bool
}

type channelRec struct {
	pb     *eventarcpb.Channel
	policy *iampb.Policy
}

// InMemoryBackend holds all Eventarc state under one lock.
type InMemoryBackend struct {
	svc        Services
	keys       cmek.Checker
	after      AfterFunc
	ops        *gcplro.Store
	now        func() time.Time
	triggers   map[string]*triggerRec
	channels   map[string]*channelRec
	conns      map[string]*eventarcpb.ChannelConnection
	configs    map[string]*eventarcpb.GoogleChannelConfig
	mu         *lockmetrics.RWMutex
	client     *http.Client
	opMu       *lockmetrics.RWMutex
	adv        map[string]map[string]proto.Message
	timers     map[uint64]func()
	pendingOps map[string]*pendingOp
	msgEnv     *cel.Env
	pushBase   string
	timerSeq   uint64
	opDelay    time.Duration
	retryMin   time.Duration
	retryMax   time.Duration
	seq        uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	env, _ := newMessageEnv()

	return &InMemoryBackend{
		msgEnv:     env,
		mu:         lockmetrics.New("gcpeventarc"),
		opMu:       lockmetrics.New("gcpeventarc.ops"),
		after:      realAfter,
		opDelay:    defaultOperationDelay,
		pendingOps: make(map[string]*pendingOp),
		timers:     make(map[uint64]func()),
		adv:        make(map[string]map[string]proto.Message),
		now:        time.Now,
		ops:        gcplro.New("gcpeventarc"),
		client:     newDeliveryClient(),
		triggers:   make(map[string]*triggerRec),
		channels:   make(map[string]*channelRec),
		conns:      make(map[string]*eventarcpb.ChannelConnection),
		configs:    make(map[string]*eventarcpb.GoogleChannelConfig),
		retryMin:   defaultRetryMin,
		retryMax:   defaultRetryMax,
	}
}

func newDeliveryClient() *http.Client {
	tr := httpclient.NewTransport()
	tr.DialContext = loopbackDialer(tr.DialContext)

	return &http.Client{Timeout: deliveryTimeout, Transport: tr}
}

// SetRetryBackoff sets the minimum and maximum redelivery backoff of triggers created from now on.
func (b *InMemoryBackend) SetRetryBackoff(minimum, maximum time.Duration) {
	b.mu.Lock("SetRetryBackoff")
	defer b.mu.Unlock()

	b.retryMin, b.retryMax = minimum, maximum
}

// SetPushBase records the base URL (scheme and host:port) the Pub/Sub emulator pushes events to.
func (b *InMemoryBackend) SetPushBase(base string) {
	b.mu.Lock("SetPushBase")
	defer b.mu.Unlock()

	b.pushBase = base
}

// CloseIdleConnections releases pooled delivery connections.
func (b *InMemoryBackend) CloseIdleConnections() { b.client.CloseIdleConnections() }

// Close stops pending timers.
func (b *InMemoryBackend) Close() { b.stopOps() }

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.now()) }

func clone[T proto.Message](m T) T {
	out, _ := proto.Clone(m).(T)

	return out
}

// etagOf is a checksum of the resource with its etag cleared.
func etagOf(m proto.Message) string {
	c := proto.Clone(m)
	if t, ok := c.(*eventarcpb.Trigger); ok {
		t.Etag = ""
	}

	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(raw)

	return base64.RawURLEncoding.EncodeToString(sum[:etagLen])
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.triggers = make(map[string]*triggerRec)
	b.channels = make(map[string]*channelRec)
	b.conns = make(map[string]*eventarcpb.ChannelConnection)
	b.configs = make(map[string]*eventarcpb.GoogleChannelConfig)
	b.adv = make(map[string]map[string]proto.Message)
	b.mu.Unlock()

	b.stopOps()
	b.ops.Reset()
}
