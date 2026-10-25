package iot

import (
	"context"
	"crypto/tls"
	"net/url"
	"strings"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	authorizerNameParam      = "x-amz-customauthorizer-name"
	authorizerSignatureParam = "x-amz-customauthorizer-signature"
)

// BrokerClock is the broker's time source for authorizer policy refreshes and forced disconnects.
type BrokerClock struct {
	Now       func() time.Time
	AfterFunc func(d time.Duration, f func()) (stop func() bool)
}

func realBrokerClock() BrokerClock {
	return BrokerClock{
		Now: time.Now,
		AfterFunc: func(d time.Duration, f func()) func() bool {
			return time.AfterFunc(d, f).Stop
		},
	}
}

// SetClock replaces the broker's time source (tests drive expiry deterministically through it).
func (b *Broker) SetClock(c BrokerClock) { b.clock.Store(&c) }

func (b *Broker) brokerClock() BrokerClock {
	if c := b.clock.Load(); c != nil {
		return *c
	}

	return realBrokerClock()
}

func (b *Broker) lifecycleContext() context.Context {
	if c := b.runCtx.Load(); c != nil {
		return *c
	}

	return context.Background()
}

// customSession is the cached result of a custom-authorizer invocation for one connection.
type customSession struct {
	refreshAt time.Time
	stop      func() bool
	req       AuthorizerRequest
	authName  string
	docs      []string
	refresh   time.Duration
	mu        sync.Mutex
}

// connectCustom authenticates a client that presented no certificate through a custom authorizer.
func (h *authHook) connectCustom(cl *mqtt.Client, pk packets.Packet) bool {
	b := h.broker
	username := string(pk.Connect.Username)
	_, query, _ := strings.Cut(username, "?")

	params, err := url.ParseQuery(query)
	if err != nil {
		return false
	}

	name := params.Get(authorizerNameParam)
	if name == "" {
		def, derr := b.backend.DescribeDefaultAuthorizer()
		if derr != nil {
			return false
		}

		name = def
	}

	auth, err := b.backend.DescribeAuthorizer(name)
	if err != nil {
		return false
	}

	req := AuthorizerRequest{
		MQTT:           &AuthorizerMQTTContext{ClientID: cl.ID, Username: username, Password: pk.Connect.Password},
		TLS:            &AuthorizerTLSContext{ServerName: tlsServerName(cl)},
		TokenSignature: params.Get(authorizerSignatureParam),
	}

	if auth.TokenKeyName != "" {
		req.Token = params.Get(auth.TokenKeyName)
	}

	res, err := b.backend.InvokeAuthorizer(b.lifecycleContext(), name, &req)
	if err != nil || !res.IsAuthenticated {
		return false
	}

	sess := &customSession{
		authName: name, req: req, docs: res.PolicyDocuments,
		refresh: time.Duration(res.RefreshAfterInSeconds) * time.Second,
	}

	clock := b.brokerClock()
	sess.refreshAt = clock.Now().Add(sess.refresh)

	if !b.backend.authorizeDocs(sess.docs, cl.ID, "iot:Connect", "client/"+cl.ID) {
		return false
	}

	disconnect := time.Duration(res.DisconnectAfterInSeconds) * time.Second
	sess.stop = clock.AfterFunc(disconnect, func() { h.disconnect(cl) })
	h.sessions.Store(cl, sess)

	return true
}

func (h *authHook) disconnect(cl *mqtt.Client) {
	if s := h.broker.server.Load(); s != nil {
		_ = s.DisconnectClient(cl, packets.CodeDisconnect)
	}
}

func tlsServerName(cl *mqtt.Client) string {
	if conn, ok := cl.Net.Conn.(*tls.Conn); ok {
		return conn.ConnectionState().ServerName
	}

	return ""
}

// customAllowed evaluates action against the connection's cached authorizer policies, re-invoking
// the authorizer first once refreshAfterInSeconds has elapsed.
func (h *authHook) customAllowed(cl *mqtt.Client, action, resource string) bool {
	v, ok := h.sessions.Load(cl)
	if !ok {
		return false
	}

	sess, _ := v.(*customSession)

	sess.mu.Lock()
	defer sess.mu.Unlock()

	if sess.refresh > 0 && !h.broker.brokerClock().Now().Before(sess.refreshAt) {
		res, err := h.broker.backend.InvokeAuthorizer(h.broker.lifecycleContext(), sess.authName, &sess.req)
		if err != nil || !res.IsAuthenticated {
			go h.disconnect(cl)

			return false
		}

		sess.docs = res.PolicyDocuments
		sess.refresh = time.Duration(res.RefreshAfterInSeconds) * time.Second
		sess.refreshAt = h.broker.brokerClock().Now().Add(sess.refresh)
	}

	return h.broker.backend.authorizeDocs(sess.docs, cl.ID, action, resource)
}

func (h *authHook) dropSession(cl *mqtt.Client) {
	if v, ok := h.sessions.LoadAndDelete(cl); ok {
		if sess, isSess := v.(*customSession); isSess && sess.stop != nil {
			sess.stop()
		}
	}
}

// authorizeDocs evaluates authorizer-returned policy documents; ${iot:ClientId} is substituted.
func (b *InMemoryBackend) authorizeDocs(docs []string, clientID, action, resource string) bool {
	ctx := iam.ConditionContext{Extra: map[string]string{ctxKeyClientID: clientID}}
	escaped := iam.ConditionContext{Extra: map[string]string{ctxKeyClientID: jsonEscape(clientID)}}

	substituted := make([]string, 0, len(docs))
	for _, d := range docs {
		substituted = append(substituted, iam.SubstituteVariables(d, escaped))
	}

	b.mu.RLock("authorizeDocs")
	full := arn.Build("iot", b.region, b.accountID, resource)
	b.mu.RUnlock()

	return iam.EvaluatePolicies(substituted, action, full, ctx) == iam.EvalAllow
}
