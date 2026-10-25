package iot

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

const brokerServerCertValidity = 24 * time.Hour

var (
	errNoPeerCertificate          = errors.New("iot broker: client presented no certificate")
	errCertificateOutsideValidity = errors.New("iot broker: client certificate is outside its validity period")
)

// SetAuthRequired switches the broker to X.509 mutual-TLS authentication with IoT policy
// authorisation of CONNECT, PUBLISH, SUBSCRIBE and RECEIVE. Call before Start.
func (b *Broker) SetAuthRequired(on bool) { b.authRequired.Store(on) }

// brokerTLSConfig builds the listener config: an ephemeral self-signed server certificate and
// a client certificate that must be registered and ACTIVE.
func (b *Broker) brokerTLSConfig() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("iot broker: server key: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "gopherstack-iot"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(brokerServerCertValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return nil, fmt.Errorf("iot broker: server certificate: %w", err)
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		ClientAuth:   tls.RequireAnyClientCert,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errNoPeerCertificate
			}

			now := time.Now()
			if leaf := cs.PeerCertificates[0]; now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
				return errCertificateOutsideValidity
			}

			if b.activeCertARN(cs.PeerCertificates[0].Raw) == "" {
				return fmt.Errorf("iot broker: %w", ErrCertificateNotFound)
			}

			return nil
		},
	}, nil
}

func (b *Broker) backends() []*InMemoryBackend {
	out := []*InMemoryBackend{b.backend}
	if b.others != nil {
		out = append(out, b.others()...)
	}

	return out
}

// activeCertARN is the ARN of the registered ACTIVE certificate with this DER encoding, or "".
func (b *Broker) activeCertARN(der []byte) string {
	sum := sha256.Sum256(der)
	id := hex.EncodeToString(sum[:])

	for _, be := range b.backends() {
		be.mu.RLock("mqttActiveCert")
		c, ok := be.certificates.Get(id)
		active := ok && c.Status == certStatusActive
		arnStr := ""

		if active {
			arnStr = c.ARN
		}

		be.mu.RUnlock()

		if active {
			return arnStr
		}
	}

	return ""
}

func (b *Broker) principalOf(cl *mqtt.Client) (string, *x509.Certificate, *InMemoryBackend) {
	conn, ok := cl.Net.Conn.(*tls.Conn)
	if !ok {
		return "", nil, nil
	}

	peers := conn.ConnectionState().PeerCertificates
	if len(peers) == 0 {
		return "", nil, nil
	}

	sum := sha256.Sum256(peers[0].Raw)
	id := hex.EncodeToString(sum[:])

	for _, cand := range b.backends() {
		cand.mu.RLock("mqttPrincipal")
		c, found := cand.certificates.Get(id)
		arnStr := ""

		if found && c.Status == certStatusActive {
			arnStr = c.ARN
		}

		cand.mu.RUnlock()

		if arnStr != "" {
			return arnStr, peers[0], cand
		}
	}

	return "", nil, nil
}

// authHook enforces IoT policies on a broker running with authentication enabled.
type authHook struct {
	mqtt.HookBase

	broker  *Broker
	pending sync.Map
}

func (h *authHook) ID() string { return "iot-auth-hook" }

func (h *authHook) Provides(b byte) bool {
	switch b {
	case mqtt.OnConnectAuthenticate, mqtt.OnACLCheck, mqtt.OnPacketRead, mqtt.OnSubscribed, mqtt.OnDisconnect:
		return true
	default:
		return false
	}
}

func pendingKey(clientID, filter string) string { return clientID + "\x00" + filter }

// OnPacketRead remembers SUBSCRIBE filters so the ACL callback can tell Subscribe from Receive.
func (h *authHook) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if pk.FixedHeader.Type == packets.Subscribe {
		for _, f := range pk.Filters {
			h.pending.Store(pendingKey(cl.ID, f.Filter), struct{}{})
		}
	}

	return pk, nil
}

func (h *authHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, _ []byte) {
	for _, f := range pk.Filters {
		h.pending.Delete(pendingKey(cl.ID, f.Filter))
	}
}

func (h *authHook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	h.pending.Range(func(k, _ any) bool {
		if key, ok := k.(string); ok && strings.HasPrefix(key, cl.ID+"\x00") {
			h.pending.Delete(k)
		}

		return true
	})
}

func (h *authHook) OnConnectAuthenticate(cl *mqtt.Client, _ packets.Packet) bool {
	return h.allowed(cl, "iot:Connect", "client/"+cl.ID)
}

func (h *authHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	switch {
	case write:
		return h.allowed(cl, "iot:Publish", "topic/"+topic)
	case h.isPendingFilter(cl.ID, topic):
		return h.allowed(cl, "iot:Subscribe", "topicfilter/"+topic)
	default:
		return h.allowed(cl, "iot:Receive", "topic/"+topic)
	}
}

func (h *authHook) isPendingFilter(clientID, topic string) bool {
	_, ok := h.pending.Load(pendingKey(clientID, topic))

	return ok
}

func (h *authHook) allowed(cl *mqtt.Client, action, resource string) bool {
	principal, cert, be := h.broker.principalOf(cl)
	if principal == "" {
		return false
	}

	return be.authorizeMQTT(principal, cl.ID, cert, action, resource)
}

// authorizeMQTT evaluates the policies attached to principal, and for a thing connecting
// as itself those of its thing groups and their ancestors. An explicit Deny wins.
func (b *InMemoryBackend) authorizeMQTT(
	principal, clientID string, cert *x509.Certificate, action, resource string,
) bool {
	b.mu.RLock("authorizeMQTT")
	defer b.mu.RUnlock()

	ctx := iam.ConditionContext{Extra: map[string]string{
		"iot:clientid":                    clientID,
		"iot:connection.thing.isattached": "false",
	}}

	maps.Copy(ctx.Extra, certPolicyVariables(cert))

	targets := []string{principal}

	if principals, ok := b.thingPrincipals[clientID]; ok && slices.Contains(principals, principal) {
		ctx.Extra["iot:connection.thing.isattached"] = keyBoolTrue
		ctx.Extra["iot:connection.thing.thingname"] = clientID

		if t, found := b.things.Get(clientID); found {
			ctx.Extra["iot:connection.thing.thingtypename"] = t.ThingTypeName

			for k, v := range t.Attributes {
				ctx.Extra["iot:connection.thing.attributes["+strings.ToLower(k)+"]"] = v
			}
		}

		targets = append(targets, b.thingGroupARNsLocked(clientID)...)
	}

	escaped := iam.ConditionContext{Extra: make(map[string]string, len(ctx.Extra))}
	for k, v := range ctx.Extra {
		escaped.Extra[k] = jsonEscape(v)
	}

	var docs []string

	for name, attached := range b.policyTargets {
		if !slices.ContainsFunc(attached, func(t string) bool { return slices.Contains(targets, t) }) {
			continue
		}

		if p, ok := b.policies.Get(name); ok {
			docs = append(docs, iam.SubstituteVariables(p.PolicyDocument, escaped))
		}
	}

	full := arn.Build("iot", b.region, b.accountID, resource)

	return iam.EvaluatePolicies(docs, action, full, ctx) == iam.EvalAllow
}

// thingGroupARNsLocked returns the ARNs of every group containing thingName plus all their
// ancestor groups. Call with b.mu held.
func (b *InMemoryBackend) thingGroupARNsLocked(thingName string) []string {
	var out []string

	seen := map[string]bool{}

	for group, members := range b.thingGroupMembers {
		if !slices.Contains(members, thingName) {
			continue
		}

		for name := group; name != "" && !seen[name]; {
			seen[name] = true

			tg, ok := b.thingGroups.Get(name)
			if !ok {
				break
			}

			out = append(out, tg.ThingGroupARN)
			name = tg.ParentGroupName
		}
	}

	return out
}

func jsonEscape(v string) string {
	raw, err := json.Marshal(v)
	if err != nil || len(raw) < 2 {
		return ""
	}

	return string(raw[1 : len(raw)-1])
}
