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
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const brokerServerCertValidity = 24 * time.Hour

var errNoPeerCertificate = errors.New("iot broker: client presented no certificate")

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

func (b *Broker) principalOf(cl *mqtt.Client) (string, *InMemoryBackend) {
	conn, ok := cl.Net.Conn.(*tls.Conn)
	if !ok {
		return "", nil
	}

	peers := conn.ConnectionState().PeerCertificates
	if len(peers) == 0 {
		return "", nil
	}

	sum := sha256.Sum256(peers[0].Raw)
	id := hex.EncodeToString(sum[:])

	for _, be := range b.backends() {
		be.mu.RLock("mqttPrincipal")
		c, found := be.certificates.Get(id)
		principal := ""

		if found && c.Status == certStatusActive {
			principal = c.ARN
		}

		be.mu.RUnlock()

		if principal != "" {
			return principal, be
		}
	}

	return "", nil
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
	principal, be := h.broker.principalOf(cl)
	if principal == "" {
		return false
	}

	return be.authorizeMQTT(principal, cl.ID, action, resource)
}

// authorizeMQTT evaluates the policies attached to principal: an explicit Deny wins, otherwise an Allow is required.
func (b *InMemoryBackend) authorizeMQTT(principal, clientID, action, resource string) bool {
	b.mu.RLock("authorizeMQTT")
	defer b.mu.RUnlock()

	vars := map[string]string{"${iot:ClientId}": clientID}

	if principals, ok := b.thingPrincipals[clientID]; ok && slices.Contains(principals, principal) {
		vars["${iot:Connection.Thing.ThingName}"] = clientID
	}

	full := arn.Build("iot", b.region, b.accountID, resource)
	allowed := false

	for name, targets := range b.policyTargets {
		if !slices.Contains(targets, principal) {
			continue
		}

		p, ok := b.policies.Get(name)
		if !ok {
			continue
		}

		stmts, err := parseIoTPolicyDocument(p.PolicyDocument)
		if err != nil {
			continue
		}

		for _, s := range stmts {
			if !statementMatches(substituteVars(s, vars), action, full) {
				continue
			}

			if strings.EqualFold(s.Effect, "Deny") {
				return false
			}

			allowed = true
		}
	}

	return allowed
}

func substituteVars(s iamStatement, vars map[string]string) iamStatement {
	oldNew := make([]string, 0, len(vars)+len(vars))
	for k, v := range vars {
		oldNew = append(oldNew, k, v)
	}

	r := strings.NewReplacer(oldNew...)
	out := iamStatement{Effect: s.Effect, Action: s.Action, Resource: make([]string, len(s.Resource))}

	for i, res := range s.Resource {
		out.Resource[i] = r.Replace(res)
	}

	return out
}
