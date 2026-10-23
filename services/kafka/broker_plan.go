package kafka

import (
	"net"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

const (
	listenerPlaintext = "PLAINTEXT"
	listenerTLS       = "TLS"
	listenerScram     = "SCRAM"
	listenerIAM       = "IAMBACK"
	listenerAdmin     = "ADMIN"

	containerPortAdmin     = "9091"
	containerPortPlaintext = "9092"
	containerPortTLS       = "9094"
	containerPortScram     = "9096"
	containerPortIAM       = "9098"

	keystoreMountDir = "/etc/kafka/secrets"
	keystoreFile     = "keystore.pem"
	truststoreFile   = "truststore.pem"

	scramJAAS = "org.apache.kafka.common.security.scram.ScramLoginModule required;"
	plainJAAS = "org.apache.kafka.common.security.plain.PlainLoginModule required user_" +
		iamProxyBackendUser + "=\"%s\";"
)

// brokerPlan is the set of listeners a cluster's security settings call for.
// A cluster without explicit encryption or authentication keeps the original single plaintext listener.
type brokerPlan struct {
	caArns       []string
	plaintext    bool
	mtls         bool
	mtlsOptional bool
	tls          bool
	scram        bool
	iam          bool
	secure       bool
}

// planFor derives listeners from EncryptionInfo and ClientAuthentication the way MSK does:
// the client-broker mode picks PLAINTEXT/TLS, SASL/SCRAM and SASL/IAM each add a SASL_SSL listener.
func planFor(c *Cluster) brokerPlan {
	explicitMode := c.EncryptionInfo != nil && c.EncryptionInfo.EncryptionInTransit != nil &&
		c.EncryptionInfo.EncryptionInTransit.ClientBroker != ""
	auth := c.ClientAuthentication

	if !explicitMode && auth == nil {
		return brokerPlan{plaintext: true}
	}

	mode := clientBrokerMode(c)
	p := brokerPlan{
		secure:    true,
		plaintext: mode == clientBrokerPlaintext || mode == clientBrokerTLSPlaintext,
		tls:       mode == clientBrokerTLS || mode == clientBrokerTLSPlaintext,
	}

	if auth != nil && auth.Sasl != nil {
		p.scram = auth.Sasl.Scram != nil && auth.Sasl.Scram.Enabled
		p.iam = auth.Sasl.Iam != nil && auth.Sasl.Iam.Enabled
	}

	p.tls = p.tls || (auth != nil && auth.TLS != nil && auth.TLS.Enabled)

	return p.withMutualTLS(auth)
}

// withMutualTLS requires client certificates on the TLS listener when TLS authentication names CAs;
// an enabled unauthenticated mode keeps the certificate optional, as MSK does.
func (p brokerPlan) withMutualTLS(auth *ClientAuthentication) brokerPlan {
	if auth == nil || auth.TLS == nil || !auth.TLS.Enabled || len(auth.TLS.CertificateAuthorityArnList) == 0 {
		return p
	}

	p.mtls = true
	p.caArns = slices.Clone(auth.TLS.CertificateAuthorityArnList)
	p.mtlsOptional = auth.Unauthenticated != nil && auth.Unauthenticated.Enabled

	return p
}

func (p brokerPlan) clientAuthMode() string {
	if p.mtlsOptional {
		return "requested"
	}

	return "required"
}

func (p brokerPlan) needsKeystore() bool { return p.tls || p.scram || p.iam }

// ManagedEndpoints are the real broker endpoints of a docker-backed cluster, one per enabled mode.
type ManagedEndpoints struct {
	Plaintext string
	TLS       string
	SaslScram string
	SaslIam   string
}

type brokerRes struct {
	proxy       *iamProxy
	ports       map[string]int
	keystoreDir string
	iamPassword string
	relays      []*sgfilter.Proxy
}

type listenerDef struct {
	name       string
	protocol   string
	cport      string
	advertised string
	bind       string
	hostPort   int
}

func (e *brokerEngine) listenerDefs(plan brokerPlan, res *brokerRes) []listenerDef {
	local := func(name, protocol, cport string, public bool) listenerDef {
		host, bind := brokerDefaultHost, container.LoopbackHost
		if public {
			host = e.cfg.Host

			if res.ports[name+relaySuffix] == 0 {
				bind = container.BindHostFor(e.cfg.Host)
			}
		}

		return listenerDef{
			name: name, protocol: protocol, cport: cport, hostPort: res.ports[name], bind: bind,
			advertised: net.JoinHostPort(host, strconv.Itoa(res.clientPort(name))),
		}
	}

	var defs []listenerDef

	if plan.secure {
		defs = append(defs, local(listenerAdmin, "PLAINTEXT", containerPortAdmin, false))
	}

	if plan.plaintext {
		defs = append(defs, local(listenerPlaintext, "PLAINTEXT", containerPortPlaintext, true))
	}

	if plan.tls {
		defs = append(defs, local(listenerTLS, "SSL", containerPortTLS, true))
	}

	if plan.scram {
		defs = append(defs, local(listenerScram, "SASL_SSL", containerPortScram, true))
	}

	if plan.iam {
		d := local(listenerIAM, "SASL_PLAINTEXT", containerPortIAM, false)
		d.advertised = net.JoinHostPort(e.cfg.Host, strconv.Itoa(res.ports[listenerIAMPublic]))
		defs = append(defs, d)
	}

	return defs
}

// listenerIAMPublic keys the port the IAM proxy listens on for clients.
const listenerIAMPublic = "IAM_PUBLIC"

func joinDefs(defs []listenerDef, f func(listenerDef) string, extra ...string) string {
	var out strings.Builder

	for _, d := range defs {
		out.WriteString(f(d) + ",")
	}

	for _, x := range extra {
		out.WriteString(x + ",")
	}

	return out.String()[:max(len(out.String())-1, 0)]
}

func (e *brokerEngine) spec(name string, plan brokerPlan, res *brokerRes) container.Spec {
	defs := e.listenerDefs(plan, res)

	ports := make([]string, 0, len(defs))
	for _, d := range defs {
		ports = append(ports, container.PortSpec(d.bind, strconv.Itoa(d.hostPort), d.cport))
	}

	env := []string{
		"KAFKA_NODE_ID=1",
		"KAFKA_PROCESS_ROLES=broker,controller",
		"KAFKA_LISTENERS=" + joinDefs(
			defs,
			func(d listenerDef) string { return d.name + "://:" + d.cport },
			"CONTROLLER://:9093",
		),
		"KAFKA_ADVERTISED_LISTENERS=" + joinDefs(
			defs,
			func(d listenerDef) string { return d.name + "://" + d.advertised },
		),
		"KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=" + joinDefs(defs,
			func(d listenerDef) string { return d.name + ":" + d.protocol }, "CONTROLLER:PLAINTEXT"),
		"KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
	}

	s := container.Spec{
		Image: e.cfg.Image,
		Name:  "gopherstack-msk-" + name + "-" + shortID(),
		Ports: ports,
		Env:   env,
	}

	if !plan.secure {
		return s
	}

	s.Env = append(s.Env, "KAFKA_INTER_BROKER_LISTENER_NAME="+listenerAdmin)

	if plan.needsKeystore() {
		s.Mounts = append(s.Mounts, res.keystoreDir+":"+keystoreMountDir+":ro")
		s.Env = append(s.Env,
			"KAFKA_SSL_KEYSTORE_TYPE=PEM",
			"KAFKA_SSL_KEYSTORE_LOCATION="+keystoreMountDir+"/"+keystoreFile)
	}

	if plan.mtls {
		s.Env = append(s.Env,
			"KAFKA_SSL_TRUSTSTORE_TYPE=PEM",
			"KAFKA_SSL_TRUSTSTORE_LOCATION="+keystoreMountDir+"/"+truststoreFile,
			"KAFKA_LISTENER_NAME_TLS_SSL_CLIENT_AUTH="+plan.clientAuthMode())
	}

	if plan.scram {
		s.Env = append(s.Env,
			"KAFKA_LISTENER_NAME_SCRAM_SASL_ENABLED_MECHANISMS=SCRAM-SHA-512",
			"KAFKA_LISTENER_NAME_SCRAM_SCRAM___SHA___512_SASL_JAAS_CONFIG="+scramJAAS)
	}

	if plan.iam {
		s.Env = append(s.Env,
			"KAFKA_LISTENER_NAME_IAMBACK_SASL_ENABLED_MECHANISMS=PLAIN",
			"KAFKA_LISTENER_NAME_IAMBACK_PLAIN_SASL_JAAS_CONFIG="+sprintfJAAS(res.iamPassword))
	}

	return s
}
