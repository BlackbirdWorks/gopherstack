package mq

import (
	"net"
	"strings"
)

const (
	rabbitClusterStandbys = 2
	portNameAMQP          = "amqp"
	portNameConsole       = "console"
)

const (
	activeMQTransports = `<sslContext><sslContext keyStore="file:${activemq.conf}/broker.ks" ` +
		`keyStorePassword="password"/></sslContext>` +
		`<transportConnectors>` +
		`<transportConnector name="openwire" uri="tcp://0.0.0.0:61616"/>` +
		`<transportConnector name="openwire+ssl" uri="ssl://0.0.0.0:61617"/>` +
		`<transportConnector name="amqp" uri="amqp://0.0.0.0:5672"/>` +
		`<transportConnector name="amqp+ssl" uri="amqp+ssl://0.0.0.0:5671"/>` +
		`<transportConnector name="stomp" uri="stomp://0.0.0.0:61613"/>` +
		`<transportConnector name="stomp+ssl" uri="stomp+ssl://0.0.0.0:61614"/>` +
		`<transportConnector name="mqtt" uri="mqtt://0.0.0.0:1883"/>` +
		`<transportConnector name="mqtt+ssl" uri="mqtt+ssl://0.0.0.0:8883"/>` +
		`<transportConnector name="wss" uri="wss://0.0.0.0:61619"/>` +
		`</transportConnectors>`

	activeMQTLSScript = `set -e
C=/opt/apache-activemq/conf
if [ ! -f $C/.gopherstack-tls ]; then
rm -f $C/broker.ks
keytool -genkeypair -alias broker -keyalg RSA -keysize 2048 -validity 3650 -dname "CN=$GS_TLS_HOST" ` +
		`-ext "SAN=$GS_TLS_SAN" -keystore $C/broker.ks -storetype JKS -storepass password -keypass password
sed -i '/<transportConnectors>/,/<\/transportConnectors>/c\` + activeMQTransports + `' $C/activemq.xml
sed -i -e 's/<!-- bean id="SecureConnector"/<bean id="SecureConnector"/' -e 's#</bean -->#</bean>#' $C/jetty.xml
touch $C/.gopherstack-tls
fi
exec /usr/local/bin/entrypoint.sh "$@"`

	rabbitMQTLSScript = `set -e
D=/etc/rabbitmq/tls
if [ ! -f $D/cert.pem ]; then
mkdir -p $D
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=$GS_TLS_HOST" -addext "subjectAltName=$GS_TLS_SAN" ` +
		`-keyout $D/key.pem -out $D/cert.pem
chown -R rabbitmq:rabbitmq $D
chmod 600 $D/key.pem
cat > /etc/rabbitmq/conf.d/30-gopherstack-tls.conf <<CONF
listeners.ssl.default = 5671
ssl_options.certfile = $D/cert.pem
ssl_options.keyfile = $D/key.pem
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
management.tcp.port = 15672
management.ssl.port = 15671
management.ssl.certfile = $D/cert.pem
management.ssl.keyfile = $D/key.pem
CONF
fi
exec docker-entrypoint.sh "$@"`
)

func portMapsFor(engine string) []portMap {
	if engine == EngineTypeRabbitMQ {
		return []portMap{{portNameAMQP, 5672}, {"amqp-tls", 5671}, {portNameConsole, 15672}, {"console-tls", 15671}}
	}

	return []portMap{
		{"openwire", 61616}, {"openwire-tls", 61617},
		{portNameAMQP, 5672}, {"amqp-tls", 5671},
		{"stomp", 61613}, {"stomp-tls", 61614},
		{"mqtt", 1883}, {"mqtt-tls", 8883},
		{"wss", 61619},
		{portNameConsole, 8161}, {"console-tls", 8162},
	}
}

// tlsEntrypoint wraps the image entrypoint with a script that generates a self-signed certificate
// and enables the broker's TLS listeners before handing over to the image's own entrypoint.
func tlsEntrypoint(engine string) ([]string, []string) {
	if engine == EngineTypeRabbitMQ {
		return []string{"sh", "-c", rabbitMQTLSScript, "gopherstack-mq"}, []string{"rabbitmq-server"}
	}

	return []string{"sh", "-c", activeMQTLSScript, "gopherstack-mq"}, []string{"activemq", "console"}
}

func tlsEnv(host string) []string {
	cn, san := "localhost", []string{"DNS:localhost", "IP:127.0.0.1"}

	if host != "" && host != "localhost" && host != brokerDefaultHost {
		cn = host
		if net.ParseIP(host) != nil {
			san = append(san, "IP:"+host)
		} else {
			san = append(san, "DNS:"+host)
		}
	}

	return []string{"GS_TLS_HOST=" + cn, "GS_TLS_SAN=" + strings.Join(san, ",")}
}

// liveEndpoints returns the AWS-form TLS endpoints and console URL for one broker node.
func (e *brokerEngine) liveEndpoints(engine string, n *liveNode) ([]string, string) {
	return e.endpointsWith(engine, func(name string) string { return e.nodeAddr(n, name) })
}

// endpointsWith renders the TLS endpoints with addr resolving each port name to a host:port.
func (e *brokerEngine) endpointsWith(engine string, addr func(name string) string) ([]string, string) {
	if engine == EngineTypeRabbitMQ {
		return []string{"amqps://" + addr("amqp-tls")}, "https://" + addr("console-tls")
	}

	return []string{
		"ssl://" + addr("openwire-tls"),
		"amqp+ssl://" + addr("amqp-tls"),
		"stomp+ssl://" + addr("stomp-tls"),
		"mqtt+ssl://" + addr("mqtt-tls"),
		"wss://" + addr("wss"),
	}, "https://" + addr("console-tls")
}

// standbyCount is how many extra containers a deployment mode runs beside the primary.
func standbyCount(engine, mode string) int {
	switch {
	case mode == DeploymentModeActiveStandby && engine == EngineTypeActiveMQ:
		return 1
	case mode == DeploymentModeCluster && engine == EngineTypeRabbitMQ:
		return rabbitClusterStandbys
	default:
		return 0
	}
}
