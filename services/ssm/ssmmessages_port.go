package ssm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// PortDialer opens a TCP connection for a port-forwarding session. An empty host targets the
// instance container itself; otherwise host is dialed from the instance's network context.
type PortDialer interface {
	DialTarget(ctx context.Context, containerID, host string, port int) (net.Conn, error)
}

const (
	docPortForward    = "AWS-StartPortForwardingSession"
	docPortForwardRH  = "AWS-StartPortForwardingSessionToRemoteHost"
	docSSH            = "AWS-StartSSHSession"
	defaultSSHPort    = 22
	maxPort           = 65535
	portForwardType   = "LocalPortForwarding"
	portAgentVersion  = "2.3.0.0"
	portReadChunk     = 32 * 1024
	paramPortNumber   = "portNumber"
	paramRemoteHost   = "host"
	propertyPortType  = "type"
	flagPayloadLength = 4
)

var (
	errNoPortDialer = errors.New("no port forwarding backend is configured (EC2 compute provider is not docker)")
	errBadPort      = errors.New("invalid portNumber")
	errMissingParam = errors.New("missing session parameter")
)

type portTarget struct {
	host    string
	port    int
	forward bool
}

func isPortDocument(doc string) bool {
	return doc == docPortForward || doc == docPortForwardRH || doc == docSSH
}

func firstParam(params map[string][]string, key string) string {
	if v := params[key]; len(v) > 0 {
		return strings.TrimSpace(v[0])
	}

	return ""
}

func parsePortTarget(doc string, params map[string][]string) (portTarget, error) {
	if !isPortDocument(doc) {
		return portTarget{}, fmt.Errorf("%w: %s", errBadPort, doc)
	}

	t := portTarget{forward: doc != docSSH, port: defaultSSHPort}

	if raw := firstParam(params, paramPortNumber); raw != "" || doc != docSSH {
		p, err := strconv.Atoi(raw)
		if err != nil || p < 1 || p > maxPort {
			return portTarget{}, fmt.Errorf("%w: %q", errBadPort, raw)
		}

		t.port = p
	}

	if doc == docPortForwardRH {
		if t.host = firstParam(params, paramRemoteHost); t.host == "" {
			return portTarget{}, fmt.Errorf("%w: %s", errMissingParam, paramRemoteHost)
		}
	}

	return t, nil
}

func (t portTarget) properties() map[string]any {
	props := map[string]any{paramPortNumber: strconv.Itoa(t.port), propertyPortType: ""}
	if t.forward {
		props[propertyPortType] = portForwardType
	}

	if t.host != "" {
		props[paramRemoteHost] = t.host
	}

	return props
}

// SetPortDialer enables Session Manager port forwarding and SSH sessions.
func (b *InMemoryBackend) SetPortDialer(p PortDialer) {
	b.mu.Lock("SetPortDialer")
	defer b.mu.Unlock()

	b.portDialer = p
}

func (b *InMemoryBackend) portDialerOrNil() PortDialer {
	b.mu.RLock("portDialer")
	defer b.mu.RUnlock()

	return b.portDialer
}

func (d *dataChannel) startPort(ctx context.Context) error {
	target, err := parsePortTarget(d.document, d.params)
	if err != nil {
		return err
	}

	dialer := d.b.portDialerOrNil()
	if dialer == nil {
		return errNoPortDialer
	}

	containerID, ok := d.b.shellContainer(d.region, d.target)
	if !ok {
		return fmt.Errorf("%w: %s", errTargetNotContainer, d.target)
	}

	conn, err := dialer.DialTarget(ctx, containerID, target.host, target.port)
	if err != nil {
		return err
	}

	d.conn = conn
	d.wg.Go(func() { d.pumpPort(conn) })

	return nil
}

func (d *dataChannel) pumpPort(conn net.Conn) {
	defer close(d.exited)

	buf := make([]byte, portReadChunk)

	for {
		n, err := conn.Read(buf)
		if n > 0 && d.sendOutput(PayloadOutput, buf[:n]) != nil {
			return
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				flag := make([]byte, flagPayloadLength)
				binary.BigEndian.PutUint32(flag, flagDisconnectToPort)
				_ = d.sendOutput(PayloadFlag, flag)
			}

			return
		}
	}
}
