package elbv2

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	udpIdleTimeout     = 120 * time.Second
	udpMaxDatagram     = 64 * 1024
	tlsHandshakeLimit  = 10 * time.Second
	proxyProtocolAttr  = "proxy_protocol_v2.enabled"
	ppv2CmdProxy       = 0x21
	ppv2FamTCP4        = 0x11
	ppv2FamTCP6        = 0x21
	ppv2IPv4AddrBytes  = 4
	ppv2IPv6AddrBytes  = 16
	ppv2PortBytes      = 2
	ppv2SignatureBytes = 12
)

var ppv2Signature = [ppv2SignatureBytes]byte{ //nolint:gochecknoglobals // fixed wire constant
	0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
}

type closeWriter interface{ CloseWrite() error }

// pipeConns copies both directions until each side finishes or ctx is cancelled, then closes both.
func pipeConns(ctx context.Context, client net.Conn, clientR io.Reader, target net.Conn, targetR io.Reader) {
	finished := make(chan struct{})
	watcherDone := make(chan struct{})

	go func() {
		defer close(watcherDone)

		select {
		case <-ctx.Done():
			_ = client.Close()
			_ = target.Close()
		case <-finished:
		}
	}()

	var wg sync.WaitGroup

	cp := func(dst net.Conn, src io.Reader) {
		_, _ = io.Copy(dst, src)

		if cw, ok := dst.(closeWriter); ok {
			_ = cw.CloseWrite()
		}
	}

	wg.Go(func() { cp(target, clientR) })
	wg.Go(func() { cp(client, targetR) })

	wg.Wait()
	close(finished)
	<-watcherDone

	_ = client.Close()
	_ = target.Close()
}

// serveL4Listener starts the TCP/TLS accept loop and UDP relay for an NLB listener. Callers must hold b.mu.
func (b *InMemoryBackend) serveL4Listener(bl *boundListener, h *Handler, arn string) {
	if bl.tcp != nil {
		ln := bl.tcp
		if bl.endpoint.Protocol == protoTLS {
			ln = tls.NewListener(ln, &tls.Config{GetConfigForClient: b.clientConfig(arn, true)})
		}

		bl.wg.Go(func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}

				bl.wg.Go(func() { h.serveTCPConn(bl.ctx, b, arn, c) })
			}
		})
	}

	if bl.udp != nil {
		bl.wg.Go(func() {
			h.serveUDP(bl, b, arn)
		})
	}
}

// pickL4Target resolves the forward action's target group and one routable target address.
func (h *Handler) pickL4Target(
	be StorageBackend,
	l *Listener,
	lb *LoadBalancer,
	client netip.Addr,
) (*TargetGroup, netip.AddrPort, bool) {
	var tuples []TargetGroupTuple

	for _, a := range l.DefaultActions {
		if a.Type == actionTypeForward {
			tuples = forwardTuples(a)

			break
		}
	}

	tgArn := pickWeightedGroup(tuples)
	if tgArn == "" {
		return nil, netip.AddrPort{}, false
	}

	groups, err := be.DescribeTargetGroups([]string{tgArn}, nil, "")
	if err != nil || len(groups) == 0 {
		return nil, netip.AddrPort{}, false
	}

	health, err := be.DescribeTargetHealth(tgArn)
	if err != nil {
		return nil, netip.AddrPort{}, false
	}

	cands := candidateTargets(health)
	if len(cands) == 0 {
		return nil, netip.AddrPort{}, false
	}

	tg := &groups[0]
	t, _ := h.dp.chooseTarget(tg, cands, &dpRequest{
		r: &http.Request{Header: http.Header{}}, client: client, lb: lb, listener: l,
	})

	ap, ok := h.resolveAddr(tg, t)

	return tg, ap, ok
}

func (h *Handler) serveTCPConn(ctx context.Context, b *InMemoryBackend, arn string, c net.Conn) {
	defer c.Close()

	if tc, ok := c.(*tls.Conn); ok {
		hctx, cancel := context.WithTimeout(ctx, tlsHandshakeLimit)
		err := tc.HandshakeContext(hctx)

		cancel()

		if err != nil {
			return
		}
	}

	l, lb, ok := b.listenerView(arn)
	if !ok {
		return
	}

	client := netip.Addr{}
	if ap, err := netip.ParseAddrPort(c.RemoteAddr().String()); err == nil {
		client = ap.Addr().Unmap()
	}

	tg, ap, ok := h.pickL4Target(b, l, lb, client)
	if !ok {
		return
	}

	d := net.Dialer{Timeout: dialTimeout}

	tconn, err := d.DialContext(ctx, "tcp", ap.String())
	if err != nil {
		logger.Load(ctx).DebugContext(ctx, "elbv2: nlb target dial failed", "target", ap.String(), "error", err)

		return
	}

	defer tconn.Close()

	if tg.TargetGroupAttributes[proxyProtocolAttr] == attrValueTrue {
		if _, err = tconn.Write(proxyProtocolV2Header(c.RemoteAddr(), c.LocalAddr(), l.Port)); err != nil {
			return
		}
	}

	if tg.Protocol == protoTLS {
		tc := tls.Client(tconn, targetTLSConfig())
		if err = tc.HandshakeContext(ctx); err != nil {
			return
		}

		tconn = tc
	}

	pipeConns(ctx, c, c, tconn, tconn)
}

// proxyProtocolV2Header encodes a PROXY v2 header (no TLVs) for a TCP connection.
func proxyProtocolV2Header(src, dst net.Addr, listenerPort int32) []byte {
	sa, _ := netip.ParseAddrPort(src.String())
	da, _ := netip.ParseAddrPort(dst.String())
	dstPort := uint16(listenerPort) //nolint:gosec // validated 1-65535

	sIP, dIP := sa.Addr().Unmap(), da.Addr().Unmap()

	hdr := append([]byte(nil), ppv2Signature[:]...)
	hdr = append(hdr, ppv2CmdProxy)

	if sIP.Is4() && dIP.Is4() {
		hdr = append(hdr, ppv2FamTCP4)
		hdr = binary.BigEndian.AppendUint16(hdr, 2*ppv2IPv4AddrBytes+2*ppv2PortBytes)
		s, d := sIP.As4(), dIP.As4()
		hdr = append(hdr, s[:]...)
		hdr = append(hdr, d[:]...)
	} else {
		hdr = append(hdr, ppv2FamTCP6)
		hdr = binary.BigEndian.AppendUint16(hdr, 2*ppv2IPv6AddrBytes+2*ppv2PortBytes)
		s, d := sIP.As16(), dIP.As16()
		hdr = append(hdr, s[:]...)
		hdr = append(hdr, d[:]...)
	}

	hdr = binary.BigEndian.AppendUint16(hdr, sa.Port())

	return binary.BigEndian.AppendUint16(hdr, dstPort)
}

type udpSession struct{ conn net.Conn }

// serveUDP relays datagrams per client address to a target chosen when the flow starts.
func (h *Handler) serveUDP(bl *boundListener, b *InMemoryBackend, arn string) {
	var sessions sync.Map

	buf := make([]byte, udpMaxDatagram)

	for {
		n, caddr, err := bl.udp.ReadFrom(buf)
		if err != nil {
			return
		}

		key := caddr.String()

		v, ok := sessions.Load(key)
		if !ok {
			sess := h.newUDPSession(bl, b, arn, caddr, key, &sessions)
			if sess == nil {
				continue
			}

			v = sess
		}

		if s, isSess := v.(*udpSession); isSess {
			_, _ = s.conn.Write(buf[:n])
		}
	}
}

func (h *Handler) newUDPSession(
	bl *boundListener, b *InMemoryBackend, arn string, caddr net.Addr, key string, sessions *sync.Map,
) *udpSession {
	l, lb, ok := b.listenerView(arn)
	if !ok {
		return nil
	}

	client := netip.Addr{}
	if ap, err := netip.ParseAddrPort(caddr.String()); err == nil {
		client = ap.Addr().Unmap()
	}

	_, ap, ok := h.pickL4Target(b, l, lb, client)
	if !ok {
		return nil
	}

	d := net.Dialer{Timeout: dialTimeout}

	conn, err := d.DialContext(bl.ctx, "udp", ap.String())
	if err != nil {
		return nil
	}

	sess := &udpSession{conn: conn}
	sessions.Store(key, sess)

	bl.wg.Go(func() {
		defer conn.Close()
		defer sessions.Delete(key)
		defer context.AfterFunc(bl.ctx, func() { _ = conn.Close() })()

		buf := make([]byte, udpMaxDatagram)

		for {
			_ = conn.SetReadDeadline(time.Now().Add(udpIdleTimeout))

			n, rerr := conn.Read(buf)
			if rerr != nil {
				return
			}

			if _, werr := bl.udp.WriteTo(buf[:n], caddr); werr != nil {
				return
			}
		}
	})

	return sess
}
