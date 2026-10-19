package kafka

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	iamMechanism          = "AWS_MSK_IAM"
	iamProxyDialTimeout   = 10 * time.Second
	iamProxyAuthDeadline  = 30 * time.Second
	iamProxyMaxFrame      = 1 << 20
	iamProxyBackendUser   = "iamproxy"
	iamProxyAuthVersion   = 1
	kafkaErrSASLAuth      = 58
	kafkaErrUnsupportedM  = 33
	apiKeyAPIVersions     = 18
	apiKeySaslHandshake   = 17
	apiKeySaslAuth        = 36
	frameHeaderMin        = 8
	spliceDirections      = 2
	upstreamHandshakeCorr = 1
	upstreamAuthCorr      = 2
	maxInt16              = 0x7fff
	nullString            = 0xFFFF
)

var (
	errIAMProxyFrame     = errors.New("kafka iam proxy: malformed frame")
	errIAMProxyBackend   = errors.New("kafka iam proxy: backend authentication failed")
	errIAMProxyUnordered = errors.New("kafka iam proxy: unexpected request before authentication")
)

// iamAuthFunc validates an AWS_MSK_IAM payload; remote is the client address.
type iamAuthFunc func(payload []byte, remote string) error

// iamProxy terminates TLS and AWS_MSK_IAM SASL for a broker whose own listener only speaks SASL/PLAIN.
// After a successful IAM check it authenticates upstream with an internal credential and splices bytes.
type iamProxy struct {
	auth     iamAuthFunc
	ln       net.Listener
	backend  string
	password string
	wg       sync.WaitGroup
}

func startIAMProxy(bind string, cfg *tls.Config, backend, password string, auth iamAuthFunc) (*iamProxy, error) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("kafka iam proxy listen %s: %w", bind, err)
	}

	p := &iamProxy{ln: tls.NewListener(ln, cfg), backend: backend, password: password, auth: auth}
	p.wg.Go(p.serve)

	return p, nil
}

func (p *iamProxy) close() {
	_ = p.ln.Close()
	p.wg.Wait()
}

func (p *iamProxy) serve() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}

		p.wg.Go(func() { p.handle(conn) })
	}
}

func (p *iamProxy) handle(client net.Conn) {
	defer client.Close()

	up, err := (&net.Dialer{Timeout: iamProxyDialTimeout}).DialContext(context.Background(), "tcp", p.backend)
	if err != nil {
		return
	}

	defer up.Close()

	_ = client.SetDeadline(time.Now().Add(iamProxyAuthDeadline))

	if err = p.negotiate(client, up); err != nil {
		if !errors.Is(err, io.EOF) {
			logger.Load(context.Background()).DebugContext(context.Background(),
				"kafka iam proxy: connection rejected", "error", err)
		}

		return
	}

	_ = client.SetDeadline(time.Time{})

	done := make(chan struct{}, spliceDirections)
	splice := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		_ = dst.Close()
		done <- struct{}{}
	}

	go splice(up, client)
	go splice(client, up)

	<-done
	_ = client.Close()
	_ = up.Close()
	<-done
}

// negotiate relays pre-auth ApiVersions, runs the AWS_MSK_IAM exchange locally, then authenticates upstream.
func (p *iamProxy) negotiate(client, up net.Conn) error {
	handshaken := false

	for {
		frame, err := readFrame(client)
		if err != nil {
			return err
		}

		if len(frame) < frameHeaderMin {
			return errIAMProxyFrame
		}

		key := binary.BigEndian.Uint16(frame[0:2])
		version := int16(binary.BigEndian.Uint16(frame[2:4]) & maxInt16)
		corr := binary.BigEndian.Uint32(frame[4:8])

		switch key {
		case apiKeyAPIVersions:
			if err = relay(client, up, frame); err != nil {
				return err
			}
		case apiKeySaslHandshake:
			ok, herr := replyHandshake(client, frame, version, corr)
			if herr != nil {
				return herr
			}

			handshaken = ok
		case apiKeySaslAuth:
			if !handshaken {
				return errIAMProxyUnordered
			}

			return p.authenticate(client, up, frame, version, corr)
		default:
			return errIAMProxyUnordered
		}
	}
}

func relay(client, up net.Conn, frame []byte) error {
	if err := writeFrame(up, frame); err != nil {
		return err
	}

	resp, err := readFrame(up)
	if err != nil {
		return err
	}

	return writeFrame(client, resp)
}

func replyHandshake(client net.Conn, frame []byte, version int16, corr uint32) (bool, error) {
	req := kmsg.NewPtrSASLHandshakeRequest()
	req.SetVersion(version)

	if err := readRequestBody(frame, req); err != nil {
		return false, err
	}

	resp := kmsg.NewPtrSASLHandshakeResponse()
	resp.SetVersion(version)
	resp.SupportedMechanisms = []string{iamMechanism}

	ok := req.Mechanism == iamMechanism && version >= iamProxyAuthVersion
	if !ok {
		resp.ErrorCode = kafkaErrUnsupportedM
	}

	return ok, writeResponse(client, corr, resp)
}

func (p *iamProxy) authenticate(client, up net.Conn, frame []byte, version int16, corr uint32) error {
	req := kmsg.NewPtrSASLAuthenticateRequest()
	req.SetVersion(version)

	if err := readRequestBody(frame, req); err != nil {
		return err
	}

	resp := kmsg.NewPtrSASLAuthenticateResponse()
	resp.SetVersion(version)

	remote := ""
	if host, _, err := net.SplitHostPort(client.RemoteAddr().String()); err == nil {
		remote = host
	}

	if err := p.auth(req.SASLAuthBytes, remote); err != nil {
		msg := err.Error()
		resp.ErrorCode = kafkaErrSASLAuth
		resp.ErrorMessage = &msg
		_ = writeResponse(client, corr, resp)

		return err
	}

	resp.SASLAuthBytes = []byte(`{"version":"2020_10_22","request-id":"gopherstack"}`)

	if err := p.authUpstream(up); err != nil {
		return err
	}

	return writeResponse(client, corr, resp)
}

func (p *iamProxy) authUpstream(up net.Conn) error {
	hs := kmsg.NewPtrSASLHandshakeRequest()
	hs.SetVersion(iamProxyAuthVersion)
	hs.Mechanism = "PLAIN"

	if err := writeRequest(up, upstreamHandshakeCorr, hs); err != nil {
		return err
	}

	hsResp := kmsg.NewPtrSASLHandshakeResponse()
	hsResp.SetVersion(iamProxyAuthVersion)

	if err := readResponse(up, hsResp); err != nil || hsResp.ErrorCode != 0 {
		return errIAMProxyBackend
	}

	auth := kmsg.NewPtrSASLAuthenticateRequest()
	auth.SetVersion(iamProxyAuthVersion)
	auth.SASLAuthBytes = []byte("\x00" + iamProxyBackendUser + "\x00" + p.password)

	if err := writeRequest(up, upstreamAuthCorr, auth); err != nil {
		return err
	}

	authResp := kmsg.NewPtrSASLAuthenticateResponse()
	authResp.SetVersion(iamProxyAuthVersion)

	if err := readResponse(up, authResp); err != nil || authResp.ErrorCode != 0 {
		return errIAMProxyBackend
	}

	return nil
}

func readFrame(r io.Reader) ([]byte, error) {
	var sz [4]byte
	if _, err := io.ReadFull(r, sz[:]); err != nil {
		return nil, err //nolint:wrapcheck // io.EOF is matched by callers
	}

	n := binary.BigEndian.Uint32(sz[:])
	if n == 0 || n > iamProxyMaxFrame {
		return nil, errIAMProxyFrame
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("kafka iam proxy read: %w", err)
	}

	return buf, nil
}

func writeFrame(w io.Writer, payload []byte) error {
	out := make([]byte, frameLenBytes, frameLenBytes+len(payload))
	binary.BigEndian.PutUint32(out, uint32(len(payload)&maxFrameLen))
	out = append(out, payload...)

	if _, err := w.Write(out); err != nil {
		return fmt.Errorf("kafka iam proxy write: %w", err)
	}

	return nil
}

// readRequestBody skips the request header (including flexible tagged fields) and decodes the body.
func readRequestBody(frame []byte, req kmsg.Request) error {
	const fixed = 8

	if len(frame) < fixed+2 {
		return errIAMProxyFrame
	}

	off := fixed
	idLen := binary.BigEndian.Uint16(frame[off : off+2])
	off += 2

	if idLen != nullString {
		off += int(idLen)
	}

	if off > len(frame) {
		return errIAMProxyFrame
	}

	if req.IsFlexible() {
		var err error
		if off, err = skipTaggedFields(frame, off); err != nil {
			return err
		}
	}

	if err := req.ReadFrom(frame[off:]); err != nil {
		return fmt.Errorf("%w: %w", errIAMProxyFrame, err)
	}

	return nil
}

func skipTaggedFields(b []byte, off int) (int, error) {
	count, n := binary.Uvarint(b[off:])
	if n <= 0 {
		return 0, errIAMProxyFrame
	}

	off += n

	for range count {
		_, n = binary.Uvarint(b[off:])
		if n <= 0 {
			return 0, errIAMProxyFrame
		}

		off += n

		size, n2 := binary.Uvarint(b[off:])
		if n2 <= 0 {
			return 0, errIAMProxyFrame
		}

		off += n2 + int(size) //nolint:gosec // bounds checked below
		if off > len(b) {
			return 0, errIAMProxyFrame
		}
	}

	return off, nil
}

func writeResponse(w io.Writer, corr uint32, resp kmsg.Response) error {
	buf := binary.BigEndian.AppendUint32(nil, corr)
	if resp.IsFlexible() {
		buf = append(buf, 0)
	}

	return writeFrame(w, resp.AppendTo(buf))
}

func writeRequest(w io.Writer, corr uint32, req kmsg.Request) error {
	buf := binary.BigEndian.AppendUint16(nil, uint16(req.Key()&maxInt16))
	buf = binary.BigEndian.AppendUint16(buf, uint16(req.GetVersion()&maxInt16))
	buf = binary.BigEndian.AppendUint32(buf, corr)
	buf = binary.BigEndian.AppendUint16(buf, nullString)

	if req.IsFlexible() {
		buf = append(buf, 0)
	}

	return writeFrame(w, req.AppendTo(buf))
}

func readResponse(r io.Reader, resp kmsg.Response) error {
	frame, err := readFrame(r)
	if err != nil {
		return err
	}

	const corrLen = 4

	off := corrLen
	if resp.IsFlexible() {
		off++
	}

	if len(frame) < off {
		return errIAMProxyFrame
	}

	return resp.ReadFrom(frame[off:]) //nolint:wrapcheck // callers only test for failure
}

const (
	frameLenBytes = 4
	maxFrameLen   = 0x7fffffff
)
