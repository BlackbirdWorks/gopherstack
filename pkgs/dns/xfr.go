package dns

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/miekg/dns"
)

const (
	tsigFudge       = 300
	xfrDialTimeout  = 5 * time.Second
	xfrReadTimeout  = 10 * time.Second
	xfrBatch        = 100
	notifyTimeout   = 3 * time.Second
	minTransferRRs  = 2
	maxTransferSize = 1 << 22
)

// ErrTransferRefused is returned when the primary answers an AXFR request with an error rcode.
var ErrTransferRefused = errors.New("dns: zone transfer refused")

// ErrBadTransfer is returned when an AXFR stream is malformed.
var ErrBadTransfer = errors.New("dns: malformed zone transfer")

// ErrNotifyRefused is returned when a NOTIFY is answered with an error rcode.
var ErrNotifyRefused = errors.New("dns: notify refused")

// StreamInterceptor is an optional Interceptor extension for exchanges that need the response writer: it writes any
// number of messages and reports true, or reports false to let the query take the single-message path.
type StreamInterceptor interface {
	InterceptStream(w dns.ResponseWriter, req *dns.Msg) bool
}

// FetchZone pulls zone from the primary at addr with AXFR over TCP and returns every record, the opening and closing
// SOA included. A non-nil key signs the request and requires every response message to verify.
func FetchZone(ctx context.Context, addr, zone string, key *TSIGKey) ([]dns.RR, error) {
	d := net.Dialer{Timeout: xfrDialTimeout}

	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dns: dial %s: %w", addr, err)
	}

	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	tr := &dns.Transfer{Conn: &dns.Conn{Conn: conn}, ReadTimeout: xfrReadTimeout}

	req := new(dns.Msg)
	req.SetAxfr(dns.Fqdn(zone))

	if key != nil {
		if err = CheckTSIGAlgorithm(key.Algorithm); err != nil {
			_ = conn.Close()

			return nil, err
		}

		tr.TsigProvider = SingleKeyProvider(*key)
		req.SetTsig(CanonicalKeyName(key.Name), CanonicalTSIGAlgorithm(key.Algorithm), tsigFudge, time.Now().Unix())
	}

	envs, err := tr.In(req, addr)
	if err != nil {
		_ = conn.Close()

		return nil, fmt.Errorf("dns: axfr %s from %s: %w", zone, addr, err)
	}

	return collect(envs, zone)
}

func collect(envs chan *dns.Envelope, zone string) ([]dns.RR, error) {
	var (
		out      []dns.RR
		firstErr error
	)

	for env := range envs {
		if env.Error != nil && firstErr == nil {
			firstErr = env.Error
		}

		if firstErr == nil {
			out = append(out, env.RR...)
		}

		if len(out) > maxTransferSize {
			firstErr = fmt.Errorf("%w: more than %d records", ErrBadTransfer, maxTransferSize)
		}
	}

	if firstErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrTransferRefused, firstErr)
	}

	if len(out) < minTransferRRs || out[0].Header().Rrtype != dns.TypeSOA ||
		out[len(out)-1].Header().Rrtype != dns.TypeSOA ||
		CanonicalKeyName(out[0].Header().Name) != CanonicalKeyName(zone) {
		return nil, fmt.Errorf("%w: stream is not bracketed by the zone SOA", ErrBadTransfer)
	}

	return out, nil
}

// ServeTransfer streams rrs (opening SOA, records, closing SOA) to the AXFR requester in batches. TSIG is applied by
// the response writer when the request was signed.
func ServeTransfer(w dns.ResponseWriter, req *dns.Msg, rrs []dns.RR) error {
	ch := make(chan *dns.Envelope)
	errCh := make(chan error, 1)

	go func() {
		errCh <- (&dns.Transfer{}).Out(w, req, ch)
	}()

	for start := 0; start < len(rrs); start += xfrBatch {
		select {
		case ch <- &dns.Envelope{RR: rrs[start:min(start+xfrBatch, len(rrs))]}:
		case err := <-errCh:
			return err
		}
	}

	close(ch)

	return <-errCh
}

// SendNotify sends a NOTIFY (RFC 1996) for zone to addr over UDP, signed when key is non-nil.
func SendNotify(ctx context.Context, addr, zone string, soa dns.RR, key *TSIGKey) error {
	m := new(dns.Msg)
	m.SetNotify(dns.Fqdn(zone))
	m.Authoritative = true

	if soa != nil {
		m.Answer = []dns.RR{soa}
	}

	c := &dns.Client{Net: "udp", Timeout: notifyTimeout}

	if key != nil {
		if err := CheckTSIGAlgorithm(key.Algorithm); err != nil {
			return err
		}

		c.TsigProvider = SingleKeyProvider(*key)
		m.SetTsig(CanonicalKeyName(key.Name), CanonicalTSIGAlgorithm(key.Algorithm), tsigFudge, time.Now().Unix())
	}

	resp, _, err := c.ExchangeContext(ctx, m, addr)
	if err != nil {
		return fmt.Errorf("dns: notify %s at %s: %w", zone, addr, err)
	}

	if resp.Rcode != dns.RcodeSuccess {
		return fmt.Errorf("%w: %s", ErrNotifyRefused, dns.RcodeToString[resp.Rcode])
	}

	return nil
}
