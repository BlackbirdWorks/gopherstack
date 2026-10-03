// Package smtprelay delivers emulated SES mail to a real SMTP server using
// the standard library, asynchronously through a bounded queue.
package smtprelay

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	defaultPort      = "25"
	defaultQueueSize = 256
	dialTimeout      = 10 * time.Second
	sessionTimeout   = 60 * time.Second
)

var (
	// ErrNoRecipients is returned when a message has no deliverable recipient.
	ErrNoRecipients = errors.New("smtprelay: no recipients")
	// ErrPlaintextCredentials is returned when AUTH would send credentials unencrypted to a non-local host.
	ErrPlaintextCredentials = errors.New(
		"smtprelay: refusing to send credentials over an unencrypted non-local connection",
	)
)

// Config is the relay target, mirroring LocalStack's SMTP_HOST/SMTP_USER/SMTP_PASS.
type Config struct {
	Host string
	User string
	Pass string
}

// ConfigFromEnv reads SMTP_HOST, SMTP_USER and SMTP_PASS via getenv; ok is false when SMTP_HOST is unset.
func ConfigFromEnv(getenv func(string) string) (Config, bool) {
	host := strings.TrimSpace(getenv("SMTP_HOST"))
	if host == "" {
		return Config{}, false
	}

	return Config{Host: host, User: getenv("SMTP_USER"), Pass: getenv("SMTP_PASS")}, true
}

// Option customises a Relay.
type Option func(*Relay)

// WithTLSConfig overrides the STARTTLS client configuration (tests use it to trust a self-signed cert).
func WithTLSConfig(c *tls.Config) Option { return func(r *Relay) { r.tlsConfig = c } }

// WithQueueSize sets the bounded queue capacity; values below 1 are ignored.
func WithQueueSize(n int) Option {
	return func(r *Relay) {
		if n > 0 {
			r.queueSize = n
		}
	}
}

// Relay queues messages and delivers them from a single worker goroutine.
type Relay struct {
	tlsConfig *tls.Config
	queue     chan Message
	done      chan struct{}
	cancel    context.CancelFunc
	cfg       Config
	addr      string
	hostname  string
	wg        sync.WaitGroup
	closeOnce sync.Once
	queueSize int
}

// New starts a relay worker for cfg. Call Close to stop it.
func New(cfg Config, opts ...Option) *Relay {
	addr, host := splitHostPort(cfg.Host)
	r := &Relay{cfg: cfg, addr: addr, hostname: host, queueSize: defaultQueueSize, done: make(chan struct{})}

	for _, o := range opts {
		o(r)
	}

	r.queue = make(chan Message, r.queueSize)

	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(1)

	go r.run(ctx)

	return r
}

// Enqueue adds m to the queue without blocking. It returns false when the queue is full or the relay is closed.
func (r *Relay) Enqueue(m Message) bool {
	select {
	case <-r.done:
		return false
	default:
	}

	select {
	case r.queue <- m:
		return true
	default:
		return false
	}
}

// Close stops the worker, abandoning queued messages, and waits for it to exit. It is idempotent.
func (r *Relay) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
		r.cancel()
	})
	r.wg.Wait()
}

func (r *Relay) run(ctx context.Context) {
	defer r.wg.Done()

	for {
		select {
		case <-r.done:
			return
		case m := <-r.queue:
			if err := r.deliver(ctx, m); err != nil && ctx.Err() == nil {
				logger.Load(ctx).WarnContext(ctx, "SES SMTP relay delivery failed",
					"messageId", m.ID, "recipients", len(m.recipients()), "error", err)
			}
		}
	}
}

func (r *Relay) deliver(ctx context.Context, m Message) error {
	rcpts := m.recipients()
	if len(rcpts) == 0 {
		return ErrNoRecipients
	}

	body, err := m.Bytes()
	if err != nil {
		return err
	}

	from, err := bareAddress(m.envelopeFrom())
	if err != nil {
		return fmt.Errorf("envelope sender: %w", err)
	}

	d := net.Dialer{Timeout: dialTimeout}

	conn, err := d.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	_ = conn.SetDeadline(time.Now().Add(sessionTimeout))
	defer context.AfterFunc(ctx, func() { _ = conn.Close() })()

	c, err := smtp.NewClient(conn, r.hostname)
	if err != nil {
		_ = conn.Close()

		return fmt.Errorf("greeting: %w", err)
	}
	defer c.Close()

	if err = r.negotiate(c); err != nil {
		return err
	}

	return send(c, from, rcpts, body)
}

func (r *Relay) negotiate(c *smtp.Client) error {
	secure := false

	if ok, _ := c.Extension("STARTTLS"); ok {
		cfg := r.tlsConfig
		if cfg == nil {
			cfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}

		cfg = cfg.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName = r.hostname
		}

		if err := c.StartTLS(cfg); err != nil {
			return fmt.Errorf("starttls: %w", err)
		}

		secure = true
	}

	if r.cfg.User == "" {
		return nil
	}

	if !secure && !isLocalHost(r.hostname) {
		return ErrPlaintextCredentials
	}

	if err := c.Auth(smtp.PlainAuth("", r.cfg.User, r.cfg.Pass, r.hostname)); err != nil {
		return fmt.Errorf("auth: %w", err)
	}

	return nil
}

func send(c *smtp.Client, from string, rcpts []string, body []byte) error {
	if err := c.Mail(from); err != nil {
		return fmt.Errorf("MAIL FROM: %w", err)
	}

	for _, rc := range rcpts {
		if err := c.Rcpt(rc); err != nil {
			return fmt.Errorf("RCPT TO: %w", err)
		}
	}

	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("DATA: %w", err)
	}

	if _, err = w.Write(body); err != nil {
		_ = w.Close()

		return fmt.Errorf("write body: %w", err)
	}

	if err = w.Close(); err != nil {
		return fmt.Errorf("end of data: %w", err)
	}

	return c.Quit()
}

func splitHostPort(hostport string) (string, string) {
	if h, p, err := net.SplitHostPort(hostport); err == nil {
		return net.JoinHostPort(h, p), h
	}

	h := strings.Trim(hostport, "[]")

	return net.JoinHostPort(h, defaultPort), h
}

func isLocalHost(h string) bool {
	return h == "localhost" || h == "127.0.0.1" || h == "::1"
}

func bareAddress(s string) (string, error) {
	a, err := mail.ParseAddress(s)
	if err != nil {
		return "", fmt.Errorf("parse address: %w", err)
	}

	return a.Address, nil
}
