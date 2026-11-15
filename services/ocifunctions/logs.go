package ocifunctions

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	logReadLimit   = 10 * time.Second
	syslogDial     = 5 * time.Second
	syslogMaxLine  = 8192
	syslogInfoPri  = 14
	syslogErrorPri = 11
	syslogHost     = "gopherstack"
	streamStdout   = "stdout"
	streamStderr   = "stderr"
)

// LogLine is one line a function wrote to stdout or stderr during an invocation.
type LogLine struct {
	Time         time.Time
	AppID        string
	AppName      string
	FunctionID   string
	FunctionName string
	CallID       string
	Stream       string
	Text         string
	LineFormat   string
}

// SetLogSink installs the receiver of function output lines (OCI Logging); it returns how many logs took the line.
func (b *InMemoryBackend) SetLogSink(fn func(LogLine) int) {
	b.mu.Lock("SetLogSink")
	b.logSink = fn
	b.mu.Unlock()
}

func fresh(all []byte, seen *int) []string {
	if *seen > len(all) {
		*seen = 0
	}

	tail := all[*seen:]
	*seen = len(all)

	var lines []string

	for l := range strings.SplitSeq(strings.TrimRight(string(tail), "\n"), "\n") {
		if l = strings.TrimRight(l, "\r"); l != "" {
			lines = append(lines, l)
		}
	}

	return lines
}

// captureLogs forwards what the container wrote since the previous call; runtimes that cannot read logs yield nothing.
func (e *executor) captureLogs(rt Runner, w *warm, p execPlan, callID string) {
	lr, ok := rt.(container.LogReader)
	if !ok || e.logs == nil || w.id == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(e.runCtx), logReadLimit)
	defer cancel()

	var so, se bytes.Buffer
	if err := lr.ReadLogs(ctx, w.id, "", false, &so, &se); err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "ocifunctions: reading container logs failed", "container", w.id, "error", err)

		return
	}

	if lines := fresh(so.Bytes(), &w.outSeen); len(lines) > 0 {
		e.logs(p, callID, streamStdout, lines)
	}

	if lines := fresh(se.Bytes(), &w.errSeen); len(lines) > 0 {
		e.logs(p, callID, streamStderr, lines)
	}
}

// routeLogs sends output to OCI Logging; when no log of the application took it, it goes to the syslog URL.
func (b *InMemoryBackend) routeLogs(p execPlan, callID, stream string, lines []string) {
	b.mu.RLock("routeLogs")
	sink := b.logSink
	b.mu.RUnlock()

	ctx := context.WithoutCancel(b.exec.runCtx)
	now := b.nowUTC()

	var unsent []string

	for _, l := range lines {
		n := 0
		if sink != nil {
			n = sink(LogLine{
				Time: now, AppID: p.appID, AppName: p.appName, FunctionID: p.fnID, FunctionName: p.fnName,
				CallID: callID, Stream: stream, Text: l, LineFormat: p.lineFormat,
			})
		}

		if n == 0 {
			unsent = append(unsent, l)
		}
	}

	if len(unsent) == 0 || p.syslog == "" {
		return
	}

	if err := sendSyslog(ctx, p.syslog, now, p, callID, stream, unsent); err != nil {
		logger.Load(ctx).WarnContext(ctx, "ocifunctions: syslog delivery failed",
			"function", p.fnID, "syslogUrl", p.syslog, "error", err)
	}
}

func syslogDialer(ctx context.Context, u *url.URL) (net.Conn, error) {
	d := net.Dialer{Timeout: syslogDial}
	addr := u.Host

	switch u.Scheme {
	case schemeUDP:
		return d.DialContext(ctx, "udp", addr)
	case "tcp+tls":
		td := tls.Dialer{NetDialer: &d, Config: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname()}}

		return td.DialContext(ctx, "tcp", addr)
	}

	return d.DialContext(ctx, "tcp", addr)
}

// sendSyslog writes RFC 5424 messages (newline-framed over a stream, one datagram per message over udp).
func sendSyslog(
	ctx context.Context,
	raw string,
	at time.Time,
	p execPlan,
	callID, stream string,
	lines []string,
) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse syslog url: %w", err)
	}

	conn, err := syslogDialer(ctx, u)
	if err != nil {
		return fmt.Errorf("dial syslog: %w", err)
	}

	defer conn.Close()

	_ = conn.SetWriteDeadline(time.Now().Add(syslogDial))

	pri := syslogInfoPri
	if stream == streamStderr {
		pri = syslogErrorPri
	}

	for _, l := range lines {
		if len(l) > syslogMaxLine {
			l = l[:syslogMaxLine]
		}

		msg := fmt.Sprintf("<%d>1 %s %s %s %s %s - %s", pri, at.Format(time.RFC3339Nano), syslogHost,
			sanitizeToken(p.appName), callID, sanitizeToken(p.fnName), l)

		if u.Scheme != schemeUDP {
			msg += "\n"
		}

		if _, err = conn.Write([]byte(msg)); err != nil {
			return fmt.Errorf("write syslog: %w", err)
		}
	}

	return nil
}

func sanitizeToken(s string) string {
	const maxTok = 48

	s = strings.Map(func(r rune) rune {
		if r <= ' ' || r > '~' {
			return '_'
		}

		return r
	}, s)

	if len(s) > maxTok {
		s = s[:maxTok]
	}

	if s == "" {
		return "-"
	}

	return s
}
