package ociapigateway

import (
	"context"
	"net/http/httptrace"
	"sync"
	"time"
)

type connectTimeoutKey struct{}

// phaseTimers enforce the per-phase HTTP back end timeouts: sending the request, then waiting for the response.
type phaseTimers struct {
	cancel context.CancelFunc
	timer  *time.Timer
	read   time.Duration
	send   time.Duration
	mu     sync.Mutex
}

func (p *phaseTimers) arm(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.timer != nil {
		p.timer.Stop()
	}

	p.timer = time.AfterFunc(d, p.cancel)
}

func (p *phaseTimers) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
}

func (p *phaseTimers) trace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GotConn:              func(httptrace.GotConnInfo) { p.arm(p.send) },
		WroteRequest:         func(httptrace.WroteRequestInfo) { p.arm(p.read) },
		GotFirstResponseByte: p.stop,
	}
}

// withBackendTimeouts bounds connecting, sending and waiting for the response of one back end call.
func withBackendTimeouts(ctx context.Context, be *backendDoc) (context.Context, *phaseTimers, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	p := &phaseTimers{
		cancel: cancel, read: seconds(be.ReadTimeout, defaultReadSec), send: seconds(be.SendTimeout, defaultSendSec),
	}

	ctx = context.WithValue(ctx, connectTimeoutKey{}, seconds(be.ConnectTimeout, defaultConnectSec))
	ctx = httptrace.WithClientTrace(ctx, p.trace())

	return ctx, p, func() {
		p.stop()
		cancel()
	}
}
