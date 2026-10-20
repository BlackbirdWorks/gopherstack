package rds

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// portSwitch is a listener prepared on a new port, swapped in once the modification is accepted.
type portSwitch struct {
	ln   net.Listener
	be   *InMemoryBackend
	lb   *liveDB
	port int
}

// prepareInstancePort binds the requested DBPortNumber for a ready standalone engine instance;
// an empty switch means there is nothing to move.
func (b *InMemoryBackend) prepareInstancePort(id string, port int) (*portSwitch, error) {
	if port <= 0 {
		return &portSwitch{}, nil
	}

	b.mu.RLock("prepareInstancePort")

	var lb *liveDB

	if b.engine != nil {
		if inst, ok := b.instances.Get(normalizeID(id)); ok {
			lb = b.standaloneUnitLocked(inst)
		}
	}

	ready := lb != nil && lb.ready
	b.mu.RUnlock()

	if !ready || lb.advertisedPort() == port {
		return &portSwitch{}, nil
	}

	e := b.engine
	if e.cfg.Ports != nil {
		if e.cfg.Ports.Reserve(port, "rds") != nil {
			return nil, portUnavailable(port)
		}
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp",
		net.JoinHostPort(container.BindHostFor(e.cfg.Host), strconv.Itoa(port)))
	if err != nil {
		e.releasePort(port)

		return nil, portUnavailable(port)
	}

	return &portSwitch{be: b, lb: lb, ln: ln, port: port}, nil
}

func portUnavailable(port int) error {
	return fmt.Errorf("%w: port %d is not available on this host", ErrInvalidParameter, port)
}

// abort drops the prepared listener when the modification is rejected.
func (s *portSwitch) abort() {
	if s.ln == nil {
		return
	}

	_ = s.ln.Close()
	s.be.engine.releasePort(s.port)
}

// commit makes the prepared listener the instance's endpoint and retires the previous one.
func (s *portSwitch) commit() {
	if s.ln == nil {
		return
	}

	select {
	case <-s.lb.gone:
		s.abort()

		return
	default:
	}

	s.lb.dbPort.Store(int32(s.port)) //nolint:gosec // TCP port fits int32

	if s.lb.kind == enginePostgres {
		p := &pgProxy{be: s.be, lb: s.lb, ln: s.ln, port: s.port, tlsCfg: s.be.engine.serverTLS()}
		p.wg.Add(1)

		old := s.lb.proxy.Swap(p)

		go func() {
			defer p.wg.Done()

			p.acceptLoop()
		}()

		if old != nil {
			old.close()
		}

		return
	}

	f := &tcpForwarder{be: s.be, lb: s.lb, ln: s.ln, port: s.port}
	f.wg.Add(1)

	old := s.lb.fwd.Swap(f)

	go func() {
		defer f.wg.Done()

		f.acceptLoop()
	}()

	if old != nil {
		old.close()
	}
}
