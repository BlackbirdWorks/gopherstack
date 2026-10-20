package transparent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
)

// PlainServer is a running plain-HTTP listener (for http:// AWS endpoint URLs).
type PlainServer struct {
	srv  *http.Server
	ln   net.Listener
	done chan struct{}
}

// StartHTTP listens on addr and serves h over plain HTTP until ctx is done.
func StartHTTP(ctx context.Context, addr string, h http.Handler) (*PlainServer, error) {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transparent: listen %s: %w", addr, err)
	}

	s := &PlainServer{
		ln:   ln,
		done: make(chan struct{}),
		srv:  &http.Server{Handler: h, ReadHeaderTimeout: readHeaderTimeout},
	}

	go func() {
		defer close(s.done)

		if serr := s.srv.Serve(ln); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			_ = ln.Close()
		}
	}()

	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()

	return s, nil
}

// Addr returns the bound listen address.
func (s *PlainServer) Addr() net.Addr { return s.ln.Addr() }

// Close gracefully shuts the listener down and waits for it to exit.
func (s *PlainServer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err := s.srv.Shutdown(ctx)
	<-s.done

	return err
}
