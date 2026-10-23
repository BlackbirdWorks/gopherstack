package azureeventhubs

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type amqpService struct {
	srv *amqp1.Server
	wg  sync.WaitGroup
}

func (h *Handler) amqpCertificate() (tls.Certificate, error) {
	if h.AMQPCert != nil {
		return *h.AMQPCert, nil
	}

	cert, err := devtls.GenerateSelfSignedCert()
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("azureeventhubs: amqp tls cert: %w", err)
	}

	return cert, nil
}

func (h *Handler) newAMQPServer() (*amqp1.Server, error) {
	cert, err := h.amqpCertificate()
	if err != nil {
		return nil, err
	}

	return amqp1.NewServer(amqp1.Config{
		Handler:        &amqpHandler{h: h},
		TLS:            &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		ContainerID:    "gopherstack-eventhubs",
		MaxMessageSize: MaxEventBytes,
	}), nil
}

// ServeAMQP serves AMQP 1.0 on ln (TLS-wrapped when useTLS) until Shutdown.
func (h *Handler) ServeAMQP(ln net.Listener, useTLS bool) error {
	svc, err := h.amqpServer()
	if err != nil {
		_ = ln.Close()

		return err
	}

	svc.wg.Go(func() {
		if serveErr := svc.srv.Serve(ln, useTLS); serveErr != nil {
			logger.Load(context.Background()).Error("azureeventhubs: amqp listener stopped", "error", serveErr)
		}
	})

	return nil
}

func (h *Handler) amqpServer() (*amqpService, error) {
	h.amqpMu.Lock("amqp")
	defer h.amqpMu.Unlock()

	if h.amqp != nil {
		return h.amqp, nil
	}

	srv, err := h.newAMQPServer()
	if err != nil {
		return nil, err
	}

	h.amqp = &amqpService{srv: srv}

	return h.amqp, nil
}

func (h *Handler) startAMQP(ctx context.Context) error {
	for _, l := range []struct {
		port int
		tls  bool
	}{{h.AMQPPort, false}, {h.AMQPTLSPort, true}} {
		if l.port <= 0 {
			continue
		}

		var lc net.ListenConfig

		ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", l.port))
		if err != nil {
			h.stopAMQP()

			return fmt.Errorf("azureeventhubs: bind amqp port %d: %w", l.port, err)
		}

		if serveErr := h.ServeAMQP(ln, l.tls); serveErr != nil {
			return serveErr
		}

		logger.Load(ctx).InfoContext(ctx, "azureeventhubs: starting AMQP listener", "port", l.port, "tls", l.tls)
	}

	return nil
}

func (h *Handler) stopAMQP() {
	h.amqpMu.Lock("amqp")
	svc := h.amqp
	h.amqp = nil
	h.amqpMu.Unlock()

	if svc == nil {
		return
	}

	svc.srv.Close()
	svc.wg.Wait()
}

type amqpHandler struct{ h *Handler }

func (a *amqpHandler) Authenticate(mech amqp1.Symbol, user, password string) bool {
	if mech == "ANONYMOUS" {
		return true
	}

	return mech == "PLAIN" && (!a.h.ValidateSAS || a.h.Backend.PlainAuthorized(user, password))
}

func (a *amqpHandler) NewConn() amqp1.ConnHandler {
	return &amqpConn{h: a.h, b: a.h.Backend, replies: map[string]chan *amqp1.Outgoing{}, mu: newConnLock()}
}
