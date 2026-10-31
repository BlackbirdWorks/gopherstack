package gcpdns

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
	dnsapi "google.golang.org/api/dns/v1"

	pkgdns "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	maxBodyBytes      = 8 << 20
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 5 * time.Minute
	idleTimeout       = 120 * time.Second
)

type opFunc func(c *echo.Context, rt route) error

// Handler serves the Cloud DNS REST API and the authoritative DNS data plane.
type Handler struct {
	Backend *InMemoryBackend
	srvMu   *lockmetrics.RWMutex
	srv     *http.Server
	dnsSrv  *pkgdns.Server
	ops     map[string]opFunc
	addr    string
	Port    int
	DNSPort int
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler; callers override Port and DNSPort from Settings.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{
		Backend: backend, Port: DefaultPort, DNSPort: DefaultDNSPort, srvMu: lockmetrics.New("gcpdns.server"),
		ops: map[string]opFunc{},
	}
	h.registerZoneOps()
	h.registerRecordOps()
	h.registerPolicyOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPDNS" }

// GetSupportedOperations lists the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	names := make([]string, 0, len(h.ops))
	for n := range h.ops {
		names = append(names, n)
	}

	return names
}

// RouteMatcher never matches: Cloud DNS runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	if op := parseRoute(c.Request()).op; op != "" {
		return op
	}

	return "Unknown"
}

// ExtractResource returns project[/zone] for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rt := parseRoute(c.Request())
	if rt.zone != "" {
		return rt.project + "/" + rt.zone
	}

	return rt.project
}

// Addr returns the bound REST listener address, or "" before StartWorker.
func (h *Handler) Addr() string {
	h.srvMu.RLock("Addr")
	defer h.srvMu.RUnlock()

	return h.addr
}

// Reset removes all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Handler returns the Echo handler for all Cloud DNS REST requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		rt := parseRoute(c.Request())

		op, ok := h.ops[rt.op]
		if !ok {
			return h.fail(c, newErr(http.StatusNotFound, "notFound", "Not found: %s %s",
				c.Request().Method, c.Request().URL.Path))
		}

		if err := op(c, rt); err != nil {
			return h.fail(c, err)
		}

		return nil
	}
}

type errorItem struct {
	Message string `json:"message"`
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
}

type errorBody struct {
	Message string      `json:"message"`
	Errors  []errorItem `json:"errors"`
	Code    int         `json:"code"`
}

func (h *Handler) fail(c *echo.Context, err error) error {
	var ae *APIError
	if !errors.As(err, &ae) {
		logger.Load(c.Request().Context()).
			ErrorContext(c.Request().Context(), "gcpdns: internal error", "error", err)

		ae = newErr(http.StatusInternalServerError, "backendError", "Internal Error")
	}

	return c.JSON(ae.Status, map[string]errorBody{"error": {
		Code: ae.Status, Message: ae.Message,
		Errors: []errorItem{{Message: ae.Message, Domain: "global", Reason: ae.Reason}},
	}})
}

func readBody(c *echo.Context) ([]byte, error) {
	data, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxBodyBytes))
	if err != nil {
		return nil, newErr(http.StatusRequestEntityTooLarge, "invalid", "Request body is too large or unreadable.")
	}

	return data, nil
}

// decode reads a JSON body into T and, when wantMap, the raw object for merge patches.
func decode[T any](c *echo.Context) (*T, map[string]any, error) {
	data, err := readBody(c)
	if err != nil {
		return nil, nil, err
	}

	v := new(T)
	raw := map[string]any{}

	if len(strings.TrimSpace(string(data))) == 0 {
		return v, raw, nil
	}

	if err = json.Unmarshal(data, v); err != nil {
		return nil, nil, errInvalid("Invalid JSON payload received. %v", err)
	}

	if err = json.Unmarshal(data, &raw); err != nil {
		return nil, nil, errInvalid("Invalid JSON payload received. %v", err)
	}

	return v, raw, nil
}

func queryLimit(c *echo.Context) (int, error) {
	raw := c.QueryParam("maxResults")
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, errInvalidValue("maxResults", raw)
	}

	return n, nil
}

func respond(c *echo.Context, v any) error { return c.JSON(http.StatusOK, v) }

func noContent(c *echo.Context) error { return c.NoContent(http.StatusNoContent) }

func digestList(c *echo.Context) []string {
	raw := c.QueryParam("digestType")
	if raw == "" {
		return nil
	}

	return strings.Split(raw, ",")
}

func (h *Handler) registerZoneOps() {
	h.ops["Projects.Get"] = func(c *echo.Context, rt route) error { return respond(c, h.Backend.GetProject(rt.project)) }
	h.ops["ManagedZones.Create"] = func(c *echo.Context, rt route) error {
		in, _, err := decode[dnsapi.ManagedZone](c)
		if err != nil {
			return err
		}

		z, err := h.Backend.CreateZone(rt.project, in)
		if err != nil {
			return err
		}

		return respond(c, z)
	}
	h.ops["ManagedZones.Get"] = func(c *echo.Context, rt route) error {
		z, err := h.Backend.GetZone(rt.project, rt.zone)
		if err != nil {
			return err
		}

		return respond(c, z)
	}
	h.ops["ManagedZones.List"] = func(c *echo.Context, rt route) error {
		limit, err := queryLimit(c)
		if err != nil {
			return err
		}

		pg, err := h.Backend.ListZones(rt.project, c.QueryParam("dnsName"), c.QueryParam("pageToken"), limit)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.ManagedZonesListResponse{
			Kind: "dns#managedZonesListResponse", ManagedZones: pg.Data, NextPageToken: pg.Next,
		})
	}
	h.ops["ManagedZones.Delete"] = func(c *echo.Context, rt route) error {
		if err := h.Backend.DeleteZone(rt.project, rt.zone); err != nil {
			return err
		}

		return noContent(c)
	}
	h.ops["ManagedZones.Patch"] = func(c *echo.Context, rt route) error {
		_, raw, err := decode[dnsapi.ManagedZone](c)
		if err != nil {
			return err
		}

		op, err := h.Backend.UpdateZone(rt.project, rt.zone, nil, raw)
		if err != nil {
			return err
		}

		return respond(c, op)
	}
	h.ops["ManagedZones.Update"] = func(c *echo.Context, rt route) error {
		in, _, err := decode[dnsapi.ManagedZone](c)
		if err != nil {
			return err
		}

		op, err := h.Backend.UpdateZone(rt.project, rt.zone, in, nil)
		if err != nil {
			return err
		}

		return respond(c, op)
	}
	h.registerZoneChildOps()
}

func (h *Handler) registerZoneChildOps() {
	h.ops["DnsKeys.Get"] = func(c *echo.Context, rt route) error {
		k, err := h.Backend.GetKey(rt.project, rt.zone, rt.id, digestList(c))
		if err != nil {
			return err
		}

		return respond(c, k)
	}
	h.ops["DnsKeys.List"] = func(c *echo.Context, rt route) error {
		limit, err := queryLimit(c)
		if err != nil {
			return err
		}

		pg, err := h.Backend.ListKeys(rt.project, rt.zone, c.QueryParam("pageToken"), limit, digestList(c))
		if err != nil {
			return err
		}

		return respond(
			c,
			&dnsapi.DnsKeysListResponse{Kind: "dns#dnsKeysListResponse", DnsKeys: pg.Data, NextPageToken: pg.Next},
		)
	}
	h.ops["ManagedZoneOperations.Get"] = func(c *echo.Context, rt route) error {
		op, err := h.Backend.GetOperation(rt.project, rt.zone, rt.id)
		if err != nil {
			return err
		}

		return respond(c, op)
	}
	h.ops["ManagedZoneOperations.List"] = func(c *echo.Context, rt route) error {
		limit, err := queryLimit(c)
		if err != nil {
			return err
		}

		pg, err := h.Backend.ListOperations(
			rt.project,
			rt.zone,
			c.QueryParam("sortBy"),
			c.QueryParam("pageToken"),
			limit,
		)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.ManagedZoneOperationsListResponse{
			Kind: "dns#managedZoneOperationsListResponse", Operations: pg.Data, NextPageToken: pg.Next,
		})
	}
}

func (h *Handler) registerRecordOps() {
	h.ops["Changes.Create"] = func(c *echo.Context, rt route) error {
		in, _, err := decode[dnsapi.Change](c)
		if err != nil {
			return err
		}

		ch, err := h.Backend.CreateChange(rt.project, rt.zone, in)
		if err != nil {
			return err
		}

		return respond(c, ch)
	}
	h.ops["Changes.Get"] = func(c *echo.Context, rt route) error {
		ch, err := h.Backend.GetChange(rt.project, rt.zone, rt.id)
		if err != nil {
			return err
		}

		return respond(c, ch)
	}
	h.ops["Changes.List"] = func(c *echo.Context, rt route) error {
		limit, err := queryLimit(c)
		if err != nil {
			return err
		}

		pg, err := h.Backend.ListChanges(
			rt.project, rt.zone, c.QueryParam("sortBy"), c.QueryParam("sortOrder"), c.QueryParam("pageToken"), limit)
		if err != nil {
			return err
		}

		return respond(
			c,
			&dnsapi.ChangesListResponse{Kind: "dns#changesListResponse", Changes: pg.Data, NextPageToken: pg.Next},
		)
	}
	h.registerRRSetOps()
}

func (h *Handler) registerRRSetOps() {
	h.ops["ResourceRecordSets.Create"] = func(c *echo.Context, rt route) error {
		in, _, err := decode[dnsapi.ResourceRecordSet](c)
		if err != nil {
			return err
		}

		rr, err := h.Backend.CreateRRSet(rt.project, rt.zone, in)
		if err != nil {
			return err
		}

		return respond(c, rr)
	}
	h.ops["ResourceRecordSets.Get"] = func(c *echo.Context, rt route) error {
		rr, err := h.Backend.GetRRSet(rt.project, rt.zone, rt.rrName, rt.rrType)
		if err != nil {
			return err
		}

		return respond(c, rr)
	}
	h.ops["ResourceRecordSets.List"] = func(c *echo.Context, rt route) error {
		if c.QueryParam("filter") != "" {
			return errInvalidValue("filter", c.QueryParam("filter"))
		}

		limit, err := queryLimit(c)
		if err != nil {
			return err
		}

		pg, err := h.Backend.ListRRSets(
			rt.project, rt.zone, c.QueryParam("name"), c.QueryParam("type"), c.QueryParam("pageToken"), limit)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.ResourceRecordSetsListResponse{
			Kind: "dns#resourceRecordSetsListResponse", Rrsets: pg.Data, NextPageToken: pg.Next,
		})
	}
	h.ops["ResourceRecordSets.Patch"] = func(c *echo.Context, rt route) error {
		_, raw, err := decode[dnsapi.ResourceRecordSet](c)
		if err != nil {
			return err
		}

		rr, err := h.Backend.PatchRRSet(rt.project, rt.zone, rt.rrName, rt.rrType, raw)
		if err != nil {
			return err
		}

		return respond(c, rr)
	}
	h.ops["ResourceRecordSets.Delete"] = func(c *echo.Context, rt route) error {
		if err := h.Backend.DeleteRRSet(rt.project, rt.zone, rt.rrName, rt.rrType); err != nil {
			return err
		}

		return respond(c, map[string]any{})
	}
}

// StartWorker binds the DNS server and the REST listener synchronously so a port conflict is returned.
func (h *Handler) StartWorker(ctx context.Context) error {
	workerCtx := logger.WithWorker(ctx, "gcpdns", "listener")
	log := logger.Load(workerCtx)

	var dnsSrv *pkgdns.Server

	if h.DNSPort > 0 {
		var err error

		if dnsSrv, err = h.startDNS(workerCtx); err != nil {
			return err
		}
	}

	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		if dnsSrv != nil {
			_ = dnsSrv.Stop()
		}

		return fmt.Errorf("gcpdns: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(log))
	e.Use(gcpenforce.EchoMiddleware(ctx, gcpenforce.Target{Host: "dns.googleapis.com"}))
	e.Any("/*", telemetry.WrapEchoHandler("GCPDNS", h.Handler(), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv, h.dnsSrv, h.addr = srv, dnsSrv, listener.Addr().String()
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "gcpdns: starting dedicated listener", "port", h.Port, "dnsPort", h.DNSPort)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "gcpdns: listener stopped", "error", serveErr)
		}
	}()

	return nil
}

func (h *Handler) startDNS(ctx context.Context) (*pkgdns.Server, error) {
	srv, err := pkgdns.New(pkgdns.Config{ListenAddr: fmt.Sprintf(":%d", h.DNSPort), Logger: logger.Load(ctx)})
	if err != nil {
		return nil, fmt.Errorf("gcpdns: dns server: %w", err)
	}

	srv.SetInterceptor(authority{b: h.Backend})

	if err = srv.Start(ctx); err != nil {
		return nil, fmt.Errorf("gcpdns: bind dns port %d: %w", h.DNSPort, err)
	}

	return srv, nil
}

// Shutdown stops both listeners.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv, dnsSrv := h.srv, h.dnsSrv
	h.srv, h.dnsSrv = nil, nil
	h.srvMu.Unlock()

	log := logger.Load(ctx)

	if dnsSrv != nil {
		if err := dnsSrv.Stop(); err != nil {
			log.ErrorContext(ctx, "gcpdns: dns server stop failed", "error", err)
		}
	}

	if srv == nil {
		return
	}

	if err := srv.Shutdown(ctx); err != nil {
		log.ErrorContext(ctx, "gcpdns: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "gcpdns: forced close also failed", "error", closeErr)
		}
	}
}
