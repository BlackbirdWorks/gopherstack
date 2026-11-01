package gcpstorage

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	maxBodyBytes      = 512 << 20
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 5 * time.Minute
	idleTimeout       = 120 * time.Second
)

type opFunc func(c *echo.Context, rt route) error

// Handler serves the Cloud Storage JSON and XML APIs.
type Handler struct {
	Backend *InMemoryBackend
	srvMu   *lockmetrics.RWMutex
	srv     *http.Server
	ops     map[string]opFunc
	Port    int
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler; callers override Port from Settings.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{
		Backend: backend,
		Port:    DefaultPort,
		srvMu:   lockmetrics.New("gcpstorage.server"),
	}

	h.ops = map[string]opFunc{
		opListBuckets: h.listBuckets, opInsertBucket: h.insertBucket, opGetBucket: h.getBucket,
		opPatchBucket: h.updateBucket, opUpdateBucket: h.updateBucket, opDeleteBucket: h.deleteBucket,
		opGetIamPolicy: h.getIamPolicy, opSetIamPolicy: h.setIamPolicy, opTestIamPermissions: h.testIamPermissions,
		opListObjects: h.listObjects, opGetObject: h.getObject, opPatchObject: h.updateObject,
		opUpdateObject: h.updateObject, opDeleteObject: h.deleteObject, opCopyObject: h.copyObject,
		opRewriteObject: h.copyObject, opComposeObject: h.composeObject, opInsertObject: h.insertObject,
		opResumableUpload: h.resumableUpload, opCancelUpload: h.cancelUpload, opDownloadObject: h.downloadObject,
		opXMLGetObject: h.xmlGetObject, opXMLHeadObject: h.xmlGetObject, opXMLPutObject: h.xmlPutObject,
		opXMLDeleteObject: h.xmlDeleteObject,
	}

	h.registerACL()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPStorage" }

// GetSupportedOperations lists the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return append(aclOpNames(), []string{
		opListBuckets, opInsertBucket, opGetBucket, opPatchBucket, opUpdateBucket, opDeleteBucket,
		opGetIamPolicy, opSetIamPolicy, opTestIamPermissions, opListObjects, opGetObject, opPatchObject,
		opUpdateObject, opDeleteObject, opCopyObject, opRewriteObject, opComposeObject, opInsertObject,
		opResumableUpload, opCancelUpload, opDownloadObject, opXMLGetObject, opXMLHeadObject,
		opXMLPutObject, opXMLDeleteObject,
	}...)
}

// RouteMatcher never matches: GCS runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string { return parseRoute(c.Request()).op }

// ExtractResource returns bucket[/object] for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rt := parseRoute(c.Request())
	if rt.object != "" {
		return rt.bucket + "/" + rt.object
	}

	return rt.bucket
}

// Reset clears all in-memory state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Handler returns the Echo handler for all GCS requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		rt := parseRoute(c.Request())

		op, ok := h.ops[rt.op]
		if !ok {
			return h.fail(c, errNotFound("Not Found"), false)
		}

		if err := op(c, rt); err != nil {
			return h.fail(c, err, strings.HasPrefix(rt.op, "XML") || rt.op == opDownloadObject)
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

type xmlError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func xmlErrorCode(ae *APIError) string {
	switch {
	case ae.Status == http.StatusNotFound && strings.HasPrefix(ae.Message, "No such object"):
		return "NoSuchKey"
	case ae.Status == http.StatusNotFound:
		return "NoSuchBucket"
	case ae.Status == http.StatusPreconditionFailed:
		return "PreconditionFailed"
	case ae.Status == http.StatusConflict:
		return "BucketNotEmpty"
	case ae.Status == http.StatusRequestedRangeNotSatisfiable:
		return "InvalidRange"
	default:
		return "InvalidArgument"
	}
}

func (h *Handler) fail(c *echo.Context, err error, xmlStyle bool) error {
	var ae *APIError
	if !errors.As(err, &ae) {
		logger.Load(c.Request().Context()).
			ErrorContext(c.Request().Context(), "gcpstorage: internal error", "error", err)

		ae = newErr(http.StatusInternalServerError, "backendError", "Internal Server Error")
	}

	if ae.Status == http.StatusNotModified {
		return c.NoContent(ae.Status)
	}

	if xmlStyle {
		out, _ := xml.Marshal(xmlError{Code: xmlErrorCode(ae), Message: ae.Message}) //nolint:errchkjson // static shape

		return c.Blob(ae.Status, "application/xml; charset=UTF-8", append([]byte(xml.Header), out...))
	}

	return c.JSON(ae.Status, map[string]errorBody{"error": {
		Code: ae.Status, Message: ae.Message,
		Errors: []errorItem{{Message: ae.Message, Domain: "global", Reason: ae.Reason}},
	}})
}

func readBody(c *echo.Context) ([]byte, error) {
	r := c.Request()

	data, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, maxBodyBytes))
	if err != nil {
		return nil, newErr(
			http.StatusRequestEntityTooLarge,
			"uploadTooLarge",
			"Request body is too large or unreadable.",
		)
	}

	return data, nil
}

// withACLParams copies the predefined-ACL query parameters into the request patch.
func withACLParams(c *echo.Context, p Patch, keys ...string) Patch {
	if p == nil {
		p = Patch{}
	}

	for _, k := range keys {
		if v := c.QueryParam(k); v != "" {
			p[k] = marshalRaw(v)
		}
	}

	return p
}

// withDestinationACL maps destinationPredefinedAcl, used by copy, rewrite and compose, onto the patch.
func withDestinationACL(c *echo.Context, p Patch) Patch {
	if p == nil {
		p = Patch{}
	}

	if v := c.QueryParam("destinationPredefinedAcl"); v != "" {
		p[keyPredefined] = marshalRaw(v)
	}

	return p
}

func readPatch(c *echo.Context) (Patch, error) {
	body, err := readBody(c)
	if err != nil {
		return nil, err
	}

	p, apiErr := decodePatch(body)
	if apiErr != nil {
		return nil, apiErr
	}

	return p, nil
}

// StartWorker binds the dedicated listener synchronously so a port conflict is
// returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcpstorage: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Use(gcpenforce.EchoMiddleware(ctx, gcpenforce.Target{Host: "storage.googleapis.com", QueryOnly: true}))
	e.Any("/*", telemetry.WrapEchoHandler("GCPStorage", h.Handler(), h))

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "gcpstorage", "listener")
	log := logger.Load(workerCtx)

	log.InfoContext(workerCtx, "gcpstorage: starting dedicated listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "gcpstorage: listener stopped", "error", serveErr)
		}
	}()

	return nil
}

// Shutdown stops the dedicated listener, forcing a close if graceful shutdown fails.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv := h.srv
	h.srv = nil
	h.srvMu.Unlock()

	if srv == nil {
		return
	}

	log := logger.Load(ctx)

	if err := srv.Shutdown(ctx); err != nil {
		log.ErrorContext(ctx, "gcpstorage: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "gcpstorage: forced close also failed", "error", closeErr)
		}
	}
}
