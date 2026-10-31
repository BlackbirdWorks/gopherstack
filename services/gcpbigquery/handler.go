package gcpbigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"strconv"
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
	maxBodyBytes      = 256 << 20
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 5 * time.Minute
	idleTimeout       = 120 * time.Second
)

type opFunc func(c *echo.Context, rt route) error

// Handler serves the BigQuery v2 REST API.
type Handler struct {
	Backend *InMemoryBackend
	srvMu   *lockmetrics.RWMutex
	srv     *http.Server
	ops     map[string]opFunc
	uploads *uploadSessions
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
		Backend: backend, Port: DefaultPort, srvMu: lockmetrics.New("gcpbigquery.server"), uploads: newUploadSessions(),
	}

	h.ops = map[string]opFunc{
		opListProjects: h.listProjects, opInsertDataset: h.insertDataset, opGetDataset: h.getDataset,
		opListDatasets: h.listDatasets, opPatchDataset: h.updateDataset, opUpdateDataset: h.updateDataset,
		opDeleteDataset: h.deleteDataset, opInsertTable: h.insertTable, opGetTable: h.getTable,
		opListTables: h.listTables, opPatchTable: h.updateTable, opUpdateTable: h.updateTable,
		opDeleteTable: h.deleteTable, opListTableData: h.listTableData, opInsertAll: h.insertAll,
		opInsertJob: h.insertJob, opUploadJob: h.uploadJob, opGetJob: h.getJob, opListJobs: h.listJobs,
		opCancelJob: h.cancelJob, opDeleteJob: h.deleteJob, opQuery: h.query, opGetQueryResults: h.getQueryResults,
	}

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPBigQuery" }

// GetSupportedOperations lists the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opListProjects, opInsertDataset, opGetDataset, opListDatasets, opPatchDataset, opUpdateDataset,
		opDeleteDataset, opInsertTable, opGetTable, opListTables, opPatchTable, opUpdateTable, opDeleteTable,
		opListTableData, opInsertAll, opInsertJob, opUploadJob, opGetJob, opListJobs, opCancelJob, opDeleteJob,
		opQuery, opGetQueryResults,
	}
}

// RouteMatcher never matches: BigQuery runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string { return parseRoute(c.Request()).op }

// ExtractResource returns project[/dataset[.table]] for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rt := parseRoute(c.Request())

	switch {
	case rt.table != "":
		return rt.project + "/" + rt.dataset + "." + rt.table
	case rt.dataset != "":
		return rt.project + "/" + rt.dataset
	case rt.job != "":
		return rt.project + "/" + rt.job
	}

	return rt.project
}

// Reset clears all in-memory state.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.uploads.reset()
}

// Handler returns the Echo handler for all BigQuery requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		rt := parseRoute(c.Request())

		op, ok := h.ops[rt.op]
		if !ok {
			return h.fail(c, errNotFound("Not found: %s %s", c.Request().Method, c.Request().URL.Path))
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
	Status  string      `json:"status"`
	Errors  []errorItem `json:"errors"`
	Code    int         `json:"code"`
}

func (h *Handler) fail(c *echo.Context, err error) error {
	var ae *APIError
	if !errors.As(err, &ae) {
		logger.Load(c.Request().Context()).
			ErrorContext(c.Request().Context(), "gcpbigquery: internal error", "error", err)

		ae = newErr(http.StatusInternalServerError, "backendError", "Internal Server Error")
	}

	return c.JSON(ae.Status, map[string]errorBody{"error": {
		Code: ae.Status, Message: ae.Message, Status: statusText(ae.Status),
		Errors: []errorItem{{Message: ae.Message, Domain: "global", Reason: ae.Reason}},
	}})
}

// decodeBody reads a JSON object body; numbers stay json.Number so large integers survive.
func decodeBody(c *echo.Context) (map[string]any, error) {
	r := c.Request()

	data, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, maxBodyBytes))
	if err != nil {
		return nil, newErr(
			http.StatusRequestEntityTooLarge,
			"uploadTooLarge",
			"Request body is too large or unreadable.",
		)
	}

	return decodeObject(data)
}

func decodeObject(data []byte) (map[string]any, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return map[string]any{}, nil
	}

	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()

	var body map[string]any
	if err := dec.Decode(&body); err != nil {
		return nil, errInvalid("Invalid JSON payload received: %v", err)
	}

	if body == nil {
		body = map[string]any{}
	}

	return body, nil
}

func queryInt(c *echo.Context, key string) int {
	n, _ := strconv.Atoi(c.QueryParam(key))

	return max(n, 0)
}

func queryBool(c *echo.Context, key string) bool {
	b, _ := strconv.ParseBool(c.QueryParam(key))

	return b
}

func listOptions(c *echo.Context) ListOptions {
	return ListOptions{
		Filter: c.QueryParam("filter"), PageToken: c.QueryParam("pageToken"), Max: queryInt(c, "maxResults"),
		All: queryBool(c, "all"),
	}
}

func listResponse(
	kind, itemKey string,
	items []map[string]any,
	next string,
	extra map[string]any,
) map[string]any {
	out := map[string]any{litKind: kind, litEtag: etagOf(kind, strconv.Itoa(len(items))), itemKey: items}
	if next != "" {
		out["nextPageToken"] = next
	}

	maps.Copy(out, extra)

	return out
}

func (h *Handler) listProjects(c *echo.Context, _ route) error {
	items := h.Backend.ListProjects()

	return c.JSON(http.StatusOK, listResponse("bigquery#projectList", "projects", items, "",
		map[string]any{"totalItems": len(items)}))
}

func (h *Handler) insertDataset(c *echo.Context, rt route) error {
	body, err := decodeBody(c)
	if err != nil {
		return err
	}

	out, err := h.Backend.CreateDataset(rt.project, body)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) getDataset(c *echo.Context, rt route) error {
	out, err := h.Backend.GetDataset(rt.project, rt.dataset)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) updateDataset(c *echo.Context, rt route) error {
	body, err := decodeBody(c)
	if err != nil {
		return err
	}

	out, err := h.Backend.UpdateDataset(rt.project, rt.dataset, body, c.Request().Method == http.MethodPut)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) deleteDataset(c *echo.Context, rt route) error {
	if err := h.Backend.DeleteDataset(rt.project, rt.dataset, queryBool(c, "deleteContents")); err != nil {
		return err
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) listDatasets(c *echo.Context, rt route) error {
	pg := h.Backend.ListDatasets(rt.project, listOptions(c))

	return c.JSON(
		http.StatusOK,
		listResponse("bigquery#datasetList", "datasets", pg.Data, pg.Next, nil),
	)
}

func (h *Handler) insertTable(c *echo.Context, rt route) error {
	body, err := decodeBody(c)
	if err != nil {
		return err
	}

	out, err := h.Backend.CreateTable(rt.project, rt.dataset, body)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) getTable(c *echo.Context, rt route) error {
	out, err := h.Backend.GetTable(rt.project, rt.dataset, rt.table)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) updateTable(c *echo.Context, rt route) error {
	body, err := decodeBody(c)
	if err != nil {
		return err
	}

	out, err := h.Backend.UpdateTable(rt.project, rt.dataset, rt.table, body, c.Request().Method == http.MethodPut)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) deleteTable(c *echo.Context, rt route) error {
	if err := h.Backend.DeleteTable(rt.project, rt.dataset, rt.table); err != nil {
		return err
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) listTables(c *echo.Context, rt route) error {
	pg, err := h.Backend.ListTables(rt.project, rt.dataset, listOptions(c))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, listResponse("bigquery#tableList", "tables", pg.Data, pg.Next,
		map[string]any{"totalItems": len(pg.Data)}))
}

func (h *Handler) listTableData(c *echo.Context, rt route) error {
	out, err := h.Backend.ListData(rt.project, rt.dataset, rt.table, DataOptions{
		PageToken: c.QueryParam("pageToken"), SelectedFields: c.QueryParam("selectedFields"),
		StartIndex: queryInt(c, "startIndex"), Max: queryInt(c, "maxResults"),
		Int64Timestamp: queryBool(c, "formatOptions.useInt64Timestamp"),
	})
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) insertAll(c *echo.Context, rt route) error {
	var req InsertRequest

	data, rerr := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxBodyBytes))
	if rerr != nil {
		return newErr(http.StatusRequestEntityTooLarge, "uploadTooLarge", "Request body is too large or unreadable.")
	}

	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()

	if err := dec.Decode(&req); err != nil {
		return errInvalid("Invalid JSON payload received: %v", err)
	}

	out, err := h.Backend.InsertAll(rt.project, rt.dataset, rt.table, req)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) insertJob(c *echo.Context, rt route) error {
	body, err := decodeBody(c)
	if err != nil {
		return err
	}

	out, err := h.Backend.InsertJob(c.Request().Context(), rt.project, body, nil)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) getJob(c *echo.Context, rt route) error {
	out, err := h.Backend.GetJob(rt.project, rt.job)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) cancelJob(c *echo.Context, rt route) error {
	out, err := h.Backend.CancelJob(rt.project, rt.job)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) deleteJob(c *echo.Context, rt route) error {
	if err := h.Backend.DeleteJob(rt.project, rt.job); err != nil {
		return err
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) listJobs(c *echo.Context, rt route) error {
	o := JobListOptions{
		PageToken: c.QueryParam(
			"pageToken",
		),
		Max:         queryInt(c, "maxResults"),
		StateFilter: c.QueryParams()["stateFilter"],
	}

	o.MinCreated, _ = strconv.ParseInt(c.QueryParam("minCreationTime"), 10, 64)
	o.MaxCreated, _ = strconv.ParseInt(c.QueryParam("maxCreationTime"), 10, 64)

	pg := h.Backend.ListJobs(rt.project, o)

	return c.JSON(http.StatusOK, listResponse("bigquery#jobList", "jobs", pg.Data, pg.Next, nil))
}

func (h *Handler) query(c *echo.Context, rt route) error {
	body, err := decodeBody(c)
	if err != nil {
		return err
	}

	out, err := h.Backend.RunQuery(c.Request().Context(), rt.project, body)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) getQueryResults(c *echo.Context, rt route) error {
	out, err := h.Backend.GetQueryResults(rt.project, rt.job, QueryResultOptions{
		PageToken: c.QueryParam("pageToken"), StartIndex: queryInt(c, "startIndex"), Max: queryInt(c, "maxResults"),
		Int64Timestamp: queryBool(c, "formatOptions.useInt64Timestamp"),
	})
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcpbigquery: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Use(gcpenforce.EchoMiddleware(ctx, gcpenforce.Target{Host: "bigquery.googleapis.com"}))
	e.Any("/*", telemetry.WrapEchoHandler("GCPBigQuery", h.Handler(), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, IdleTimeout: idleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "gcpbigquery", "listener")
	log := logger.Load(workerCtx)

	log.InfoContext(workerCtx, "gcpbigquery: starting dedicated listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "gcpbigquery: listener stopped", "error", serveErr)
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
		log.ErrorContext(ctx, "gcpbigquery: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "gcpbigquery: forced close also failed", "error", closeErr)
		}
	}
}
