package cosmosdb

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	adminPrefix          = "/_gopherstack/cosmosdb/"
	headerConsistency    = "X-Ms-Consistency-Level"
	headerSession        = "X-Ms-Session-Token"
	subStatusWriteDenied = "3"
	subStatusAccountGone = "1008"
	subStatusSessionGone = "1002"
	opListConflicts      = "ListConflicts"
	opGetConflict        = "GetConflict"
	opDeleteConflict     = "DeleteConflict"
	opQueryConflicts     = "QueryConflicts"
	conflictsSegment     = "conflicts"
	resourceTypeDocument = "document"
	adminRegionsPath     = adminPrefix + "regions"
	minStalenessPrefix1  = 10
	minStalenessSecs1    = 5
	minStalenessPrefixN  = 100_000
	minStalenessSecsN    = 300
)

type regionCtxKey struct{}

// regionOfRequest is the region whose endpoint received the request; "" for the account's global endpoint.
func regionOfRequest(r *http.Request) string {
	name, _ := r.Context().Value(regionCtxKey{}).(string)

	return name
}

// regionListener is the dedicated endpoint of one region.
type regionListener struct {
	srv  *http.Server
	port int
}

func isWriteRequest(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	case http.MethodPost:
		return !isQueryRequest(r)
	}

	return true
}

// guardRegion applies the per-region rules before dispatch: an offline region answers DatabaseAccountNotFound
// (403/1008), a read region refuses writes (403/3 WriteForbidden), and x-ms-consistency-level may only weaken the
// account default (400). handled is true when an error response has been written.
func (h *Handler) guardRegion(c *echo.Context) (bool, error) {
	r := c.Request()
	regs := h.accountRegions()

	if _, err := checkRequestConsistency(
		canonicalConsistency(r.Header.Get(headerConsistency)),
		regs.consistency(),
	); err != nil ||
		(r.Header.Get(headerConsistency) != "" && canonicalConsistency(r.Header.Get(headerConsistency)) == "") {
		if err == nil {
			err = fmt.Errorf("%w: unknown consistency level %q", ErrInvalidConsistency, r.Header.Get(headerConsistency))
		}

		return true, h.writeError(c, http.StatusBadRequest, "BadRequest", err.Error())
	}

	name := regionOfRequest(r)
	if name == "" {
		name = regs.home()
	}

	reg, _, ok := regs.find(name)
	if !ok {
		return false, nil
	}

	if reg.Offline {
		c.Response().Header().Set(headerSubStatus, subStatusAccountGone)

		return true, h.writeError(c, http.StatusForbidden, "Forbidden",
			"The database account is not available in region "+reg.Name+".")
	}

	if isWriteRequest(r) && !slices.Contains(regs.writers(), reg.Name) {
		c.Response().Header().Set(headerSubStatus, subStatusWriteDenied)

		return true, h.writeError(
			c,
			http.StatusForbidden,
			"Forbidden",
			"The requested operation cannot be performed in this region: writes go to the write region "+regs.home()+".",
		)
	}

	return false, nil
}

// be returns the storage backend for the request: the plain backend, or one bound to the request's region when the
// account keeps separate copies per region.
func (h *Handler) be(c *echo.Context) StorageBackend {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.Backend
	}

	regs := b.AccountRegions()
	if !regs.replicated() {
		return h.Backend
	}

	r := c.Request()

	region := regionOfRequest(r)
	if region == "" {
		region = regs.home()
	} else if reg, _, found := regs.find(region); found {
		region = reg.Name
	}

	level, err := checkRequestConsistency(canonicalConsistency(r.Header.Get(headerConsistency)), regs.consistency())
	if err != nil {
		level = regs.consistency().Level
	}

	rb := &regionBackend{
		InMemoryBackend: b,
		call:            regionCall{region: region, level: level},
		setToken:        func(t string) { c.Response().Header().Set(headerSession, t) },
	}

	if tok := r.Header.Get(headerSession); tok != "" {
		rb.call.token, rb.call.hasToken = b.parseSessionToken(tok)
	}

	return rb
}

// --- listeners ---

// syncRegionListeners gives each region of a multi-region account its own endpoint and stops the listeners of
// regions that are gone.
func (h *Handler) syncRegionListeners() {
	h.srvMu.Lock("syncRegionListeners")
	defer h.srvMu.Unlock()

	if h.srv == nil {
		return
	}

	regs := h.accountRegions()
	want := map[string]bool{}

	if len(regs.Regions) > 1 {
		for _, reg := range regs.Regions {
			want[reg.Name] = true
		}
	}

	for name, l := range h.regionSrvs {
		if !want[name] {
			_ = l.srv.Close()

			delete(h.regionSrvs, name)
		}
	}

	for i, reg := range regs.Regions {
		if !want[reg.Name] || h.regionSrvs[reg.Name] != nil {
			continue
		}

		if l, err := h.startRegionListener(reg.Name, i); err != nil {
			logger.Load(h.workerCtx).ErrorContext(h.workerCtx, "cosmosdb: region listener failed", "region", reg.Name,
				"error", err)
		} else {
			h.regionSrvs[reg.Name] = l
		}
	}
}

func (h *Handler) startRegionListener(name string, index int) (*regionListener, error) {
	port := 0
	if h.RegionPortBase > 0 {
		port = h.RegionPortBase + index
	}

	var lc net.ListenConfig

	ln, err := lc.Listen(h.workerCtx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("cosmosdb: bind region %s: %w", name, err)
	}

	if h.TLS && h.tlsCert != nil {
		ln = tls.NewListener(ln, &tls.Config{
			Certificates: []tls.Certificate{*h.tlsCert}, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"},
		})
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(h.workerCtx)))
	e.Any("/*", telemetry.WrapEchoHandler("CosmosDB", h.regionHandler(name), h))

	srv := &http.Server{
		Handler: e, ReadHeaderTimeout: cosmosdbReadHeaderTimeout, ReadTimeout: cosmosdbReadTimeout,
		WriteTimeout: cosmosdbWriteTimeout, IdleTimeout: cosmosdbIdleTimeout,
	}

	go func() { _ = srv.Serve(ln) }()

	p := 0
	if addr, ok := ln.Addr().(*net.TCPAddr); ok {
		p = addr.Port
	}

	return &regionListener{srv: srv, port: p}, nil
}

// regionHandler serves a region's endpoint: the shared handler with the region attached to every request.
func (h *Handler) regionHandler(name string) echo.HandlerFunc {
	inner := h.Handler()

	return func(c *echo.Context) error {
		r := c.Request()
		c.SetRequest(r.WithContext(context.WithValue(r.Context(), regionCtxKey{}, name)))

		return inner(c)
	}
}

// regionPort is the port of a region's endpoint, 0 when it has none.
func (h *Handler) regionPort(name string) int {
	h.srvMu.RLock("regionPort")
	defer h.srvMu.RUnlock()

	if l := h.regionSrvs[name]; l != nil {
		return l.port
	}

	return 0
}

func (h *Handler) stopRegionListeners() {
	h.srvMu.Lock("stopRegionListeners")
	srvs := h.regionSrvs
	h.regionSrvs = map[string]*regionListener{}
	h.srvMu.Unlock()

	for _, l := range srvs {
		_ = l.srv.Close()
	}
}

// regionEndpoint is the URL of a region's own endpoint; the request's own host stands in when it has none.
func (h *Handler) regionEndpoint(name, scheme, host string) string {
	p := h.regionPort(name)
	if p == 0 {
		return scheme + "://" + host + "/"
	}

	hostname := host
	if hn, _, err := net.SplitHostPort(host); err == nil {
		hostname = hn
	}

	return scheme + "://" + net.JoinHostPort(hostname, strconv.Itoa(p)) + "/"
}

// locationEntries renders regions as the root resource's location list, each with its own endpoint.
func (h *Handler) locationEntries(regions []AccountRegion, scheme, host string) []map[string]any {
	out := make([]map[string]any, 0, len(regions))

	for _, r := range regions {
		out = append(out, map[string]any{
			"name": r.Name, "databaseAccountEndpoint": h.regionEndpoint(r.Name, scheme, host),
		})
	}

	return out
}

// --- admin API ---

type adminRegion struct {
	Name             string `json:"name"`
	Endpoint         string `json:"endpoint,omitempty"`
	FailoverPriority int    `json:"failoverPriority"`
	ReplicationLagMs int    `json:"replicationLagMs"`
	Offline          bool   `json:"offline"`
	Writable         bool   `json:"writable"`
}

type adminTopology struct {
	Consistency       *ConsistencyPolicy `json:"consistency,omitempty"`
	WriteRegion       string             `json:"writeRegion,omitempty"`
	Regions           []adminRegion      `json:"regions"`
	MultiWrite        bool               `json:"multiWrite"`
	AutomaticFailover bool               `json:"automaticFailover"`
}

func (h *Handler) adminBody(r *http.Request) adminTopology {
	regs := h.accountRegions()
	pol := regs.consistency()
	out := adminTopology{
		MultiWrite: regs.MultiWrite, AutomaticFailover: !regs.NoAutoFailover, WriteRegion: regs.home(),
		Consistency: &pol, Regions: make([]adminRegion, 0, len(regs.Regions)),
	}

	scheme := "http"
	if h.TLS {
		scheme = "https"
	}

	writers := regs.writers()

	for _, reg := range regs.Regions {
		out.Regions = append(out.Regions, adminRegion{
			Name: reg.Name, FailoverPriority: reg.FailoverPriority, ReplicationLagMs: reg.ReplicationLagMs,
			Offline: reg.Offline, Writable: slices.Contains(writers, reg.Name),
			Endpoint: h.regionEndpoint(reg.Name, scheme, r.Host),
		})
	}

	return out
}

// handleAdmin serves the unauthenticated emulator control endpoints under /_gopherstack/cosmosdb/regions:
//
//	GET  regions                 the topology
//	PUT  regions                 replace it (regions, lag, consistency, multiWrite, automaticFailover)
//	POST regions/{name}/offline  take a region down; the write region fails over by failover priority
//	POST regions/{name}/online   bring it back
func (h *Handler) handleAdmin(c *echo.Context) error {
	r := c.Request()
	path := strings.TrimSuffix(r.URL.Path, "/")

	if path == adminRegionsPath {
		switch r.Method {
		case http.MethodGet:
			return h.writeJSON(c, http.StatusOK, h.adminBody(r))
		case http.MethodPut:
			return h.putAdminTopology(c)
		}

		return h.methodNotAllowed(c)
	}

	rest, ok := strings.CutPrefix(path, adminRegionsPath+"/")
	if !ok || r.Method != http.MethodPost {
		return h.writeError(c, http.StatusNotFound, "NotFound", "Unknown admin endpoint.")
	}

	name, action, _ := strings.Cut(rest, "/")
	if action != "offline" && action != "online" {
		return h.writeError(c, http.StatusNotFound, "NotFound", "Unknown admin endpoint.")
	}

	b, isMem := h.Backend.(*InMemoryBackend)
	if !isMem {
		return h.writeError(c, http.StatusNotImplemented, "NotImplemented", "The backend has no regions.")
	}

	if err := b.SetRegionOffline(name, action == "offline"); err != nil {
		return h.writeError(c, http.StatusNotFound, "NotFound", "Unknown region "+name+".")
	}

	return h.writeJSON(c, http.StatusOK, h.adminBody(r))
}

func (h *Handler) putAdminTopology(c *echo.Context) error {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusNotImplemented, "NotImplemented", "The backend has no regions.")
	}

	raw, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var req adminTopology
	if jsonErr := json.Unmarshal(raw, &req); jsonErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
	}

	next := AccountRegions{
		MultiWrite:     req.MultiWrite,
		NoAutoFailover: !req.AutomaticFailover,
		Consistency:    req.Consistency,
	}
	for _, r := range req.Regions {
		next.Regions = append(next.Regions, AccountRegion{
			Name:             r.Name,
			FailoverPriority: r.FailoverPriority,
			ReplicationLagMs: r.ReplicationLagMs,
			Offline:          r.Offline,
		})
	}

	next = next.normalized()
	if vErr := validateTopology(next); vErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", vErr.Error())
	}

	b.ConfigureAccountRegions(next)

	return h.writeJSON(c, http.StatusOK, h.adminBody(c.Request()))
}

// validateTopology applies the account rules: known consistency level, no Strong with multiple write regions
// (Learn, "Consistency level choices"), bounded staleness no tighter than the documented minimums.
func validateTopology(r AccountRegions) error {
	for _, reg := range r.Regions {
		if reg.ReplicationLagMs < 0 {
			return fmt.Errorf("%w: replicationLagMs of %s is negative", ErrInvalidConsistency, reg.Name)
		}
	}

	if r.Consistency == nil {
		return nil
	}

	level := canonicalConsistency(r.Consistency.Level)
	if level == "" {
		return fmt.Errorf("%w: unknown consistency level %q", ErrInvalidConsistency, r.Consistency.Level)
	}

	r.Consistency.Level = level

	if level == ConsistencyStrong && r.MultiWrite {
		return fmt.Errorf("%w: Strong consistency is not available with multiple write regions", ErrInvalidConsistency)
	}

	if level != ConsistencyBoundedStaleness {
		return nil
	}

	minK, minT := int64(minStalenessPrefix1), minStalenessSecs1
	if len(r.Regions) > 1 {
		minK, minT = minStalenessPrefixN, minStalenessSecsN
	}

	if (r.Consistency.MaxStalenessPrefix != 0 && r.Consistency.MaxStalenessPrefix < minK) ||
		(r.Consistency.MaxIntervalInSeconds != 0 && r.Consistency.MaxIntervalInSeconds < minT) {
		return fmt.Errorf("%w: bounded staleness needs at least %d versions and %d seconds for this account",
			ErrInvalidConsistency, minK, minT)
	}

	return nil
}

// --- conflicts feed ---

type conflictBackend interface {
	ListConflicts(dbID, collID string) ([]ConflictInfo, error)
	GetConflict(dbID, collID, id string) (ConflictInfo, error)
	DeleteConflict(dbID, collID, id string) error
}

// conflictBodyOf renders a conflicts-feed entry as the service does: operationType, resourceType, the id of the
// conflicting item and its content as a JSON string.
func (h *Handler) conflictBodyOf(info ConflictInfo) map[string]any {
	m := map[string]any{
		"id": info.ID, sysPropRID: info.RID, sysPropSelf: info.Self, sysPropTS: info.Time.Unix(),
		sysPropETag: resourceETag(info.Time, info.ID), "operationType": info.Op, "resourceType": resourceTypeDocument,
		"resourceId": info.DocRID, "conflictLSN": info.LSN,
	}

	if info.Content != nil {
		raw, _ := jsonMarshal(h.encodeDocument(*info.Content))
		m["content"] = string(raw)
	}

	return m
}

func (h *Handler) conflictsBackend(c *echo.Context) (conflictBackend, bool) {
	cb, ok := h.be(c).(conflictBackend)
	if ok {
		return cb, true
	}

	if mem, isMem := h.Backend.(*InMemoryBackend); isMem {
		return plainConflicts{mem}, true
	}

	return nil, false
}

type plainConflicts struct{ b *InMemoryBackend }

func (p plainConflicts) ListConflicts(dbID, collID string) ([]ConflictInfo, error) {
	return p.b.ListConflicts(dbID, collID, "")
}

func (p plainConflicts) GetConflict(dbID, collID, id string) (ConflictInfo, error) {
	return p.b.GetConflict(dbID, collID, "", id)
}

func (p plainConflicts) DeleteConflict(dbID, collID, id string) error {
	return p.b.DeleteConflict(dbID, collID, "", id)
}

func (h *Handler) handleConflicts(c *echo.Context, dbID, collID string) error {
	cb, ok := h.conflictsBackend(c)
	if !ok {
		return h.methodNotAllowed(c)
	}

	r := c.Request()
	if r.Method != http.MethodGet && (r.Method != http.MethodPost || !isQueryRequest(r)) {
		return h.methodNotAllowed(c)
	}

	infos, err := cb.ListConflicts(dbID, collID)
	if err != nil {
		return h.writeDocumentError(c, err)
	}

	bodies := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		bodies = append(bodies, h.conflictBodyOf(info))
	}

	if r.Method == http.MethodPost {
		return h.queryResources(c, "Conflicts", bodies)
	}

	setCharge(c, ruMetadata)

	return h.writeJSON(c, http.StatusOK, map[string]any{
		sysPropRID: "", "Conflicts": bodies, sysPropCount: len(bodies),
	})
}

func (h *Handler) handleConflictItem(c *echo.Context, dbID, collID, id string) error {
	cb, ok := h.conflictsBackend(c)
	if !ok {
		return h.methodNotAllowed(c)
	}

	switch c.Request().Method {
	case http.MethodGet:
		info, err := cb.GetConflict(dbID, collID, id)
		if err != nil {
			return h.writeDocumentError(c, err)
		}

		setCharge(c, ruMetadata)

		return h.writeJSON(c, http.StatusOK, h.conflictBodyOf(info))
	case http.MethodDelete:
		if err := cb.DeleteConflict(dbID, collID, id); err != nil {
			return h.writeDocumentError(c, err)
		}

		setCharge(c, ruMetadata)

		return c.NoContent(http.StatusNoContent)
	}

	return h.methodNotAllowed(c)
}

func conflictOperationFor(kind resourceKind, r *http.Request) (string, bool) {
	if kind == resourceConflicts {
		return collectionOperationFor(r, unknownOperation, opListConflicts, opQueryConflicts), true
	}

	if kind == resourceConflictItem {
		return itemOperationFor(r.Method, opGetConflict, "", opDeleteConflict), true
	}

	return "", false
}

func (h *Handler) dispatchConflicts(c *echo.Context, kind resourceKind, dbID, collID, id string) (bool, error) {
	if kind == resourceConflicts {
		return true, h.handleConflicts(c, dbID, collID)
	}

	if kind == resourceConflictItem {
		return true, h.handleConflictItem(c, dbID, collID, id)
	}

	return false, nil
}

func conflictSupportedOperations() []string {
	return []string{opListConflicts, opGetConflict, opDeleteConflict, opQueryConflicts}
}
