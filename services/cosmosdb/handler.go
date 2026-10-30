package cosmosdb

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// cosmosVersion is the x-ms-version value echoed on every response -- a
// plausible, commonly-supported Cosmos DB REST API version.
const cosmosVersion = "2020-07-15"

// Operation name constants used for metrics (ExtractOperation) and
// GetSupportedOperations.
const (
	opGetDatabaseAccount = "GetDatabaseAccount"
	opListDatabases      = "ListDatabases"
	opCreateDatabase     = "CreateDatabase"
	opGetDatabase        = "GetDatabase"
	opDeleteDatabase     = "DeleteDatabase"
	opListContainers     = "ListContainers"
	opCreateContainer    = "CreateContainer"
	opGetContainer       = "GetContainer"
	opDeleteContainer    = "DeleteContainer"
	opCreateDocument     = "CreateDocument"
	opQueryDocuments     = "QueryDocuments"
	opQueryDatabases     = "QueryDatabases"
	opQueryContainers    = "QueryContainers"
	opReplaceContainer   = "ReplaceContainer"
	opListDocuments      = "ListDocuments"
	opGetDocument        = "GetDocument"
	opReplaceDocument    = "ReplaceDocument"
	opDeleteDocument     = "DeleteDocument"
	opPatchDocument      = "PatchDocument"
	opExecuteBatch       = "ExecuteTransactionalBatch"
	opReadChangeFeed     = "ReadChangeFeed"
	opReadPKRanges       = "ReadPartitionKeyRanges"
	opListOffers         = "ListOffers"
	opQueryOffers        = "QueryOffers"
	opGetOffer           = "GetOffer"
	opReplaceOffer       = "ReplaceOffer"
	unknownOperation     = "Unknown"
)

// Header names used throughout the wire protocol. The session-token header
// name is intentionally NOT a named constant here (it's inlined at its one
// call site in setCommonHeaders): a `headerSessionToken = "X-Ms-..."`-shaped
// declaration trips gosec's G101 hardcoded-credentials heuristic on the
// "Token" substring even though this is a header NAME, not a secret value.
const (
	headerIsQuery          = "X-Ms-Documentdb-Isquery"
	headerIsUpsert         = "X-Ms-Documentdb-Is-Upsert"
	headerPartitionKey     = "X-Ms-Documentdb-Partitionkey"
	headerRequestCharge    = "X-Ms-Request-Charge"
	headerActivityID       = "X-Ms-Activity-Id"
	headerContentTypeQuery = "application/query+json"
)

// coreOpsCapacity is the number of Core/SQL operations GetSupportedOperations lists.
const coreOpsCapacity = 61

// defaultRequestCharge is reported by routes without a size-based charge (see ru.go).
const defaultRequestCharge = "1"

// Handler is the Echo HTTP handler for Azure Cosmos DB (Core/SQL API)
// operations.
type Handler struct {
	workerCtx               context.Context
	Backend                 StorageBackend
	janitor                 *worker.Group
	keysMu                  *lockmetrics.RWMutex
	extraKeys               map[string]struct{}
	srv                     *http.Server
	srvMu                   *lockmetrics.RWMutex
	clock                   func() time.Time
	meter                   *ruMeter
	TableBackend            *odatatable.InMemoryBackend
	tlsCert                 *tls.Certificate
	regionSrvs              map[string]*regionListener
	MasterKey               string
	certPEM                 []byte
	TLSHosts                []string
	RegionPortBase          int
	Port                    int
	ResourceTokenMaxSeconds int
	SweepInterval           time.Duration
	ValidateAuth            bool
	TLS                     bool
	EnforceThroughput       bool
	BurstCapacity           bool
}

// defaultSweepInterval is the TTL janitor period; reads already hide expired documents.
const (
	defaultSweepInterval    = 10 * time.Second
	replicationPumpInterval = time.Second
)

// tableAPIBackendLockMetricsLabel is the lockmetrics label for TableBackend,
// distinguishing its lock-contention metrics from services/azuretable's own
// odatatable.InMemoryBackend instance (which passes "azuretable") -- both
// otherwise construct the exact same type and would collide onto one shared
// label if the engine picked its own internally.
const tableAPIBackendLockMetricsLabel = "cosmosdb"

// tableAPIBackendVersion is the snapshot version TableBackend enforces on Restore.
const tableAPIBackendVersion = 2

// NewHandler creates a new Cosmos DB Handler. Port/MasterKey default to
// DefaultPort/DefaultMasterKey; callers (typically provider.go) override
// them from Settings.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend:      backend,
		TableBackend: odatatable.NewInMemoryBackend(tableAPIBackendLockMetricsLabel, tableAPIBackendVersion),
		Port:         DefaultPort,
		MasterKey:    DefaultMasterKey,
		srvMu:        lockmetrics.New("cosmosdb.server"),
		keysMu:       lockmetrics.New("cosmosdb.keys"),
		extraKeys:    make(map[string]struct{}),
		meter:        newRUMeter(),
		regionSrvs:   map[string]*regionListener{},
	}

	if mem, ok := backend.(*InMemoryBackend); ok {
		mem.regionsChanged = h.syncRegionListeners
	}

	if a, ok := backend.(interface{ AttachTableAPI(t tableSnapshotter) }); ok {
		a.AttachTableAPI(h.TableBackend)
	}

	return h
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// Name returns the service name.
func (h *Handler) Name() string { return "CosmosDB" }

// GetSupportedOperations returns the list of supported Cosmos DB operations,
// Core/SQL and Table API combined.
func (h *Handler) GetSupportedOperations() []string {
	coreOps := make([]string, 0, coreOpsCapacity)
	coreOps = append(coreOps,
		opGetDatabaseAccount,
		opListDatabases, opQueryDatabases, opCreateDatabase, opGetDatabase, opDeleteDatabase,
		opListContainers, opQueryContainers, opCreateContainer, opGetContainer, opReplaceContainer, opDeleteContainer,
		opCreateDocument, opQueryDocuments, opListDocuments, opGetDocument, opReplaceDocument, opDeleteDocument,
		opPatchDocument, opExecuteBatch, opReadChangeFeed, opReadPKRanges,
		opListOffers, opQueryOffers, opGetOffer, opReplaceOffer,
	)
	coreOps = append(coreOps, conflictSupportedOperations()...)
	coreOps = append(coreOps, userSupportedOperations()...)
	coreOps = append(coreOps, scriptSupportedOperations()...)
	tableOps := tableAPISupportedOperations()

	ops := make([]string, 0, len(coreOps)+len(tableOps))
	ops = append(ops, coreOps...)
	ops = append(ops, tableOps...)

	return ops
}

// RouteMatcher exists only to satisfy service.Registerable's interface
// contract -- see provider.go's Provider doc comment. CosmosDB never
// matches on the shared AWS single-port Router; it runs on its own
// dedicated listener started by StartWorker.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority returns the routing priority for the CosmosDB handler.
// Irrelevant in practice since RouteMatcher never matches; 0 (lowest) is
// the safe default.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation extracts the Cosmos DB operation name from the request,
// for metrics labeling.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return operationFor(c.Request())
}

// ExtractResource extracts the resource path, for metrics labeling.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return strings.Trim(c.Request().URL.Path, "/")
}

// Reset clears all in-memory state, Core/SQL and Table API alike.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.TableBackend.Reset()
	h.meter.reset()
}

// Handler returns the Echo handler function for Cosmos DB operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()

		h.setCommonHeaders(c)

		if strings.HasPrefix(r.URL.Path, adminPrefix) {
			return h.handleAdmin(c)
		}

		if authErr := h.authorize(r); authErr != nil {
			logger.Load(r.Context()).WarnContext(r.Context(), "cosmosdb: request rejected", "error", authErr)

			status, code := authErrorStatus(authErr)

			return h.writeError(c, status, code, authErrorMessage(authErr))
		}

		if isTableAPIPath(r.URL.Path) {
			return h.handleTableAPI(c, strings.Trim(r.URL.Path, "/"))
		}

		if handled, err := h.guardRegion(c); handled {
			return err
		}

		kind, dbID, collID, docID := parseResourcePath(r.URL.Path)

		if h.EnforceThroughput && isMetered(kind) {
			return h.serveMetered(c, dbID, collID, func() error { return h.dispatch(c, kind, dbID, collID, docID) })
		}

		if h.EnforceThroughput && isMetadata(kind) {
			return h.serveMetadata(c, func() error { return h.dispatch(c, kind, dbID, collID, docID) })
		}

		return h.dispatch(c, kind, dbID, collID, docID)
	}
}

func (h *Handler) dispatch(c *echo.Context, kind resourceKind, dbID, collID, docID string) error {
	switch kind {
	case resourceAccountRoot:
		return h.handleAccountRoot(c)
	case resourceDatabases:
		return h.handleDatabases(c)
	case resourceDatabaseItem:
		return h.handleDatabaseItem(c, dbID)
	case resourceContainers:
		return h.handleContainers(c, dbID)
	case resourceContainerItem:
		return h.handleContainerItem(c, dbID, collID)
	case resourceDocuments:
		return h.handleDocuments(c, dbID, collID)
	case resourceDocumentItem:
		return h.handleDocumentItem(c, dbID, collID, docID)
	case resourcePKRanges:
		return h.handlePKRanges(c, dbID, collID)
	case resourceOffers:
		return h.handleOffers(c)
	case resourceOfferItem:
		return h.handleOfferItem(c, dbID)
	default:
		if handled, err := h.dispatchUsers(c, kind, dbID, collID, docID); handled {
			return err
		}

		if handled, err := h.dispatchConflicts(c, kind, dbID, collID, docID); handled {
			return err
		}

		if sk, isItem, ok := scriptKindOf(kind); ok {
			return h.handleScripts(c, sk, isItem, dbID, collID, docID)
		}

		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequest",
			"The requested URI does not represent a valid Cosmos DB resource.",
		)
	}
}

// setCommonHeaders sets the headers real Cosmos SDKs expect on every
// response, success or error: x-ms-version, x-ms-request-id, Date,
// x-ms-request-charge (fake RU accounting), x-ms-session-token, and
// x-ms-activity-id.
func (h *Handler) setCommonHeaders(c *echo.Context) {
	hdr := c.Response().Header()
	hdr.Set("X-Ms-Version", cosmosVersion)
	hdr.Set("X-Ms-Request-Id", newRequestID())
	hdr.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	hdr.Set(headerRequestCharge, defaultRequestCharge)
	hdr.Set("X-Ms-Session-Token", "0:0#0")
	hdr.Set(headerActivityID, newRequestID())
}

// newRequestID generates a plausible request/activity-id (UUID-shaped, not
// cryptographically meaningful).
func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "00000000-0000-0000-0000-000000000000"
	}

	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// --- Resource path parsing ---

// resourceKind classifies a parsed Cosmos DB resource path.
type resourceKind int

const (
	resourceInvalid resourceKind = iota
	// resourceAccountRoot is the database-account root resource ("" or
	// "/"). Every real Cosmos SDK -- azcosmos included -- issues a GET
	// here before it will make a single data-plane call: it's how the SDK
	// discovers the account's readable/writable regional endpoints (see
	// handleAccountRoot's doc comment). Omitting this resource entirely
	// (an earlier version of this handler did exactly that, falling
	// through to the generic 400 default branch) makes the service
	// unreachable by any unmodified SDK, not merely missing one feature.
	resourceAccountRoot
	resourceDatabases
	resourceDatabaseItem
	resourceContainers
	resourceContainerItem
	resourceDocuments
	resourceDocumentItem
	resourceSprocs
	resourceSprocItem
	resourceTriggers
	resourceTriggerItem
	resourceUDFs
	resourceUDFItem
	resourcePKRanges
	resourceOffers
	resourceOfferItem
	resourceConflicts
	resourceConflictItem
	resourceUsers
	resourceUserItem
	resourcePermissions
	resourcePermissionItem
)

// parseResourcePath classifies path ("", "/dbs", "/dbs/{db}",
// "/dbs/{db}/colls", "/dbs/{db}/colls/{coll}",
// "/dbs/{db}/colls/{coll}/docs", or "/dbs/{db}/colls/{coll}/docs/{id}") and
// extracts its database/container/document identifiers.
//
// Path segment counts (named, not magic numbers): "/dbs" -> 1, "/dbs/{db}"
// -> 2, ".../colls" -> 3, ".../colls/{coll}" -> 4, ".../docs" -> 5,
// ".../docs/{id}" -> 6.
const (
	segCountDatabases     = 1
	segCountDatabaseItem  = 2
	segCountContainers    = 3
	segCountContainerItem = 4
	segCountDocuments     = 5
	segCountDocumentItem  = 6
)

func parseResourcePath(path string) (resourceKind, string, string, string) {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return resourceAccountRoot, "", "", ""
	}

	segments := strings.Split(trimmed, "/")
	if segments[0] == offersSegment {
		return parseOffersPath(segments)
	}

	if segments[0] != dbsSegment {
		return resourceInvalid, "", "", ""
	}

	switch len(segments) {
	case segCountDatabases:
		return resourceDatabases, "", "", ""
	case segCountDatabaseItem:
		return resourceDatabaseItem, segments[1], "", ""
	case segCountContainers:
		return parseContainersPath(segments)
	case segCountContainerItem:
		return parseContainerItemPath(segments)
	case segCountDocuments:
		return parseDocumentsPath(segments)
	case segCountDocumentItem:
		return parseDocumentItemPath(segments)
	default:
		return resourceInvalid, "", "", ""
	}
}

// parseOffersPath classifies "/offers" and "/offers/{rid}"; the offer rid is returned in the database slot.
func parseOffersPath(segments []string) (resourceKind, string, string, string) {
	switch len(segments) {
	case segCountDatabases:
		return resourceOffers, "", "", ""
	case segCountDatabaseItem:
		return resourceOfferItem, segments[1], "", ""
	default:
		return resourceInvalid, "", "", ""
	}
}

func parseContainersPath(segments []string) (resourceKind, string, string, string) {
	if segments[2] == usersSegment {
		return resourceUsers, segments[1], "", ""
	}

	if segments[2] != collsSegment {
		return resourceInvalid, "", "", ""
	}

	return resourceContainers, segments[1], "", ""
}

func parseContainerItemPath(segments []string) (resourceKind, string, string, string) {
	if segments[2] == usersSegment {
		return resourceUserItem, segments[1], segments[3], ""
	}

	if segments[2] != collsSegment {
		return resourceInvalid, "", "", ""
	}

	return resourceContainerItem, segments[1], segments[3], ""
}

// collectionKindsFor maps the container's child-collection segment to its collection and item resource kinds.
func collectionKindsFor(segment string) ([2]resourceKind, bool) {
	switch segment {
	case docsSegment:
		return [2]resourceKind{resourceDocuments, resourceDocumentItem}, true
	case "sprocs":
		return [2]resourceKind{resourceSprocs, resourceSprocItem}, true
	case "triggers":
		return [2]resourceKind{resourceTriggers, resourceTriggerItem}, true
	case "udfs":
		return [2]resourceKind{resourceUDFs, resourceUDFItem}, true
	case conflictsSegment:
		return [2]resourceKind{resourceConflicts, resourceConflictItem}, true
	default:
		return [2]resourceKind{}, false
	}
}

func parseDocumentsPath(segments []string) (resourceKind, string, string, string) {
	if segments[2] == usersSegment && segments[4] == permissionsSegment {
		return resourcePermissions, segments[1], segments[3], ""
	}

	if segments[2] != collsSegment {
		return resourceInvalid, "", "", ""
	}

	if segments[4] == "pkranges" {
		return resourcePKRanges, segments[1], segments[3], ""
	}

	kinds, ok := collectionKindsFor(segments[4])
	if !ok {
		return resourceInvalid, "", "", ""
	}

	return kinds[0], segments[1], segments[3], ""
}

func parseDocumentItemPath(segments []string) (resourceKind, string, string, string) {
	if segments[2] == usersSegment && segments[4] == permissionsSegment {
		return resourcePermissionItem, segments[1], segments[3], segments[5]
	}

	if segments[2] != collsSegment {
		return resourceInvalid, "", "", ""
	}

	kinds, ok := collectionKindsFor(segments[4])
	if !ok {
		return resourceInvalid, "", "", ""
	}

	return kinds[1], segments[1], segments[3], segments[5]
}

// operationFor determines the Cosmos DB operation name for a request, for
// metrics labeling. Mirrors the dispatch logic in Handler() without side
// effects.
func operationFor(r *http.Request) string {
	if op, ok := tableAPIOperationFor(r); ok {
		return op
	}

	kind, _, _, _ := parseResourcePath(r.URL.Path)

	switch kind {
	case resourceAccountRoot:
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			return opGetDatabaseAccount
		}

		return unknownOperation
	case resourceDatabases:
		return collectionOperationFor(r, opCreateDatabase, opListDatabases, opQueryDatabases)
	case resourceDatabaseItem:
		return itemOperationFor(r.Method, opGetDatabase, "", opDeleteDatabase)
	case resourceContainers:
		return collectionOperationFor(r, opCreateContainer, opListContainers, opQueryContainers)
	case resourceContainerItem:
		return itemOperationFor(r.Method, opGetContainer, opReplaceContainer, opDeleteContainer)
	case resourceDocuments:
		return documentsCollectionOperationFor(r)
	case resourceDocumentItem:
		if r.Method == http.MethodPatch {
			return opPatchDocument
		}

		return itemOperationFor(r.Method, opGetDocument, opReplaceDocument, opDeleteDocument)
	case resourcePKRanges:
		return opReadPKRanges
	default:
		if op, ok := conflictOperationFor(kind, r); ok {
			return op
		}

		return offerOrScriptOperationFor(kind, r)
	}
}

func offerOrScriptOperationFor(kind resourceKind, r *http.Request) string {
	if op, ok := userOperationFor(kind, r); ok {
		return op
	}

	switch kind {
	case resourceOffers:
		return collectionOperationFor(r, unknownOperation, opListOffers, opQueryOffers)
	case resourceOfferItem:
		return itemOperationFor(r.Method, opGetOffer, opReplaceOffer, unknownOperation)
	default:
		return scriptOperationFor(kind, r)
	}
}

func collectionOperationFor(r *http.Request, createOp, listOp, queryOp string) string {
	switch r.Method {
	case http.MethodPost:
		if isQueryRequest(r) {
			return queryOp
		}

		return createOp
	case http.MethodGet:
		return listOp
	default:
		return unknownOperation
	}
}

func itemOperationFor(method, getOp, putOp, deleteOp string) string {
	switch method {
	case http.MethodGet:
		return getOp
	case http.MethodPut:
		if putOp == "" {
			return unknownOperation
		}

		return putOp
	case http.MethodDelete:
		return deleteOp
	default:
		return unknownOperation
	}
}

func documentsCollectionOperationFor(r *http.Request) string {
	switch r.Method {
	case http.MethodGet:
		if isChangeFeedRequest(r) {
			return opReadChangeFeed
		}

		return opListDocuments
	case http.MethodPost:
		if isBatchRequest(r) {
			return opExecuteBatch
		}

		if isQueryRequest(r) {
			return opQueryDocuments
		}

		return opCreateDocument
	default:
		return unknownOperation
	}
}

// isQueryRequest reports whether a POST to a documents collection is a SQL
// query (as opposed to a document create/upsert), per AZURE.md section 3:
// either the x-ms-documentdb-isquery header is "true" (case-insensitively)
// or the Content-Type is application/query+json.
func isQueryRequest(r *http.Request) bool {
	if strings.EqualFold(r.Header.Get(headerIsQuery), "true") {
		return true
	}

	return strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), headerContentTypeQuery)
}

// StartWorker binds the dedicated Cosmos DB listener (optionally TLS) and starts the TTL janitor. The bind is
// synchronous so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("cosmosdb: bind port %d: %w", h.Port, err)
	}

	// Port 0 binds an ephemeral port; reflect it so tests can read it without a release-and-rebind race.
	if addr, ok := listener.Addr().(*net.TCPAddr); ok {
		h.Port = addr.Port
	}

	if h.TLS {
		tlsListener, tlsErr := h.wrapTLS(listener)
		if tlsErr != nil {
			_ = listener.Close()

			return tlsErr
		}

		listener = tlsListener
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("CosmosDB", h.Handler(), h))

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: cosmosdbReadHeaderTimeout,
		ReadTimeout:       cosmosdbReadTimeout,
		WriteTimeout:      cosmosdbWriteTimeout,
		IdleTimeout:       cosmosdbIdleTimeout,
	}

	workerCtx := logger.WithWorker(ctx, "cosmosdb", "listener")
	log := logger.Load(workerCtx)

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.workerCtx = ctx
	h.janitor = h.startJanitor(ctx)
	h.srvMu.Unlock()

	h.syncRegionListeners()

	log.InfoContext(workerCtx, "cosmosdb: starting dedicated listener", "port", h.Port, "tls", h.TLS)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "cosmosdb: listener stopped", "error", serveErr)
		}
	}()

	return nil
}

// wrapTLS serves listener over TLS with an in-memory self-signed certificate valid for localhost and TLSHosts.
func (h *Handler) wrapTLS(listener net.Listener) (net.Listener, error) {
	certPEM, keyPEM, err := devtls.GenerateSelfSignedCertPEM(h.TLSHosts...)
	if err != nil {
		return nil, fmt.Errorf("cosmosdb: generate self-signed certificate: %w", err)
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("cosmosdb: load self-signed certificate: %w", err)
	}

	h.srvMu.Lock("wrapTLS")
	h.certPEM = certPEM
	h.tlsCert = &cert
	h.srvMu.Unlock()

	return tls.NewListener(listener, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"http/1.1"},
	}), nil
}

// TLSCertPEM returns the self-signed certificate (PEM) the TLS listener serves, for clients to trust; nil unless
// TLS is enabled and the worker started.
func (h *Handler) TLSCertPEM() []byte {
	h.srvMu.RLock("TLSCertPEM")
	defer h.srvMu.RUnlock()

	return h.certPEM
}

// startJanitor starts the TTL sweeper when the backend supports it.
func (h *Handler) startJanitor(ctx context.Context) *worker.Group {
	sweeper, ok := h.Backend.(interface{ SweepExpired() int })
	if !ok {
		return nil
	}

	interval := h.SweepInterval
	if interval <= 0 {
		interval = defaultSweepInterval
	}

	g := worker.NewGroup(ctx, "cosmosdb")
	g.Ticker("TTLSweeper", interval, 0, func(sweepCtx context.Context) {
		if n := sweeper.SweepExpired(); n > 0 {
			telemetry.RecordWorkerItems("cosmosdb", "TTLSweeper", n)
			logger.Load(sweepCtx).DebugContext(sweepCtx, "cosmosdb: expired documents removed", "count", n)
		}
	})

	if rp, rpOK := h.Backend.(interface{ SweepReplication() int }); rpOK {
		g.Ticker("ReplicationPump", replicationPumpInterval, 0, func(context.Context) {
			if n := rp.SweepReplication(); n > 0 {
				telemetry.RecordWorkerItems("cosmosdb", "ReplicationPump", n)
			}
		})
	}

	if sp, spOK := h.Backend.(interface{ SweepPartitions() int }); spOK {
		g.Ticker("PartitionSplitter", interval, 0, func(sweepCtx context.Context) {
			if n := sp.SweepPartitions(); n > 0 {
				telemetry.RecordWorkerItems("cosmosdb", "PartitionSplitter", n)
				logger.Load(sweepCtx).DebugContext(sweepCtx, "cosmosdb: physical partitions split", "count", n)
			}
		})
	}

	return g
}

// Timeouts for the dedicated Cosmos DB http.Server. ReadHeaderTimeout/
// ReadTimeout/IdleTimeout mirror services/azuretable's identical constants
// and their Slowloris rationale.
//
// WriteTimeout is set here, deliberately DIFFERENT from
// services/azureblob's http.Server, which just as deliberately does NOT set
// one (see its handler.go's own doc comment): Blob's rationale is that a
// finite WriteTimeout would cap how long a large blob download is allowed
// to take, which is actively wrong for that service's workload. That
// rationale does not transfer here -- every Cosmos DB response is a small
// JSON document (a single database/container/document resource, or one
// page of a query/read-feed result, none of which stream arbitrarily large
// payloads the way a blob GET can), so bounding write time is pure
// Slowloris-class hardening with no legitimate workload it could cut off.
// A future reader should not "fix" this back to match azureblob's choice --
// the two services' write-timeout needs are genuinely different, not an
// oversight in either one.
const (
	cosmosdbReadHeaderTimeout = 10 * time.Second
	cosmosdbReadTimeout       = 60 * time.Second
	cosmosdbWriteTimeout      = 60 * time.Second
	cosmosdbIdleTimeout       = 120 * time.Second
)

// Shutdown stops the dedicated Cosmos DB listener.
func (h *Handler) Shutdown(ctx context.Context) {
	h.stopRegionListeners()

	h.srvMu.Lock("Shutdown")
	srv, janitor := h.srv, h.janitor
	h.srv, h.janitor = nil, nil
	h.srvMu.Unlock()

	if janitor != nil {
		janitor.Stop()
	}

	if srv == nil {
		return
	}

	log := logger.Load(ctx)

	if err := srv.Shutdown(ctx); err != nil {
		log.ErrorContext(ctx, "cosmosdb: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "cosmosdb: forced close also failed", "error", closeErr)
		}
	}
}

// writeJSON marshals v and writes it as the response body with
// Content-Type: application/json.
func (h *Handler) writeJSON(c *echo.Context, status int, v any) error {
	body, err := jsonMarshal(v)
	if err != nil {
		return h.writeErrorNoRecurse(c, http.StatusInternalServerError, "InternalError", "Failed to marshal response.")
	}

	return c.Blob(status, "application/json", body)
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// cosmosErrorBody is Cosmos DB's JSON error envelope shape:
// {"code":"NotFound","message":"..."}.
type cosmosErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeError writes Cosmos DB's standard JSON error envelope.
func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	return h.writeJSON(c, status, cosmosErrorBody{Code: code, Message: message})
}

// writeErrorNoRecurse is writeJSON's own marshal-failure fallback: it must
// not call back into writeJSON (which could recurse if the error envelope
// itself somehow failed to marshal, though it never does in practice since
// it's built from plain strings).
func (h *Handler) writeErrorNoRecurse(c *echo.Context, status int, code, message string) error {
	body, _ := json.Marshal(cosmosErrorBody{Code: code, Message: message})

	return c.Blob(status, "application/json", body)
}
