package neptune

import (
	"encoding/json/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	keyVersion          = "version"
	keyStatisticsID     = "statisticsId"
	statusPath          = "/status"
	statsRDFPath        = "/rdf/statistics"
	statsSparqlPath     = "/sparql/statistics"
	statsPGPath         = "/propertygraph/statistics"
	statsPGShortPath    = "/pg/statistics"
	statsGremlinPath    = "/gremlin/statistics"
	actionStatsStatus   = "neptune-db:GetStatisticsStatus"
	actionStatsManage   = "neptune-db:ManageStatistics"
	actionStatsDelete   = "neptune-db:DeleteStatistics"
	actionEngineStatus  = "neptune-db:GetEngineStatus"
	modeEnableAuto      = "enableAutoCompute"
	modeDisableAuto     = "disableAutoCompute"
	modeRefresh         = "refresh"
	javaDateLayout      = "Mon Jan 02 15:04:05 MST 2006"
	gremlinEngineLabel  = "tinkerpop-3.7.1"
	sparqlEngineLabel   = "sparql-1.1"
	openCypherLabel     = "Neptune-9.0.20190305-1.0"
	defaultQueryTimeout = "120000"
	defaultSlowLogMs    = "5000"
	statusEnabled       = "enabled"
	statusDisabled      = "disabled"
)

// engineInfo is the live cluster state /status reports.
type engineInfo struct {
	version      string
	queryTimeout string
	slowLog      string
	slowMs       string
	minCapacity  float64
	maxCapacity  float64
	iamAuth      bool
	streams      bool
	audit        bool
	serverless   bool
}

type statsState struct {
	date   time.Time
	mu     *lockmetrics.RWMutex
	id     int64
	sigs   int
	insts  int
	preds  int
	active bool
	auto   bool
}

func newStatsState() *statsState {
	return &statsState{mu: lockmetrics.New("neptune-stats"), id: -1, auto: true}
}

func (f *cypherFront) serveGraphAdmin(w http.ResponseWriter, r *http.Request) bool {
	path := strings.ToLower(strings.TrimRight(r.URL.Path, "/"))

	switch path {
	case statusPath:
		f.serveEngineStatus(w, r)
	case statsRDFPath, statsSparqlPath:
		f.serveStats(w, r, langSparql, f.rdf.rdfStats, f.rdfStatsCompute)
	case statsPGPath, statsPGShortPath, statsGremlinPath:
		f.serveStats(w, r, langOpenCypher, f.rdf.pgStats, f.pgStatsCompute)
	default:
		return false
	}

	return true
}

func (f *cypherFront) rdfStatsCompute() (int, int, int) { return f.rdf.store.Stats() }

func (f *cypherFront) pgStatsCompute() (int, int, int) { return f.graph.Stats() }

func (f *cypherFront) serveEngineStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeCypherError(w, http.StatusMethodNotAllowed, "BadRequestException", "Unsupported method for this path")

		return
	}

	if !authorizeActions(w, r, langSparql, actionEngineStatus) {
		return
	}

	info := engineInfo{queryTimeout: defaultQueryTimeout, slowLog: statusDisabled, slowMs: defaultSlowLogMs}
	if f.rdf.info != nil {
		info = f.rdf.info()
	}

	features := map[string]any{
		"SlowQueryLogs": info.slowLog, "ResultCache": map[string]string{"status": statusDisabled},
		"IAMAuthentication": enabledWord(info.iamAuth), "Streams": enabledWord(info.streams),
		"AuditLog": enabledWord(info.audit), "QueryTimeout": info.queryTimeout,
	}

	body := map[string]any{
		"status":          "healthy",
		"startTime":       f.started.UTC().Format(javaDateLayout),
		"dbEngineVersion": info.version,
		"role":            "writer",
		"dfeQueryEngine":  "viaQueryHint",
		"gremlin":         map[string]string{keyVersion: gremlinEngineLabel},
		"sparql":          map[string]string{keyVersion: sparqlEngineLabel},
		"opencypher":      map[string]string{keyVersion: openCypherLabel},
		"labMode": map[string]string{
			"ObjectIndex":                statusDisabled,
			"ReadWriteConflictDetection": statusEnabled,
		},
		"features": features,
		"settings": map[string]string{
			"clusterQueryTimeoutInMs": info.queryTimeout,
			"SlowQueryLogsThreshold":  info.slowMs,
		},
	}

	if info.serverless {
		body["serverlessConfiguration"] = map[string]string{
			"minCapacity": strconv.FormatFloat(info.minCapacity, 'f', 1, 64),
			"maxCapacity": strconv.FormatFloat(info.maxCapacity, 'f', 1, 64),
		}
	}

	writeCypherJSON(w, http.StatusOK, body)
}

func enabledWord(v bool) string {
	if v {
		return statusEnabled
	}

	return statusDisabled
}

func (f *cypherFront) serveStats(
	w http.ResponseWriter, r *http.Request, lang string, st *statsState, compute func() (int, int, int),
) {
	switch r.Method {
	case http.MethodGet:
		if authorizeActions(w, r, lang, actionStatsStatus) {
			writeCypherJSON(w, http.StatusOK, envelope(st.payload()))
		}
	case http.MethodPost:
		if authorizeActions(w, r, lang, actionStatsManage) {
			manageStats(w, r, st, compute)
		}
	case http.MethodDelete:
		if authorizeActions(w, r, lang, actionStatsDelete) {
			deleteStats(w, st)
		}
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		writeCypherError(w, http.StatusMethodNotAllowed, "BadRequestException", "Unsupported method for this path")
	}
}

func (s *statsState) payload() map[string]any {
	s.mu.RLock("statsPayload")
	defer s.mu.RUnlock()

	p := map[string]any{"autoCompute": s.auto, "active": s.active, keyStatisticsID: strconv.FormatInt(s.id, 10)}
	if s.active {
		p["date"] = s.date.UTC().Format("2006-01-02T15:04:05.000Z")
		p["signatureInfo"] = map[string]int{
			"signatureCount": s.sigs, "instanceCount": s.insts, "predicateCount": s.preds,
		}
	}

	return p
}

func manageStats(w http.ResponseWriter, r *http.Request, st *statsState, compute func() (int, int, int)) {
	if mediaType(r.Header.Get("Content-Type")) == "" {
		writeCypherError(w, http.StatusBadRequest, codeMissingParam, "Content-type header not specified.")

		return
	}

	var req struct {
		Mode string `json:"mode"`
	}

	r.Body = http.MaxBytesReader(w, r.Body, cypherMaxBody)

	if err := json.UnmarshalRead(r.Body, &req, jsonx.V1()); err != nil {
		writeCypherError(w, http.StatusBadRequest, codeBadRequest, "Invalid statistics request: "+err.Error())

		return
	}

	st.mu.Lock("manageStats")
	defer st.mu.Unlock()

	switch req.Mode {
	case modeEnableAuto:
		st.auto = true
		writeCypherJSON(w, http.StatusOK, map[string]string{keyStatus: statusOK200})
	case modeDisableAuto:
		st.auto = false
		writeCypherJSON(w, http.StatusOK, map[string]string{keyStatus: statusOK200})
	case modeRefresh:
		st.sigs, st.insts, st.preds = compute()
		st.id, st.date, st.active = time.Now().UnixMilli(), time.Now().UTC(), true
		writeCypherJSON(w, http.StatusOK, envelope(map[string]string{keyStatisticsID: strconv.FormatInt(st.id, 10)}))
	default:
		writeCypherError(w, http.StatusBadRequest, codeInvalidParam,
			"Statistics command parameter 'mode' has unsupported value '"+req.Mode+"'")
	}
}

func deleteStats(w http.ResponseWriter, st *statsState) {
	st.mu.Lock("deleteStats")
	defer st.mu.Unlock()

	if !st.active {
		w.WriteHeader(http.StatusNoContent)

		return
	}

	st.active, st.id, st.sigs, st.insts, st.preds = false, -1, 0, 0, 0
	writeCypherJSON(w, http.StatusOK, envelope(map[string]any{"active": false, keyStatisticsID: "-1"}))
}

func paramValue(params []EngineParameter, name, def string) string {
	for _, p := range params {
		if p.ParameterName == name && p.ParameterValue != "" {
			return p.ParameterValue
		}
	}

	return def
}

// engineInfoFn reports the live cluster's configuration for the /status endpoint.
func (b *InMemoryBackend) engineInfoFn(lb *liveCluster) func() engineInfo {
	return func() engineInfo {
		b.mu.RLock("neptuneEngineInfo")
		defer b.mu.RUnlock()

		c, ok := b.clusterGet(lb.region, lb.id)
		if !ok {
			return engineInfo{queryTimeout: defaultQueryTimeout, slowLog: statusDisabled, slowMs: defaultSlowLogMs}
		}

		params := describeParameters(
			neptuneClusterParameterCatalog(),
			b.clusterParameterOverrides[regionKey(lb.region, c.DBClusterParameterGroupName)],
		)

		info := engineInfo{
			version: c.EngineVersion, iamAuth: c.EnableIAMDatabaseAuthentication,
			streams:      paramValue(params, "neptune_streams", "0") == "1",
			audit:        paramValue(params, "neptune_enable_audit_log", "0") == "1",
			queryTimeout: paramValue(params, "neptune_query_timeout", defaultQueryTimeout),
			slowMs:       paramValue(params, "neptune_slow_query_log_threshold", defaultSlowLogMs),
		}

		info.slowLog = statusDisabled
		if v := strings.ToLower(paramValue(params, "neptune_enable_slow_query_log", "")); v == "info" || v == "debug" {
			info.slowLog = v
		}

		if sc := c.ServerlessV2ScalingConfig; sc != nil {
			info.serverless, info.minCapacity, info.maxCapacity = true, sc.MinCapacity, sc.MaxCapacity
		}

		return info
	}
}
