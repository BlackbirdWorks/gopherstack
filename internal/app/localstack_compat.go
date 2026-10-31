package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/version"
)

const (
	localstackPort      = "4566"
	localstackCompatEnv = "GOPHERSTACK_LOCALSTACK_COMPAT"
	extraListenEnv      = "GOPHERSTACK_EXTRA_LISTEN"
	initStageReady      = "READY"
	initStageShutdown   = "SHUTDOWN"
	editionCommunity    = "community"
	levelInfo           = "info"
	levelWarn           = "warn"
	levelError          = "error"
	initStateSuccess    = "SUCCESSFUL"
	initStateError      = "ERROR"
)

// localstackTriggerKeys enable compat mode when present, besides any LOCALSTACK_* variable.
func localstackTriggerKeys() []string {
	return []string{"GATEWAY_LISTEN", "LS_LOG", "HOSTNAME_EXTERNAL", "PERSISTENCE"}
}

// localstackEnvPlan translates LocalStack environment variables into Gopherstack ones.
// It never overrides a variable the user already set. enable reports whether compat
// mode should turn on; ignored lists LocalStack variables with no equivalent.
func localstackEnvPlan(environ []string) (map[string]string, []string, bool) {
	p := newEnvPlan(environ)

	p.planLogging()
	p.planStorage()
	p.planListen()

	if v, ok := p.env["LAMBDA_REMOVE_CONTAINERS"]; ok {
		if b, err := strconv.ParseBool(strings.TrimSpace(v)); err == nil && !b {
			p.put("LAMBDA_KEEP_CONTAINERS", "true")
		}
	}

	p.planOptions()

	ignored := p.unsupportedKeys()
	enable := len(presentKeys(p.env, localstackTriggerKeys())) > 0 || hasPrefixKey(p.env, "LOCALSTACK_")

	return p.set, ignored, enable
}

func newEnvPlan(environ []string) *envPlan {
	p := &envPlan{env: make(map[string]string, len(environ)), set: map[string]string{}}
	for _, kv := range environ {
		k, v, _ := strings.Cut(kv, "=")
		p.env[k] = v
	}

	return p
}

type envPlan struct {
	env         map[string]string
	set         map[string]string
	unsupported []string
}

// unsupportedKeys lists present options Gopherstack cannot honour (warned about
// once at startup), sorted and deduplicated. Benign infrastructure options are
// excluded; see benignKeys.
func (p *envPlan) unsupportedKeys() []string {
	out := append([]string(nil), p.unsupported...)
	out = append(out, presentKeys(p.env, localstackUnsupportedKeys())...)

	for k := range p.env {
		if strings.HasPrefix(k, "LOCALSTACK_") && !isHandledLocalstackKey(k) &&
			!slices.Contains(localstackBenignKeys(), k) {
			out = append(out, k)
		}
	}

	sort.Strings(out)

	return slices.Compact(out)
}

// benignKeys lists present options that only describe LocalStack's own infrastructure.
func (p *envPlan) benignKeys() []string {
	out := presentKeys(p.env, localstackBenignKeys())
	prefixes, suffixes := localstackBenignAffixes()

	for k := range p.env {
		if hasAnyPrefix(k, prefixes) || hasAnySuffix(k, suffixes) {
			out = append(out, k)
		}
	}

	sort.Strings(out)

	return slices.Compact(out)
}

func isHandledLocalstackKey(k string) bool {
	return k == "LOCALSTACK_PERSISTENCE" || k == "LOCALSTACK_RESPONSE_HEADER_ENABLED"
}

func hasAnyPrefix(s string, prefixes []string) bool {
	return slices.ContainsFunc(prefixes, func(p string) bool { return strings.HasPrefix(s, p) })
}

func hasAnySuffix(s string, suffixes []string) bool {
	return slices.ContainsFunc(suffixes, func(p string) bool { return strings.HasSuffix(s, p) })
}

// put records k=v unless the user set k or an earlier rule already did.
func (p *envPlan) put(k, v string) {
	if _, ok := p.env[k]; ok {
		return
	}

	if _, ok := p.set[k]; !ok {
		p.set[k] = v
	}
}

func (p *envPlan) val(k string) string { return strings.TrimSpace(p.env[k]) }

func (p *envPlan) truthy(k string) bool {
	b, err := strconv.ParseBool(p.val(k))

	return err == nil && b
}

func (p *envPlan) planLogging() {
	if lvl := lsLogLevel(p.val("LS_LOG")); lvl != "" {
		p.put("LOG_LEVEL", lvl)
	}

	if p.truthy("DEBUG") {
		p.put("LOG_LEVEL", "debug")
	}
}

func (p *envPlan) planStorage() {
	if p.truthy("PERSISTENCE") || p.truthy("LOCALSTACK_PERSISTENCE") {
		p.put("PERSIST", "true")
	}

	if d := p.val("DATA_DIR"); d != "" {
		p.put("GOPHERSTACK_DATA_DIR", d)
	}

	if r := p.val("DEFAULT_REGION"); r != "" {
		p.put("REGION", r)
	}
}

func (p *envPlan) planListen() {
	ports := gatewayListenPorts(p.val("GATEWAY_LISTEN"))
	if len(ports) == 0 {
		return
	}

	p.put("PORT", ports[0])

	if len(ports) > 1 {
		p.put(extraListenEnv, strings.Join(ports[1:], ","))
	}
}

func lsLogLevel(v string) string {
	switch strings.ToLower(v) {
	case "trace", "trace-internal", "debug":
		return "debug"
	case levelInfo:
		return levelInfo
	case levelWarn, "warning":
		return levelWarn
	case levelError, "critical":
		return levelError
	default:
		return ""
	}
}

// gatewayListenPorts extracts the ports from a GATEWAY_LISTEN list ("0.0.0.0:4566,0.0.0.0:443").
func gatewayListenPorts(v string) []string {
	var out []string

	for entry := range strings.SplitSeq(v, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		_, port, err := net.SplitHostPort(entry)
		if err != nil {
			port = strings.TrimPrefix(entry, ":")
		}

		if n, convErr := strconv.Atoi(port); convErr == nil && n > 0 && n < 65536 {
			out = append(out, port)
		}
	}

	return out
}

func presentKeys(env map[string]string, keys []string) []string {
	var out []string

	for _, k := range keys {
		if _, ok := env[k]; ok {
			out = append(out, k)
		}
	}

	return out
}

func hasPrefixKey(env map[string]string, prefix string) bool {
	for k := range env {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}

	return false
}

// applyLocalstackEnv exports the translated variables so kong picks them up.
func applyLocalstackEnv() {
	environ := os.Environ()
	set, ignored, enable := localstackEnvPlan(environ)

	if enable {
		if _, ok := os.LookupEnv(localstackCompatEnv); !ok {
			set[localstackCompatEnv] = "true"
		}
	}

	for k, v := range set {
		_ = os.Setenv(k, v)
	}

	ctx := context.Background()
	log := logger.Load(ctx)

	for _, k := range ignored {
		log.WarnContext(ctx, "LocalStack option "+k+" is not supported")
	}

	if benign := newEnvPlan(environ).benignKeys(); len(benign) > 0 {
		log.DebugContext(ctx, "LocalStack infrastructure options have no Gopherstack equivalent", "vars", benign)
	}
}

// extraListenPorts returns the additional ports to serve on, deduplicated and excluding the main port.
func extraListenPorts(cli *CLI) []string {
	main := strings.TrimPrefix(cli.Port, ":")
	seen := map[string]bool{main: true}
	var out []string

	add := func(p string) {
		p = strings.TrimPrefix(strings.TrimSpace(p), ":")
		if p != "" && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}

	if cli.LocalstackCompat {
		add(localstackPort)
	}

	for _, p := range cli.ExtraListen {
		add(p)
	}

	return out
}

// serveExtraPorts serves h on every extra port until ctx is done. Bind failures are
// logged and skipped: the main listener stays authoritative.
func serveExtraPorts(ctx context.Context, h http.Handler, ports []string, tlsCfg tlsSettings, log *slog.Logger) {
	for _, p := range ports {
		go serveExtraPort(ctx, h, p, tlsCfg, log)
	}
}

func serveExtraPort(ctx context.Context, h http.Handler, port string, tlsCfg tlsSettings, log *slog.Logger) {
	srv := &http.Server{
		Addr:              normalizeListenAddr(port),
		Handler:           h,
		ReadTimeout:       defaultTimeout,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		IdleTimeout:       defaultTimeout,
	}

	if !tlsCfg.enabled {
		protocols := new(http.Protocols)
		protocols.SetHTTP1(true)
		protocols.SetUnencryptedHTTP2(true)
		srv.Protocols = protocols
	}

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()

		_ = srv.Shutdown(shutdownCtx)
	}()

	log.InfoContext(ctx, "Serving additional listener", "port", port)

	if err := serveHTTP(srv, tlsCfg); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.WarnContext(ctx, "Additional listener unavailable", "port", port, "error", err)
	}
}

type localstackInitScript struct {
	Name  string
	State string
}

type localstackInitSnapshot struct {
	scripts []localstackInitScript
}

// localstackInitState records init-script outcomes for /_localstack/init.
type localstackInitState struct {
	snap atomic.Pointer[localstackInitSnapshot]
	mgr  atomic.Pointer[inithooks.Manager]
}

func newLocalstackInitState() *localstackInitState {
	s := &localstackInitState{}
	s.snap.Store(&localstackInitSnapshot{})

	return s
}

func (s *localstackInitState) record(results []inithooks.Result) {
	if s == nil {
		return
	}

	scripts := make([]localstackInitScript, 0, len(results))

	for _, r := range results {
		st := initStateSuccess
		if !r.OK {
			st = initStateError
		}

		scripts = append(scripts, localstackInitScript{Name: r.Script, State: st})
	}

	s.snap.Store(&localstackInitSnapshot{scripts: scripts})
}

func (s *localstackInitState) setManager(m *inithooks.Manager) {
	if s != nil {
		s.mgr.Store(m)
	}
}

// completed reports stage completion; without a stage-dir manager the legacy
// behaviour holds (everything but SHUTDOWN is done once the listener is up).
func (s *localstackInitState) completed(stage string) bool {
	if s != nil {
		if m := s.mgr.Load(); m != nil {
			return m.Completed(inithooks.Stage(stage))
		}
	}

	return stage != initStageShutdown
}

func (s *localstackInitState) scripts() []map[string]string {
	out := []map[string]string{}
	if s == nil {
		return out
	}

	if m := s.mgr.Load(); m != nil {
		for _, sc := range m.Status() {
			e := map[string]string{"stage": string(sc.Stage), "name": sc.Name, "state": string(sc.State)}
			if sc.Error != "" {
				e["error"] = sc.Error
			}

			out = append(out, e)
		}
	}

	for _, sc := range s.snap.Load().scripts {
		out = append(out, map[string]string{"stage": initStageReady, "name": sc.Name, "state": sc.State})
	}

	return out
}

// buildLocalstackInitHandler serves /_localstack/init (all stages) and
// /_localstack/init/:stage. Init hooks run before the listener opens, so every
// stage a request can observe is already complete except SHUTDOWN.
func buildLocalstackInitHandler(st *localstackInitState, stage string) echo.HandlerFunc {
	return func(c *echo.Context) error {
		scripts := st.scripts()
		stage := stage

		if stage == "" {
			stage = c.Param("stage")
		}

		if stage == "" {
			return c.JSON(http.StatusOK, map[string]any{
				"completed": map[string]bool{
					"BOOT": st.completed("BOOT"), "START": st.completed("START"),
					"READY": st.completed(initStageReady), initStageShutdown: st.completed(initStageShutdown),
				},
				"scripts": scripts,
			})
		}

		stage = strings.ToUpper(stage)

		switch stage {
		case "BOOT", "START", "READY", initStageShutdown:
		default:
			return c.JSON(http.StatusNotFound, map[string]string{"error": "invalid stage " + stage})
		}

		stageScripts := []map[string]string{}
		for _, sc := range scripts {
			if sc["stage"] == stage {
				stageScripts = append(stageScripts, sc)
			}
		}

		return c.JSON(http.StatusOK, map[string]any{"completed": st.completed(stage), "scripts": stageScripts})
	}
}

// localstackConfig is the read-only config subset exposed at /_localstack/config.
func localstackConfig(cli *CLI) map[string]any {
	return map[string]any{
		"DEBUG":          strings.EqualFold(cli.LogLevel, "debug"),
		"LS_LOG":         cli.LogLevel,
		"GATEWAY_LISTEN": ":" + strings.TrimPrefix(cli.Port, ":"),
		"EXTRA_LISTEN":   extraListenPorts(cli),
		"DEFAULT_REGION": cli.Region,
		"PERSISTENCE":    cli.Persist,
		"DATA_DIR":       cli.DataDir,
		"ACCOUNT_ID":     cli.AccountID,
		"ENFORCE_IAM":    cli.EnforceIAM,
	}
}

func buildLocalstackConfigGet(cli *CLI) echo.HandlerFunc {
	return func(c *echo.Context) error { return c.JSON(http.StatusOK, localstackConfig(cli)) }
}

func buildLocalstackConfigPost() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "runtime config updates are not supported; set the variable and restart",
		})
	}
}

func serviceNames(services []service.Registerable) []string {
	names := make([]string, 0, len(services))
	for _, svc := range services {
		names = append(names, strings.ToLower(svc.Name()))
	}

	sort.Strings(names)

	return names
}

func buildLocalstackDiagnoseHandler(cli *CLI, services []service.Registerable) echo.HandlerFunc {
	started := time.Now()

	return func(c *echo.Context) error {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		return c.JSON(http.StatusOK, map[string]any{
			podKeyVersion: map[string]string{"localstack-version": version.Get()},
			"info":        map[string]any{"edition": editionCommunity, "uptime": int(time.Since(started).Seconds())},
			"config":      localstackConfig(cli),
			"services":    serviceNames(services),
			"runtime":     map[string]any{"goroutines": runtime.NumGoroutine(), "heap_alloc_bytes": ms.HeapAlloc},
			"go-version":  runtime.Version(),
		})
	}
}

func buildLocalstackPluginsHandler(services []service.Registerable) echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string][]string{"localstack.aws.provider": serviceNames(services)})
	}
}

func registerLocalstackInternalEndpoints(e *echo.Echo, cli *CLI, services []service.Registerable) {
	e.GET("/_localstack/init", buildLocalstackInitHandler(cli.localstackInit, ""))
	e.GET("/_localstack/init/:stage", buildLocalstackInitHandler(cli.localstackInit, ""))
	e.POST("/_gopherstack/init/run", buildInitRunHandler(cli.localstackInit))
	e.GET("/_localstack/config", buildLocalstackConfigGet(cli))
	e.POST("/_localstack/config", buildLocalstackConfigPost())
	e.GET("/_localstack/diagnose", buildLocalstackDiagnoseHandler(cli, services))
	e.GET("/_localstack/plugins", buildLocalstackPluginsHandler(services))
	e.POST("/_localstack/health", buildLocalstackHealthPost(services, cli.Localstack.HealthKill, terminateSelf))
	registerLocalstackSwagger(e)
	chaos.RegisterLocalStackRoutes(e, cli.faultStore)
}

// serveAll serves the main port plus any compat/extra ports.
func serveAll(ctx context.Context, cli *CLI, h *echo.Echo, log *slog.Logger) error {
	// SHUTDOWN scripts run while the listeners are still up; they stop only
	// once srvCtx is cancelled after the stage.
	srvCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	var once sync.Once

	shutdownStage := func() { once.Do(func() { runShutdownStage(ctx, cli) }) }

	go func() {
		select {
		case <-ctx.Done():
			shutdownStage()
			cancel()
		case <-srvCtx.Done():
		}
	}()

	tlsCfg := tlsConfigFromCLI(cli)
	warnIfRemote(log, cli.Network, inContainer())
	serveExtraPorts(srvCtx, h, hostedPorts(cli.Network.Host, extraListenPorts(cli)), tlsCfg, log)

	err := startServer(srvCtx, listenAddr(cli.Network.Host, cli.Port), h, tlsCfg, &cli.shutdownDeadline)
	shutdownStage()

	return err
}

// LocalstackSettings holds opt-in LocalStack behaviours.
type LocalstackSettings struct {
	ResponseHeader bool `name:"response-header" env:"GOPHERSTACK_LOCALSTACK_RESPONSE_HEADER,LOCALSTACK_RESPONSE_HEADER_ENABLED" default:"false" help:"Add the X-Localstack: true response header."`                                                           //nolint:lll // config struct tags are intentionally verbose
	HealthKill     bool `name:"health-kill"     env:"GOPHERSTACK_LOCALSTACK_HEALTH_KILL"                                        default:"false" help:"Allow POST /_localstack/health with action=kill (loopback peers only) to gracefully stop the process."` //nolint:lll // config struct tags are intentionally verbose
}

const localstackResponseHeader = "X-Localstack"

// localstackResponseHeaderMiddleware stamps every response like LocalStack does.
func localstackResponseHeaderMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		c.Response().Header().Set(localstackResponseHeader, "true")

		return next(c)
	}
}
