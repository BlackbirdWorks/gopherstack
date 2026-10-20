package app

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const defaultFlushInterval = 5 * time.Second

// PersistModeSettings selects persistence modes (effective only with --persist).
type PersistModeSettings struct {
	PersistMode          string        `name:"persist-mode"           env:"GOPHERSTACK_PERSIST_MODE"           help:"Persistence mode: snapshot (debounced per-request) or hybrid (periodic flush of changed services)."   default:"snapshot" enum:"snapshot,hybrid"` //nolint:lll // config struct tags are intentionally verbose
	PersistServices      []string      `name:"persist-services"       env:"GOPHERSTACK_PERSIST_SERVICES"       help:"Only persist these services (comma-separated, case-insensitive). Empty = all."`                                                                  //nolint:lll // config struct tags are intentionally verbose
	PersistExclude       []string      `name:"persist-exclude"        env:"GOPHERSTACK_PERSIST_EXCLUDE"        help:"Do not persist these services (comma-separated, case-insensitive). Applied after --persist-services."`                                           //nolint:lll // config struct tags are intentionally verbose
	PersistServiceMode   []string      `name:"persist-service-mode"   env:"GOPHERSTACK_PERSIST_SERVICE_MODE"   help:"Per-service mode override as service=mode (comma-separated), e.g. s3=hybrid,sqs=snapshot."`                                                      //nolint:lll // config struct tags are intentionally verbose
	PersistFlushInterval time.Duration `name:"persist-flush-interval" env:"GOPHERSTACK_PERSIST_FLUSH_INTERVAL" help:"Hybrid mode flush interval; worst-case data loss on a crash."                                         default:"5s"`                              //nolint:lll // config struct tags are intentionally verbose
}

// persistRuntime owns the background hybrid flusher.
type persistRuntime struct {
	m     *persistence.Manager
	stops []func()
	save  bool
}

// initPersistence builds the manager and starts the persistence modes; the
// returned runtime's stop performs the final SaveAll.
func initPersistence(ctx context.Context, cli *CLI) (*persistence.Manager, *persistRuntime, error) {
	m, err := initPersistenceManager(ctx, cli)
	if err != nil {
		return nil, nil, err
	}

	rt, err := setupPersistModes(ctx, cli, m)
	if err != nil {
		return nil, nil, err
	}

	return m, rt, nil
}

func (s PersistModeSettings) policy() (persistence.Policy, error) {
	mode, err := persistence.ParseMode(s.PersistMode)
	if err != nil {
		return persistence.Policy{}, err
	}

	return persistence.NewPolicy(mode, s.PersistServices, s.PersistExclude, s.PersistServiceMode)
}

// setupPersistModes installs the persistence policy and starts the hybrid
// flusher. It must run before setupPersistence restores state.
func setupPersistModes(ctx context.Context, cli *CLI, m *persistence.Manager) (*persistRuntime, error) {
	rt := &persistRuntime{m: m, save: cli.Persist}
	if !cli.Persist || cli.Demo {
		return rt, nil
	}

	pol, err := cli.PersistModes.policy()
	if err != nil {
		return nil, err
	}

	m.SetPolicy(pol)

	if pol.Uses(persistence.ModeHybrid) {
		interval := cli.PersistModes.PersistFlushInterval
		if interval <= 0 {
			interval = defaultFlushInterval
		}

		rt.stops = append(rt.stops, m.StartFlusher(ctx, interval))
	}

	return rt, nil
}

// stop halts the background flusher, then saves every service.
func (rt *persistRuntime) stop(ctx context.Context) {
	if rt == nil {
		return
	}

	for _, s := range rt.stops {
		s()
	}

	if rt.save {
		rt.m.SaveAll(ctx)
	}
}

// persistenceMiddleware marks the target service dirty after each non-read
// mutating-method request (debounced save or hybrid flush, per policy).
func persistenceMiddleware(
	m *persistence.Manager,
	services []service.Registerable,
) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			handlerErr := next(c)

			if isMutatingMethod(c.Request().Method) && !isReadOp(c.Request()) {
				if svcName := extractServiceName(c, services); svcName != "" {
					m.Notify(svcName)
				}
			}

			return handlerErr
		}
	}
}

func readOpPrefixes() []string {
	return []string{"Get", "List", "Describe", "Head", "Query", "Scan", "BatchGet"}
}

// isReadOp classifies a request as a read from its operation name
// (X-Amz-Target suffix or Action query parameter). Unknown ops count as writes.
func isReadOp(r *http.Request) bool {
	op := r.Header.Get("X-Amz-Target")
	if i := strings.LastIndexByte(op, '.'); i >= 0 {
		op = op[i+1:]
	}

	if op == "" {
		op = r.URL.Query().Get("Action")
	}

	for _, p := range readOpPrefixes() {
		if strings.HasPrefix(op, p) {
			return true
		}
	}

	return false
}
