package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/pods"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	strfold "github.com/blackbirdworks/gopherstack/pkgs/strs"
)

var errUnknownMergeStrategy = errors.New("unknown pod merge strategy")

const (
	podStrategyOverwrite    = "overwrite"
	podStrategyServiceMerge = "service-merge"
	podsBasePath            = "/_gopherstack/pods"
	podKeyName              = "name"
	podKeyVersion           = "version"
	podKeyStrategy          = "strategy"
	podKeyServices          = "services"
	podKeyStatus            = "status"
	podKeyMerge             = "merge"
)

// PodsConfig configures named, versioned state snapshots (pods).
type PodsConfig struct {
	Dir           string `name:"dir"           env:"GOPHERSTACK_POD_DIR"           help:"Local pod directory (default: <data-dir>/pods)."`                                                                    //nolint:lll // config struct tags are intentionally verbose
	S3Bucket      string `name:"s3-bucket"     env:"GOPHERSTACK_POD_S3_BUCKET"     help:"Store pods in this S3 bucket instead of the local directory."`                                                       //nolint:lll // config struct tags are intentionally verbose
	S3Prefix      string `name:"s3-prefix"     env:"GOPHERSTACK_POD_S3_PREFIX"     help:"Key prefix for pods in the S3 bucket."`                                                                              //nolint:lll // config struct tags are intentionally verbose
	S3Endpoint    string `name:"s3-endpoint"   env:"GOPHERSTACK_POD_S3_ENDPOINT"   help:"S3-compatible endpoint URL (empty means real AWS S3)."`                                                              //nolint:lll // config struct tags are intentionally verbose
	S3Region      string `name:"s3-region"     env:"GOPHERSTACK_POD_S3_REGION"     help:"Region of the pod S3 bucket."                                          default:"us-east-1"`                          //nolint:lll // config struct tags are intentionally verbose
	S3AccessKey   string `name:"s3-access-key" env:"GOPHERSTACK_POD_S3_ACCESS_KEY" help:"Access key for the pod S3 bucket (default credential chain if empty)."`                                              //nolint:lll // config struct tags are intentionally verbose
	S3SecretKey   string `name:"s3-secret-key" env:"GOPHERSTACK_POD_S3_SECRET_KEY" help:"Secret key for the pod S3 bucket."`                                                                                  //nolint:lll // config struct tags are intentionally verbose
	Autoload      string `name:"autoload"      env:"GOPHERSTACK_POD_AUTOLOAD,AUTO_LOAD_POD" help:"Load pods NAME[:VERSION] (comma-separated, in order) at startup."`                                          //nolint:lll // config struct tags are intentionally verbose
	MergeStrategy string `name:"merge-strategy" env:"GOPHERSTACK_POD_MERGE_STRATEGY,MERGE_STRATEGY" help:"How autoloaded pods combine with current state: overwrite, service-merge or account-region-merge."` //nolint:lll // config struct tags are intentionally verbose
	S3PathStyle   bool   `name:"s3-path-style" env:"GOPHERSTACK_POD_S3_PATH_STYLE" help:"Use path-style S3 addressing."`                                                                                      //nolint:lll // config struct tags are intentionally verbose
}

// newPodService builds the pod service from cfg; dataDir is the default local root.
func newPodService(ctx context.Context, cfg PodsConfig, dataDir string) (*pods.Service, error) {
	if cfg.S3Bucket != "" {
		st, err := pods.NewS3Store(ctx, pods.S3Config{
			Bucket: cfg.S3Bucket, Prefix: cfg.S3Prefix, Endpoint: cfg.S3Endpoint, Region: cfg.S3Region,
			AccessKey: cfg.S3AccessKey, SecretKey: cfg.S3SecretKey, PathStyle: cfg.S3PathStyle,
		})
		if err != nil {
			return nil, err
		}

		return pods.New(st), nil
	}

	dir := cfg.Dir
	if dir == "" {
		dir = filepath.Join(dataDir, "pods")
	}

	st, err := pods.NewDirStore(dir)
	if err != nil {
		return nil, err
	}

	return pods.New(st), nil
}

// podAPI serves the /_gopherstack/pods endpoints.
type podAPI struct {
	svc      *pods.Service
	manager  *persistence.Manager
	services []service.Registerable
}

func podHTTPError(err error) error {
	switch {
	case errors.Is(err, pods.ErrNotFound):
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	case errors.Is(err, pods.ErrInvalidName):
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	default:
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
}

func podVersionParam(raw string) (int, error) {
	if raw == "" || raw == "latest" {
		return 0, nil
	}

	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "invalid version "+strconv.Quote(raw))
	}

	return v, nil
}

func splitCSV(s string) []string {
	var out []string

	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}

	return out
}

func canonicalServiceName(snaps map[string][]byte, name string) (string, bool) {
	for k := range snaps {
		if strfold.Equal(k, name) {
			return k, true
		}
	}

	return "", false
}

// pickServices narrows snaps to the named services (case-insensitive); empty means all.
func pickServices(snaps map[string][]byte, names []string) (map[string][]byte, error) {
	if len(names) == 0 {
		return snaps, nil
	}

	subset := make(map[string][]byte, len(names))

	for _, n := range names {
		canon, ok := canonicalServiceName(snaps, n)
		if !ok {
			return nil, echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("service %q has no state to save", n))
		}

		subset[canon] = snaps[canon]
	}

	return subset, nil
}

type podSaveRequest struct {
	Message  string   `json:"message"`
	Services []string `json:"services"`
}

func (a *podAPI) save(c *echo.Context) error {
	var req podSaveRequest
	if c.Request().ContentLength != 0 {
		if err := c.Bind(&req); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid request body: "+err.Error())
		}
	}

	snaps, err := pickServices(a.manager.ExportAll(), req.Services)
	if err != nil {
		return err
	}

	m, err := a.svc.Save(c.Request().Context(), c.Param(podKeyName), req.Message, snaps)
	if err != nil {
		return podHTTPError(err)
	}

	return c.JSON(http.StatusCreated, m)
}

func podMergeParam(c *echo.Context) bool { return c.QueryParam(podKeyMerge) == ccTrue }

func (a *podAPI) load(c *echo.Context) error {
	strategy := c.QueryParam(podKeyStrategy)
	if strategy == "" {
		strategy = podStrategyOverwrite
	}

	if strategy != podStrategyOverwrite && strategy != podStrategyServiceMerge {
		return echo.NewHTTPError(
			http.StatusBadRequest,
			fmt.Sprintf(
				"unsupported strategy %q (supported: %s, %s)",
				strategy,
				podStrategyOverwrite,
				podStrategyServiceMerge,
			),
		)
	}

	version, err := podVersionParam(c.QueryParam(podKeyVersion))
	if err != nil {
		return err
	}

	ctx := c.Request().Context()

	m, snaps, err := a.svc.Load(ctx, c.Param(podKeyName), version, splitCSV(c.QueryParam(podKeyServices)))
	if err != nil {
		return podHTTPError(err)
	}

	merge := podMergeParam(c)

	if err = a.apply(ctx, strategy, merge, snaps); err != nil {
		return podApplyHTTPError(err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		podKeyStatus:   "ok",
		podKeyName:     m.Name,
		podKeyVersion:  m.Version,
		podKeyStrategy: strategy,
		podKeyMerge:    merge,
		"loaded":       len(snaps),
	})
}

func (a *podAPI) list(c *echo.Context) error {
	l, err := a.svc.List(c.Request().Context())
	if err != nil {
		return podHTTPError(err)
	}

	return c.JSON(http.StatusOK, map[string]any{"pods": l})
}

func (a *podAPI) versions(c *echo.Context) error {
	v, err := a.svc.Versions(c.Request().Context(), c.Param(podKeyName))
	if err != nil {
		return podHTTPError(err)
	}

	return c.JSON(http.StatusOK, map[string]any{podKeyName: c.Param(podKeyName), "versions": v})
}

func (a *podAPI) inspect(c *echo.Context) error {
	version, err := podVersionParam(c.Param("version"))
	if err != nil {
		return err
	}

	m, err := a.svc.Inspect(c.Request().Context(), c.Param(podKeyName), version)
	if err != nil {
		return podHTTPError(err)
	}

	return c.JSON(http.StatusOK, m)
}

func (a *podAPI) remove(c *echo.Context) error {
	version, err := podVersionParam(c.QueryParam(podKeyVersion))
	if err != nil {
		return err
	}

	if err = a.svc.Delete(c.Request().Context(), c.Param(podKeyName), version); err != nil {
		return podHTTPError(err)
	}

	return c.JSON(http.StatusOK, map[string]any{podKeyStatus: "ok"})
}

// registerPodEndpoints mounts the pod API on e and runs the optional startup autoload.
func registerPodEndpoints(
	ctx context.Context,
	log *slog.Logger,
	e *echo.Echo,
	cfg PodsConfig,
	dataDir string,
	m *persistence.Manager,
	services []service.Registerable,
) {
	svc, err := newPodService(ctx, cfg, dataDir)
	if err != nil {
		log.ErrorContext(ctx, "pods disabled: storage init failed", "error", err)

		return
	}

	a := &podAPI{svc: svc, manager: m, services: services}

	e.GET(podsBasePath, a.list)
	e.POST(podsBasePath+"/:name", a.save)
	e.GET(podsBasePath+"/:name", a.versions)
	e.DELETE(podsBasePath+"/:name", a.remove)
	e.GET(podsBasePath+"/:name/versions/:version", a.inspect)
	e.POST(podsBasePath+"/:name/load", a.load)
	a.registerLocalstackAliases(e)

	if cfg.Autoload != "" {
		if err = a.autoload(ctx, cfg.Autoload, cfg.MergeStrategy); err != nil {
			logger.Load(ctx).ErrorContext(ctx, "pod autoload failed", "pod", cfg.Autoload, "error", err)
		} else {
			log.InfoContext(ctx, "pod autoloaded", "pod", cfg.Autoload)
		}
	}
}

// podStrategyAccountRegionMerge is LocalStack's MERGE_STRATEGY name for a scoped merge.
const podStrategyAccountRegionMerge = "account-region-merge"

// autoload loads each comma-separated NAME[:VERSION] in order; strategy is
// overwrite (default), service-merge or account-region-merge.
func (a *podAPI) autoload(ctx context.Context, spec, strategy string) error {
	strategy = strings.ToLower(strings.TrimSpace(strategy))
	merge := strategy == podStrategyAccountRegionMerge

	switch strategy {
	case "", podStrategyOverwrite:
		strategy = podStrategyOverwrite
	case podStrategyServiceMerge:
	case podStrategyAccountRegionMerge:
		strategy = podStrategyServiceMerge
	default:
		return fmt.Errorf("%w: %q", errUnknownMergeStrategy, strategy)
	}

	for _, one := range splitCSV(spec) {
		name, ver, _ := strings.Cut(one, ":")

		version, err := podVersionParam(ver)
		if err != nil {
			return err
		}

		_, snaps, err := a.svc.Load(ctx, name, version, nil)
		if err != nil {
			return fmt.Errorf("load pod %s: %w", name, err)
		}

		if err = a.apply(ctx, strategy, merge, snaps); err != nil {
			return err
		}
	}

	return nil
}
