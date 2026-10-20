package mwaa

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	lastUpdateFailed = "FAILED"
	airflowHome      = "/opt/airflow"
	mwaaMountDir     = "/mwaa"
	airflowStateDir  = "/mwaa-state"
	webserverConfig  = "from flask_appbuilder.const import AUTH_DB\n" +
		"AUTH_TYPE = AUTH_DB\n"
)

// airflowLaunch is the immutable input one container launch needs, copied under the backend lock.
type airflowLaunch struct {
	options        map[string]string
	key            string
	region         string
	name           string
	version        string
	bucketARN      string
	dagPath        string
	reqPath        string
	reqVersion     string
	pluginsPath    string
	pluginsVersion string
	startupPath    string
	startupVersion string
	adminPass      string
}

func newAirflowLaunch(env *Environment) airflowLaunch {
	opts := make(map[string]string, len(env.AirflowConfigurationOptions))
	maps.Copy(opts, env.AirflowConfigurationOptions)

	return airflowLaunch{
		key:            regionKey(env.region, env.Name),
		region:         env.region,
		name:           env.Name,
		version:        env.AirflowVersion,
		bucketARN:      env.SourceBucketArn,
		dagPath:        env.DagS3Path,
		reqPath:        env.RequirementsS3Path,
		reqVersion:     env.RequirementsS3ObjectVersion,
		pluginsPath:    env.PluginsS3Path,
		pluginsVersion: env.PluginsS3ObjectVersion,
		startupPath:    env.StartupScriptS3Path,
		startupVersion: env.StartupScriptS3ObjectVersion,
		options:        opts,
	}
}

// launchAirflowLocked registers a container for env and boots it in the background; callers hold b.mu.
// prior, when set, is the previous container of the same environment, stopped first.
func (b *InMemoryBackend) launchAirflowLocked(env *Environment, prior *airflowEnv) bool {
	e := b.airflow
	if e == nil {
		return false
	}

	in := newAirflowLaunch(env)
	in.adminPass = newAdminPassword()
	ctx, cancel := context.WithCancel(context.Background())
	ae := &airflowEnv{
		ctx: ctx, cancel: cancel, done: make(chan struct{}), version: env.AirflowVersion, adminPass: in.adminPass,
	}
	e.envs[in.key] = ae

	e.wg.Go(func() {
		if prior != nil {
			prior.cancel()
			<-prior.done
		}

		b.runAirflow(e, ae, in)
	})

	return true
}

func (b *InMemoryBackend) runAirflow(e *airflowEngine, ae *airflowEnv, in airflowLaunch) {
	defer close(ae.done)
	defer e.teardown(ae)

	err := b.bootAirflow(e, ae, in)
	if ae.ctx.Err() != nil {
		return
	}

	b.finishAirflow(ae, in.key, err)

	if err == nil {
		b.watchAirflow(e, ae, in.key)
	}
}

func (b *InMemoryBackend) bootAirflow(e *airflowEngine, ae *airflowEnv, in airflowLaunch) error {
	ctx, cancel := context.WithTimeout(ae.ctx, e.cfg.StartTimeout)
	defer cancel()

	dir, err := os.MkdirTemp("", "mwaa-stage-*")
	if err != nil {
		return fmt.Errorf("create stage dir: %w", err)
	}

	b.mu.Lock("bootAirflow")
	ae.stageDir = dir
	b.mu.Unlock()

	if err = b.syncAirflow(ctx, e, dir, in, true); err != nil {
		return err
	}

	port, err := e.acquirePort(in.name)
	if err != nil {
		return err
	}

	b.mu.Lock("bootAirflow")
	ae.port = port
	ae.base = e.baseURL(port)
	b.setWebserverURLLocked(in.key, ae.base)
	b.mu.Unlock()

	id, err := e.cfg.Runtime.CreateAndStart(ctx, e.spec(in, dir, port))
	if err != nil {
		return fmt.Errorf("start airflow container: %w", err)
	}

	b.mu.Lock("bootAirflow")
	ae.containerID = id
	b.mu.Unlock()

	return pollUntil(ctx, e.cfg.PollInterval, func(c context.Context) (bool, error) {
		if ferr := containerExitError(dir); ferr != nil {
			return false, ferr
		}

		return e.healthy(c, ae.base), nil
	})
}

func (b *InMemoryBackend) setWebserverURLLocked(key, url string) {
	if env, ok := b.environments.Get(key); ok {
		env.WebserverURL = url
	}
}

// finishAirflow settles the environment status once a boot or resync ended.
func (b *InMemoryBackend) finishAirflow(ae *airflowEnv, key string, err error) {
	b.mu.Lock("finishAirflow")
	defer b.mu.Unlock()

	env, ok := b.environments.Get(key)
	if !ok || b.airflow == nil || b.airflow.envs[key] != ae {
		return
	}

	if err != nil {
		failAirflowEnv(env, err)

		return
	}

	env.Status = envStatusAvailable

	if env.LastUpdate != nil && env.LastUpdate.Status == lastUpdatePending {
		env.LastUpdate.Status = lastUpdateSuccess
	}
}

func failAirflowEnv(env *Environment, err error) {
	if env.Status == envStatusCreating {
		env.Status = envStatusCreateFailed

		return
	}

	env.Status = envStatusUpdateFailed

	if env.LastUpdate != nil {
		env.LastUpdate.Status = lastUpdateFailed
		env.LastUpdate.Error = &UpdateError{ErrorCode: "500", ErrorMessage: err.Error()}
	}
}

// watchAirflow re-syncs DAGs from S3 until the environment is deleted or restarted.
func (b *InMemoryBackend) watchAirflow(e *airflowEngine, ae *airflowEnv, key string) {
	t := time.NewTicker(e.cfg.SyncInterval)
	defer t.Stop()

	for {
		select {
		case <-ae.ctx.Done():
			return
		case <-t.C:
			b.resyncDags(e, ae, key)
		}
	}
}

func (b *InMemoryBackend) resyncDags(e *airflowEngine, ae *airflowEnv, key string) {
	b.mu.RLock("resyncDags")
	env, ok := b.environments.Get(key)

	if !ok || env.Status != envStatusAvailable {
		b.mu.RUnlock()

		return
	}

	in, dir := newAirflowLaunch(env), ae.stageDir
	b.mu.RUnlock()

	if err := b.syncAirflow(ae.ctx, e, dir, in, false); err != nil && ae.ctx.Err() == nil {
		logger.Load(ae.ctx).WarnContext(ae.ctx, "mwaa: dag sync failed", "environment", in.name, "error", err)
	}
}

// resyncAirflowLocked re-reads DAGs for an UPDATING environment that needs no restart.
func (b *InMemoryBackend) resyncAirflowLocked(env *Environment, ae *airflowEnv) {
	e := b.airflow
	in, dir := newAirflowLaunch(env), ae.stageDir

	e.wg.Go(func() {
		err := b.syncAirflow(ae.ctx, e, dir, in, false)
		if ae.ctx.Err() == nil {
			b.finishAirflow(ae, in.key, err)
		}
	})
}

func (e *airflowEngine) image(in airflowLaunch) string {
	if e.cfg.Image != "" {
		return e.cfg.Image
	}

	return airflowImage(in.version)
}

func (e *airflowEngine) spec(in airflowLaunch, stageDir string, port int) container.Spec {
	bind := ""
	if e.cfg.Host == airflowDefaultHost {
		bind = airflowDefaultHost + ":"
	}

	return container.Spec{
		Image: e.image(in),
		Env:   e.envVars(in),
		Cmd:   []string{"bash", "-c", startScript(in.version)},
		Ports: []string{bind + strconv.Itoa(port) + ":" + airflowWebPort},
		Mounts: []string{
			filepath.Join(stageDir, "dags") + ":" + airflowHome + "/dags",
			filepath.Join(stageDir, "plugins") + ":" + airflowHome + "/plugins",
			filepath.Join(stageDir, "webserver_config.py") + ":" + airflowHome + "/webserver_config.py:ro",
			filepath.Join(stageDir, "requirements.txt") + ":" + mwaaMountDir + "/requirements.txt:ro",
			filepath.Join(stageDir, "startup.sh") + ":" + mwaaMountDir + "/startup.sh:ro",
			filepath.Join(stageDir, "state") + ":" + airflowStateDir,
			filepath.Join(stageDir, "simple_auth_passwords.json") + ":" + airflowHome + "/simple_auth_passwords.json",
		},
	}
}

// envVars builds the container environment: defaults, then AirflowConfigurationOptions as AIRFLOW__SECTION__KEY.
func (e *airflowEngine) envVars(in airflowLaunch) []string {
	vars := map[string]string{
		"AIRFLOW__CORE__LOAD_EXAMPLES":                  "False",
		"AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL":     "5",
		"AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL": "5",
		"AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL":      "5",
		"AWS_REGION":            in.region,
		"AWS_DEFAULT_REGION":    in.region,
		"AWS_ACCESS_KEY_ID":     airflowDefaultKey,
		"AWS_SECRET_ACCESS_KEY": airflowDefaultKey,
		"MWAA_ADMIN_PASSWORD":   in.adminPass,
	}

	maps.Copy(vars, authEnv(in.version))

	if e.cfg.EndpointURL != "" {
		vars["AWS_ENDPOINT_URL"] = e.cfg.EndpointURL
	}

	for k, v := range in.options {
		if name, ok := airflowOptionEnv(k); ok {
			vars[name] = v
		}
	}

	out := make([]string, 0, len(vars))
	for k, v := range vars {
		out = append(out, k+"="+v)
	}

	sort.Strings(out)

	return out
}

func authEnv(version string) map[string]string {
	if isAirflowV3(version) {
		return map[string]string{
			"AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS":          airflowAdminUser + ":admin",
			"AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE": airflowHome + "/simple_auth_passwords.json",
		}
	}

	return map[string]string{
		"AIRFLOW__API__AUTH_BACKENDS": "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session",
	}
}

// airflowOptionEnv maps "core.dag_concurrency" to AIRFLOW__CORE__DAG_CONCURRENCY.
func airflowOptionEnv(option string) (string, bool) {
	section, key, ok := strings.Cut(option, ".")
	if !ok || section == "" || key == "" {
		return "", false
	}

	return "AIRFLOW__" + strings.ToUpper(section) + "__" + strings.ToUpper(key), true
}

// reconcileAirflowLocked relaunches containers for restored environments; callers hold b.mu.
func (b *InMemoryBackend) reconcileAirflowLocked() {
	if b.airflow == nil {
		return
	}

	for _, env := range b.environments.All() {
		if env.Status == envStatusDeleting {
			continue
		}

		env.Status = envStatusCreating
		b.launchAirflowLocked(env, nil)
	}
}

func (b *InMemoryBackend) airflowEnvLocked(key string) *airflowEnv {
	if b.airflow == nil {
		return nil
	}

	return b.airflow.envs[key]
}

// deleteAirflowEnvLocked marks env DELETING, stops its container and removes it once gone.
func (b *InMemoryBackend) deleteAirflowEnvLocked(env *Environment, ae *airflowEnv) {
	if env.Status == envStatusDeleting {
		return
	}

	env.Status = envStatusDeleting
	key, region, name := regionKey(env.region, env.Name), env.region, env.Name
	ae.cancel()

	b.airflow.wg.Go(func() {
		<-ae.done

		b.mu.Lock("deleteAirflowEnv")
		defer b.mu.Unlock()

		if cur := b.airflowEnvLocked(key); cur == ae {
			delete(b.airflow.envs, key)
			b.environments.Delete(key)
			delete(b.metricsStore(region), name)
		}
	})
}

// updateAirflowLocked applies an UpdateEnvironment to the container: restart when the runtime changed, else re-sync.
func (b *InMemoryBackend) updateAirflowLocked(env *Environment, req *updateEnvironmentRequest) bool {
	ae := b.airflowEnvLocked(regionKey(env.region, env.Name))
	if ae == nil {
		return false
	}

	if needsAirflowRestart(req) {
		b.launchAirflowLocked(env, ae)
	} else {
		b.resyncAirflowLocked(env, ae)
	}

	return true
}

func needsAirflowRestart(req *updateEnvironmentRequest) bool {
	return req.AirflowVersion != "" || req.AirflowConfigurationOptions != nil ||
		req.RequirementsS3Path != "" || req.PluginsS3Path != "" || req.StartupScriptS3Path != ""
}
