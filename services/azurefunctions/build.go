package azurefunctions

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	buildMount   = "/build"
	buildTimeout = 15 * time.Minute

	settingBuildOnDeploy = "SCM_DO_BUILD_DURING_DEPLOYMENT"
	settingOryxBuild     = "ENABLE_ORYX_BUILD"
)

var (
	errBuildFailed      = errors.New("remote build failed")
	errBuildUnsupported = errors.New("remote build supports node and python function apps")
	errBuildNoExec      = errors.New("the container runtime cannot run commands, so remote build is unavailable")
)

// Dependencies are installed where the Functions host looks for them: node_modules next to package.json, and
// .python_packages/lib/site-packages for Python (Learn: Azure Functions Python developer guide, "Build options").
// The Python order is Learn "Configure Linux Python apps", "Customize build automation": pre-build command,
// requirements.txt (pip), else setup.py or pyproject.toml (pip install .; lock files are not honoured), Django
// collectstatic, post-build command.
const (
	nodeBuildScript = `cd /build
if [ -f package.json ]; then
  npm install --no-audit --no-fund && npm run build --if-present && npm prune --omit=dev || exit 1
else
  echo "No package.json found; skipping npm install."
fi`
	pythonInstallScript = `cd /build
if [ -f requirements.txt ]; then
  pip install --disable-pip-version-check --target .python_packages/lib/site-packages -r requirements.txt || exit 1
elif [ -f setup.py ] || [ -f pyproject.toml ]; then
  pip install --disable-pip-version-check --target .python_packages/lib/site-packages . || exit 1
else
  echo "Could not find setup.py or requirements.txt; Not running pip install."
fi
`
	pythonCollectStatic = `if [ -f manage.py ]; then
  PYTHONPATH=/build/.python_packages/lib/site-packages python manage.py collectstatic --noinput || exit 1
fi
`
	buildPermScript = "chmod -R a+rwX /build"
)

func userBuildHook(settings map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(settings[k]); v != "" {
			return "cd /build\nsh -c " + shQuote(v) + " || exit 1\n"
		}
	}

	return ""
}

func truthySetting(settings map[string]string, k string) bool {
	v := strings.ToLower(strings.TrimSpace(settings[k]))

	return v == "true" || v == "1"
}

// wantsRemoteBuild reports whether a deployment is built on the server: RemoteBuild=true on publish (the only
// switch Flex Consumption honours), or SCM_DO_BUILD_DURING_DEPLOYMENT / ENABLE_ORYX_BUILD on other apps.
func wantsRemoteBuild(queryFlag bool, settings map[string]string, flex bool) bool {
	if queryFlag {
		return true
	}

	if flex {
		return false
	}

	return truthySetting(settings, settingBuildOnDeploy) || truthySetting(settings, settingOryxBuild)
}

func buildScript(worker string, settings map[string]string) (string, error) {
	pre := userBuildHook(settings, "PRE_BUILD_COMMAND", "PRE_BUILD_SCRIPT_PATH")
	post := userBuildHook(settings, "POST_BUILD_COMMAND", "POST_BUILD_SCRIPT_PATH")

	switch worker {
	case workerNode:
		return pre + nodeBuildScript + "\n" + post, nil
	case workerPython:
		collect := pythonCollectStatic
		if truthySetting(settings, "DISABLE_COLLECTSTATIC") {
			collect = ""
		}

		return pre + pythonInstallScript + collect + post, nil
	}

	return "", fmt.Errorf("%w, not %q", errBuildUnsupported, worker)
}

func buildImage(settings map[string]string, linuxFx string, web bool) (string, string, error) {
	if web {
		rt, err := chooseWebRuntime(linuxFx)
		if err != nil {
			return "", "", err
		}

		if rt.custom {
			return "", "", fmt.Errorf("%w, not a DOCKER app", errBuildUnsupported)
		}

		return rt.image, rt.stack, nil
	}

	rc, err := chooseRuntime(settings, linuxFx)
	if err != nil {
		return "", "", err
	}

	if rc.custom {
		return "", "", fmt.Errorf("%w, not a DOCKER app", errBuildUnsupported)
	}

	return rc.image, rc.worker, nil
}

func zipDir(dir string) ([]byte, error) {
	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	root := filepath.Clean(dir)

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || p == root || !d.Type().IsRegular() && !d.IsDir() {
			return walkErr
		}

		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}

		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}

		hdr, hdrErr := zip.FileInfoHeader(info)
		if hdrErr != nil {
			return hdrErr
		}

		hdr.Name = filepath.ToSlash(rel)
		if d.IsDir() {
			hdr.Name += "/"
		} else {
			hdr.Method = zip.Deflate
		}

		w, createErr := zw.CreateHeader(hdr)
		if createErr != nil || d.IsDir() {
			return createErr
		}

		return copyFileTo(w, p)
	})
	if err != nil {
		return nil, fmt.Errorf("package built site: %w", err)
	}

	if err = zw.Close(); err != nil {
		return nil, fmt.Errorf("package built site: %w", err)
	}

	return buf.Bytes(), nil
}

// Build installs the package's dependencies in a throwaway container of the app's runtime image and returns the
// rebuilt zip and the build log.
func (b *InMemoryBackend) Build(ctx context.Context, name string, zipData []byte) ([]byte, string, error) {
	b.mu.RLock("Build")

	a, err := b.appLocked(name)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	settings, linuxFx, web := maps.Clone(a.Settings), a.LinuxFx, a.isWeb()
	b.mu.RUnlock()

	image, worker, err := buildImage(settings, linuxFx, web)
	if err != nil {
		return nil, "", err
	}

	script, err := buildScript(worker, settings)
	if err != nil {
		return nil, "", err
	}

	runner, err := b.runner()
	if err != nil {
		return nil, "", err
	}

	ex, ok := runner.(container.Execer)
	if !ok {
		return nil, "", errBuildNoExec
	}

	dir, err := b.materialize(zipData)
	if err != nil {
		return nil, "", err
	}
	defer removeDir(dir)

	log, err := runBuild(ctx, runner, ex, image, dir, script)
	if err != nil {
		return nil, log, err
	}

	built, err := zipDir(dir)

	return built, log, err
}

func runBuild(
	ctx context.Context, runner Runner, ex container.Execer, image, dir, script string,
) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, buildTimeout)
	defer cancel()

	id, err := runner.CreateAndStart(ctx, container.Spec{
		Image:      image,
		Name:       "gopherstack-azfn-build-" + randomHex(deployIDExtra),
		Entrypoint: []string{"sleep"},
		Cmd:        []string{"3600"},
		Mounts:     []string{dir + ":" + buildMount},
		WorkingDir: buildMount,
	})
	if err != nil {
		return "", fmt.Errorf("start build container: %w", err)
	}

	defer func() {
		stopCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), teardownTimeout)
		defer stop()

		if stopErr := runner.StopAndRemove(stopCtx, id); stopErr != nil {
			logger.Load(ctx).WarnContext(ctx, "azurefunctions: build container removal failed",
				"container", id, "error", stopErr)
		}
	}()

	res, err := ex.Exec(ctx, id, []string{"sh", "-c", script})
	out := strings.TrimSpace(res.Stdout + res.Stderr)

	if err != nil {
		return out, fmt.Errorf("%w: %w", errBuildFailed, err)
	}

	if res.ExitCode != 0 {
		return out, fmt.Errorf("%w: exit code %d", errBuildFailed, res.ExitCode)
	}

	if _, err = ex.Exec(ctx, id, []string{"sh", "-c", buildPermScript}); err != nil {
		return out, fmt.Errorf("%w: %w", errBuildFailed, err)
	}

	return out, nil
}

func (b *InMemoryBackend) recordFailedDeployment(name, deployer, buildLog string) {
	b.mu.Lock("recordFailedDeployment")
	defer b.mu.Unlock()

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		return
	}

	now := b.now().UTC()
	a.Deployments = append(a.Deployments, Deployment{
		ID: randomHex(publishPassBytes + deployIDExtra), Received: now, Finished: now, Deployer: deployer,
		Message: "Created via a push deployment", Log: buildLog,
	})

	if len(a.Deployments) > maxDeployments {
		a.Deployments = a.Deployments[len(a.Deployments)-maxDeployments:]
	}
}

func copyFileTo(w io.Writer, path string) error {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(w, f)

	return err
}
