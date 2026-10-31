package azurefunctions

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	kindFunctionApp = "functionapp"

	settingWebsitesPort = "WEBSITES_PORT"
	settingPort         = "PORT"
	settingAppStorage   = "WEBSITES_ENABLE_APP_SERVICE_STORAGE"

	stackNode   = workerNode
	stackPython = workerPython
	stackDotnet = "dotnetcore"
	stackJava   = workerJava
	stackPHP    = "php"

	codePort       = 8080
	customPort     = 80
	phpPort        = 80
	phpDocRoot     = "/var/www/html"
	webImagePrefix = "gopherstack-azapp-"
)

// webFamily is a built-in App Service code stack and the stock image that runs it.
type webFamily struct {
	image    func(version string) string
	versions []string
}

// webFamilies maps linuxFxVersion stacks to upstream images with moving tags: the mcr.microsoft.com/appsvc images
// publish only dated tags, so a pinned tag would rot.
func webFamilies() map[string]webFamily {
	return map[string]webFamily{
		stackNode: {
			versions: []string{"18", "20", "22", "24"},
			image:    func(v string) string { return "node:" + v + "-slim" },
		},
		stackPython: {
			versions: []string{"3.9", "3.10", ver311, "3.12", "3.13"},
			image:    func(v string) string { return "python:" + v + "-slim" },
		},
		stackDotnet: {
			versions: []string{"6.0", "7.0", ver80, "9.0", "10.0"},
			image:    func(v string) string { return "mcr.microsoft.com/dotnet/aspnet:" + v },
		},
		stackJava: {
			versions: []string{"11", "17", "21", "25"},
			image:    func(v string) string { return "eclipse-temurin:" + v + "-jre" },
		},
		stackPHP: {
			versions: []string{"8.2", "8.3", "8.4"},
			image:    func(v string) string { return "php:" + v + "-apache" },
		},
	}
}

// webRuntime is the image and stack a web app runs in.
type webRuntime struct {
	stack  string
	image  string
	custom bool
}

func isFunctionKind(kind string) bool {
	return kind == "" || strings.Contains(strings.ToLower(kind), kindFunctionApp)
}

func (a *appState) isWeb() bool { return !isFunctionKind(a.Kind) }

func stackFromFx(head string) string {
	switch strings.ToLower(head) {
	case workerNode:
		return stackNode
	case workerPython:
		return stackPython
	case stackDotnet, workerDotnet:
		return stackDotnet
	case workerJava:
		return stackJava
	case "php":
		return stackPHP
	}

	return ""
}

// webStackVersion reduces "20-lts", "17-java17" and "8.0" to the bare version.
func webStackVersion(v string) string {
	v, _, _ = strings.Cut(strings.TrimSpace(v), "-")

	return v
}

// chooseWebRuntime resolves a web app's linuxFxVersion to an image.
func chooseWebRuntime(linuxFx string) (webRuntime, error) {
	if after, ok := strings.CutPrefix(linuxFx, dockerPrefix); ok && after != "" {
		return webRuntime{image: after, custom: true}, nil
	}

	head, rest, found := strings.Cut(linuxFx, "|")
	if !found || head == "" {
		return webRuntime{}, fmt.Errorf(
			"%w: set siteConfig.linuxFxVersion to DOCKER|<image> or a stack such as NODE|20-lts", errUnsupportedRT)
	}

	stack := stackFromFx(head)
	fam, ok := webFamilies()[stack]

	if !ok {
		return webRuntime{}, fmt.Errorf(
			"%w: linuxFxVersion stack %q (supported: DOCKER, NODE, PYTHON, DOTNETCORE, DOTNET, JAVA, PHP)",
			errUnsupportedRT, head)
	}

	version := webStackVersion(rest)
	if !slices.Contains(fam.versions, version) {
		return webRuntime{}, fmt.Errorf("%w: %s %q; supported: %s", errUnsupportedRT, strings.ToUpper(head), version,
			strings.Join(fam.versions, ", "))
	}

	return webRuntime{stack: stack, image: fam.image(version)}, nil
}

// ValidateConfig reports whether the emulator can run the app's stack; function apps are validated at start.
func ValidateConfig(cfg AppConfig) error {
	if isFunctionKind(cfg.Kind) {
		return nil
	}

	if _, err := chooseWebRuntime(cfg.LinuxFx); err != nil {
		return err
	}

	if p := cfg.HealthCheckPath; p != "" && !strings.HasPrefix(p, "/") {
		return fmt.Errorf("%w: healthCheckPath must start with /", errInvalidConfig)
	}

	return nil
}

// containerPort is the port the app listens on inside the container.
func containerPort(settings map[string]string, rt webRuntime) int {
	if rt.stack == stackPHP {
		return phpPort
	}

	for _, k := range []string{settingWebsitesPort, settingPort} {
		if p, err := strconv.Atoi(settings[k]); err == nil && p > 0 && p < 1<<16 {
			return p
		}
	}

	if rt.custom {
		return customPort
	}

	return codePort
}

func firstExisting(dir string, names ...string) string {
	for _, n := range names {
		if st, err := os.Stat(filepath.Join(dir, n)); err == nil && !st.IsDir() {
			return n
		}
	}

	return ""
}

func hasStartScript(dir string) bool {
	raw, err := os.ReadFile(filepath.Join(dir, "package.json"))
	if err != nil {
		return false
	}

	var pkg struct {
		Scripts map[string]string `json:"scripts"`
	}

	return json.Unmarshal(raw, &pkg) == nil && pkg.Scripts["start"] != ""
}

func dotnetEntry(dir string) string {
	matches, _ := filepath.Glob(filepath.Join(dir, "*.runtimeconfig.json"))
	if len(matches) != 1 {
		return ""
	}

	return strings.TrimSuffix(filepath.Base(matches[0]), ".runtimeconfig.json") + ".dll"
}

func javaEntry(dir string) string {
	if n := firstExisting(dir, "app.jar"); n != "" {
		return n
	}

	matches, _ := filepath.Glob(filepath.Join(dir, "*.jar"))
	if len(matches) == 1 {
		return filepath.Base(matches[0])
	}

	return ""
}

// stackCommand picks the container command for a code stack: appCommandLine, else the stack's App Service default.
func stackCommand(rt webRuntime, appCmd, dir string) ([]string, error) {
	if appCmd != "" {
		return []string{"sh", "-c", appCmd}, nil
	}

	var cmd []string

	switch rt.stack {
	case stackNode:
		if hasStartScript(dir) {
			cmd = []string{"npm", "start"}
		} else if f := firstExisting(dir, "server.js", "app.js", "index.js", "bin/www", "hostingstart.js"); f != "" {
			cmd = []string{"node", f}
		}
	case stackPython:
		if f := firstExisting(dir, "app.py", "application.py", "main.py", "server.py"); f != "" {
			cmd = []string{"python", f}
		}
	case stackDotnet:
		if f := dotnetEntry(dir); f != "" {
			cmd = []string{"dotnet", f}
		}
	case stackJava:
		if f := javaEntry(dir); f != "" {
			cmd = []string{"java", "-jar", siteRoot + "/" + f}
		}
	case stackPHP:
		return nil, nil
	}

	if cmd == nil {
		return nil, fmt.Errorf("%w: no startup command found in the deployed package; set siteConfig.appCommandLine",
			errStartFailed)
	}

	return cmd, nil
}

// splitCommand splits a container startup command into arguments, honouring single and double quotes.
func splitCommand(s string) []string {
	var (
		out   []string
		cur   strings.Builder
		quote rune
		has   bool
	)

	for _, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '"' || r == '\'':
			quote, has = r, true
		case r == ' ' || r == '\t' || r == '\n':
			if has || cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()

				has = false
			}
		default:
			cur.WriteRune(r)
		}
	}

	if has || cur.Len() > 0 {
		out = append(out, cur.String())
	}

	return out
}

func webEnv(a *appState, rt webRuntime, host string, port int) []string {
	env := map[string]string{
		"WEBSITE_SITE_NAME": a.Name,
		"WEBSITE_SLOT_NAME": a.slotName(),
		"WEBSITE_HOSTNAME":  host,
		settingPort:         strconv.Itoa(port),
	}

	switch rt.stack {
	case stackDotnet:
		env["ASPNETCORE_URLS"] = "http://0.0.0.0:" + strconv.Itoa(port)
	case stackPython:
		env["PYTHONPATH"] = siteRoot
	}

	maps.Copy(env, a.Settings)

	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+env[k])
	}

	return out
}

func (a *appState) storageMounted() bool {
	return strings.EqualFold(a.Settings[settingAppStorage], "true")
}

// webSpec builds the container spec for a web app revision; dir is the unpacked site content, if any.
func webSpec(a *appState, rt webRuntime, host, dir string, hostPort int) (container.Spec, error) {
	port := containerPort(a.Settings, rt)
	spec := container.Spec{
		Image: rt.image,
		Name:  webImagePrefix + strs.Fold(a.Name) + "-" + strconv.Itoa(hostPort),
		Env:   webEnv(a, rt, host, port),
		Ports: []string{container.PortSpec(container.LoopbackHost, strconv.Itoa(hostPort), strconv.Itoa(port))},
	}

	if rt.custom {
		spec.Cmd = splitCommand(a.AppCommandLine)

		if dir != "" && a.storageMounted() {
			spec.Mounts = []string{dir + ":" + siteRoot}
		}

		return spec, nil
	}

	cmd, err := stackCommand(rt, a.AppCommandLine, dir)
	if err != nil {
		return container.Spec{}, err
	}

	spec.Cmd = cmd
	spec.WorkingDir = siteRoot
	spec.Mounts = []string{dir + ":" + siteRoot}

	if rt.stack == stackPHP {
		spec.WorkingDir = ""
		spec.Mounts = append(spec.Mounts, dir+":"+phpDocRoot)
	}

	return spec, nil
}

// webServable reports whether the web app has content or an image to run.
func (a *app) webServable() bool {
	rt, err := chooseWebRuntime(a.LinuxFx)

	return err == nil && (rt.custom || len(a.Zip) > 0)
}

// launchWeb starts a web app's container and waits until it answers, honouring siteConfig.healthCheckPath.
func (b *InMemoryBackend) launchWeb(st *appState, host string) (box, error) {
	rt, err := chooseWebRuntime(st.LinuxFx)
	if err != nil {
		return box{}, err
	}

	if _, err = b.runner(); err != nil {
		return box{}, err
	}

	x := box{}

	if len(st.Zip) > 0 {
		if x.dir, err = b.materialize(st.Zip); err != nil {
			return box{}, err
		}
	}

	if _, err = webSpec(st, rt, host, x.dir, 0); err != nil {
		b.teardown(x)

		return box{}, err
	}

	ready := func(ctx context.Context, port int) error {
		if readyErr := gcpserve.WaitReady(ctx, port); readyErr != nil {
			return readyErr
		}

		return waitHealthy(ctx, port, st.HealthCheckPath)
	}

	return b.startContainer(x, func(port int) container.Spec {
		spec, _ := webSpec(st, rt, host, x.dir, port)

		return spec
	}, ready)
}
