package azurefunctions

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Runner is the slice of the container runtime that function apps need.
type Runner = gcpserve.Runner

// PortPool hands out host ports for published containers.
type PortPool = gcpserve.PortPool

const (
	maxDeployments   = 50
	publishPassLen   = 32
	deployIDExtra    = 4
	publishPassBytes = publishPassLen / 2
	defaultKindName  = "functionapp,linux"
)

// Deployment is one Kudu deployment record.
type Deployment struct {
	Received  time.Time `json:"received"`
	Finished  time.Time `json:"finished"`
	ID        string    `json:"id"`
	Deployer  string    `json:"deployer"`
	Message   string    `json:"message"`
	Succeeded bool      `json:"succeeded"`
}

// appState is the persisted part of a function app.
type appState struct {
	Created         time.Time                    `json:"created"`
	Settings        map[string]string            `json:"settings"`
	HostKeys        map[string]string            `json:"hostKeys"`
	SystemKeys      map[string]string            `json:"systemKeys"`
	FunctionKeys    map[string]map[string]string `json:"functionKeys"`
	Name            string                       `json:"name"`
	Slot            string                       `json:"slot,omitempty"`
	Kind            string                       `json:"kind"`
	LinuxFx         string                       `json:"linuxFx"`
	AppCommandLine  string                       `json:"appCommandLine,omitempty"`
	HealthCheckPath string                       `json:"healthCheckPath,omitempty"`
	MasterKey       string                       `json:"masterKey"`
	PublishPassword string                       `json:"publishPassword"`
	Zip             []byte                       `json:"zip"`
	Deployments     []Deployment                 `json:"deployments"`
	Stopped         bool                         `json:"stopped"`
}

// app is a function app with its running host, if any.
type app struct {
	starting    chan struct{}
	containerID string
	dir         string
	secretsDir  string
	startErr    string
	appState
	hostPort int
	epoch    int
}

// InMemoryBackend holds all function apps under one lock.
type InMemoryBackend struct {
	now            func() time.Time
	mu             *lockmetrics.RWMutex
	runCtx         context.Context //nolint:containedctx // workload lifetime, set by the worker
	runtime        func() (Runner, error)
	onClose        func()
	ports          PortPool
	transport      *http.Transport
	wg             *sync.WaitGroup
	apps           map[string]*app
	domain         string
	startupTimeout time.Duration
	port           int
}

// NewInMemoryBackend creates an empty backend with no container runtime.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:             lockmetrics.New("azurefunctions"),
		now:            time.Now,
		runCtx:         context.Background(),
		wg:             &sync.WaitGroup{},
		transport:      httpclient.NewTransport(),
		apps:           make(map[string]*app),
		domain:         DefaultDomain,
		port:           DefaultPort,
		startupTimeout: DefaultStartupTimeout,
	}
}

// Configure applies the listener-facing settings.
func (b *InMemoryBackend) Configure(s Settings) {
	b.mu.Lock("Configure")
	defer b.mu.Unlock()

	if s.Domain != "" {
		b.domain = s.Domain
	}

	if s.Port != 0 {
		b.port = s.Port
	}

	if s.StartupTimeout > 0 {
		b.startupTimeout = s.StartupTimeout
	}
}

// SetRuntime wires the container runtime source; nil disables function hosts.
func (b *InMemoryBackend) SetRuntime(fn func() (Runner, error)) {
	b.mu.Lock("SetRuntime")
	defer b.mu.Unlock()

	b.runtime = fn
}

// SetOnClose registers a hook run by Close after every container is stopped.
func (b *InMemoryBackend) SetOnClose(fn func()) {
	b.mu.Lock("SetOnClose")
	defer b.mu.Unlock()

	b.onClose = fn
}

// SetPortPool wires the host port source for published containers.
func (b *InMemoryBackend) SetPortPool(p PortPool) {
	b.mu.Lock("SetPortPool")
	defer b.mu.Unlock()

	b.ports = p
}

func (b *InMemoryBackend) setRunCtx(ctx context.Context) {
	b.mu.Lock("setRunCtx")
	defer b.mu.Unlock()

	b.runCtx = ctx
}

// Reset stops every host and clears all apps.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	boxes := b.takeAllLocked()
	b.apps = make(map[string]*app)
	b.mu.Unlock()

	b.wg.Wait()

	for _, x := range boxes {
		b.teardown(x)
	}
}

// Close stops every host and runs the onClose hook.
func (b *InMemoryBackend) Close() {
	b.Reset()

	b.mu.RLock("Close")
	fn := b.onClose
	b.mu.RUnlock()

	if fn != nil {
		fn()
	}
}

func randomHex(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

// HostName is the default hostname apps are served under, including the listener port.
func (b *InMemoryBackend) HostName(name string) string {
	b.mu.RLock("HostName")
	defer b.mu.RUnlock()

	if a, ok := b.apps[strs.Fold(name)]; ok {
		return b.hostNameLocked(a.label())
	}

	return b.hostNameLocked(name)
}

func (b *InMemoryBackend) hostNameLocked(label string) string {
	return label + "." + b.domain + ":" + strconv.Itoa(b.port)
}

func (b *InMemoryBackend) scmHostNameLocked(label string) string {
	return label + ".scm." + b.domain + ":" + strconv.Itoa(b.port)
}

// SlotSep joins an app name and slot name into the data-plane name of a deployment slot.
const SlotSep = "/"

// SlotName is the data-plane name of an app's deployment slot.
func SlotName(app, slot string) string { return app + SlotSep + slot }

// key is the data-plane name: the app name, or app/slot for a deployment slot.
func (a *appState) key() string {
	if a.Slot == "" {
		return a.Name
	}

	return SlotName(a.Name, a.Slot)
}

// label is the first DNS label of the app's hostnames: <app>, or <app>-<slot> for a slot.
func (a *appState) label() string {
	if a.Slot == "" {
		return a.Name
	}

	return a.Name + "-" + a.Slot
}

// AppConfig is what ARM supplies about a site: its kind and the runtime stack it selected.
type AppConfig struct {
	Kind            string
	LinuxFx         string
	AppCommandLine  string
	HealthCheckPath string
}

// AppInfo is the data plane's view of an app.
type AppInfo struct {
	Created    time.Time
	LastDeploy time.Time
	HostName   string
	SCMHost    string
	Image      string
	Worker     string
	Failure    string
	Stopped    bool
	Deployed   bool
}

func (b *InMemoryBackend) infoLocked(a *app) AppInfo {
	info := AppInfo{
		Created: a.Created, HostName: b.hostNameLocked(a.label()), SCMHost: b.scmHostNameLocked(a.label()),
		Stopped: a.Stopped, Deployed: len(a.Zip) > 0,
	}

	if n := len(a.Deployments); n > 0 {
		info.LastDeploy = a.Deployments[n-1].Finished
	}

	switch {
	case a.isWeb():
		if rt, err := chooseWebRuntime(a.LinuxFx); err == nil {
			info.Image = rt.image
		}
	default:
		if rc, err := chooseRuntime(a.Settings, a.LinuxFx); err == nil {
			info.Image, info.Worker = rc.image, rc.worker
		}
	}

	switch {
	case b.runtime == nil:
		info.Failure = errNoRuntime.Error()
	case a.startErr != "":
		info.Failure = a.startErr
	}

	return info
}

// PutApp creates the app or updates its kind and stack, keeping settings, keys and package.
func (b *InMemoryBackend) PutApp(name string, cfg AppConfig) AppInfo {
	b.mu.Lock("PutApp")

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		a = &app{}
		appName, slot, _ := strings.Cut(name, SlotSep)
		a.appState = appState{
			Name:            appName,
			Slot:            slot,
			Created:         b.now().UTC(),
			Settings:        map[string]string{},
			HostKeys:        map[string]string{defaultKeyName: newKey()},
			SystemKeys:      map[string]string{},
			FunctionKeys:    map[string]map[string]string{},
			MasterKey:       newKey(),
			PublishPassword: randomHex(publishPassBytes),
		}
		b.apps[strs.Fold(name)] = a
	}

	a.Kind = cfg.Kind
	if a.Kind == "" {
		a.Kind = defaultKindName
	}

	var old box

	if a.LinuxFx != cfg.LinuxFx || a.AppCommandLine != cfg.AppCommandLine || a.HealthCheckPath != cfg.HealthCheckPath {
		a.LinuxFx, a.AppCommandLine, a.HealthCheckPath = cfg.LinuxFx, cfg.AppCommandLine, cfg.HealthCheckPath
		a.startErr = ""
		old = b.takeBoxLocked(a)
	}

	info := b.infoLocked(a)
	b.mu.Unlock()

	b.teardown(old)

	return info
}

// DeleteApp stops and removes the app.
func (b *InMemoryBackend) DeleteApp(name string) {
	b.mu.Lock("DeleteApp")

	var olds []box

	if a, ok := b.apps[strs.Fold(name)]; ok {
		olds = append(olds, b.takeBoxLocked(a))
		delete(b.apps, strs.Fold(name))
	}

	if !strings.Contains(name, SlotSep) {
		prefix := strs.Fold(name) + SlotSep

		for k, a := range b.apps {
			if strings.HasPrefix(k, prefix) {
				olds = append(olds, b.takeBoxLocked(a))
				delete(b.apps, k)
			}
		}
	}
	b.mu.Unlock()

	for _, x := range olds {
		b.teardown(x)
	}
}

// App returns the app's data-plane view.
func (b *InMemoryBackend) App(name string) (AppInfo, bool) {
	b.mu.RLock("App")
	defer b.mu.RUnlock()

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		return AppInfo{}, false
	}

	return b.infoLocked(a), true
}

// Names lists app names in order.
func (b *InMemoryBackend) Names() []string {
	b.mu.RLock("Names")
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.apps))
	for _, a := range b.apps {
		out = append(out, a.key())
	}

	sort.Strings(out)

	return out
}

// Settings returns a copy of the app's application settings.
func (b *InMemoryBackend) Settings(name string) (map[string]string, error) {
	b.mu.RLock("Settings")
	defer b.mu.RUnlock()

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		return nil, ErrAppNotFound
	}

	return maps.Clone(a.Settings), nil
}

// SetSettings replaces the app's application settings; a running host restarts on its next request.
func (b *InMemoryBackend) SetSettings(name string, settings map[string]string) error {
	b.mu.Lock("SetSettings")

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		b.mu.Unlock()

		return ErrAppNotFound
	}

	a.Settings = maps.Clone(settings)
	if a.Settings == nil {
		a.Settings = map[string]string{}
	}

	old := b.takeBoxLocked(a)
	b.mu.Unlock()

	b.teardown(old)

	return nil
}

// SetStopped starts (false) or stops (true) the app.
func (b *InMemoryBackend) SetStopped(name string, stopped bool) error {
	b.mu.Lock("SetStopped")

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		b.mu.Unlock()

		return ErrAppNotFound
	}

	a.Stopped = stopped
	old := b.takeBoxLocked(a)
	b.mu.Unlock()

	b.teardown(old)

	return nil
}

// Restart discards the running host; the next request starts a fresh one.
func (b *InMemoryBackend) Restart(name string) error {
	b.mu.Lock("Restart")

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		b.mu.Unlock()

		return ErrAppNotFound
	}

	old := b.takeBoxLocked(a)
	b.mu.Unlock()

	b.teardown(old)

	return nil
}

// PublishingCredentials returns the Kudu user name, password and base URL.
func (b *InMemoryBackend) PublishingCredentials(name string) (string, string, string, error) {
	b.mu.RLock("PublishingCredentials")
	defer b.mu.RUnlock()

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		return "", "", "", ErrAppNotFound
	}

	user := "$" + a.Name
	if a.Slot != "" {
		user += "__" + a.Slot
	}

	return user, a.PublishPassword, "http://" + b.scmHostNameLocked(a.label()), nil
}

// Deploy stores a zip package as the app's site content and records the deployment.
func (b *InMemoryBackend) Deploy(name string, zipData []byte, deployer, message string) (Deployment, error) {
	if err := validatePackage(zipData); err != nil {
		return Deployment{}, err
	}

	b.mu.Lock("Deploy")

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		b.mu.Unlock()

		return Deployment{}, ErrAppNotFound
	}

	now := b.now().UTC()
	d := Deployment{
		ID:        randomHex(publishPassBytes + deployIDExtra),
		Received:  now,
		Finished:  now,
		Deployer:  deployer,
		Message:   message,
		Succeeded: true,
	}

	a.Zip = zipData
	a.Deployments = append(a.Deployments, d)

	if len(a.Deployments) > maxDeployments {
		a.Deployments = a.Deployments[len(a.Deployments)-maxDeployments:]
	}

	old := b.takeBoxLocked(a)
	b.mu.Unlock()

	b.teardown(old)

	return d, nil
}

// Deployments returns the app's deployments, newest last.
func (b *InMemoryBackend) Deployments(name string) ([]Deployment, error) {
	b.mu.RLock("Deployments")
	defer b.mu.RUnlock()

	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		return nil, ErrAppNotFound
	}

	return append([]Deployment(nil), a.Deployments...), nil
}

// Package returns the deployed zip, or nil.
func (b *InMemoryBackend) Package(name string) []byte {
	b.mu.RLock("Package")
	defer b.mu.RUnlock()

	if a, ok := b.apps[strs.Fold(name)]; ok {
		return a.Zip
	}

	return nil
}

func (a *appState) slotName() string {
	if a.Slot == "" {
		return "Production"
	}

	return a.Slot
}
