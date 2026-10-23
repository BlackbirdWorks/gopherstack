package azurecontainerregistry

import (
	"context"
	"crypto/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	secretBytes   = 32
	passwordChars = 32
	credPassword  = "password"
	credPassword2 = "password2"
)

var registryName = regexp.MustCompile(`^[a-zA-Z0-9]{5,50}$`)

// RegistryInfo is the control-plane view of a registry.
type RegistryInfo struct {
	CreatedAt    time.Time
	Name         string
	AdminEnabled bool
}

// Credentials are a registry's admin-user credentials.
type Credentials struct {
	Username  string
	Password1 string
	Password2 string
}

type registry struct {
	CreatedAt time.Time          `json:"createdAt"`
	Repos     ociregistry.Ledger `json:"repos"`
	raw       http.Handler
	app       http.Handler
	Name      string `json:"name"`
	Password1 string `json:"password1"`
	Password2 string `json:"password2"`
	Admin     bool   `json:"admin"`
}

// InMemoryBackend holds every registry's control-plane state under one lock; blobs and manifests live in
// per-registry ociregistry handlers.
type InMemoryBackend struct {
	now     func() time.Time
	ctx     context.Context //nolint:containedctx // lifetime of the lazily built registries
	mu      *lockmetrics.RWMutex
	regs    map[string]*registry
	dataDir string
	secret  []byte
}

// NewInMemoryBackend creates an empty backend that keeps blobs in memory.
func NewInMemoryBackend() *InMemoryBackend {
	secret := make([]byte, secretBytes)
	_, _ = rand.Read(secret) // crypto/rand.Read never fails on supported platforms

	return &InMemoryBackend{
		now: time.Now, ctx: context.Background(), secret: secret,
		mu: lockmetrics.New("azurecontainerregistry"), regs: map[string]*registry{},
	}
}

// SetDataDir keeps registry blobs under dir (one subdirectory per registry); empty keeps them in memory.
func (b *InMemoryBackend) SetDataDir(dir string) {
	b.mu.Lock("SetDataDir")
	defer b.mu.Unlock()

	b.dataDir = dir
}

// SetContext bounds the lifetime of the registries the backend builds.
func (b *InMemoryBackend) SetContext(ctx context.Context) {
	b.mu.Lock("SetContext")
	defer b.mu.Unlock()

	b.ctx = ctx
}

func newRegistry(name string, now time.Time) *registry {
	return &registry{
		Name: name, CreatedAt: now, Repos: ociregistry.Ledger{},
		Password1: awsid.NewN("", passwordChars), Password2: awsid.NewN("", passwordChars),
	}
}

func (r *registry) info() RegistryInfo {
	return RegistryInfo{Name: r.Name, CreatedAt: r.CreatedAt, AdminEnabled: r.Admin}
}

// PutRegistry creates the registry or updates its admin-user flag.
func (b *InMemoryBackend) PutRegistry(name string, adminEnabled bool) (RegistryInfo, error) {
	if !registryName.MatchString(name) {
		return RegistryInfo{}, ErrInvalidName
	}

	b.mu.Lock("PutRegistry")
	defer b.mu.Unlock()

	key := strs.Fold(name)

	r, ok := b.regs[key]
	if !ok {
		r = newRegistry(name, b.now().UTC())
		b.regs[key] = r
	}

	r.Admin = adminEnabled

	return r.info(), nil
}

// Registry returns the registry named name.
func (b *InMemoryBackend) Registry(name string) (RegistryInfo, bool) {
	b.mu.RLock("Registry")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(name)]
	if !ok {
		return RegistryInfo{}, false
	}

	return r.info(), true
}

// DeleteRegistry removes the registry with its repositories, blobs and manifests.
func (b *InMemoryBackend) DeleteRegistry(name string) error {
	b.mu.Lock("DeleteRegistry")
	defer b.mu.Unlock()

	key := strs.Fold(name)

	r, ok := b.regs[key]
	if !ok {
		return ErrRegistryNotFound
	}

	delete(b.regs, key)
	b.release(r, true)

	return nil
}

func (b *InMemoryBackend) release(r *registry, purge bool) {
	if r.raw != nil {
		ociregistry.Shutdown(r.raw)
		r.raw, r.app = nil, nil
	}

	if purge && b.dataDir != "" {
		_ = os.RemoveAll(filepath.Join(b.dataDir, strs.Fold(r.Name)))
	}
}

// Credentials returns the admin credentials; the admin user must be enabled.
func (b *InMemoryBackend) Credentials(name string) (Credentials, error) {
	b.mu.RLock("Credentials")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(name)]
	if !ok {
		return Credentials{}, ErrRegistryNotFound
	}

	if !r.Admin {
		return Credentials{}, ErrAdminDisabled
	}

	return Credentials{Username: r.Name, Password1: r.Password1, Password2: r.Password2}, nil
}

// RegenerateCredential replaces the "password" or "password2" credential and returns both.
func (b *InMemoryBackend) RegenerateCredential(name, which string) (Credentials, error) {
	b.mu.Lock("RegenerateCredential")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(name)]
	if !ok {
		return Credentials{}, ErrRegistryNotFound
	}

	switch {
	case strs.Equal(which, credPassword):
		r.Password1 = awsid.NewN("", passwordChars)
	case strs.Equal(which, credPassword2):
		r.Password2 = awsid.NewN("", passwordChars)
	default:
		return Credentials{}, ErrInvalidCred
	}

	return Credentials{Username: r.Name, Password1: r.Password1, Password2: r.Password2}, nil
}

// Names lists registry names, in lookup order of the single-registry fallback.
func (b *InMemoryBackend) Names() []string {
	b.mu.RLock("Names")
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.regs))
	for _, r := range b.regs {
		out = append(out, r.Name)
	}

	return out
}

// app returns the registry's size-limited storage handler, building it on first use.
func (b *InMemoryBackend) app(name string) (http.Handler, bool) {
	b.mu.Lock("app")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(name)]
	if !ok {
		return nil, false
	}

	if r.app == nil {
		dir := ""
		if b.dataDir != "" {
			dir = filepath.Join(b.dataDir, strs.Fold(r.Name))
		}

		r.raw = ociregistry.New(b.ctx, ociregistry.Options{
			InstanceID: "gopherstack-acr-" + strs.Fold(r.Name), RootDir: dir,
		})
		r.app = ociregistry.Limit(r.raw, 0)
	}

	return r.app, true
}

// Reset removes every registry and its stored images.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, r := range b.regs {
		b.release(r, true)
	}

	b.regs = map[string]*registry{}
}

// Close releases every registry's storage handler without deleting stored images.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	defer b.mu.Unlock()

	for _, r := range b.regs {
		b.release(r, false)
	}
}
