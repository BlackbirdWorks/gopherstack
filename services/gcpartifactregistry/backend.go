package gcpartifactregistry

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

const (
	maxRepositories = 256
	defaultPage     = 100
)

type repoRec struct {
	pb  *artifactregistrypb.Repository
	iam *iampb.Policy
}

// InMemoryBackend holds all Artifact Registry state under one lock. Docker images live in one embedded
// registry whose repositories are named <project>/<repository>/<image>.
type InMemoryBackend struct {
	now      func() time.Time
	ctx      context.Context //nolint:containedctx // lifetime of the lazily built registry
	mu       *lockmetrics.RWMutex
	repos    map[string]*repoRec
	meta     map[string]*resMeta
	genPkgs  map[string]*resMeta
	genVers  map[string]*resMeta
	genFiles map[string]*genFile
	pkgTags  map[string]map[string]string
	rules    map[string]*artifactregistrypb.Rule
	settings map[string]*artifactregistrypb.ProjectSettings
	vpcsc    map[string]*artifactregistrypb.VPCSCConfig
	ledger   ociregistry.Ledger
	ops      *gcplro.Store
	raw      http.Handler
	app      http.Handler
	dataDir  string
	host     string
	hostPort string
}

// NewInMemoryBackend creates an empty backend that keeps images in memory.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		now: time.Now, ctx: context.Background(), mu: lockmetrics.New("gcpartifactregistry"),
		repos: map[string]*repoRec{}, ledger: ociregistry.Ledger{}, ops: gcplro.New("gcpartifactregistry"),
		host: DefaultRegistryHost,
	}

	b.resetExtras()

	return b
}

func (b *InMemoryBackend) resetExtras() {
	b.meta, b.genPkgs, b.genVers = map[string]*resMeta{}, map[string]*resMeta{}, map[string]*resMeta{}
	b.genFiles = map[string]*genFile{}
	b.pkgTags = map[string]map[string]string{}
	b.rules = map[string]*artifactregistrypb.Rule{}
	b.settings = map[string]*artifactregistrypb.ProjectSettings{}
	b.vpcsc = map[string]*artifactregistrypb.VPCSCConfig{}
}

// SetDataDir keeps image blobs and manifests under dir; empty keeps them in memory.
func (b *InMemoryBackend) SetDataDir(dir string) {
	b.mu.Lock("SetDataDir")
	defer b.mu.Unlock()

	b.dataDir = dir
}

// SetContext bounds the lifetime of the embedded registry.
func (b *InMemoryBackend) SetContext(ctx context.Context) {
	b.mu.Lock("SetContext")
	defer b.mu.Unlock()

	b.ctx = ctx
}

// SetRegistryAddress sets the host and port advertised in registry_uri and image URIs.
func (b *InMemoryBackend) SetRegistryAddress(host, port string) {
	b.mu.Lock("SetRegistryAddress")
	defer b.mu.Unlock()

	b.host, b.hostPort = host, port
}

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.now()) }

func (b *InMemoryBackend) registryURI(r repoRef) string {
	host := b.host
	if b.hostPort != "" {
		host += ":" + b.hostPort
	}

	return host + "/" + r.dockerKey()
}

// store returns the embedded registry, building it on first use. Must hold b.mu for writing.
func (b *InMemoryBackend) store() http.Handler {
	if b.app == nil {
		dir := ""
		if b.dataDir != "" {
			dir = filepath.Join(b.dataDir, "images")
		}

		b.raw = ociregistry.New(b.ctx, ociregistry.Options{InstanceID: "gopherstack-artifactregistry", RootDir: dir})
		b.app = ociregistry.Limit(b.raw, 0)
	}

	return b.app
}

// Registry returns the embedded Docker registry handler (no authentication).
func (b *InMemoryBackend) Registry() http.Handler {
	b.mu.Lock("Registry")
	defer b.mu.Unlock()

	return b.store()
}

func (b *InMemoryBackend) client() ociregistry.Client { return ociregistry.NewClient(b.Registry()) }

func cloneRepo(pb *artifactregistrypb.Repository) *artifactregistrypb.Repository {
	out, _ := proto.Clone(pb).(*artifactregistrypb.Repository)

	return out
}

// repoByKey finds the repository owning a docker registry path (project/repository/...). Must hold b.mu.
func (b *InMemoryBackend) repoByKey(project, id string) (string, *repoRec, bool) {
	for name, rec := range b.repos {
		ref, err := parseRepo(name)
		if err == nil && ref.project == project && ref.id == id {
			return name, rec, true
		}
	}

	return "", nil, false
}

// DockerRepoInfo describes the repository a registry path belongs to.
type DockerRepoInfo struct {
	Location  string
	Immutable bool
	Docker    bool
}

// LookupDockerRepo resolves project/repository to its location and format.
func (b *InMemoryBackend) LookupDockerRepo(project, id string) (DockerRepoInfo, bool) {
	b.mu.RLock("LookupDockerRepo")
	defer b.mu.RUnlock()

	name, rec, ok := b.repoByKey(project, id)
	if !ok {
		return DockerRepoInfo{}, false
	}

	ref, _ := parseRepo(name)

	return DockerRepoInfo{
		Location:  ref.location,
		Docker:    rec.pb.GetFormat() == artifactregistrypb.Repository_DOCKER,
		Immutable: rec.pb.GetDockerConfig().GetImmutableTags(),
	}, true
}

// RecordManifestPut notes a manifest stored through the Docker API.
func (b *InMemoryBackend) RecordManifestPut(repo, ref, digest string) {
	b.mu.Lock("RecordManifestPut")
	defer b.mu.Unlock()

	b.ledger.RecordPut(repo, ref, digest, b.now().UTC())
}

// RecordManifestDelete notes a manifest or tag deleted through the Docker API.
func (b *InMemoryBackend) RecordManifestDelete(repo, ref string) {
	b.mu.Lock("RecordManifestDelete")
	defer b.mu.Unlock()

	b.ledger.RecordDelete(repo, ref)
}

// TagExists reports whether tag already points at a manifest in repo.
func (b *InMemoryBackend) TagExists(repo, tag string) bool {
	b.mu.RLock("TagExists")
	defer b.mu.RUnlock()

	r, ok := b.ledger[repo]
	if !ok {
		return false
	}

	_, ok = r.Tags[tag]

	return ok
}

// ImageNames lists the images in the repository at registry path prefix project/repository, sorted.
func (b *InMemoryBackend) ImageNames(project, id string) []string {
	b.mu.RLock("ImageNames")
	defer b.mu.RUnlock()

	return b.imagesLocked(repoRef{project: project, id: id})
}

func (b *InMemoryBackend) imagesLocked(r repoRef) []string {
	prefix := r.dockerKey() + "/"
	out := make([]string, 0)

	for key := range b.ledger {
		if image, ok := strings.CutPrefix(key, prefix); ok {
			out = append(out, image)
		}
	}

	sortStrings(out)

	return out
}

// Reset removes every repository and image.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.repos = map[string]*repoRec{}
	b.ledger = ociregistry.Ledger{}
	b.resetExtras()
	b.ops.Reset()
	b.release(true)
}

func (b *InMemoryBackend) release(purge bool) {
	if b.raw != nil {
		ociregistry.Shutdown(b.raw)
		b.raw, b.app = nil, nil
	}

	if purge && b.dataDir != "" {
		removeAll(filepath.Join(b.dataDir, "images"))
	}
}

// Close releases the embedded registry without deleting stored images.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	defer b.mu.Unlock()

	b.release(false)
}
