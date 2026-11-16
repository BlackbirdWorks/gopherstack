package azurecontainerregistry

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	dockerHubName = "docker.io"
	dockerHubHost = "registry-1.docker.io"
	wildSuffix    = "/*"
)

// ErrCacheRuleNotFound is returned for an unknown cache rule.
var ErrCacheRuleNotFound = errors.New("azurecontainerregistry: cache rule not found")

// CacheCredentials locates the upstream login in Key Vault secrets.
type CacheCredentials struct {
	LoginServer    string `json:"loginServer"`
	UsernameSecret string `json:"usernameSecret"`
	PasswordSecret string `json:"passwordSecret"`
}

// CacheRule pulls Source (host/path, optionally ending in /*) into Target on first pull.
type CacheRule struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Credentials *CacheCredentials `json:"credentials,omitempty"`
	Name        string            `json:"name"`
	Source      string            `json:"source"`
	Target      string            `json:"target"`
}

// SecretResolver reads the value a Key Vault secret identifier URI points at.
type SecretResolver func(uri string) (string, error)

// SetSecretResolver lets cache rules read upstream credentials from Key Vault.
func (b *InMemoryBackend) SetSecretResolver(r SecretResolver) {
	b.mu.Lock("SetSecretResolver")
	defer b.mu.Unlock()

	b.secrets = r
}

// SetCacheRule creates or replaces a cache rule.
func (b *InMemoryBackend) SetCacheRule(reg string, rule CacheRule) error {
	b.mu.Lock("SetCacheRule")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return ErrRegistryNotFound
	}

	if r.CacheRules == nil {
		r.CacheRules = map[string]*CacheRule{}
	}

	key := strs.Fold(rule.Name)
	if old, exists := r.CacheRules[key]; exists {
		rule.CreatedAt = old.CreatedAt
	} else {
		rule.CreatedAt = b.now().UTC()
	}

	r.CacheRules[key] = &rule

	return nil
}

// DeleteCacheRule removes a cache rule; pulled images stay.
func (b *InMemoryBackend) DeleteCacheRule(reg, name string) error {
	b.mu.Lock("DeleteCacheRule")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return ErrRegistryNotFound
	}

	if _, found := r.CacheRules[strs.Fold(name)]; !found {
		return ErrCacheRuleNotFound
	}

	delete(r.CacheRules, strs.Fold(name))

	return nil
}

// cacheSource maps a requested repository to the upstream host and path of the first rule that covers it.
func (b *InMemoryBackend) cacheSource(reg, repo string) (string, string, *CacheCredentials, bool) {
	b.mu.RLock("cacheSource")
	defer b.mu.RUnlock()

	r, found := b.regs[strs.Fold(reg)]
	if !found {
		return "", "", nil, false
	}

	for _, rule := range r.CacheRules {
		src, matched := ruleSource(rule, repo)
		if !matched {
			continue
		}

		host, path, _ := strings.Cut(src, "/")

		return host, path, rule.Credentials, true
	}

	return "", "", nil, false
}

func ruleSource(rule *CacheRule, repo string) (string, bool) {
	if prefix, wild := strings.CutSuffix(rule.Target, wildSuffix); wild {
		rest, ok := strings.CutPrefix(repo, prefix+"/")

		return strings.TrimSuffix(rule.Source, "*") + rest, ok && rest != ""
	}

	return rule.Source, repo == rule.Target
}

func (b *InMemoryBackend) resolveCacheCredentials(c *CacheCredentials) (string, string, error) {
	if c == nil {
		return "", "", nil
	}

	b.mu.RLock("resolveCacheCredentials")
	resolve := b.secrets
	b.mu.RUnlock()

	if resolve == nil {
		return "", "", ErrImportAuth
	}

	user, err := resolve(c.UsernameSecret)
	if err != nil {
		return "", "", err
	}

	pass, err := resolve(c.PasswordSecret)
	if err != nil {
		return "", "", err
	}

	return user, pass, nil
}

// pullThrough fills repo:ref from the upstream of a matching cache rule when it is not stored yet.
func (b *InMemoryBackend) pullThrough(ctx context.Context, reg string, app http.Handler, repo, ref string) {
	host, path, creds, ok := b.cacheSource(reg, repo)
	if !ok {
		return
	}

	if _, err := ociregistry.NewClient(app).GetManifest(ctx, repo, ref); err == nil {
		return
	}

	if host == dockerHubName && !strings.Contains(path, "/") {
		path = "library/" + path
	}

	user, pass, err := b.resolveCacheCredentials(creds)
	if err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "azurecontainerregistry: cache rule credentials", "registry", reg, "error", err)

		return
	}

	uri := host
	if host == dockerHubName {
		uri = dockerHubHost
	}

	req := ImportRequest{
		Source: ImportSource{RegistryURI: uri, Username: user, Password: pass}, Force: true,
	}

	if ociregistry.IsDigest(ref) {
		req.Source.Image, req.UntaggedTargets = path+"@"+ref, []string{repo}
	} else {
		req.Source.Image, req.TargetTags = path+":"+ref, []string{repo + ":" + ref}
	}

	if err = b.ImportImage(ctx, reg, req); err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "azurecontainerregistry: cache rule pull", "registry", reg, "repository", repo,
				"error", err)
	}
}

// manifestRef parses /v2/<repo>/manifests/<ref>.
func manifestRef(path string) (string, string, bool) {
	rest := strings.TrimPrefix(path, prefixV2+"/")

	i := strings.LastIndex(rest, "/manifests/")
	if i <= 0 {
		return "", "", false
	}

	return rest[:i], rest[i+len("/manifests/"):], true
}

// CredentialHealth reports whether both secret identifiers resolve; code and message describe the first failure.
func (b *InMemoryBackend) CredentialHealth(usernameURI, passwordURI string) (bool, string, string) {
	b.mu.RLock("CredentialHealth")
	resolve := b.secrets
	b.mu.RUnlock()

	if resolve == nil {
		return false, "SecretsUnavailable", "Key Vault is not available."
	}

	for _, uri := range []string{usernameURI, passwordURI} {
		if _, err := resolve(uri); err != nil {
			return false, "SecretNotFound", "The secret " + uri + " could not be read."
		}
	}

	return true, "", ""
}
