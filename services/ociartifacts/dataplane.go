package ociartifacts

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

const (
	actionPull  = "pull"
	actionPush  = "push"
	actionDel   = "delete"
	tokenTTL    = time.Hour
	scopeRepo   = "repository"
	maxManifest = 4 << 20
)

// Permit outcomes for a manifest request.
const (
	permitOK = iota
	permitUnknown
	permitTagExists
	permitLimit
)

// repoAccess is what the registry front end needs to know about a repository.
type repoAccess struct {
	ID        string
	Public    bool
	Immutable bool
}

type tokenClaims struct {
	Sub    string   `json:"sub"`
	Scopes []string `json:"scopes"`
	Exp    int64    `json:"exp"`
	Anon   bool     `json:"anon"`
}

// Scope is one `repository:<name>:<actions>` grant.
type Scope struct {
	Name    string
	Actions []string
}

func (s Scope) String() string { return scopeRepo + ":" + s.Name + ":" + strings.Join(s.Actions, ",") }

// ParseScope parses `repository:<name>:<action>[,<action>]`; other resource types are not supported.
func ParseScope(raw string) (Scope, bool) {
	typ, rest, ok := strings.Cut(raw, ":")
	if !ok || typ != scopeRepo {
		return Scope{}, false
	}

	i := strings.LastIndex(rest, ":")
	if i <= 0 {
		return Scope{}, false
	}

	return Scope{Name: rest[:i], Actions: strings.Split(rest[i+1:], ",")}, true
}

func (c tokenClaims) allows(name, action string) bool {
	for _, raw := range c.Scopes {
		s, ok := ParseScope(raw)
		if ok && s.Name == name && (slices.Contains(s.Actions, action) || slices.Contains(s.Actions, "*")) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) sign(payload string) string {
	m := hmac.New(sha256.New, b.secret)
	m.Write([]byte(payload))

	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// IssueToken returns a signed registry token and its lifetime in seconds. Scopes are granted as requested; an
// anonymous token is limited to pull.
func (b *InMemoryBackend) IssueToken(user string, anon bool, scopes []Scope) (string, []Scope, int) {
	granted := make([]Scope, 0, len(scopes))

	for _, s := range scopes {
		if anon {
			if !slices.Contains(s.Actions, actionPull) {
				continue
			}

			s.Actions = []string{actionPull}
		}

		granted = append(granted, s)
	}

	raw := make([]string, 0, len(granted))
	for _, s := range granted {
		raw = append(raw, s.String())
	}

	b.mu.RLock("IssueToken")
	exp := b.now().Add(tokenTTL).Unix()
	b.mu.RUnlock()

	body, _ := json.Marshal(
		tokenClaims{Sub: user, Anon: anon, Scopes: raw, Exp: exp}, jsonx.V1(),
	) //nolint:errchkjson // static shape
	payload := base64.RawURLEncoding.EncodeToString(body)

	return payload + "." + b.sign(payload), granted, int(tokenTTL.Seconds())
}

// ParseToken verifies a registry token's signature and expiry.
func (b *InMemoryBackend) ParseToken(token string) (tokenClaims, bool) {
	payload, sig, ok := strings.Cut(token, ".")
	if !ok || !hmac.Equal([]byte(sig), []byte(b.sign(payload))) {
		return tokenClaims{}, false
	}

	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return tokenClaims{}, false
	}

	var c tokenClaims
	if json.Unmarshal(raw, &c, jsonx.V1()) != nil {
		return tokenClaims{}, false
	}

	b.mu.RLock("ParseToken")
	defer b.mu.RUnlock()

	return c, b.now().Unix() < c.Exp
}

// ValidLogin accepts `<namespace>/<user>` with an auth token. With OCI Identity linked the token must be a
// current auth token of that user and the user's OCID is returned; unlinked, any non-empty token passes and
// the user part is returned.
func (b *InMemoryBackend) ValidLogin(username, password string) (string, bool) {
	ns, user, ok := strings.Cut(username, "/")
	if !ok || user == "" || password == "" {
		return "", false
	}

	b.mu.RLock("ValidLogin")
	verify, namespace := b.verifyToken, b.namespace
	b.mu.RUnlock()

	if !strings.EqualFold(ns, namespace) {
		return "", false
	}

	if verify == nil {
		return user, true
	}

	return verify(stripFederation(user), password)
}

const federationPrefix = "oracleidentitycloudservice/"

// stripFederation drops the identity-provider segment of a federated login name
// (`<namespace>/oracleidentitycloudservice/<user>`).
func stripFederation(user string) string {
	if len(user) > len(federationPrefix) && strings.EqualFold(user[:len(federationPrefix)], federationPrefix) {
		return user[len(federationPrefix):]
	}

	return user
}

// SplitRegistryName strips the namespace from a registry repository name.
func (b *InMemoryBackend) SplitRegistryName(name string) (string, bool) {
	b.mu.RLock("SplitRegistryName")
	defer b.mu.RUnlock()

	prefix := strings.ToLower(b.namespace) + "/"
	if len(name) <= len(prefix) || !strings.EqualFold(name[:len(prefix)], prefix) {
		return "", false
	}

	return name[len(prefix):], true
}

// RepoAccess looks up the repository addressed by a display name.
func (b *InMemoryBackend) RepoAccess(display string) (repoAccess, bool) {
	b.mu.RLock("RepoAccess")
	defer b.mu.RUnlock()

	r := b.repoByName(display)
	if r == nil {
		return repoAccess{}, false
	}

	return repoAccess{ID: r.ID, Public: r.IsPublic, Immutable: r.IsImmutable}, true
}

// EnsureRepo creates the repository on first push when the container configuration allows it.
func (b *InMemoryBackend) EnsureRepo(display, user string) *APIError {
	if err := validRepoName(display); err != nil {
		return err
	}

	b.mu.Lock("EnsureRepo")
	defer b.mu.Unlock()

	if b.repoByName(display) != nil {
		return nil
	}

	if !b.createOnPush {
		return newErr(
			http.StatusForbidden,
			"DENIED",
			"The repository does not exist and creating repositories on first push is disabled",
		)
	}

	if err := b.repoLimit(); err != nil {
		return err
	}

	b.newRepo(display, b.tenancy, user)

	return nil
}

// PermitManifest decides whether a manifest read, write or delete may proceed. digest is the digest of the body
// being written, when known.
func (b *InMemoryBackend) PermitManifest(display, ref, op, digest string, body []byte) int {
	b.mu.Lock("PermitManifest")
	defer b.mu.Unlock()

	r := b.repoByName(display)
	if r == nil {
		return permitOK
	}

	switch op {
	case ociregistry.OpRead:
		im := b.anyImageByRef(r.ID, ref)
		if im == nil {
			return permitOK
		}

		if im.State == stateDeleted {
			return permitUnknown
		}
	case ociregistry.OpWrite:
		if r.IsImmutable && !ociregistry.IsDigest(ref) {
			if im := b.anyImageByRef(r.ID, ref); im != nil && im.Digest != digest {
				return permitTagExists
			}
		}

		if !b.withinLimits(r, body) {
			return permitLimit
		}
	}

	return permitOK
}

func manifestSum(body []byte) string {
	h := sha256.Sum256(body)

	return "sha256:" + hex.EncodeToString(h[:])
}

// withinLimits reports whether storing the manifest body keeps the repository and region under their limits.
func (b *InMemoryBackend) withinLimits(r *Repo, body []byte) bool {
	var doc manifestDoc

	_ = json.Unmarshal(body, &doc, jsonx.V1())

	sum := manifestSum(body)
	if im := b.imageByDigest(r.ID, sum); im != nil && im.State != stateDeleted {
		return true
	}

	added := int64(len(body))
	for _, l := range doc.Layers {
		added += l.Size
	}

	var count int

	var used int64

	for _, im := range b.images {
		if im.State == stateDeleted {
			continue
		}

		used += im.LayersSize + int64(im.ManifestSize)

		if im.RepoID == r.ID {
			count++
		}
	}

	return count < b.maxImages && used+added <= b.maxBytes
}

// RecordPull counts a manifest GET the registry answered with 200.
func (b *InMemoryBackend) RecordPull(display, ref string) {
	b.mu.Lock("RecordPull")
	defer b.mu.Unlock()

	r := b.repoByName(display)
	if r == nil {
		return
	}

	if im := b.anyImageByRef(r.ID, ref); im != nil && im.State != stateDeleted {
		now := b.nowUTC()
		im.PullCount++
		im.TimeLastPulled = &now
	}
}

func (b *InMemoryBackend) anyImageByRef(repoID, ref string) *Image {
	if ociregistry.IsDigest(ref) {
		return b.imageByDigest(repoID, ref)
	}

	for _, im := range b.images {
		if im.RepoID == repoID && im.hasVersion(ref) {
			return im
		}
	}

	return nil
}

type manifestDoc struct {
	Layers []struct {
		Digest string `json:"digest"`
		Size   int64  `json:"size"`
	} `json:"layers"`
}

// RecordPush notes a stored manifest as an image of the repository, tagging it when ref is a tag.
func (b *InMemoryBackend) RecordPush(display, ref, digest, user string, body []byte) {
	if digest == "" {
		return
	}

	var doc manifestDoc

	_ = json.Unmarshal(body, &doc, jsonx.V1())

	b.mu.Lock("RecordPush")
	defer b.mu.Unlock()

	r := b.repoByName(display)
	if r == nil {
		return
	}

	now := b.nowUTC()
	r.TimeLastPushed = &now

	im := b.imageByDigest(r.ID, digest)
	if im == nil {
		im = &Image{
			ID: newOCID("containerimage"), RepoID: r.ID, Digest: digest, CreatedBy: user, State: stateAvailable,
			TimeCreated: now, Seq: b.nextSeq(), ManifestSize: len(body),
		}
		b.images[im.ID] = im
		b.setLayers(im, doc, now)
	} else if im.State == stateDeleted {
		im.State, im.Versions = stateAvailable, nil
	}

	if !ociregistry.IsDigest(ref) {
		b.moveTag(r.ID, im, ref, user)
	}

	im.Etag = newEtag()
}

func (b *InMemoryBackend) setLayers(im *Image, doc manifestDoc, now time.Time) {
	seen := map[string]struct{}{}

	for _, l := range doc.Layers {
		if _, dup := seen[l.Digest]; dup {
			continue
		}

		seen[l.Digest] = struct{}{}
		im.Layers = append(im.Layers, Layer{Digest: l.Digest, Size: l.Size, TimeCreated: now})
		im.LayersSize += l.Size
	}
}

// RecordDelete forgets a manifest deleted through the registry API, or just the tag when ref is one.
func (b *InMemoryBackend) RecordDelete(display, ref string) {
	b.mu.Lock("RecordDelete")
	defer b.mu.Unlock()

	r := b.repoByName(display)
	if r == nil {
		return
	}

	im := b.anyImageByRef(r.ID, ref)
	if im == nil {
		return
	}

	if !ociregistry.IsDigest(ref) {
		b.dropVersion(im, ref)
		im.Etag = newEtag()

		return
	}

	for sid, s := range b.sigs {
		if s.ImageID == im.ID {
			delete(b.sigs, sid)
		}
	}

	delete(b.images, im.ID)
}
