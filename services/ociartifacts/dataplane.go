package ociartifacts

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"time"

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
		tokenClaims{Sub: user, Anon: anon, Scopes: raw, Exp: exp},
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
	if json.Unmarshal(raw, &c) != nil {
		return tokenClaims{}, false
	}

	b.mu.RLock("ParseToken")
	defer b.mu.RUnlock()

	return c, b.now().Unix() < c.Exp
}

// ValidLogin accepts `<namespace>/<user>` with any non-empty auth token and returns the user part. Auth tokens
// are not stored by OCI Identity, so there is nothing to compare the password with.
func (b *InMemoryBackend) ValidLogin(username, password string) (string, bool) {
	ns, user, ok := strings.Cut(username, "/")
	if !ok || user == "" || password == "" {
		return "", false
	}

	b.mu.RLock("ValidLogin")
	defer b.mu.RUnlock()

	return user, strings.EqualFold(ns, b.namespace)
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

	b.newRepo(display, b.tenancy, user)

	return nil
}

// PermitManifest decides whether a manifest read, write or delete may proceed. digest is the digest of the body
// being written, when known.
func (b *InMemoryBackend) PermitManifest(display, ref, op, digest string, pull bool) int {
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

		if pull {
			now := b.nowUTC()
			im.PullCount++
			im.TimeLastPulled = &now
		}
	case ociregistry.OpWrite:
		if r.IsImmutable && !ociregistry.IsDigest(ref) {
			if im := b.anyImageByRef(r.ID, ref); im != nil && im.Digest != digest {
				return permitTagExists
			}
		}
	}

	return permitOK
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

	_ = json.Unmarshal(body, &doc)

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
