package ociregistry

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrNullEntry is returned by Normalize when a repository entry is null.
var ErrNullEntry = errors.New("ociregistry: null ledger entry")

// Operations a ledger entry can forbid.
const (
	OpRead   = "read"
	OpWrite  = "write"
	OpDelete = "delete"
)

// Attrs are the changeable attributes of a repository, manifest or tag. The zero value enables everything.
type Attrs struct {
	DeleteDisabled bool `json:"deleteDisabled,omitempty"`
	WriteDisabled  bool `json:"writeDisabled,omitempty"`
	ReadDisabled   bool `json:"readDisabled,omitempty"`
	ListDisabled   bool `json:"listDisabled,omitempty"`
}

// Allows reports whether op is enabled.
func (a Attrs) Allows(op string) bool {
	switch op {
	case OpDelete:
		return !a.DeleteDisabled
	case OpRead:
		return !a.ReadDisabled
	}

	return !a.WriteDisabled
}

// TagEntry is a tag's ledger entry.
type TagEntry struct {
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	Digest  string    `json:"digest"`
	Attrs   Attrs     `json:"attrs"`
}

// ManifestEntry is a manifest's ledger entry.
type ManifestEntry struct {
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	Attrs   Attrs     `json:"attrs"`
}

// RepoEntry is a repository's ledger entry.
type RepoEntry struct {
	Created   time.Time                 `json:"created"`
	Updated   time.Time                 `json:"updated"`
	Manifests map[string]*ManifestEntry `json:"manifests"`
	Tags      map[string]*TagEntry      `json:"tags"`
	Attrs     Attrs                     `json:"attrs"`
}

// Ledger records when manifests and tags were pushed, which the registry itself does not keep. Repositories
// disappear from it with their last manifest. It is not safe for concurrent use: callers hold their own lock.
type Ledger map[string]*RepoEntry

// IsDigest reports whether a manifest reference is a digest rather than a tag.
func IsDigest(ref string) bool { return strings.Contains(ref, ":") }

// RecordPut notes a stored manifest, and the tag when ref is one.
func (l Ledger) RecordPut(repo, ref, digest string, now time.Time) {
	if digest == "" {
		return
	}

	r, ok := l[repo]
	if !ok {
		r = &RepoEntry{Created: now, Manifests: map[string]*ManifestEntry{}, Tags: map[string]*TagEntry{}}
		l[repo] = r
	}

	r.Updated = now

	if m, exists := r.Manifests[digest]; exists {
		m.Updated = now
	} else {
		r.Manifests[digest] = &ManifestEntry{Created: now, Updated: now}
	}

	if IsDigest(ref) {
		return
	}

	if t, exists := r.Tags[ref]; exists {
		t.Digest, t.Updated = digest, now
	} else {
		r.Tags[ref] = &TagEntry{Created: now, Updated: now, Digest: digest}
	}
}

// RecordDelete forgets a deleted manifest with its tags, or just the tag when ref is one.
func (l Ledger) RecordDelete(repo, ref string) {
	r, ok := l[repo]
	if !ok {
		return
	}

	if IsDigest(ref) {
		delete(r.Manifests, ref)

		for name, t := range r.Tags {
			if t.Digest == ref {
				delete(r.Tags, name)
			}
		}
	} else {
		delete(r.Tags, ref)
	}

	if len(r.Manifests) == 0 {
		delete(l, repo)
	}
}

// Allowed reports whether op may touch repo:ref. An operation on a digest also needs every tag pointing at it
// to allow it.
func (l Ledger) Allowed(repo, ref, op string) bool {
	r, ok := l[repo]
	if !ok {
		return true
	}

	if !r.Attrs.Allows(op) {
		return false
	}

	if !IsDigest(ref) {
		t, found := r.Tags[ref]

		return !found || t.Attrs.Allows(op)
	}

	if m, found := r.Manifests[ref]; found && !m.Attrs.Allows(op) {
		return false
	}

	for _, t := range r.Tags {
		if t.Digest == ref && !t.Attrs.Allows(op) {
			return false
		}
	}

	return true
}

// Normalize fills nil maps after a decode and rejects null entries.
func (l Ledger) Normalize() error {
	for name, r := range l {
		if r == nil {
			return fmt.Errorf("%w for repository %q", ErrNullEntry, name)
		}

		if r.Manifests == nil {
			r.Manifests = map[string]*ManifestEntry{}
		}

		if r.Tags == nil {
			r.Tags = map[string]*TagEntry{}
		}
	}

	return nil
}
