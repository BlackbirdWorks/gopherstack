package ociartifacts

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the stored types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Repos        map[string]*Repo            `json:"repos"`
	Images       map[string]*Image           `json:"images"`
	Signatures   map[string]*Signature       `json:"signatures"`
	GenericRepos map[string]*GenericRepo     `json:"genericRepos"`
	Artifacts    map[string]*GenericArtifact `json:"artifacts"`
	ConfigEtag   string                      `json:"configEtag"`
	Auth         []ociauth.KeyRecord         `json:"authKeys"`
	Version      int                         `json:"version"`
	Seq          int64                       `json:"seq"`
	CreateOnPush bool                        `json:"createOnPush"`
}

// Snapshot serialises every resource and the registered API keys. Image blobs and manifests are not part of it:
// they live under the data directory when one is configured.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ociartifacts", backendSnapshot{
		Version: snapshotVersion, Repos: b.repos, Images: b.images, Signatures: b.sigs, GenericRepos: b.gens,
		Artifacts: b.arts, ConfigEtag: b.configEtag, Auth: keys, Seq: b.seq, CreateOnPush: b.createOnPush,
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ociartifacts", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ociartifacts: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := snap.normalize(); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.repos, b.images, b.sigs, b.gens, b.arts = snap.Repos, snap.Images, snap.Signatures, snap.GenericRepos, snap.Artifacts
	b.configEtag, b.seq, b.createOnPush = snap.ConfigEtag, snap.Seq, snap.CreateOnPush

	if b.dataDir == "" && len(b.images) > 0 {
		logger.Load(ctx).
			WarnContext(ctx, "ociartifacts: image blobs were not persisted (no data dir), dropping image records")

		b.images, b.sigs = map[string]*Image{}, map[string]*Signature{}
	}

	b.Auth.Load(snap.Auth)

	return nil
}

func (s *backendSnapshot) normalize() error {
	if s.Repos == nil {
		s.Repos = map[string]*Repo{}
	}

	if s.Images == nil {
		s.Images = map[string]*Image{}
	}

	if s.Signatures == nil {
		s.Signatures = map[string]*Signature{}
	}

	if s.GenericRepos == nil {
		s.GenericRepos = map[string]*GenericRepo{}
	}

	if s.Artifacts == nil {
		s.Artifacts = map[string]*GenericArtifact{}
	}

	if s.ConfigEtag == "" {
		s.ConfigEtag = newEtag()
	}

	return s.checkNull()
}

func (s *backendSnapshot) checkNull() error {
	for id, v := range s.Repos {
		if v == nil {
			return fmt.Errorf("%w: container repository %q", ErrSnapshotNull, id)
		}
	}

	for id, v := range s.Images {
		if v == nil {
			return fmt.Errorf("%w: image %q", ErrSnapshotNull, id)
		}
	}

	for id, v := range s.Signatures {
		if v == nil {
			return fmt.Errorf("%w: signature %q", ErrSnapshotNull, id)
		}
	}

	for id, v := range s.GenericRepos {
		if v == nil {
			return fmt.Errorf("%w: repository %q", ErrSnapshotNull, id)
		}
	}

	for id, v := range s.Artifacts {
		if v == nil {
			return fmt.Errorf("%w: artifact %q", ErrSnapshotNull, id)
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("ociartifacts: restore snapshot: %w", err)
	}

	return nil
}
