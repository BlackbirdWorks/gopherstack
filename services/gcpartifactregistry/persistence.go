package gcpartifactregistry

import (
	"context"
	"encoding/json/jsontext"
	"fmt"
	"path/filepath"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type repoSnap struct {
	Repository jsontext.Value `json:"repository"`
	IAM        jsontext.Value `json:"iam,omitempty"`
}

type backendSnapshot struct {
	Repositories map[string]repoSnap          `json:"repositories"`
	Ledger       ociregistry.Ledger           `json:"ledger"`
	Meta         map[string]*resMeta          `json:"meta,omitempty"`
	GenericPkgs  map[string]*resMeta          `json:"genericPackages,omitempty"`
	GenericVers  map[string]*resMeta          `json:"genericVersions,omitempty"`
	GenericFiles map[string]*genFile          `json:"genericFiles,omitempty"`
	PackageTags  map[string]map[string]string `json:"packageTags,omitempty"`
	Rules        map[string]jsontext.Value    `json:"rules,omitempty"`
	Settings     map[string]jsontext.Value    `json:"projectSettings,omitempty"`
	VPCSC        map[string]jsontext.Value    `json:"vpcscConfigs,omitempty"`
	Attachments  map[string]jsontext.Value    `json:"attachments,omitempty"`
	SigningKeys  map[string][]byte            `json:"signingKeys,omitempty"`
	Operations   map[string]jsontext.Value    `json:"operations"`
	Version      int                          `json:"version"`
	Layout       int                          `json:"layout,omitempty"`
}

func marshalPB(m proto.Message) jsontext.Value {
	out, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}

	return out
}

func unmarshalPB(raw jsontext.Value, m proto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, m)
}

// Snapshot serialises repositories, IAM policies, the ledger, generic artifacts, rules, settings and
// operations. Image blobs and manifests are not part of it: they live under the data directory when one is configured.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:      snapshotVersion,
		Layout:       snapshotLayout,
		Ledger:       b.ledger,
		Operations:   b.ops.Snapshot(),
		Repositories: make(map[string]repoSnap, len(b.repos)),
		Meta:         b.meta,
		GenericPkgs:  b.genPkgs,
		GenericVers:  b.genVers,
		GenericFiles: b.genFiles,
		PackageTags:  b.pkgTags,
		Rules:        marshalAll(b.rules),
		Settings:     marshalAll(b.settings),
		VPCSC:        marshalAll(b.vpcsc),
		Attachments:  marshalAll(b.attach),
		SigningKeys:  b.signKeys,
	}

	for name, rec := range b.repos {
		rs := repoSnap{Repository: marshalPB(rec.pb)}
		if rec.iam != nil {
			rs.IAM = marshalPB(rec.iam)
		}

		snap.Repositories[name] = rs
	}

	return persistence.MarshalSnapshot(ctx, "gcpartifactregistry", snap)
}

// Restore loads a snapshot, replacing every repository.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpartifactregistry", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).
			WarnContext(ctx, "gcpartifactregistry: discarding incompatible snapshot version, starting empty",
				"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.repos, b.ledger = map[string]*repoRec{}, ociregistry.Ledger{}
		b.resetExtras()
		b.ops.Reset()

		return nil
	}

	repos, err := restoreRepos(snap.Repositories)
	if err != nil {
		return err
	}

	if snap.Ledger == nil {
		snap.Ledger = ociregistry.Ledger{}
	}

	if err = snap.Ledger.Normalize(); err != nil {
		return fmt.Errorf("gcpartifactregistry: restore ledger: %w", err)
	}

	if err = b.ops.Restore(snap.Operations); err != nil {
		return fmt.Errorf("gcpartifactregistry: restore operations: %w", err)
	}

	if err = b.restoreExtras(&snap); err != nil {
		return err
	}

	b.ops.AbortPending(grpcstatus.New(codes.Aborted, "The operation did not survive a restart."))

	if snap.Layout < snapshotLayout {
		owners := legacyDockerLocations(repos)
		snap.Ledger = migrateLedger(snap.Ledger, owners)

		b.repos, b.ledger = repos, snap.Ledger
		b.release(false)

		if b.dataDir == "" {
			return nil
		}

		return migrateImageDirs(filepath.Join(b.dataDir, "images"), owners)
	}

	b.repos, b.ledger = repos, snap.Ledger
	b.release(false)

	return nil
}

func marshalAll[T proto.Message](in map[string]T) map[string]jsontext.Value {
	out := make(map[string]jsontext.Value, len(in))
	for k, v := range in {
		out[k] = marshalPB(v)
	}

	return out
}

func unmarshalAll[T proto.Message](in map[string]jsontext.Value, newMsg func() T) (map[string]T, error) {
	out := make(map[string]T, len(in))

	for k, raw := range in {
		m := newMsg()
		if err := unmarshalPB(raw, m); err != nil {
			return nil, fmt.Errorf("gcpartifactregistry: restore %q: %w", k, err)
		}

		out[k] = m
	}

	return out, nil
}

func orEmpty[V any](m map[string]*V) map[string]*V {
	if m == nil {
		return map[string]*V{}
	}

	return m
}

func (b *InMemoryBackend) restoreExtras(snap *backendSnapshot) error {
	rules, err := unmarshalAll(snap.Rules, func() *artifactregistrypb.Rule { return &artifactregistrypb.Rule{} })
	if err != nil {
		return err
	}

	settings, err := unmarshalAll(snap.Settings, func() *artifactregistrypb.ProjectSettings {
		return &artifactregistrypb.ProjectSettings{}
	})
	if err != nil {
		return err
	}

	vpcsc, err := unmarshalAll(snap.VPCSC, func() *artifactregistrypb.VPCSCConfig {
		return &artifactregistrypb.VPCSCConfig{}
	})
	if err != nil {
		return err
	}

	attach, err := unmarshalAll(snap.Attachments, func() *artifactregistrypb.Attachment {
		return &artifactregistrypb.Attachment{}
	})
	if err != nil {
		return err
	}

	b.attach, b.pending = attach, map[string]*pendingOp{}

	b.signKeys = snap.SigningKeys
	if b.signKeys == nil {
		b.signKeys = map[string][]byte{}
	}

	b.meta, b.genPkgs, b.genVers = orEmpty(snap.Meta), orEmpty(snap.GenericPkgs), orEmpty(snap.GenericVers)
	b.genFiles, b.rules, b.settings, b.vpcsc = orEmpty(snap.GenericFiles), rules, settings, vpcsc
	b.pkgTags = snap.PackageTags
	if b.pkgTags == nil {
		b.pkgTags = map[string]map[string]string{}
	}

	return nil
}

func restoreRepos(in map[string]repoSnap) (map[string]*repoRec, error) {
	out := make(map[string]*repoRec, len(in))

	for name, rs := range in {
		pb := &artifactregistrypb.Repository{}
		if err := unmarshalPB(rs.Repository, pb); err != nil {
			return nil, fmt.Errorf("gcpartifactregistry: restore repository %q: %w", name, err)
		}

		rec := &repoRec{pb: pb}

		if len(rs.IAM) > 0 {
			rec.iam = &iampb.Policy{}
			if err := unmarshalPB(rs.IAM, rec.iam); err != nil {
				return nil, fmt.Errorf("gcpartifactregistry: restore policy of %q: %w", name, err)
			}
		}

		out[name] = rec
	}

	return out, nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpartifactregistry: restore snapshot: %w", err)
	}

	return nil
}
