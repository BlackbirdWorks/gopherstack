package gcpresourcemanager

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Projects  map[string]*projectRec          `json:"projects"`
	Folders   map[string]*folderRec           `json:"folders"`
	TagKeys   map[string]*tagKeyRec           `json:"tagKeys"`
	TagValues map[string]*tagValueRec         `json:"tagValues"`
	Bindings  map[string]*bindingRec          `json:"tagBindings"`
	Enabled   map[string]map[string]time.Time `json:"enabledServices"`
	Policies  map[string]json.RawMessage      `json:"policies"`
	Ops       map[string]json.RawMessage      `json:"operations"`
	Seed      string                          `json:"seed"`
	Version   int                             `json:"version"`
	PolicySeq uint64                          `json:"policySeq"`
}

// Snapshot serialises projects, folders, tags, enabled services, IAM policies and operations.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	policies, seq := b.policies.Snapshot()
	snap := backendSnapshot{
		Version: snapshotVersion, Seed: b.seed, Projects: b.projects, Folders: b.folders, TagKeys: b.tagKeys,
		TagValues: b.tagValues, Bindings: b.bindings, Enabled: b.enabled, PolicySeq: seq,
		Policies: make(map[string]json.RawMessage, len(policies)), Ops: b.lro.Snapshot(),
	}

	for n, p := range policies {
		if raw, err := protojson.Marshal(p); err == nil {
			snap.Policies[n] = raw
		}
	}

	return persistence.MarshalSnapshot(ctx, "gcpresourcemanager", snap)
}

// Restore loads a snapshot, replacing all state; the organization stays as configured.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpresourcemanager", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).
			WarnContext(ctx, "gcpresourcemanager: discarding incompatible snapshot version, starting empty",
				"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	policies := make(map[string]*iampb.Policy, len(snap.Policies))

	for n, raw := range snap.Policies {
		p := &iampb.Policy{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, p); err != nil {
			return fmt.Errorf("gcpresourcemanager: restore policy %q: %w", n, err)
		}

		policies[n] = p
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if err := b.lro.Restore(orEmpty(snap.Ops)); err != nil {
		return fmt.Errorf("gcpresourcemanager: restore operations: %w", err)
	}

	b.projects, b.folders = orEmpty(snap.Projects), orEmpty(snap.Folders)
	b.tagKeys, b.tagValues = orEmpty(snap.TagKeys), orEmpty(snap.TagValues)
	b.bindings, b.enabled = orEmpty(snap.Bindings), orEmpty(snap.Enabled)
	b.policies.Restore(policies, snap.PolicySeq)

	return nil
}

func orEmpty[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}

	return m
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpresourcemanager: restore snapshot: %w", err)
	}

	return nil
}
