package mgn

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// mgnSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (the set of resources registered on b.registry -- see registerAllTables
// in store_setup.go). Bump whenever a change there would make an older
// snapshot unsafe to decode as the current shape; Restore discards (rather
// than partially decodes) any mismatch -- see Restore.
const mgnSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the MGN backend.
type backendSnapshot struct {
	Tables             map[string]json.RawMessage `json:"tables"`
	AccountID          string                     `json:"accountId"`
	Region             string                     `json:"region"`
	Version            int                        `json:"version"`
	ServiceInitialized bool                       `json:"serviceInitialized"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "mgn: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:            mgnSnapshotVersion,
		Tables:             tables,
		AccountID:          b.accountID,
		Region:             b.region,
		ServiceInitialized: b.serviceInitialized,
	}

	return persistence.MarshalSnapshot(ctx, "mgn", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "mgn", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != mgnSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"mgn: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", mgnSnapshotVersion)

		b.registry.ResetAll()
		b.serviceInitialized = false

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("mgn: restore tables: %w", err)
	}

	restoreTagsLocked(b)

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.serviceInitialized = snap.ServiceInitialized

	return nil
}

// restoreTagsLocked rebuilds a Prometheus-named tags.Tags instance for every
// tagged resource kind after a JSON round-trip, which deserializes every
// tags.Tags with the generic "json.tags" name (see tags.Tags.UnmarshalJSON's
// doc comment) -- matching every other service in this campaign's identical
// restoreTagsLocked. Callers must hold b.mu.
func restoreTagsLocked(b *InMemoryBackend) {
	for _, s := range b.sourceServers.All() {
		s.Tags = rebuildTags(s.Tags, "mgn.sourceserver."+s.SourceServerID+".tags")
	}

	for _, t := range b.launchTemplates.All() {
		t.Tags = rebuildTags(t.Tags, "mgn.launchtemplate."+t.LaunchConfigurationTemplateID+".tags")
	}

	for _, t := range b.replicationTemplates.All() {
		t.Tags = rebuildTags(t.Tags, "mgn.replicationtemplate."+t.ReplicationConfigurationTemplateID+".tags")
	}

	for _, j := range b.jobs.All() {
		j.Tags = rebuildTags(j.Tags, "mgn.job."+j.JobID+".tags")
	}

	for _, a := range b.applications.All() {
		a.Tags = rebuildTags(a.Tags, "mgn.application."+a.ApplicationID+".tags")
	}

	for _, w := range b.waves.All() {
		w.Tags = rebuildTags(w.Tags, "mgn.wave."+w.WaveID+".tags")
	}

	for _, c := range b.connectors.All() {
		c.Tags = rebuildTags(c.Tags, "mgn.connector."+c.ConnectorID+".tags")
	}

	for _, v := range b.vcenterClients.All() {
		v.Tags = rebuildTags(v.Tags, "mgn.vcenterclient."+v.VcenterClientID+".tags")
	}

	for _, e := range b.exportTasks.All() {
		e.Tags = rebuildTags(e.Tags, "mgn.exporttask."+e.ExportID+".tags")
	}

	for _, i := range b.importTasks.All() {
		i.Tags = rebuildTags(i.Tags, "mgn.importtask."+i.ImportID+".tags")
	}

	for _, d := range b.nmDefinitions.All() {
		d.Tags = rebuildTags(d.Tags, "mgn.nmdefinition."+d.NetworkMigrationDefinitionID+".tags")
	}

	for _, e := range b.nmExecutions.All() {
		e.Tags = rebuildTags(e.Tags, "mgn.nmexecution."+e.NetworkMigrationExecutionID+".tags")
	}
}

// rebuildTags returns a fresh, correctly-named tags.Tags carrying the same
// entries as old (which may be nil after a JSON round-trip lost its name).
func rebuildTags(old *tags.Tags, name string) *tags.Tags {
	if old == nil {
		return tags.New(name)
	}

	raw := old.Clone()
	old.Close()

	return tags.FromMap(name, raw)
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
