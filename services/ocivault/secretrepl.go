package ocivault

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const maxReplicationTargets = 3

// ReplicationTarget is one entry of a secret's replicationConfig.
type ReplicationTarget struct {
	KeyID   string `json:"targetKeyId"`
	Region  string `json:"targetRegion"`
	VaultID string `json:"targetVaultId"`
}

// SecretReplication is a secret's replicationConfig.
type SecretReplication struct {
	WriteForward *bool               `json:"isWriteForwardEnabled,omitempty"`
	Targets      []ReplicationTarget `json:"replicationTargets"`
}

type sourceRegionWire struct {
	KeyID   string `json:"sourceKeyId"`
	Region  string `json:"sourceRegion"`
	VaultID string `json:"sourceVaultId"`
}

func parseReplication(raw jsontext.Value) (*SecretReplication, *APIError) {
	if t := bytes.TrimSpace(raw); len(t) == 0 || string(t) == jsonNull {
		return nil, nil
	}

	var r SecretReplication
	if err := json.Unmarshal(raw, &r, jsonx.V1()); err != nil {
		return nil, errParam("replicationConfig is not valid")
	}

	if len(r.Targets) > maxReplicationTargets {
		return nil, errParam("replicationConfig allows at most 3 replicationTargets")
	}

	return &r, nil
}

func (b *InMemoryBackend) checkReplication(r *SecretReplication, sourceVault string) *APIError {
	seen := map[string]bool{}

	for _, t := range r.Targets {
		switch {
		case t.KeyID == "" || t.VaultID == "" || t.Region == "":
			return errMissing("replicationTargets need targetKeyId, targetVaultId and targetRegion")
		case !regionRe.MatchString(t.Region):
			return errParam("targetRegion " + t.Region + " is not a region id")
		case t.VaultID == sourceVault || seen[t.VaultID]:
			return errParam("replicationTargets must name distinct vaults other than the source vault")
		}

		seen[t.VaultID] = true

		if _, aerr := b.activeVault(t.VaultID); aerr != nil {
			return aerr
		}

		if _, aerr := b.secretKey(t.VaultID, t.KeyID); aerr != nil {
			return aerr
		}
	}

	return nil
}

func (s *Secret) isReplica() bool { return s.SourceID != "" }

// replicaOf returns the replica of src in vaultID.
func (b *InMemoryBackend) replicaOf(src *Secret, vaultID string) *Secret {
	for _, s := range b.secrets {
		if s.SourceID == src.ID && s.VaultID == vaultID {
			return s
		}
	}

	return nil
}

// syncSecretReplicas brings every replica of a replicated secret level with its source and removes replicas whose
// target was dropped.
func (b *InMemoryBackend) syncSecretReplicas() {
	for id, r := range b.secrets {
		src, ok := b.secrets[r.SourceID]
		if r.isReplica() && (!ok || src.State == stateDeleted || !src.hasTarget(r.VaultID)) {
			delete(b.secrets, id)
		}
	}

	for _, src := range slices.Collect(maps.Values(b.secrets)) {
		if src.Replication == nil || src.isReplica() || src.State == stateDeleted {
			continue
		}

		for _, t := range src.Replication.Targets {
			b.syncReplica(src, t)
		}
	}
}

func (s *Secret) hasTarget(vaultID string) bool {
	return s.Replication != nil && slices.ContainsFunc(s.Replication.Targets, func(t ReplicationTarget) bool {
		return t.VaultID == vaultID
	})
}

func (b *InMemoryBackend) syncReplica(src *Secret, t ReplicationTarget) {
	r := b.replicaOf(src, t.VaultID)
	if r != nil && r.SyncedEtag == src.Etag && r.KeyID == t.KeyID {
		return
	}

	if _, aerr := b.activeVault(t.VaultID); aerr != nil {
		return
	}

	if r == nil {
		if b.nameTaken(t.VaultID, src.Name) {
			return
		}

		r = &Secret{
			ID: newOCID("vaultsecret", ""), VaultID: t.VaultID, SourceID: src.ID,
			TimeCreated: b.nowUTC(), Seq: b.nextSeq(), State: stateActive,
		}
		b.secrets[r.ID] = r
	}

	if aerr := b.copyVersions(src, r, t); aerr != nil {
		return
	}

	r.Name, r.CompartmentID, r.Description, r.Metadata = src.Name, src.CompartmentID, src.Description, src.Metadata
	r.FreeformTags, r.DefinedTags, r.Rules = src.FreeformTags, src.DefinedTags, src.Rules
	r.State, r.TimeOfDeletion = src.State, src.TimeOfDeletion
	r.Current, r.Previous, r.Pending = src.Current, src.Previous, src.Pending
	r.KeyID, r.SyncedEtag, r.Etag = t.KeyID, src.Etag, newEtag()
	r.SourceVault, r.SourceKey = src.VaultID, src.KeyID
}

// copyVersions re-encrypts every source version under the replica's key.
func (b *InMemoryBackend) copyVersions(src, r *Secret, t ReplicationTarget) *APIError {
	kv, aerr := b.secretKey(t.VaultID, t.KeyID)
	if aerr != nil {
		return aerr
	}

	versions := make([]*SecretVersion, 0, len(src.Versions))

	for _, v := range src.Versions {
		plain, perr := b.openVersion(src, v)
		if perr != nil {
			return perr
		}

		ct, serr := aesSeal(kv.ID, kv.Material, plain, nil)
		if serr != nil {
			return serr
		}

		cp := *v
		cp.Ciphertext = ct
		versions = append(versions, &cp)
	}

	r.Versions = versions

	return nil
}

func (s *Secret) sourceRegion() *sourceRegionWire {
	if !s.isReplica() {
		return nil
	}

	return &sourceRegionWire{KeyID: s.SourceKey, Region: Region, VaultID: s.SourceVault}
}
