package secretsmanager

import (
	"encoding/json"
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// secretSnapshot captures all fields of Secret including those tagged json:"-".
type secretSnapshot struct {
	Tags              *tags.Tags                `json:"tags,omitempty"`
	DeletedDate       *float64                  `json:"deletedDate,omitempty"`
	LastChangedDate   *float64                  `json:"lastChangedDate,omitempty"`
	LastRotatedDate   *float64                  `json:"lastRotatedDate,omitempty"`
	Versions          map[string]*SecretVersion `json:"versions"`
	ARN               string                    `json:"arn"`
	Name              string                    `json:"name"`
	Description       string                    `json:"description,omitempty"`
	KmsKeyID          string                    `json:"kmsKeyID,omitempty"`
	RotationLambdaARN string                    `json:"rotationLambdaARN,omitempty"`
	RotationRules     *RotationRulesType        `json:"rotationRules,omitempty"`
	CurrentVersionID  string                    `json:"currentVersionID"`
	RotationEnabled   bool                      `json:"rotationEnabled,omitempty"`
}

type backendSnapshot struct {
	Secrets            map[string]*secretSnapshot         `json:"secrets"`
	ResourcePolicies   map[string]string                  `json:"resourcePolicies,omitempty"`
	ReplicationConfigs map[string][]ReplicationStatusType `json:"replicationConfigs,omitempty"`
	AccountID          string                             `json:"accountID"`
	Region             string                             `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	secrets := make(map[string]*secretSnapshot, len(b.secrets))
	for k, s := range b.secrets {
		secrets[k] = &secretSnapshot{
			ARN:               s.ARN,
			Name:              s.Name,
			Description:       s.Description,
			KmsKeyID:          s.KmsKeyID,
			RotationLambdaARN: s.RotationLambdaARN,
			RotationRules:     cloneRotationRules(s.RotationRules),
			Tags:              s.Tags,
			DeletedDate:       s.DeletedDate,
			LastChangedDate:   s.LastChangedDate,
			LastRotatedDate:   s.LastRotatedDate,
			Versions:          s.Versions,
			CurrentVersionID:  s.CurrentVersionID,
			RotationEnabled:   s.RotationEnabled,
		}
	}

	snap := backendSnapshot{
		Secrets:            secrets,
		ResourcePolicies:   b.resourcePolicies,
		ReplicationConfigs: b.replicationConfigs,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("secretsmanager: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Close Tags on any secrets that are being replaced to prevent
	// Prometheus registry leaks.
	for _, secret := range b.secrets {
		if secret.Tags != nil {
			secret.Tags.Close()
		}
	}

	if snap.Secrets == nil {
		snap.Secrets = make(map[string]*secretSnapshot)
	}

	b.secrets = make(map[string]*Secret, len(snap.Secrets))

	for k, ss := range snap.Secrets {
		if ss.Versions == nil {
			ss.Versions = make(map[string]*SecretVersion)
		}

		b.secrets[k] = &Secret{
			ARN:               ss.ARN,
			Name:              ss.Name,
			Description:       ss.Description,
			KmsKeyID:          ss.KmsKeyID,
			RotationLambdaARN: ss.RotationLambdaARN,
			RotationRules:     cloneRotationRules(ss.RotationRules),
			Tags:              ss.Tags,
			DeletedDate:       ss.DeletedDate,
			LastChangedDate:   ss.LastChangedDate,
			LastRotatedDate:   ss.LastRotatedDate,
			Versions:          ss.Versions,
			CurrentVersionID:  ss.CurrentVersionID,
			RotationEnabled:   ss.RotationEnabled,
		}
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]string)
	}

	b.resourcePolicies = snap.ResourcePolicies

	if snap.ReplicationConfigs == nil {
		snap.ReplicationConfigs = make(map[string][]ReplicationStatusType)
	}

	b.replicationConfigs = snap.ReplicationConfigs

	b.ensureNonNilMaps()
	for _, secret := range b.secrets {
		if secret.RotationRules != nil && secret.RotationEnabled {
			b.ensureRotationScheduler()

			break
		}
	}

	return nil
}

// ensureNonNilMaps initialises any nil maps to avoid nil-map panics.
func (b *InMemoryBackend) ensureNonNilMaps() {
	if b.secrets == nil {
		b.secrets = make(map[string]*Secret)
	}

	if b.resourcePolicies == nil {
		b.resourcePolicies = make(map[string]string)
	}

	if b.replicationConfigs == nil {
		b.replicationConfigs = make(map[string][]ReplicationStatusType)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
