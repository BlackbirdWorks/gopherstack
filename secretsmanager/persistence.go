package secretsmanager

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// secretSnapshot captures all fields of Secret including those tagged json:"-".
type secretSnapshot struct {
	Tags             *tags.Tags                `json:"tags,omitempty"`
	DeletedDate      *float64                  `json:"deletedDate,omitempty"`
	Versions         map[string]*SecretVersion `json:"versions"`
	ARN              string                    `json:"arn"`
	Name             string                    `json:"name"`
	Description      string                    `json:"description,omitempty"`
	CurrentVersionID string                    `json:"currentVersionID"`
}

type backendSnapshot struct {
	Secrets   map[string]*secretSnapshot `json:"secrets"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	secrets := make(map[string]*secretSnapshot, len(b.secrets))
	for k, s := range b.secrets {
		secrets[k] = &secretSnapshot{
			ARN:              s.ARN,
			Name:             s.Name,
			Description:      s.Description,
			Tags:             s.Tags,
			DeletedDate:      s.DeletedDate,
			Versions:         s.Versions,
			CurrentVersionID: s.CurrentVersionID,
		}
	}

	snap := backendSnapshot{
		Secrets:   secrets,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
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

	if snap.Secrets == nil {
		snap.Secrets = make(map[string]*secretSnapshot)
	}

	b.secrets = make(map[string]*Secret, len(snap.Secrets))

	for k, ss := range snap.Secrets {
		if ss.Versions == nil {
			ss.Versions = make(map[string]*SecretVersion)
		}

		b.secrets[k] = &Secret{
			ARN:              ss.ARN,
			Name:             ss.Name,
			Description:      ss.Description,
			Tags:             ss.Tags,
			DeletedDate:      ss.DeletedDate,
			Versions:         ss.Versions,
			CurrentVersionID: ss.CurrentVersionID,
		}
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
