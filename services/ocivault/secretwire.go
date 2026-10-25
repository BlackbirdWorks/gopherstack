package ocivault

import (
	"encoding/json"
	"time"
)

type secretWire struct {
	TimeCreated      time.Time                 `json:"timeCreated"`
	TimeOfDeletion   *time.Time                `json:"timeOfDeletion,omitempty"`
	TimeOfCurrentExp *time.Time                `json:"timeOfCurrentVersionExpiry,omitempty"`
	CurrentVersion   *int64                    `json:"currentVersionNumber,omitempty"`
	FreeformTags     map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags      map[string]map[string]any `json:"definedTags,omitempty"`
	Metadata         map[string]any            `json:"metadata,omitempty"`
	CompartmentID    string                    `json:"compartmentId"`
	ID               string                    `json:"id"`
	LifecycleState   string                    `json:"lifecycleState"`
	SecretName       string                    `json:"secretName"`
	VaultID          string                    `json:"vaultId"`
	KeyID            string                    `json:"keyId"`
	Description      string                    `json:"description,omitempty"`
	SecretRules      []json.RawMessage         `json:"secretRules,omitempty"`
}

type secretSummaryWire struct {
	TimeCreated      time.Time                 `json:"timeCreated"`
	TimeOfDeletion   *time.Time                `json:"timeOfDeletion,omitempty"`
	TimeOfCurrentExp *time.Time                `json:"timeOfCurrentVersionExpiry,omitempty"`
	FreeformTags     map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags      map[string]map[string]any `json:"definedTags,omitempty"`
	CompartmentID    string                    `json:"compartmentId"`
	ID               string                    `json:"id"`
	LifecycleState   string                    `json:"lifecycleState"`
	SecretName       string                    `json:"secretName"`
	VaultID          string                    `json:"vaultId"`
	KeyID            string                    `json:"keyId"`
	Description      string                    `json:"description,omitempty"`
}

type secretVersionWire struct {
	TimeCreated      time.Time  `json:"timeCreated"`
	TimeOfDeletion   *time.Time `json:"timeOfDeletion,omitempty"`
	TimeOfCurrentExp *time.Time `json:"timeOfCurrentVersionExpiry,omitempty"`
	ContentType      string     `json:"contentType"`
	Name             string     `json:"name,omitempty"`
	SecretID         string     `json:"secretId"`
	Stages           []string   `json:"stages,omitempty"`
	VersionNumber    int64      `json:"versionNumber"`
}

type secretVersionSummaryWire struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeOfDeletion *time.Time `json:"timeOfDeletion,omitempty"`
	TimeOfExpiry   *time.Time `json:"timeOfExpiry,omitempty"`
	ContentType    string     `json:"contentType"`
	Name           string     `json:"name,omitempty"`
	SecretID       string     `json:"secretId"`
	Stages         []string   `json:"stages,omitempty"`
	VersionNumber  int64      `json:"versionNumber"`
}

type bundleContentWire struct {
	ContentType string `json:"contentType"`
	Content     string `json:"content"`
}

type secretBundleWire struct {
	TimeCreated    time.Time          `json:"timeCreated"`
	TimeOfDeletion *time.Time         `json:"timeOfDeletion,omitempty"`
	TimeOfExpiry   *time.Time         `json:"timeOfExpiry,omitempty"`
	Metadata       map[string]any     `json:"metadata,omitempty"`
	Content        *bundleContentWire `json:"secretBundleContent,omitempty"`
	SecretID       string             `json:"secretId"`
	VersionName    string             `json:"versionName,omitempty"`
	Stages         []string           `json:"stages,omitempty"`
	VersionNumber  int64              `json:"versionNumber"`
}

type bundleVersionSummaryWire struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeOfDeletion *time.Time `json:"timeOfDeletion,omitempty"`
	TimeOfExpiry   *time.Time `json:"timeOfExpiry,omitempty"`
	SecretID       string     `json:"secretId"`
	VersionName    string     `json:"versionName,omitempty"`
	Stages         []string   `json:"stages,omitempty"`
	VersionNumber  int64      `json:"versionNumber"`
}

const contentBase64 = "BASE64"

const (
	stageCurrent    = "CURRENT"
	stagePending    = "PENDING"
	stageLatest     = "LATEST"
	stagePrevious   = "PREVIOUS"
	stageDeprecated = "DEPRECATED"
)

func (s *Secret) latest() int64 {
	if len(s.Versions) == 0 {
		return 0
	}

	return s.Versions[len(s.Versions)-1].Number
}

func (s *Secret) stages(n int64) []string {
	var out []string

	if n == s.Current {
		out = append(out, stageCurrent)
	}

	if n == s.Pending {
		out = append(out, stagePending)
	}

	if n == s.Previous {
		out = append(out, stagePrevious)
	}

	if n == s.latest() {
		out = append(out, stageLatest)
	}

	if n != s.Current && n != s.Pending && n != s.Previous {
		out = append(out, stageDeprecated)
	}

	return out
}

func (s *Secret) version(n int64) (*SecretVersion, *APIError) {
	for _, v := range s.Versions {
		if v.Number == n {
			return v, nil
		}
	}

	return nil, errNotFound(s.ID + " version " + itoa(n))
}

func (s *Secret) currentExpiry() *time.Time {
	if v, err := s.version(s.Current); err == nil {
		return v.TimeOfExpiry
	}

	return nil
}

func secretView(s *Secret, state string) secretWire {
	if state == "" {
		state = s.State
	}

	w := secretWire{
		TimeCreated: s.TimeCreated, TimeOfDeletion: s.TimeOfDeletion, TimeOfCurrentExp: s.currentExpiry(),
		FreeformTags: s.FreeformTags, DefinedTags: s.DefinedTags, Metadata: s.Metadata,
		CompartmentID: s.CompartmentID, ID: s.ID, LifecycleState: state, SecretName: s.Name, VaultID: s.VaultID,
		KeyID: s.KeyID, Description: s.Description, SecretRules: s.Rules,
	}

	if s.Current != 0 {
		cur := s.Current
		w.CurrentVersion = &cur
	}

	return w
}

func secretSummary(s *Secret) secretSummaryWire {
	return secretSummaryWire{
		TimeCreated: s.TimeCreated, TimeOfDeletion: s.TimeOfDeletion, TimeOfCurrentExp: s.currentExpiry(),
		FreeformTags: s.FreeformTags, DefinedTags: s.DefinedTags, CompartmentID: s.CompartmentID, ID: s.ID,
		LifecycleState: s.State, SecretName: s.Name, VaultID: s.VaultID, KeyID: s.KeyID, Description: s.Description,
	}
}

func secretVersionView(s *Secret, v *SecretVersion) secretVersionWire {
	return secretVersionWire{
		TimeCreated: v.TimeCreated, TimeOfDeletion: v.TimeOfDeletion, TimeOfCurrentExp: v.TimeOfExpiry,
		VersionNumber: v.Number, ContentType: contentBase64, Name: v.Name, SecretID: s.ID, Stages: s.stages(v.Number),
	}
}

func secretVersionSummary(s *Secret, v *SecretVersion) secretVersionSummaryWire {
	return secretVersionSummaryWire{
		TimeCreated: v.TimeCreated, TimeOfDeletion: v.TimeOfDeletion, TimeOfExpiry: v.TimeOfExpiry,
		VersionNumber: v.Number, ContentType: contentBase64, Name: v.Name, SecretID: s.ID, Stages: s.stages(v.Number),
	}
}
