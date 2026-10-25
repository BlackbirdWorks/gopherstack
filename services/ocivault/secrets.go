package ocivault

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iso8601"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxSecretBytes = 25 * 1024
	ruleExpiry     = "SECRET_EXPIRY_RULE"
	ruleReuse      = "SECRET_REUSE_RULE"
)

var secretNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// SecretContent is the polymorphic secretContent body (only BASE64 exists).
type SecretContent struct {
	ContentType string `json:"contentType"`
	Name        string `json:"name"`
	Content     string `json:"content"`
	Stage       string `json:"stage"`
}

// CreateSecretIn is the CreateSecret request body.
type CreateSecretIn struct {
	FreeformTags            map[string]string         `json:"freeformTags"`
	DefinedTags             map[string]map[string]any `json:"definedTags"`
	Metadata                map[string]any            `json:"metadata"`
	SecretContent           *SecretContent            `json:"secretContent"`
	ReplicationConfig       json.RawMessage           `json:"replicationConfig"`
	RotationConfig          json.RawMessage           `json:"rotationConfig"`
	SecretGenerationContext json.RawMessage           `json:"secretGenerationContext"`
	EnableAutoGeneration    *bool                     `json:"enableAutoGeneration"`
	CompartmentID           string                    `json:"compartmentId"`
	KeyID                   string                    `json:"keyId"`
	SecretName              string                    `json:"secretName"`
	VaultID                 string                    `json:"vaultId"`
	Description             string                    `json:"description"`
	SecretRules             []json.RawMessage         `json:"secretRules"`
}

// UpdateSecretIn is the UpdateSecret request body.
type UpdateSecretIn struct {
	CurrentVersionNumber    *int64                    `json:"currentVersionNumber"`
	Description             *string                   `json:"description"`
	FreeformTags            map[string]string         `json:"freeformTags"`
	DefinedTags             map[string]map[string]any `json:"definedTags"`
	Metadata                map[string]any            `json:"metadata"`
	SecretContent           *SecretContent            `json:"secretContent"`
	ReplicationConfig       json.RawMessage           `json:"replicationConfig"`
	RotationConfig          json.RawMessage           `json:"rotationConfig"`
	SecretGenerationContext json.RawMessage           `json:"secretGenerationContext"`
	EnableAutoGeneration    *bool                     `json:"enableAutoGeneration"`
	SecretRules             []json.RawMessage         `json:"secretRules"`
}

type expiryRule struct {
	Absolute  *time.Time `json:"timeOfAbsoluteExpiry"`
	BlockedOn *bool      `json:"isSecretContentRetrievalBlockedOnExpiry"`
	Interval  string     `json:"secretVersionExpiryInterval"`
}

func unsupported(raw json.RawMessage, auto *bool, what string) *APIError {
	if len(bytes.TrimSpace(raw)) > 0 && string(bytes.TrimSpace(raw)) != "null" || auto != nil && *auto {
		return errParam(what + " is not supported by this emulator")
	}

	return nil
}

func rejectUnsupported(repl, rot, gen json.RawMessage, auto *bool) *APIError {
	for _, c := range []struct {
		what string
		raw  json.RawMessage
	}{
		{what: "replicationConfig", raw: repl},
		{what: "rotationConfig", raw: rot},
		{what: "secretGenerationContext", raw: gen},
	} {
		if aerr := unsupported(c.raw, nil, c.what); aerr != nil {
			return aerr
		}
	}

	return unsupported(nil, auto, "enableAutoGeneration")
}

func validateRules(rules []json.RawMessage) *APIError {
	for _, r := range rules {
		var head struct {
			RuleType string `json:"ruleType"`
		}

		if err := json.Unmarshal(r, &head); err != nil || (head.RuleType != ruleExpiry && head.RuleType != ruleReuse) {
			return errParam("secretRules[].ruleType must be SECRET_EXPIRY_RULE or SECRET_REUSE_RULE")
		}

		if head.RuleType == ruleExpiry {
			if _, aerr := parseExpiry(r); aerr != nil {
				return aerr
			}
		}
	}

	return nil
}

func parseExpiry(raw json.RawMessage) (expiryRule, *APIError) {
	var e expiryRule
	if err := json.Unmarshal(raw, &e); err != nil {
		return e, errParam("invalid SECRET_EXPIRY_RULE")
	}

	if e.Interval != "" {
		if _, err := iso8601.Parse(e.Interval); err != nil {
			return e, errParam("secretVersionExpiryInterval must be an ISO 8601 duration")
		}
	}

	return e, nil
}

func ruleOfType(rules []json.RawMessage, t string) json.RawMessage {
	for _, r := range rules {
		var head struct {
			RuleType string `json:"ruleType"`
		}

		if json.Unmarshal(r, &head) == nil && head.RuleType == t {
			return r
		}
	}

	return nil
}

func (b *InMemoryBackend) versionExpiry(rules []json.RawMessage) *time.Time {
	raw := ruleOfType(rules, ruleExpiry)
	if raw == nil {
		return nil
	}

	e, _ := parseExpiry(raw)

	var at *time.Time

	if d, err := iso8601.Parse(e.Interval); err == nil && e.Interval != "" {
		t := b.nowUTC().Add(d)
		at = &t
	}

	if e.Absolute != nil && (at == nil || e.Absolute.Before(*at)) {
		t := e.Absolute.UTC()
		at = &t
	}

	return at
}

func decodeContent(c *SecretContent) ([]byte, *APIError) {
	if c.ContentType != "" && c.ContentType != contentBase64 {
		return nil, errParam("secretContent.contentType must be BASE64")
	}

	raw, err := base64.StdEncoding.DecodeString(c.Content)
	if err != nil {
		return nil, errParam("secretContent.content must be base64-encoded")
	}

	if len(raw) > maxSecretBytes {
		return nil, errParam("secretContent.content exceeds the 25 KB limit")
	}

	if c.Stage != "" && c.Stage != stageCurrent && c.Stage != stagePending {
		return nil, errParam("secretContent.stage must be CURRENT or PENDING")
	}

	return raw, nil
}

// secretKey returns the usable AES key version encrypting this secret.
func (b *InMemoryBackend) secretKey(vaultID, keyID string) (*KeyVersion, *APIError) {
	k, kv, aerr := b.usable(vaultID, keyID, "")
	if aerr != nil {
		return nil, aerr
	}

	if k.Shape.Algorithm != algAES {
		return nil, errParam("keyId must reference an AES key")
	}

	return kv, nil
}

func (b *InMemoryBackend) openVersion(s *Secret, v *SecretVersion) ([]byte, *APIError) {
	versionID, body, ok := sealVersion(v.Ciphertext)
	if !ok {
		return nil, errState("stored secret content is unreadable")
	}

	_, kv, aerr := b.usable(s.VaultID, s.KeyID, versionID)
	if aerr != nil {
		return nil, aerr
	}

	return aesOpen(kv.Material, body, nil)
}

func (b *InMemoryBackend) reuseViolation(s *Secret, plain []byte) bool {
	if ruleOfType(s.Rules, ruleReuse) == nil {
		return false
	}

	for _, v := range s.Versions {
		if prev, aerr := b.openVersion(s, v); aerr == nil && bytes.Equal(prev, plain) {
			return true
		}
	}

	return false
}

// addVersion encrypts content under the secret's key and links it into the stage chain.
func (b *InMemoryBackend) addSecretVersion(s *Secret, c *SecretContent) *APIError {
	plain, aerr := decodeContent(c)
	if aerr != nil {
		return aerr
	}

	kv, aerr := b.secretKey(s.VaultID, s.KeyID)
	if aerr != nil {
		return aerr
	}

	if b.reuseViolation(s, plain) {
		return errParam("The secret content duplicates an existing version, which the secret reuse rule forbids")
	}

	ct, aerr := aesSeal(kv.ID, kv.Material, plain, nil)
	if aerr != nil {
		return aerr
	}

	v := &SecretVersion{
		Number:       s.latest() + 1,
		Name:         c.Name,
		Ciphertext:   ct,
		TimeCreated:  b.nowUTC(),
		TimeOfExpiry: b.versionExpiry(s.Rules),
	}
	s.Versions = append(s.Versions, v)

	if c.Stage == stagePending {
		s.Pending = v.Number

		return nil
	}

	promote(s, v.Number)

	return nil
}

func promote(s *Secret, n int64) {
	if s.Current != 0 && s.Current != n {
		s.Previous = s.Current
	}

	s.Current = n
	if s.Pending == n {
		s.Pending = 0
	}
}

func (b *InMemoryBackend) nameTaken(vaultID, name string) bool {
	for _, o := range b.secrets {
		if o.VaultID == vaultID && o.Name == name && o.State != stateDeleted {
			return true
		}
	}

	return false
}

func validateCreateSecret(in *CreateSecretIn) *APIError {
	switch {
	case in.CompartmentID == "" || in.KeyID == "" || in.VaultID == "":
		return errMissing("compartmentId, keyId and vaultId are required")
	case !secretNameRe.MatchString(in.SecretName):
		return errParam("secretName must be 1-255 characters of letters, digits, hyphens, underscores and periods")
	}

	if aerr := rejectUnsupported(
		in.ReplicationConfig,
		in.RotationConfig,
		in.SecretGenerationContext,
		in.EnableAutoGeneration,
	); aerr != nil {
		return aerr
	}

	return validateRules(in.SecretRules)
}

// CreateSecret creates a secret and, when content is given, version 1.
func (b *InMemoryBackend) CreateSecret(in CreateSecretIn) (secretWire, string, error) {
	if aerr := validateCreateSecret(&in); aerr != nil {
		return secretWire{}, "", aerr
	}

	b.mu.Lock("CreateSecret")
	defer b.mu.Unlock()

	if _, aerr := b.activeVault(in.VaultID); aerr != nil {
		return secretWire{}, "", aerr
	}

	if _, aerr := b.secretKey(in.VaultID, in.KeyID); aerr != nil {
		return secretWire{}, "", aerr
	}

	if b.nameTaken(in.VaultID, in.SecretName) {
		return secretWire{}, "", errExists("A secret named " + in.SecretName + " already exists in this vault")
	}

	s := &Secret{
		ID: newOCID("vaultsecret", ""), VaultID: in.VaultID, CompartmentID: in.CompartmentID, Name: in.SecretName,
		KeyID: in.KeyID, Description: in.Description, State: stateActive, TimeCreated: b.nowUTC(),
		FreeformTags: in.FreeformTags, DefinedTags: in.DefinedTags, Metadata: in.Metadata, Rules: in.SecretRules,
		Etag: newEtag(), Seq: b.nextSeq(),
	}

	if in.SecretContent != nil {
		if aerr := b.addSecretVersion(s, in.SecretContent); aerr != nil {
			return secretWire{}, "", aerr
		}
	}

	b.secrets[s.ID] = s

	return secretView(s, stateCreating), s.Etag, nil
}

func (b *InMemoryBackend) secretByID(id string) (*Secret, *APIError) {
	s, ok := b.secrets[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return s, nil
}

func (b *InMemoryBackend) writableSecret(id, ifMatch string) (*Secret, *APIError) {
	s, aerr := b.secretByID(id)
	if aerr != nil {
		return nil, aerr
	}

	if s.State != stateActive {
		return nil, errState("Secret " + id + " is in state " + s.State + "; the operation requires ACTIVE")
	}

	return s, checkEtag(s.Etag, ifMatch)
}

// GetSecret returns secret metadata.
func (b *InMemoryBackend) GetSecret(id string) (secretWire, string, error) {
	b.mu.RLock("GetSecret")
	defer b.mu.RUnlock()

	s, aerr := b.secretByID(id)
	if aerr != nil {
		return secretWire{}, "", aerr
	}

	return secretView(s, ""), s.Etag, nil
}

// SecretFilter narrows ListSecrets.
type SecretFilter struct {
	Compartment string
	Name        string
	VaultID     string
	State       string
}

// ListSecrets lists secrets in a compartment.
func (b *InMemoryBackend) ListSecrets(f SecretFilter, o ListOpts) (page.Page[secretSummaryWire], error) {
	if f.Compartment == "" {
		return page.Page[secretSummaryWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListSecrets")
	defer b.mu.RUnlock()

	var all []*Secret

	for _, s := range b.secrets {
		if s.CompartmentID == f.Compartment && (f.Name == "" || f.Name == s.Name) &&
			(f.VaultID == "" || f.VaultID == s.VaultID) && (f.State == "" || f.State == s.State) {
			all = append(all, s)
		}
	}

	pg := paginate(all, o, func(s *Secret) string { return s.Name },
		func(s *Secret) (time.Time, int64) { return s.TimeCreated, s.Seq })

	out := make([]secretSummaryWire, 0, len(pg.Data))
	for _, s := range pg.Data {
		out = append(out, secretSummary(s))
	}

	return page.Page[secretSummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdateSecret edits metadata, adds a version on content change, or promotes a version.
func (b *InMemoryBackend) UpdateSecret(id, ifMatch string, in UpdateSecretIn) (secretWire, string, error) {
	if aerr := rejectUnsupported(
		in.ReplicationConfig,
		in.RotationConfig,
		in.SecretGenerationContext,
		in.EnableAutoGeneration,
	); aerr != nil {
		return secretWire{}, "", aerr
	}

	if aerr := validateRules(in.SecretRules); aerr != nil {
		return secretWire{}, "", aerr
	}

	b.mu.Lock("UpdateSecret")
	defer b.mu.Unlock()

	s, aerr := b.writableSecret(id, ifMatch)
	if aerr != nil {
		return secretWire{}, "", aerr
	}

	if aerr = b.applySecretUpdate(s, in); aerr != nil {
		return secretWire{}, "", aerr
	}

	s.Etag = newEtag()

	return secretView(s, ""), s.Etag, nil
}

func (b *InMemoryBackend) applySecretUpdate(s *Secret, in UpdateSecretIn) *APIError {
	if in.SecretRules != nil {
		s.Rules = in.SecretRules
	}

	if in.SecretContent != nil {
		if aerr := b.addSecretVersion(s, in.SecretContent); aerr != nil {
			return aerr
		}
	}

	if in.CurrentVersionNumber != nil {
		v, aerr := s.version(*in.CurrentVersionNumber)
		if aerr != nil {
			return aerr
		}

		if v.TimeOfDeletion != nil {
			return errState("Secret version " + itoa(v.Number) + " is pending deletion and cannot become current")
		}

		promote(s, v.Number)
	}

	if in.Description != nil {
		s.Description = *in.Description
	}

	if in.FreeformTags != nil {
		s.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		s.DefinedTags = in.DefinedTags
	}

	if in.Metadata != nil {
		s.Metadata = in.Metadata
	}

	return nil
}

// ScheduleSecretDeletion moves an ACTIVE secret to PENDING_DELETION.
func (b *InMemoryBackend) ScheduleSecretDeletion(id, ifMatch string, at *time.Time) error {
	b.mu.Lock("ScheduleSecretDeletion")
	defer b.mu.Unlock()

	s, aerr := b.writableSecret(id, ifMatch)
	if aerr != nil {
		return aerr
	}

	t, aerr := b.deletionTime(at, secretMinDeletion)
	if aerr != nil {
		return aerr
	}

	s.State, s.TimeOfDeletion, s.Etag = statePendingDeletion, &t, newEtag()

	return nil
}

// CancelSecretDeletion returns a PENDING_DELETION secret to ACTIVE.
func (b *InMemoryBackend) CancelSecretDeletion(id, ifMatch string) error {
	b.mu.Lock("CancelSecretDeletion")
	defer b.mu.Unlock()

	s, aerr := b.secretByID(id)
	if aerr != nil {
		return aerr
	}

	if s.State != statePendingDeletion {
		return errState("Secret " + id + " is not pending deletion")
	}

	if aerr = checkEtag(s.Etag, ifMatch); aerr != nil {
		return aerr
	}

	s.State, s.TimeOfDeletion, s.Etag = stateActive, nil, newEtag()

	return nil
}

// ChangeSecretCompartment moves a secret.
func (b *InMemoryBackend) ChangeSecretCompartment(id, ifMatch, compartment string) error {
	if compartment == "" {
		return errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeSecretCompartment")
	defer b.mu.Unlock()

	s, aerr := b.writableSecret(id, ifMatch)
	if aerr != nil {
		return aerr
	}

	s.CompartmentID, s.Etag = compartment, newEtag()

	return nil
}

// GetSecretVersion returns one version's metadata.
func (b *InMemoryBackend) GetSecretVersion(id string, n int64) (secretVersionWire, string, error) {
	b.mu.RLock("GetSecretVersion")
	defer b.mu.RUnlock()

	s, aerr := b.secretByID(id)
	if aerr != nil {
		return secretVersionWire{}, "", aerr
	}

	v, aerr := s.version(n)
	if aerr != nil {
		return secretVersionWire{}, "", aerr
	}

	return secretVersionView(s, v), s.Etag, nil
}

// ListSecretVersions lists a secret's versions (oldest first unless DESC).
func (b *InMemoryBackend) ListSecretVersions(id string, o ListOpts) (page.Page[secretVersionSummaryWire], error) {
	b.mu.RLock("ListSecretVersions")
	defer b.mu.RUnlock()

	s, aerr := b.secretByID(id)
	if aerr != nil {
		return page.Page[secretVersionSummaryWire]{}, aerr
	}

	vs := slices.Clone(s.Versions)
	if o.desc(false) {
		slices.Reverse(vs)
	}

	pg := page.New(vs, o.Page, o.Limit, defaultListLimit)

	out := make([]secretVersionSummaryWire, 0, len(pg.Data))
	for _, v := range pg.Data {
		out = append(out, secretVersionSummary(s, v))
	}

	return page.Page[secretVersionSummaryWire]{Data: out, Next: pg.Next}, nil
}

// ScheduleSecretVersionDeletion schedules deletion of a version that is not current.
func (b *InMemoryBackend) ScheduleSecretVersionDeletion(id string, n int64, ifMatch string, at *time.Time) error {
	b.mu.Lock("ScheduleSecretVersionDeletion")
	defer b.mu.Unlock()

	s, aerr := b.writableSecret(id, ifMatch)
	if aerr != nil {
		return aerr
	}

	v, aerr := s.version(n)
	if aerr != nil {
		return aerr
	}

	if n == s.Current || v.TimeOfDeletion != nil {
		return errState("Secret version " + itoa(n) + " is current or already pending deletion")
	}

	t, aerr := b.deletionTime(at, secretMinDeletion)
	if aerr != nil {
		return aerr
	}

	v.TimeOfDeletion, s.Etag = &t, newEtag()

	return nil
}

// CancelSecretVersionDeletion cancels a scheduled version deletion.
func (b *InMemoryBackend) CancelSecretVersionDeletion(id string, n int64, ifMatch string) error {
	b.mu.Lock("CancelSecretVersionDeletion")
	defer b.mu.Unlock()

	s, aerr := b.writableSecret(id, ifMatch)
	if aerr != nil {
		return aerr
	}

	v, aerr := s.version(n)
	if aerr != nil {
		return aerr
	}

	if v.TimeOfDeletion == nil {
		return errState("Secret version " + itoa(n) + " is not pending deletion")
	}

	v.TimeOfDeletion, s.Etag = nil, newEtag()

	return nil
}
