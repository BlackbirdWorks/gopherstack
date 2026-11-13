package ocinosql

import (
	"net/http"
	"strings"
	"time"
)

const (
	envMultiTenancy = "MULTI_TENANCY"
	envHosted       = "HOSTED"
	kmsActive       = "ACTIVE"
	kmsUpdating     = "UPDATING"
	keyOCIDPrefix   = "ocid1.key."
	vaultOCIDPrefix = "ocid1.vault."
)

type kmsKeyWire struct {
	ID          *string    `json:"id"`
	KmsVaultID  *string    `json:"kmsVaultId"`
	TimeCreated *time.Time `json:"timeCreated,omitempty"`
	TimeUpdated *time.Time `json:"timeUpdated,omitempty"`
	KmsKeyState string     `json:"kmsKeyState"`
}

// ConfigurationWire is the wire form of GetConfiguration.
type ConfigurationWire struct {
	KmsKey      *kmsKeyWire `json:"kmsKey,omitempty"`
	Environment string      `json:"environment"`
}

type updateConfigIn struct {
	KmsKey      *kmsKeyIn `json:"kmsKey"`
	Environment string    `json:"environment"`
}

type kmsKeyIn struct {
	ID         *string `json:"id"`
	KmsVaultID *string `json:"kmsVaultId"`
}

// SetMultiTenancy switches the emulated environment between HOSTED (default) and MULTI_TENANCY.
func (b *InMemoryBackend) SetMultiTenancy(on bool) {
	b.mu.Lock("SetMultiTenancy")
	b.multiTenant = on
	b.mu.Unlock()
}

func (b *InMemoryBackend) environment() string {
	if b.multiTenant {
		return envMultiTenancy
	}

	return envHosted
}

func (b *InMemoryBackend) cfg() *Configuration {
	if b.config == nil {
		b.config = &Configuration{KmsState: kmsActive, Etag: newEtag()}
	}

	if due(b.config.TransitionAt, b.nowUTC()) {
		b.config.TransitionAt, b.config.KmsState = nil, kmsActive
	}

	return b.config
}

func (b *InMemoryBackend) configView() ConfigurationWire {
	out := ConfigurationWire{Environment: b.environment()}
	if b.multiTenant {
		return out
	}

	c := b.cfg()
	k := &kmsKeyWire{KmsKeyState: c.KmsState, TimeCreated: c.KmsCreated, TimeUpdated: c.KmsUpdated}

	if c.KmsKeyID != "" {
		k.ID, k.KmsVaultID = &c.KmsKeyID, &c.KmsVaultID
	}

	out.KmsKey = k

	return out
}

// GetConfiguration returns the service-level configuration and its etag.
func (b *InMemoryBackend) GetConfiguration(compartment string) (ConfigurationWire, string, error) {
	b.mu.Lock("GetConfiguration")
	defer b.mu.Unlock()

	if aerr := b.checkCompartment(compartment); aerr != nil {
		return ConfigurationWire{}, "", aerr
	}

	return b.configView(), b.cfg().Etag, nil
}

func (b *InMemoryBackend) checkConfigWrite(compartment, ifMatch string) *APIError {
	if aerr := b.checkCompartment(compartment); aerr != nil {
		return aerr
	}

	if b.multiTenant {
		return errParam("A multi-tenancy service has no key configuration to change")
	}

	return checkEtag(b.cfg().Etag, ifMatch)
}

func validateKmsKey(k *kmsKeyIn) *APIError {
	if k == nil {
		return errMissing("kmsKey is required")
	}

	if k.ID == nil || !strings.HasPrefix(*k.ID, keyOCIDPrefix) {
		return errParam("kmsKey.id must be the OCID of a KMS key")
	}

	if k.KmsVaultID == nil || !strings.HasPrefix(*k.KmsVaultID, vaultOCIDPrefix) {
		return errParam("kmsKey.kmsVaultId must be the OCID of the vault containing the key")
	}

	return nil
}

func (b *InMemoryBackend) configWR(compartment string, finish time.Time) string {
	wr := b.newWR(wrConfig, compartment, compartment, "UPDATED", finish)
	b.workReqs[wr].EntityType = entityConfig

	return wr
}

// UpdateConfiguration assigns the global KMS key; a dry run validates only and returns no work request.
func (b *InMemoryBackend) UpdateConfiguration(
	compartment, ifMatch string,
	dryRun bool,
	in updateConfigIn,
) (string, error) {
	b.mu.Lock("UpdateConfiguration")
	defer b.mu.Unlock()

	if aerr := b.checkConfigWrite(compartment, ifMatch); aerr != nil {
		return "", aerr
	}

	if in.Environment != b.environment() {
		return "", errParam("environment must be " + b.environment() + ", the environment of this service")
	}

	if aerr := validateKmsKey(in.KmsKey); aerr != nil {
		return "", aerr
	}

	if dryRun {
		return "", nil
	}

	c, now := b.cfg(), b.nowUTC()
	if c.KmsCreated == nil {
		c.KmsCreated = &now
	}

	c.KmsKeyID, c.KmsVaultID, c.KmsState = *in.KmsKey.ID, *in.KmsKey.KmsVaultID, kmsUpdating
	c.KmsUpdated, c.TransitionAt, c.Etag = &now, b.transitionAt(), newEtag()

	return b.configWR(compartment, *c.TransitionAt), nil
}

// UnassignKmsKey reverts to Oracle-managed encryption; with no key assigned it only records a work request.
func (b *InMemoryBackend) UnassignKmsKey(compartment, ifMatch string, dryRun bool) (string, error) {
	b.mu.Lock("UnassignKmsKey")
	defer b.mu.Unlock()

	if aerr := b.checkConfigWrite(compartment, ifMatch); aerr != nil {
		return "", aerr
	}

	if dryRun {
		return "", nil
	}

	c, now := b.cfg(), b.nowUTC()
	if c.KmsKeyID != "" {
		c.KmsKeyID, c.KmsVaultID, c.KmsState = "", "", kmsUpdating
		c.KmsUpdated, c.TransitionAt, c.Etag = &now, b.transitionAt(), newEtag()

		return b.configWR(compartment, *c.TransitionAt), nil
	}

	return b.configWR(compartment, now), nil
}

func (q *req) dryRun() bool { return q.r.Header.Get("Is-Opc-Dry-Run") == "true" }

func (h *Handler) getConfiguration(q *req) error {
	w, etag, err := h.Backend.GetConfiguration(q.query("compartmentId"))

	return reply(q, w, etag, err)
}

func dryRunOr(q *req, wr string, err error) error {
	if err != nil {
		return err
	}

	if wr == "" {
		q.w.WriteHeader(http.StatusAccepted)

		return nil
	}

	return q.accepted(wr)
}

func (h *Handler) updateConfiguration(q *req) error {
	var in updateConfigIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateConfiguration(q.query("compartmentId"), q.ifMatch(), q.dryRun(), in)

	return dryRunOr(q, wr, err)
}

func (h *Handler) unassignKmsKey(q *req) error {
	wr, err := h.Backend.UnassignKmsKey(q.query("compartmentId"), q.ifMatch(), q.dryRun())

	return dryRunOr(q, wr, err)
}
