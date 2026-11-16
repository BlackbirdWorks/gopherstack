package azuresearch

import (
	"net/url"
	"strings"
)

const (
	cmkEnabled      = "Enabled"
	complianceOK    = "Compliant"
	complianceNotOK = "NonCompliant"
	fieldEncryption = "encryptionKey"
)

// KeyResolver checks that a Key Vault key can be used for encryption: it exists and is enabled.
type KeyResolver interface {
	ResolveKey(vaultName, keyName, version string) error
}

// SetKeyResolver wires the in-process Key Vault emulator used to resolve encryptionKey references.
func (b *InMemoryBackend) SetKeyResolver(r KeyResolver) {
	b.mu.Lock("SetKeyResolver")
	defer b.mu.Unlock()

	b.keys = r
}

// decodeDef decodes a resource definition and resolves its encryptionKey, if any.
func (h *Handler) decodeDef(req *request) (map[string]any, error) {
	def, err := decodeDefJSON(req.body)
	if err != nil {
		return nil, err
	}

	if err = h.Backend.checkEncryptionKey(def); err != nil {
		return nil, err
	}

	return def, nil
}

func (b *InMemoryBackend) checkEncryptionKey(def map[string]any) error {
	raw, present := def[fieldEncryption]
	if !present || raw == nil {
		return nil
	}

	ek := objOf(raw)
	name, uri := strOf(ek, "keyVaultKeyName"), strOf(ek, "keyVaultUri")

	u, err := url.Parse(uri)
	if name == "" || err != nil || u.Host == "" {
		return badf("The request is invalid. Details: encryptionKey requires keyVaultKeyName and keyVaultUri.")
	}

	b.mu.RLock("checkEncryptionKey")
	keys := b.keys
	b.mu.RUnlock()

	if keys == nil {
		return nil
	}

	vault, _, _ := strings.Cut(u.Hostname(), ".")
	if kerr := keys.ResolveKey(vault, name, strOf(ek, "keyVaultKeyVersion")); kerr != nil {
		return badf("The request is invalid. Details: the encryption key '%s' in '%s' could not be used: %s.",
			name, uri, kerr.Error())
	}

	return nil
}

// EncryptionCompliance reports Compliant unless enforcement is Enabled and some object lacks an encryptionKey.
func (b *InMemoryBackend) EncryptionCompliance(name string) string {
	b.mu.RLock("EncryptionCompliance")
	defer b.mu.RUnlock()

	s, err := b.svc(name)
	if err != nil || s.Config.CMKEnforcement != cmkEnabled {
		return complianceOK
	}

	for _, ix := range s.Indexes {
		if ix.Def[fieldEncryption] == nil {
			return complianceNotOK
		}
	}

	for _, objs := range s.Objects {
		for _, o := range objs {
			if o[fieldEncryption] == nil {
				return complianceNotOK
			}
		}
	}

	return complianceOK
}
