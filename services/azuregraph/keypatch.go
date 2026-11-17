package azuregraph

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const maxKeyCredentials = 100

// splitKeyPatch removes keyCredentials from a PATCH body so the rest can go through applyBody.
func splitKeyPatch(body map[string]jsontext.Value) (map[string]jsontext.Value, jsontext.Value, bool) {
	raw, ok := body["keyCredentials"]
	if !ok {
		return body, nil, false
	}

	rest := maps.Clone(body)
	delete(rest, "keyCredentials")

	return rest, raw, true
}

// patchKeyCredentials replaces a holder's keyCredentials with the PATCHed collection, as Learn 'Update application'
// does. An entry naming an existing keyId without a key keeps that credential; one with a key is validated like
// addKey and stored. Entries left out are removed, together with their key material. The caller holds b.mu.
func (b *Backend) patchKeyCredentials(cur []KeyCredential, raw jsontext.Value) ([]KeyCredential, error) {
	var in []KeyCredential
	if string(raw) == jsonNull || json.Unmarshal(raw, &in, jsonx.V1()) != nil {
		return nil, badRequest("Invalid value specified for property 'keyCredentials' of resource 'Application'.")
	}

	if len(in) > maxKeyCredentials {
		return nil, badRequest("A holder can have at most %d key credentials.", maxKeyCredentials)
	}

	out := make([]KeyCredential, 0, len(in))
	material := map[string]string{}
	seen := map[string]bool{}

	for _, k := range in {
		cred, key, err := b.patchedKey(cur, k)
		if err != nil {
			return nil, err
		}

		if seen[lc(cred.KeyID)] {
			return nil, badRequest("Duplicate keyId '%s' in keyCredentials.", cred.KeyID)
		}

		seen[lc(cred.KeyID)] = true
		out = append(out, cred)

		if key != "" {
			material[cred.KeyID] = key
		}
	}

	for _, old := range cur {
		if !seen[lc(old.KeyID)] {
			delete(b.keyMaterial, old.KeyID)
		}
	}

	maps.Copy(b.keyMaterial, material)

	return out, nil
}

func (b *Backend) patchedKey(cur []KeyCredential, k KeyCredential) (KeyCredential, string, error) {
	if k.KeyID != "" && !isGUID(k.KeyID) {
		return KeyCredential{}, "", b.keyErr("keyId")
	}

	i := slices.IndexFunc(
		cur,
		func(c KeyCredential) bool { return k.KeyID != "" && strings.EqualFold(c.KeyID, k.KeyID) },
	)

	if k.Key == nil {
		if i < 0 {
			return KeyCredential{}, "", badRequest(
				"A value is required for property 'key' of resource 'KeyCredential'.",
			)
		}

		kept := cur[i]
		if k.DisplayName != nil {
			kept.DisplayName = k.DisplayName
		}

		return kept, "", nil
	}

	if i < 0 && k.KeyID != "" && b.keyIDTaken(k.KeyID) {
		return KeyCredential{}, "", badRequest("The keyId '%s' is already used by another credential.", k.KeyID)
	}

	cred, err := b.newKeyCredential(k)
	if err != nil {
		return KeyCredential{}, "", err
	}

	if k.KeyID != "" {
		cred.KeyID = lc(k.KeyID)
	}

	if cred.Type == typeCertPFX {
		return cred, "", nil
	}

	return cred, *k.Key, nil
}

// keyIDTaken reports whether any application, service principal or soft-deleted holder already uses keyID; key
// material is keyed by it, so a reused ID would replace another holder's certificate.
func (b *Backend) keyIDTaken(keyID string) bool {
	id := lc(keyID)
	if _, ok := b.keyMaterial[id]; ok {
		return true
	}

	has := func(keys []KeyCredential) bool {
		return slices.ContainsFunc(keys, func(k KeyCredential) bool { return lc(k.KeyID) == id })
	}

	for _, a := range b.apps.all() {
		if has(a.KeyCredentials) {
			return true
		}
	}

	for _, sp := range b.sps.all() {
		if has(sp.KeyCredentials) {
			return true
		}
	}

	for _, d := range b.deleted.all() {
		if _, ok := d.KeyMaterial[id]; ok || d.Application != nil && has(d.Application.KeyCredentials) ||
			d.ServicePrincipal != nil && has(d.ServicePrincipal.KeyCredentials) {
			return true
		}
	}

	return false
}
