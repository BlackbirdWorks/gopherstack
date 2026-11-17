package ocicontainerinstances

import (
	"regexp"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

var (
	adRe = regexp.MustCompile(`^[A-Za-z0-9]{4}:[A-Z0-9-]+-AD-[1-9]$`)
	fdRe = regexp.MustCompile(`^FAULT-DOMAIN-[1-3]$`)
)

// SetTagLookup wires defined-tag validation against a tag namespace source (OCI Identity); nil accepts any namespace.
func (b *InMemoryBackend) SetTagLookup(fn ocitags.Lookup) {
	b.mu.Lock("SetTagLookup")
	b.tagLookup = fn
	b.mu.Unlock()
}

func (b *InMemoryBackend) checkTags(free map[string]string, defined map[string]any) *APIError {
	conv := make(map[string]map[string]any, len(defined))

	for ns, v := range defined {
		m, ok := v.(map[string]any)
		if !ok {
			return errParam("definedTags." + ns + " must be an object of tag keys")
		}

		conv[ns] = m
	}

	if err := ocitags.Validate(free, conv, b.tagLookup); err != nil {
		return &APIError{Status: err.Status, Code: err.Code, Message: err.Message}
	}

	return nil
}

func checkPlacement(ad, fd string) *APIError {
	if !adRe.MatchString(ad) {
		return errParam("availabilityDomain " + ad + " is not a valid availability domain")
	}

	if fd != "" && !fdRe.MatchString(fd) {
		return errParam("faultDomain " + fd + " is not a valid fault domain")
	}

	return nil
}
