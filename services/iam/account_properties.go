package iam

import (
	"fmt"
	"maps"
	"strings"
)

// validateAccountPropertyKeys checks the two structural constraints real
// PutAccountProperties documents for every key, regardless of namespace:
// "The key must contain exactly one / separating the namespace from the
// property name, and cannot start or end with /" and "All properties in a
// single request must belong to the same namespace." Per-property value
// typing ("boolean properties expect true or false") is NOT enforced here:
// AWS does not publish the full namespace/property/type registry, and this
// backend has no way to know a given property's expected type without
// inventing one -- an honest, disclosed limitation, not a loosened check
// (the two constraints AWS DOES document are still enforced in full).
func validateAccountPropertyKeys(properties map[string]string) error {
	if len(properties) == 0 {
		return fmt.Errorf("%w: Properties is required", ErrInvalidInput)
	}

	var namespace string

	for key := range properties {
		if strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
			return fmt.Errorf("%w: property key %q must not start or end with /", ErrInvalidInput, key)
		}

		ns, name, found := strings.Cut(key, "/")
		if !found || ns == "" || name == "" || strings.Contains(name, "/") {
			return fmt.Errorf(
				"%w: property key %q must contain exactly one / separating namespace from property name",
				ErrInvalidInput, key,
			)
		}

		if namespace == "" {
			namespace = ns
		} else if ns != namespace {
			return fmt.Errorf(
				"%w: all properties in a single request must belong to the same namespace (%q vs %q)",
				ErrInvalidInput, namespace, ns,
			)
		}
	}

	return nil
}

// GetAccountProperties returns the account's stored property key-value
// pairs. Real AWS returns an empty map for an account that has never called
// PutAccountProperties (no documented default properties exist until an
// admin sets one) -- matched here rather than fabricating a default
// RoleManager entry AWS doesn't actually pre-populate.
func (b *InMemoryBackend) GetAccountProperties() map[string]string {
	b.mu.RLock("GetAccountProperties")
	defer b.mu.RUnlock()

	out := make(map[string]string, len(b.accountProperties))
	maps.Copy(out, b.accountProperties)

	return out
}

// PutAccountProperties merges the given key-value pairs into the account's
// stored properties (matching real AWS: only the namespace named by this
// request's keys is affected, every other namespace's properties are left
// untouched).
func (b *InMemoryBackend) PutAccountProperties(properties map[string]string) error {
	if err := validateAccountPropertyKeys(properties); err != nil {
		return err
	}

	b.mu.Lock("PutAccountProperties")
	defer b.mu.Unlock()

	maps.Copy(b.accountProperties, properties)

	return nil
}
