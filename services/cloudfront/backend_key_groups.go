package cloudfront

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
)

// validatePEMPublicKey parses encodedKey as a PEM-encoded public key and verifies
// that RSA keys are at least minPublicKeyBits bits.
func validatePEMPublicKey(encodedKey string) error {
	block, _ := pem.Decode([]byte(encodedKey))
	if block == nil {
		return fmt.Errorf("%w: EncodedKey must be a valid PEM-encoded public key", ErrValidation)
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("%w: EncodedKey PEM parse failed: %w", ErrValidation, err)
	}

	const bitsPerByte = 8
	// Only check bit-length for RSA keys; EC keys are accepted unconditionally.
	if rsaPub, ok := pub.(interface{ Size() int }); ok {
		bits := rsaPub.Size() * bitsPerByte
		if bits < minPublicKeyBits {
			return fmt.Errorf(
				"%w: RSA key must be at least %d bits, got %d",
				ErrValidation, minPublicKeyBits, bits,
			)
		}
	}

	return nil
}

// CreatePublicKey creates a new CloudFront Public Key.
func (b *InMemoryBackend) CreatePublicKey(
	callerRef, name, comment, encodedKey string,
) (*PublicKey, error) {
	if encodedKey != "" {
		if err := validatePEMPublicKey(encodedKey); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("CreatePublicKey")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.publicKeyByName[name]; exists {
		return nil, fmt.Errorf("%w: public key with name %q already exists", ErrPublicKeyAlreadyExists, name)
	}

	id := generateID()
	pk := &PublicKey{
		ID:              id,
		Name:            name,
		Comment:         comment,
		EncodedKey:      encodedKey,
		CallerReference: callerRef,
		ETag:            uuid.NewString(),
	}
	b.publicKeys.Put(pk)
	b.publicKeyByName[name] = id
	cp := *pk

	return &cp, nil
}

// GetPublicKey returns a CloudFront Public Key by ID.
func (b *InMemoryBackend) GetPublicKey(id string) (*PublicKey, error) {
	b.mu.RLock("GetPublicKey")
	defer b.mu.RUnlock()

	pk, ok := b.publicKeys.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: public key %s not found", ErrPublicKeyNotFound, id)
	}

	cp := *pk

	return &cp, nil
}

// ListPublicKeys returns all public keys sorted by ID.
func (b *InMemoryBackend) ListPublicKeys() []*PublicKey {
	b.mu.RLock("ListPublicKeys")
	defer b.mu.RUnlock()

	list := make([]*PublicKey, 0, b.publicKeys.Len())
	for _, pk := range b.publicKeys.All() {
		cp := *pk
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdatePublicKey updates an existing Public Key comment.
func (b *InMemoryBackend) UpdatePublicKey(id, comment string) (*PublicKey, error) {
	b.mu.Lock("UpdatePublicKey")
	defer b.mu.Unlock()

	pk, ok := b.publicKeys.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: public key %s not found", ErrPublicKeyNotFound, id)
	}

	pk.Comment = comment
	pk.ETag = uuid.NewString()
	cp := *pk

	return &cp, nil
}

// publicKeyReferencedBy reports the kind and ID of the first resource that
// references the given public key, or ("", "") if none. Must be called with the
// lock held.
func (b *InMemoryBackend) publicKeyReferencedBy(pkID string) (string, string) {
	for _, kg := range b.keyGroups.All() {
		if slices.Contains(kg.Items, pkID) {
			return "key group", kg.ID
		}
	}

	for _, p := range b.fieldLevelEncryptionProfiles.All() {
		for _, e := range p.EncryptionEntities {
			if e.PublicKeyID == pkID {
				return "field level encryption profile", p.ID
			}
		}
	}

	return "", ""
}

// DeletePublicKey deletes a Public Key by ID. It returns ErrPublicKeyInUse when
// the key is still referenced by a key group or an FLE profile.
func (b *InMemoryBackend) DeletePublicKey(id string) error {
	b.mu.Lock("DeletePublicKey")
	defer b.mu.Unlock()

	pk, ok := b.publicKeys.Get(id)
	if !ok {
		return fmt.Errorf("%w: public key %s not found", ErrPublicKeyNotFound, id)
	}

	if kind, refID := b.publicKeyReferencedBy(id); kind != "" {
		return fmt.Errorf(
			"%w: public key %s is referenced by %s %s",
			ErrPublicKeyInUse,
			id,
			kind,
			refID,
		)
	}

	delete(b.publicKeyByName, pk.Name)
	b.publicKeys.Delete(id)

	return nil
}

// --- Key Group CRUD ---

// CreateKeyGroup creates a new CloudFront Key Group.
func (b *InMemoryBackend) CreateKeyGroup(name, comment string, items []string) (*KeyGroup, error) {
	b.mu.Lock("CreateKeyGroup")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.keyGroupByName[name]; exists {
		return nil, fmt.Errorf("%w: key group with name %q already exists", ErrKeyGroupAlreadyExists, name)
	}

	for _, itemID := range items {
		if _, ok := b.publicKeys.Get(itemID); !ok {
			return nil, fmt.Errorf("%w: public key %s not found", ErrPublicKeyNotFound, itemID)
		}
	}

	id := generateID()
	kg := &KeyGroup{
		ID:      id,
		Name:    name,
		Comment: comment,
		Items:   append([]string(nil), items...),
		ETag:    uuid.NewString(),
	}
	b.keyGroups.Put(kg)
	b.keyGroupByName[name] = id

	return b.copyKeyGroup(kg), nil
}

// GetKeyGroup returns a CloudFront Key Group by ID.
func (b *InMemoryBackend) GetKeyGroup(id string) (*KeyGroup, error) {
	b.mu.RLock("GetKeyGroup")
	defer b.mu.RUnlock()

	kg, ok := b.keyGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: key group %s not found", ErrKeyGroupNotFound, id)
	}

	return b.copyKeyGroup(kg), nil
}

// ListKeyGroups returns all key groups sorted by ID.
func (b *InMemoryBackend) ListKeyGroups() []*KeyGroup {
	b.mu.RLock("ListKeyGroups")
	defer b.mu.RUnlock()

	list := make([]*KeyGroup, 0, b.keyGroups.Len())
	for _, kg := range b.keyGroups.All() {
		list = append(list, b.copyKeyGroup(kg))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateKeyGroup updates an existing Key Group.
func (b *InMemoryBackend) UpdateKeyGroup(
	id, name, comment string,
	items []string,
) (*KeyGroup, error) {
	b.mu.Lock("UpdateKeyGroup")
	defer b.mu.Unlock()

	kg, ok := b.keyGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: key group %s not found", ErrKeyGroupNotFound, id)
	}

	if name != kg.Name {
		if _, exists := b.keyGroupByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: key group with name %q already exists",
				ErrKeyGroupAlreadyExists,
				name,
			)
		}

		delete(b.keyGroupByName, kg.Name)
		b.keyGroupByName[name] = id
	}

	for _, itemID := range items {
		if _, exists := b.publicKeys.Get(itemID); !exists {
			return nil, fmt.Errorf("%w: public key %s not found", ErrPublicKeyNotFound, itemID)
		}
	}

	kg.Name = name
	kg.Comment = comment
	kg.Items = append([]string(nil), items...)
	kg.ETag = uuid.NewString()

	return b.copyKeyGroup(kg), nil
}

// DeleteKeyGroup deletes a Key Group by ID.
func (b *InMemoryBackend) DeleteKeyGroup(id string) error {
	b.mu.Lock("DeleteKeyGroup")
	defer b.mu.Unlock()

	kg, ok := b.keyGroups.Get(id)
	if !ok {
		return fmt.Errorf("%w: key group %s not found", ErrKeyGroupNotFound, id)
	}

	delete(b.keyGroupByName, kg.Name)
	b.keyGroups.Delete(id)

	return nil
}

func (b *InMemoryBackend) copyKeyGroup(kg *KeyGroup) *KeyGroup {
	cp := *kg
	cp.Items = append([]string(nil), kg.Items...)

	return &cp
}

// --- Realtime Log Config CRUD ---
