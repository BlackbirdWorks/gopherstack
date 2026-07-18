package route53

import (
	"fmt"
	"time"
)

// kskKey builds the map key for a key signing key.
func kskKey(hostedZoneID, name string) string { return hostedZoneID + "|" + name }

// CreateKeySigningKey creates a new key signing key for a hosted zone.
func (b *InMemoryBackend) CreateKeySigningKey(
	hostedZoneID, _ /* callerRef */, name, kmsArn, status string,
) (*KeySigningKey, error) {
	if hostedZoneID == "" {
		return nil, fmt.Errorf("%w: hostedZoneId is required", ErrInvalidInput)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateKeySigningKey")
	defer b.mu.Unlock()

	if _, ok := b.zones.Get(hostedZoneID); !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, hostedZoneID)
	}

	if b.keySigningKeys.Has(kskKey(hostedZoneID, name)) {
		return nil, fmt.Errorf(
			"%w: key signing key %s already exists in zone %s",
			ErrKeySigningKeyAlreadyExists,
			name,
			hostedZoneID,
		)
	}

	if status == "" {
		status = kskStatusInactive
	}

	// Generate deterministic stub signing-algorithm fields per AWS spec (ECDSAP256SHA256).
	const (
		kskKeyTagLen         = 5
		kskKeyTagBase        = 10
		kskPublicKeyLen      = 88
		kskDigestLen         = 64
		kskAlgorithmType     = 13
		kskDigestAlgType     = 2
		kskDNSKEYFlag        = 257
		kskDSRecordAlgType   = 13
		kskDSRecordDigestAlg = 2
	)
	keyTagRaw := randomID("0123456789", kskKeyTagLen)
	keyTag := 0
	for _, ch := range keyTagRaw {
		keyTag = keyTag*kskKeyTagBase + int(ch-'0')
	}
	pubKey := randomID("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", kskPublicKeyLen)
	digest := randomID("0123456789abcdef", kskDigestLen)
	dsRecord := fmt.Sprintf("%d %d %d %s", keyTag, kskDSRecordAlgType, kskDSRecordDigestAlg, digest)

	ksk := &KeySigningKey{
		HostedZoneID:             hostedZoneID,
		Name:                     name,
		KeyManagementServiceArn:  kmsArn,
		Status:                   status,
		CreatedAt:                time.Now(),
		Flag:                     kskDNSKEYFlag,
		SigningAlgorithmMnemonic: "ECDSAP256SHA256",
		SigningAlgorithmType:     kskAlgorithmType,
		DigestAlgorithmMnemonic:  "SHA-256",
		DigestAlgorithmType:      kskDigestAlgType,
		KeyTag:                   keyTag,
		PublicKey:                pubKey,
		DigestValue:              digest,
		DSRecord:                 dsRecord,
	}

	b.keySigningKeys.Put(ksk)

	cp := *ksk

	return &cp, nil
}

// ActivateKeySigningKey activates an existing key signing key.
func (b *InMemoryBackend) ActivateKeySigningKey(hostedZoneID, name string) (*KeySigningKey, error) {
	b.mu.Lock("ActivateKeySigningKey")
	defer b.mu.Unlock()

	key := kskKey(hostedZoneID, name)

	ksk, ok := b.keySigningKeys.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: key signing key %s not found in zone %s",
			ErrKeySigningKeyNotFound,
			name,
			hostedZoneID,
		)
	}

	ksk.Status = kskStatusActive

	cp := *ksk

	return &cp, nil
}

// DeactivateKeySigningKey deactivates an existing key signing key.
func (b *InMemoryBackend) DeactivateKeySigningKey(
	hostedZoneID, name string,
) (*KeySigningKey, error) {
	b.mu.Lock("DeactivateKeySigningKey")
	defer b.mu.Unlock()

	key := kskKey(hostedZoneID, name)
	ksk, ok := b.keySigningKeys.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: key signing key %s not found in zone %s",
			ErrKeySigningKeyNotFound,
			name,
			hostedZoneID,
		)
	}

	ksk.Status = kskStatusInactive
	cp := *ksk

	return &cp, nil
}

// DeleteKeySigningKey deletes a key signing key.
// The KSK must be INACTIVE; deleting an ACTIVE KSK returns ErrInvalidKeySigningKeyStatus
// (AWS: InvalidKeySigningKeyStatus).
func (b *InMemoryBackend) DeleteKeySigningKey(hostedZoneID, name string) error {
	b.mu.Lock("DeleteKeySigningKey")
	defer b.mu.Unlock()

	key := kskKey(hostedZoneID, name)
	ksk, ok := b.keySigningKeys.Get(key)
	if !ok {
		return fmt.Errorf(
			"%w: key signing key %s not found in zone %s",
			ErrKeySigningKeyNotFound,
			name,
			hostedZoneID,
		)
	}

	if ksk.Status == kskStatusActive {
		return fmt.Errorf(
			"%w: key signing key %s must be INACTIVE before deletion",
			ErrInvalidKeySigningKeyStatus,
			name,
		)
	}

	b.keySigningKeys.Delete(key)

	return nil
}

// AddKeySigningKeyInternal adds a KSK directly into the backend for testing.
func (b *InMemoryBackend) AddKeySigningKeyInternal(ksk KeySigningKey) {
	b.mu.Lock("AddKeySigningKeyInternal")
	defer b.mu.Unlock()
	cp := ksk
	b.keySigningKeys.Put(&cp)
}
