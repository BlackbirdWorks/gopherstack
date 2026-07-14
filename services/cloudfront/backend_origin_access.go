package cloudfront

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// oaiS3CanonicalUserID returns the AWS-style 64-char hex S3 canonical user ID for an OAI.
// AWS derives this deterministically per OAI; we hash the OAI ID for a stable value.
func oaiS3CanonicalUserID(id string) string {
	sum := sha256.Sum256([]byte("oai-canonical:" + id))

	return hex.EncodeToString(sum[:])
}

// oaiARN builds an ARN for an Origin Access Identity.
func (b *InMemoryBackend) oaiARN(id string) string {
	return arn.Build(
		"cloudfront",
		"",
		b.accountID,
		fmt.Sprintf("origin-access-identity/cloudfront/%s", id),
	)
}

// CreateOAI creates a new Origin Access Identity.
// If an OAI with the same CallerReference already exists, it is returned without
// creating a duplicate (idempotent).
func (b *InMemoryBackend) CreateOAI(callerRef, comment string) (*OriginAccessIdentity, error) {
	b.mu.Lock("CreateCloudFrontOriginAccessIdentity")
	defer b.mu.Unlock()

	if callerRef == "" {
		return nil, fmt.Errorf("%w: CallerReference must not be empty", ErrValidation)
	}

	// Idempotency: return existing OAI for the same CallerReference.
	if existingID, ok := b.oaiCallerRefs[callerRef]; ok {
		existing, _ := b.oais.Get(existingID)
		cp := *existing

		return &cp, nil
	}

	id := generateID()
	oai := &OriginAccessIdentity{
		ID:                id,
		ARN:               b.oaiARN(id),
		S3CanonicalUserID: oaiS3CanonicalUserID(id),
		ETag:              uuid.NewString(),
		CallerReference:   callerRef,
		Comment:           comment,
	}
	b.oais.Put(oai)
	b.oaiCallerRefs[callerRef] = id
	cp := *oai

	return &cp, nil
}

// GetOAI returns an OAI by ID.
func (b *InMemoryBackend) GetOAI(id string) (*OriginAccessIdentity, error) {
	b.mu.RLock("GetCloudFrontOriginAccessIdentity")
	defer b.mu.RUnlock()

	oai, ok := b.oais.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}
	cp := *oai

	return &cp, nil
}

// DeleteOAI deletes an OAI by ID.
func (b *InMemoryBackend) DeleteOAI(id string) error {
	b.mu.Lock("DeleteCloudFrontOriginAccessIdentity")
	defer b.mu.Unlock()

	oai, ok := b.oais.Get(id)
	if !ok {
		return fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}

	delete(b.oaiCallerRefs, oai.CallerReference)
	b.oais.Delete(id)

	return nil
}

// UpdateOAI updates an existing Origin Access Identity's comment and rotates its ETag.
func (b *InMemoryBackend) UpdateOAI(id, comment string) (*OriginAccessIdentity, error) {
	b.mu.Lock("UpdateCloudFrontOriginAccessIdentity")
	defer b.mu.Unlock()

	oai, ok := b.oais.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}

	oai.Comment = comment
	oai.ETag = uuid.NewString()

	cp := *oai

	return &cp, nil
}

// ListOAIs returns all OAIs sorted by ID.
func (b *InMemoryBackend) ListOAIs() []*OriginAccessIdentity {
	b.mu.RLock("ListCloudFrontOriginAccessIdentities")
	defer b.mu.RUnlock()

	list := make([]*OriginAccessIdentity, 0, b.oais.Len())
	for _, oai := range b.oais.All() {
		cp := *oai
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// CreateOriginAccessControl creates a new Origin Access Control.
func (b *InMemoryBackend) CreateOriginAccessControl(
	name, description, originType, signingBehavior, signingProtocol string,
) (*OriginAccessControl, error) {
	b.mu.Lock("CreateOriginAccessControl")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.originAccessControlByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: origin access control with name %q already exists",
			ErrOriginAccessControlAlreadyExists,
			name,
		)
	}

	id := generateID()
	oac := &OriginAccessControl{
		ID:              id,
		Name:            name,
		Description:     description,
		OriginType:      originType,
		SigningBehavior: signingBehavior,
		SigningProtocol: signingProtocol,
		ETag:            uuid.NewString(),
	}
	b.originAccessControls.Put(oac)
	b.originAccessControlByName[name] = id
	cp := *oac

	return &cp, nil
}

// GetOriginAccessControl returns an OAC by ID.
func (b *InMemoryBackend) GetOriginAccessControl(id string) (*OriginAccessControl, error) {
	b.mu.RLock("GetOriginAccessControl")
	defer b.mu.RUnlock()

	oac, ok := b.originAccessControls.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: origin access control %s not found", ErrOACNotFound, id)
	}

	cp := *oac

	return &cp, nil
}

// ListOriginAccessControls returns all OACs sorted by ID.
func (b *InMemoryBackend) ListOriginAccessControls() []*OriginAccessControl {
	b.mu.RLock("ListOriginAccessControls")
	defer b.mu.RUnlock()

	list := make([]*OriginAccessControl, 0, b.originAccessControls.Len())
	for _, oac := range b.originAccessControls.All() {
		cp := *oac
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateOriginAccessControl updates an existing OAC.
func (b *InMemoryBackend) UpdateOriginAccessControl(
	id, name, description, originType, signingBehavior, signingProtocol string,
) (*OriginAccessControl, error) {
	b.mu.Lock("UpdateOriginAccessControl")
	defer b.mu.Unlock()

	oac, ok := b.originAccessControls.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: origin access control %s not found", ErrOACNotFound, id)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if name != oac.Name {
		if _, exists := b.originAccessControlByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: origin access control with name %q already exists",
				ErrOriginAccessControlAlreadyExists,
				name,
			)
		}

		delete(b.originAccessControlByName, oac.Name)
		b.originAccessControlByName[name] = id
	}

	oac.Name = name
	oac.Description = description
	oac.OriginType = originType
	oac.SigningBehavior = signingBehavior
	oac.SigningProtocol = signingProtocol
	oac.ETag = uuid.NewString()
	cp := *oac

	return &cp, nil
}

// DeleteOriginAccessControl deletes an OAC by ID.
func (b *InMemoryBackend) DeleteOriginAccessControl(id string) error {
	b.mu.Lock("DeleteOriginAccessControl")
	defer b.mu.Unlock()

	oac, ok := b.originAccessControls.Get(id)
	if !ok {
		return fmt.Errorf("%w: origin access control %s not found", ErrOACNotFound, id)
	}

	delete(b.originAccessControlByName, oac.Name)
	b.originAccessControls.Delete(id)

	return nil
}

// --- Response Headers Policy CRUD ---
