package ociartifacts

import (
	"encoding/base64"
	"slices"
	"strings"
)

const idSuffixLen = 10

type createSignatureIn struct {
	Tags
	CompartmentID    string `json:"compartmentId"`
	ImageID          string `json:"imageId"`
	KmsKeyID         string `json:"kmsKeyId"`
	KmsKeyVersionID  string `json:"kmsKeyVersionId"`
	Message          string `json:"message"`
	Signature        string `json:"signature"`
	SigningAlgorithm string `json:"signingAlgorithm"`
}

//nolint:gochecknoglobals // immutable enum
var signingAlgorithms = []string{
	"SHA_224_RSA_PKCS_PSS", "SHA_256_RSA_PKCS_PSS", "SHA_384_RSA_PKCS_PSS", "SHA_512_RSA_PKCS_PSS",
}

func lastN(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[len(s)-n:]
}

func (s *Signature) displayName() string {
	return lastN(s.KmsKeyID, idSuffixLen) + "::" + lastN(s.KmsKeyVersionID, idSuffixLen) + "::" + s.Algorithm + "::" +
		lastN(s.ID, idSuffixLen)
}

func (s *Signature) view() signatureWire {
	return signatureWire{
		TimeCreated: s.TimeCreated, CompartmentID: s.CompartmentID, CreatedBy: s.CreatedBy,
		DisplayName: s.displayName(), ID: s.ID, ImageID: s.ImageID, KmsKeyID: s.KmsKeyID,
		KmsKeyVersionID: s.KmsKeyVersionID, Message: s.Message, Signature: s.Signature,
		SigningAlgorithm: s.Algorithm, LifecycleState: stateAvailable, FreeformTags: s.Tags.free(),
		DefinedTags: s.Tags.defined(), SystemTags: map[string]map[string]any{},
	}
}

func validateSignature(in createSignatureIn) *APIError {
	for name, v := range map[string]string{
		"compartmentId": in.CompartmentID, "imageId": in.ImageID, "kmsKeyId": in.KmsKeyID,
		"kmsKeyVersionId": in.KmsKeyVersionID, "message": in.Message, "signature": in.Signature,
		"signingAlgorithm": in.SigningAlgorithm,
	} {
		if v == "" {
			return errMissing(name + " is required")
		}
	}

	alg := strings.ToUpper(in.SigningAlgorithm)
	if alg != in.SigningAlgorithm || !slices.Contains(signingAlgorithms, alg) {
		return errParam("signingAlgorithm must be one of " + strings.Join(signingAlgorithms, ", "))
	}

	for name, v := range map[string]string{"message": in.Message, "signature": in.Signature} {
		if _, err := base64.StdEncoding.DecodeString(v); err != nil {
			return errParam(name + " must be base64 encoded")
		}
	}

	return nil
}

// CreateContainerImageSignature stores a signature for an image.
func (b *InMemoryBackend) CreateContainerImageSignature(
	in createSignatureIn, createdBy string,
) (signatureWire, string, error) {
	if err := validateSignature(in); err != nil {
		return signatureWire{}, "", err
	}

	b.mu.Lock("CreateContainerImageSignature")
	defer b.mu.Unlock()

	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return signatureWire{}, "", err
	}

	if _, _, err := b.availableImage(in.ImageID); err != nil {
		return signatureWire{}, "", err
	}

	for _, s := range b.sigs {
		if s.ImageID == in.ImageID && s.KmsKeyID == in.KmsKeyID && s.KmsKeyVersionID == in.KmsKeyVersionID &&
			s.Algorithm == in.SigningAlgorithm && s.Signature == in.Signature && s.Message == in.Message {
			return signatureWire{}, "", errConflict("The image already has an identical signature")
		}
	}

	s := &Signature{
		ID: newOCID("containerimagesignature"), ImageID: in.ImageID, CompartmentID: in.CompartmentID,
		CreatedBy: createdBy, KmsKeyID: in.KmsKeyID, KmsKeyVersionID: in.KmsKeyVersionID, Message: in.Message,
		Signature: in.Signature, Algorithm: in.SigningAlgorithm, TimeCreated: b.nowUTC(), Etag: newEtag(),
		Seq: b.nextSeq(), Tags: in.Tags,
	}
	b.sigs[s.ID] = s

	return s.view(), s.Etag, nil
}

func (b *InMemoryBackend) signature(id string) (*Signature, *APIError) {
	s, ok := b.sigs[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return s, nil
}

// GetContainerImageSignature returns a signature and its etag.
func (b *InMemoryBackend) GetContainerImageSignature(id string) (signatureWire, string, error) {
	b.mu.RLock("GetContainerImageSignature")
	defer b.mu.RUnlock()

	s, err := b.signature(id)
	if err != nil {
		return signatureWire{}, "", err
	}

	return s.view(), s.Etag, nil
}

type signatureFilter struct {
	compartment string
	imageID     string
	repoID      string
	repoName    string
	digest      string
	name        string
	kmsKeyID    string
	kmsVersion  string
	algorithm   string
}

func (f signatureFilter) match(b *InMemoryBackend, s *Signature, o ListOpts) bool {
	return b.inCompartment(s.CompartmentID, f.compartment, o.Subtree) && f.matchOwn(s) && f.matchImage(b, s)
}

func (f signatureFilter) matchOwn(s *Signature) bool {
	return (f.imageID == "" || f.imageID == s.ImageID) && (f.kmsKeyID == "" || f.kmsKeyID == s.KmsKeyID) &&
		(f.kmsVersion == "" || f.kmsVersion == s.KmsKeyVersionID) &&
		(f.algorithm == "" || f.algorithm == s.Algorithm) && (f.name == "" || f.name == s.displayName())
}

func (f signatureFilter) matchImage(b *InMemoryBackend, s *Signature) bool {
	if f.repoID == "" && f.repoName == "" && f.digest == "" {
		return true
	}

	im, ok := b.images[s.ImageID]
	if !ok {
		return false
	}

	repo := b.repos[im.RepoID]

	return repo != nil && (f.repoID == "" || f.repoID == repo.ID) && (f.digest == "" || f.digest == im.Digest) &&
		(f.repoName == "" || matchWild(f.repoName, repo.Name))
}

// ListContainerImageSignatures lists the signatures of a compartment.
func (b *InMemoryBackend) ListContainerImageSignatures(
	f signatureFilter, o ListOpts,
) (signatureCollectionWire, string, error) {
	if f.compartment == "" {
		return signatureCollectionWire{}, "", errMissing("compartmentId is required")
	}

	if f.algorithm != "" && !slices.Contains(signingAlgorithms, f.algorithm) {
		return signatureCollectionWire{}, "", errParam("signingAlgorithm is not a supported value")
	}

	b.mu.RLock("ListContainerImageSignatures")
	defer b.mu.RUnlock()

	var all []*Signature

	for _, s := range b.sigs {
		if f.match(b, s, o) {
			all = append(all, s)
		}
	}

	pg, remaining, err := paginate(all, o, func(s *Signature) sortKey {
		return sortKey{created: s.TimeCreated, name: s.displayName(), seq: s.Seq}
	})
	if err != nil {
		return signatureCollectionWire{}, "", err
	}

	out := signatureCollectionWire{Items: make([]signatureWire, 0, len(pg.Data)), RemainingItemsCount: remaining}
	for _, s := range pg.Data {
		out.Items = append(out.Items, s.view())
	}

	return out, pg.Next, nil
}

// UpdateContainerImageSignature changes a signature's tags.
func (b *InMemoryBackend) UpdateContainerImageSignature(
	id, ifMatch string, in updateTagsIn,
) (signatureWire, string, error) {
	b.mu.Lock("UpdateContainerImageSignature")
	defer b.mu.Unlock()

	s, err := b.signature(id)
	if err != nil {
		return signatureWire{}, "", err
	}

	if err = checkEtag(s.Etag, ifMatch); err != nil {
		return signatureWire{}, "", err
	}

	s.Tags.merge(in.Tags)
	s.Etag = newEtag()

	return s.view(), s.Etag, nil
}

// DeleteContainerImageSignature deletes a signature.
func (b *InMemoryBackend) DeleteContainerImageSignature(id, ifMatch string) error {
	b.mu.Lock("DeleteContainerImageSignature")
	defer b.mu.Unlock()

	s, err := b.signature(id)
	if err != nil {
		return err
	}

	if err = checkEtag(s.Etag, ifMatch); err != nil {
		return err
	}

	delete(b.sigs, id)

	return nil
}
