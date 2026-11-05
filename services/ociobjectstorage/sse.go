package ociobjectstorage

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"strings"
)

const (
	sseAlgorithm     = "AES256"
	sseKeyBytes      = 32
	keyStateEnabled  = "ENABLED"
	hdrSSEAlgorithm  = "opc-sse-customer-algorithm"
	hdrSSEKey        = "opc-sse-customer-key"
	hdrSSEKeySha     = "opc-sse-customer-key-sha256"
	hdrSrcSSEAlg     = "opc-source-sse-customer-algorithm"
	hdrSrcSSEKey     = "opc-source-sse-customer-key"
	hdrSrcSSEKeySha  = "opc-source-sse-customer-key-sha256"
	hdrSSEKmsKeyID   = "opc-sse-kms-key-id"
	codeSSEKeyNeeded = "InvalidSseCustomerKey"
)

// SSEKey is a customer-supplied encryption key as sent on the wire (sseCustomerKey / opc-sse-customer-* headers).
type SSEKey struct {
	Algorithm string `json:"algorithm"`
	Key       string `json:"key"`
	KeySha256 string `json:"keySha256"`
}

func (k SSEKey) present() bool { return k.Algorithm != "" || k.Key != "" || k.KeySha256 != "" }

// hash validates the key triple and returns the base64 SHA-256 of the key, the only thing that is stored.
func (k SSEKey) hash() (string, error) {
	if !strings.EqualFold(k.Algorithm, sseAlgorithm) {
		return "", errParam("opc-sse-customer-algorithm must be AES256")
	}

	raw, err := base64.StdEncoding.DecodeString(k.Key)
	if err != nil || len(raw) != sseKeyBytes {
		return "", errParam("opc-sse-customer-key must be a base64-encoded 256-bit key")
	}

	sum := sha256.Sum256(raw)
	want := base64.StdEncoding.EncodeToString(sum[:])

	if subtle.ConstantTimeCompare([]byte(want), []byte(k.KeySha256)) != 1 {
		return "", errParam("opc-sse-customer-key-sha256 does not match the SHA-256 of opc-sse-customer-key")
	}

	return want, nil
}

// SSEReq is the encryption selection of a write: an SSE-C key or a Vault key OCID.
type SSEReq struct {
	KmsKeyID string
	Key      SSEKey
}

// resolve validates the request and returns the SSE-C key hash ("" when none).
func (s SSEReq) resolve(checkKey func(string) error) (string, error) {
	var hash string

	if s.Key.present() {
		h, err := s.Key.hash()
		if err != nil {
			return "", err
		}

		hash = h
	}

	if s.KmsKeyID != "" {
		if hash != "" {
			return "", errParam("opc-sse-kms-key-id cannot be combined with opc-sse-customer-key")
		}

		if err := checkKey(s.KmsKeyID); err != nil {
			return "", err
		}
	}

	return hash, nil
}

// requireKey verifies the key presented for reading an object stored under keyHash.
func requireKey(keyHash string, k SSEKey) error {
	if keyHash == "" {
		return nil
	}

	if !k.present() {
		return newErr(http.StatusBadRequest, codeSSEKeyNeeded,
			"The object is encrypted with a customer-provided key; send the opc-sse-customer-* headers")
	}

	got, err := k.hash()
	if err != nil {
		return err
	}

	if subtle.ConstantTimeCompare([]byte(got), []byte(keyHash)) != 1 {
		return newErr(http.StatusBadRequest, codeSSEKeyNeeded,
			"The customer-provided key does not match the key the object was encrypted with")
	}

	return nil
}

// SetKeyChecker installs the Vault key lookup (OCID to lifecycle state) used to validate KMS key ids.
func (b *InMemoryBackend) SetKeyChecker(fn func(keyID string) (string, bool)) {
	b.mu.Lock("SetKeyChecker")
	b.keyState = fn
	b.mu.Unlock()
}

// checkKMSKey must be called with the lock held.
func (b *InMemoryBackend) checkKMSKey(id string) error {
	if b.keyState == nil {
		return nil
	}

	state, ok := b.keyState(id)
	if !ok {
		return newErr(http.StatusNotFound, "NotAuthorizedOrNotFound", "The Vault key '"+id+"' could not be found")
	}

	if state != keyStateEnabled {
		return errParam("The Vault key '" + id + "' is in state " + state + ", not ENABLED")
	}

	return nil
}

func sseFromHeaders(h http.Header) SSEReq {
	return SSEReq{
		KmsKeyID: h.Get(hdrSSEKmsKeyID),
		Key: SSEKey{
			Algorithm: h.Get(hdrSSEAlgorithm), Key: h.Get(hdrSSEKey), KeySha256: h.Get(hdrSSEKeySha),
		},
	}
}

func srcKeyFromHeaders(h http.Header) SSEKey {
	return SSEKey{Algorithm: h.Get(hdrSrcSSEAlg), Key: h.Get(hdrSrcSSEKey), KeySha256: h.Get(hdrSrcSSEKeySha)}
}

func readKeyFromHeaders(h http.Header) SSEKey { return sseFromHeaders(h).Key }

// ReencryptInput is the ReencryptObjectDetails payload.
type ReencryptInput struct {
	SSECustomerKey       *SSEKey `json:"sseCustomerKey"`
	SourceSSECustomerKey *SSEKey `json:"sourceSseCustomerKey"`
	KmsKeyID             string  `json:"kmsKeyId"`
}

// ReencryptObject re-wraps an object's data key under a Vault key, an SSE-C key or the bucket key.
func (b *InMemoryBackend) ReencryptObject(ns, bucket, name, versionID string, in ReencryptInput) error {
	b.mu.Lock("ReencryptObject")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return err
	}

	if err = writable(bk); err != nil {
		return err
	}

	o := pick(bk, name, versionID)
	if o == nil {
		return errObject(name)
	}

	var src SSEKey
	if in.SourceSSECustomerKey != nil {
		src = *in.SourceSSECustomerKey
	}

	if err = requireKey(o.SseKeySha256, src); err != nil {
		return err
	}

	req := SSEReq{KmsKeyID: in.KmsKeyID}
	if in.SSECustomerKey != nil {
		req.Key = *in.SSECustomerKey
	}

	hash, err := req.resolve(b.checkKMSKey)
	if err != nil {
		return err
	}

	o.SseKeySha256 = hash
	o.KmsKeyID = ""

	if hash == "" {
		o.KmsKeyID = in.KmsKeyID
		if o.KmsKeyID == "" {
			o.KmsKeyID = bk.KmsKeyID
		}
	}

	return nil
}

// ReencryptBucket re-encrypts every object not under an SSE-C key with the bucket's current key
// and returns a completed REENCRYPT work request id.
func (b *InMemoryBackend) ReencryptBucket(ns, bucket string) (string, error) {
	b.mu.Lock("ReencryptBucket")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return "", err
	}

	for _, vs := range bk.Objects {
		for _, o := range vs {
			if o.SseKeySha256 == "" && !o.IsDeleteMarker {
				o.KmsKeyID = bk.KmsKeyID
			}
		}
	}

	now := b.nowUTC()
	wr := &WorkRequest{
		ID: newWorkRequestID(), OperationType: "REENCRYPT", Status: wrCompleted, CompartmentID: bk.CompartmentID,
		TimeAccepted: now, TimeFinished: now, EntityURI: "/n/" + ns + "/b/" + bucket,
	}
	b.workRequests[wr.ID] = wr

	return wr.ID, nil
}
