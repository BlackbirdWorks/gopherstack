package gcpstorage

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
)

const (
	keyCustomerKeySHA = "customerKeySha256"
	csekAlgorithm     = "AES256"
	csekKeyBytes      = 32
	hdrEncryptionPfx  = "X-Goog-Encryption-"
	hdrCopySourcePfx  = "X-Goog-Copy-Source-Encryption-"
)

type customerEncryptionJSON struct {
	EncryptionAlgorithm string `json:"encryptionAlgorithm"`
	KeySHA256           string `json:"keySha256"`
}

func (o *Object) customerEncryption() *customerEncryptionJSON {
	if o.CustomerKeySHA256 == "" {
		return nil
	}

	return &customerEncryptionJSON{EncryptionAlgorithm: csekAlgorithm, KeySHA256: o.CustomerKeySHA256}
}

// csekFromHeaders validates the customer-supplied key headers with the given prefix and returns the
// key's SHA-256 (the key itself is discarded); "" means no key was sent.
func csekFromHeaders(h http.Header, prefix string) (string, *APIError) {
	alg, key, sum := h.Get(prefix+"Algorithm"), h.Get(prefix+"Key"), h.Get(prefix+"Key-Sha256")
	if alg == "" && key == "" && sum == "" {
		return "", nil
	}

	raw, err := base64.StdEncoding.DecodeString(key)
	if alg != csekAlgorithm || err != nil || len(raw) != csekKeyBytes {
		return "", errInvalid("The provided customer-supplied encryption algorithm or key is invalid.")
	}

	digest := sha256.Sum256(raw)
	want := base64.StdEncoding.EncodeToString(digest[:])

	if subtle.ConstantTimeCompare([]byte(want), []byte(sum)) != 1 {
		return "", errInvalid("The provided customer-supplied encryption key hash does not match the key.")
	}

	return want, nil
}

// checkKey reports 400 when the supplied key hash does not match how o was stored.
func (o *Object) checkKey(sha string) *APIError {
	if subtle.ConstantTimeCompare([]byte(o.CustomerKeySHA256), []byte(sha)) == 1 {
		return nil
	}

	if o.CustomerKeySHA256 == "" {
		return errInvalid("The object is not encrypted with a customer-supplied key.")
	}

	return errInvalid("The object is encrypted with a customer-supplied key that was missing or does not match.")
}

// maskHashes returns o without its content hashes when it is key-encrypted and sha is not its key.
func (o *Object) maskHashes(sha string) *Object {
	if o.CustomerKeySHA256 == "" || o.checkKey(sha) == nil {
		return o
	}

	c := o.clone()
	c.CRC32C, c.MD5 = "", ""

	return c
}

// withUploadKey records the request's customer key hash in meta; a body-supplied value is dropped.
func withUploadKey(r *http.Request, meta Patch) (Patch, *APIError) {
	sha, apiErr := csekFromHeaders(r.Header, hdrEncryptionPfx)
	if apiErr != nil {
		return nil, apiErr
	}

	if meta == nil {
		meta = Patch{}
	}

	delete(meta, keyCustomerKeySHA)

	if sha != "" {
		meta[keyCustomerKeySHA] = marshalRaw(sha)
	}

	return meta, nil
}

// readableObject enforces the key for content reads (the key is optional for metadata-only reads, which
// then hide the hashes of an encrypted object).
func readableObject(r *http.Request, o *Object, content bool) (*Object, *APIError) {
	sha, apiErr := csekFromHeaders(r.Header, hdrEncryptionPfx)
	if apiErr != nil {
		return nil, apiErr
	}

	if content || sha != "" {
		if apiErr = o.checkKey(sha); apiErr != nil {
			return nil, apiErr
		}
	}

	return o.maskHashes(sha), nil
}

func setEncryptionHeaders(hdr http.Header, o *Object) {
	if o.CustomerKeySHA256 != "" {
		hdr.Set(hdrEncryptionPfx+"Algorithm", csekAlgorithm)
		hdr.Set(hdrEncryptionPfx+"Key-Sha256", o.CustomerKeySHA256)
	}
}
