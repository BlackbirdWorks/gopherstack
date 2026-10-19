package azurekeyvault

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"slices"
	"strings"
)

const (
	maxRandomBytes = 128

	opEncrypt   = "encrypt"
	opDecrypt   = "decrypt"
	opSign      = "sign"
	opVerify    = "verify"
	opWrapKey   = "wrapKey"
	opUnwrapKey = "unwrapKey"
)

// KeyOperationRequest is the body shared by every key crypto operation.
type KeyOperationRequest struct {
	Alg    string `json:"alg"`
	Value  string `json:"value"`
	Digest string `json:"digest"`
	IV     string `json:"iv"`
	AAD    string `json:"aad"`
	Tag    string `json:"tag"`
}

// KeyOperationResult is the encrypt/decrypt/sign/wrap/unwrap response.
type KeyOperationResult struct {
	KID   string `json:"kid"`
	Value string `json:"value"`
	IV    string `json:"iv,omitempty"`
	Tag   string `json:"authenticationTag,omitempty"`
	AAD   string `json:"aad,omitempty"`
}

// VerifyResult is the verify response.
type VerifyResult struct {
	Value bool `json:"value"`
}

func forbidden(op string) *kvError {
	return newErr(http.StatusForbidden, "Forbidden", "Operation %s is not allowed on this key.", op)
}

func (k *KeyVersion) permits(op string, now int64) error {
	if !k.Attrs.enabled() {
		return forbidden(op)
	}

	if (k.Attrs.NBF != nil && now < *k.Attrs.NBF) || (k.Attrs.Exp != nil && now >= *k.Attrs.Exp) {
		return forbidden(op)
	}

	if !slices.ContainsFunc(k.KeyOps, func(s string) bool { return strings.EqualFold(s, op) }) {
		return forbidden(op)
	}

	return nil
}

type decodedOp struct {
	value, digest, iv, aad, tag []byte
}

func (r *KeyOperationRequest) decode() (decodedOp, error) {
	var (
		d   decodedOp
		err error
	)

	for _, f := range []struct {
		dst *[]byte
		src string
	}{{&d.value, r.Value}, {&d.digest, r.Digest}, {&d.iv, r.IV}, {&d.aad, r.AAD}, {&d.tag, r.Tag}} {
		if f.src == "" {
			continue
		}

		if *f.dst, err = unb64u(f.src); err != nil {
			return d, err
		}
	}

	return d, nil
}

func isEncryptionAlg(alg string) bool {
	return strings.HasPrefix(alg, "RSA") || strings.HasPrefix(alg, "A1") ||
		strings.HasPrefix(alg, "A2")
}

// KeyOperation runs one crypto operation against a key version.
func (b *InMemoryBackend) KeyOperation(
	vaultName, base, name, version, op string, req *KeyOperationRequest,
) (any, error) {
	var out any

	err := b.do(vaultName, func(v *vault, now int64) error {
		ver, err := v.findKey(name, version)
		if err != nil {
			return err
		}

		if err = ver.permits(op, now); err != nil {
			return err
		}

		in, err := req.decode()
		if err != nil {
			return err
		}

		out, err = ver.run(base, name, op, req.Alg, in)

		return err
	})

	return out, err
}

func (k *KeyVersion) run(base, name, op, alg string, in decodedOp) (any, error) {
	priv, err := k.private()
	if err != nil {
		return nil, err
	}

	kid := base + "/keys/" + name + "/" + k.ID

	switch op {
	case opSign:
		sig, sErr := signDigest(priv, alg, in.value)
		if sErr != nil {
			return nil, sErr
		}

		return KeyOperationResult{KID: kid, Value: b64u(sig)}, nil
	case opVerify:
		ok, vErr := verifyDigest(publicOf(priv), alg, in.digest, in.value)

		return VerifyResult{Value: ok}, vErr
	}

	res, err := k.cipherOp(priv, op, alg, in)
	if err != nil {
		return nil, err
	}

	out := KeyOperationResult{KID: kid, Value: b64u(res.Value)}
	if len(res.IV) > 0 {
		out.IV = b64u(res.IV)
	}

	if len(res.Tag) > 0 {
		out.Tag = b64u(res.Tag)
	}

	return out, nil
}

func publicOf(priv any) any {
	switch p := priv.(type) {
	case *rsa.PrivateKey:
		return &p.PublicKey
	case *ecdsa.PrivateKey:
		return &p.PublicKey
	}

	return nil
}

func (k *KeyVersion) cipherOp(priv any, op, alg string, in decodedOp) (symResult, error) {
	encrypting := op == opEncrypt || op == opWrapKey

	switch p := priv.(type) {
	case *rsa.PrivateKey:
		var (
			out []byte
			err error
		)

		if encrypting {
			out, err = rsaEncrypt(&p.PublicKey, alg, in.value)
		} else {
			out, err = rsaDecrypt(p, alg, in.value)
		}

		return symResult{Value: out}, err
	case []byte:
		if !isEncryptionAlg(alg) {
			return symResult{}, errBadParameter("Algorithm %q is not supported for symmetric keys.", alg)
		}

		if encrypting {
			return symEncrypt(p, alg, in.value, in.iv, in.aad)
		}

		out, err := symDecrypt(p, alg, in.value, in.iv, in.aad, in.tag)

		return symResult{Value: out}, err
	}

	return symResult{}, errBadParameter("Key type %q does not support %s.", k.Kty, op)
}

// RandomBytes returns count cryptographically random bytes, base64url encoded.
func RandomBytes(count int) (string, error) {
	if count < 1 || count > maxRandomBytes {
		return "", errBadParameter("count must be between 1 and %d.", maxRandomBytes)
	}

	buf := make([]byte, count)
	_, _ = rand.Read(buf)

	return b64u(buf), nil
}
