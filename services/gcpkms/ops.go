package gcpkms

import (
	"crypto/hmac"

	"cloud.google.com/go/kms/apiv1/kmspb"
)

const (
	minRandomBytes = 8
	maxRandomBytes = 1024
)

func checkSizes(plaintext, aad []byte) error {
	if len(plaintext) > maxPlaintext {
		return errInvalid("The plaintext must be at most %d bytes.", maxPlaintext)
	}

	if len(aad) > maxAAD {
		return errInvalid("additional_authenticated_data must be at most %d bytes.", maxAAD)
	}

	return nil
}

// Encrypt encrypts with the primary version (or the named version) of an ENCRYPT_DECRYPT key.
func (b *InMemoryBackend) Encrypt(req *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error) {
	if err := checkSizes(req.GetPlaintext(), req.GetAdditionalAuthenticatedData()); err != nil {
		return nil, err
	}

	vp, err := verifyCRC(req.GetPlaintextCrc32C(), req.GetPlaintext(), "plaintext_crc32c")
	if err != nil {
		return nil, err
	}

	va, err := verifyCRC(req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData(),
		"additional_authenticated_data_crc32c")
	if err != nil {
		return nil, err
	}

	t, err := b.keyOrVersionTarget(req.GetName(), "Encrypt", kmspb.CryptoKey_ENCRYPT_DECRYPT)
	if err != nil {
		return nil, err
	}

	ct, err := sealGoogle(t.material, t.id, req.GetPlaintext(), req.GetAdditionalAuthenticatedData())
	if err != nil {
		return nil, err
	}

	return &kmspb.EncryptResponse{
		Name: t.name, Ciphertext: ct, CiphertextCrc32C: crcValue(ct),
		VerifiedPlaintextCrc32C: vp, VerifiedAdditionalAuthenticatedDataCrc32C: va, ProtectionLevel: t.level,
	}, nil
}

// Decrypt decrypts a ciphertext produced by Encrypt, using the version embedded in it.
func (b *InMemoryBackend) Decrypt(req *kmspb.DecryptRequest) (*kmspb.DecryptResponse, error) {
	if _, err := verifyCRC(req.GetCiphertextCrc32C(), req.GetCiphertext(), "ciphertext_crc32c"); err != nil {
		return nil, err
	}

	if _, err := verifyCRC(req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData(),
		"additional_authenticated_data_crc32c"); err != nil {
		return nil, err
	}

	t, err := b.decryptTarget(req.GetName(), req.GetCiphertext())
	if err != nil {
		return nil, err
	}

	pt, err := openGoogle(t.material, req.GetCiphertext(), req.GetAdditionalAuthenticatedData())
	if err != nil {
		return nil, err
	}

	return &kmspb.DecryptResponse{
		Plaintext: pt, PlaintextCrc32C: crcValue(pt), UsedPrimary: t.isPrimary, ProtectionLevel: t.level,
	}, nil
}

// RawEncrypt encrypts with a RAW_ENCRYPT_DECRYPT version using the version's AES mode.
func (b *InMemoryBackend) RawEncrypt(req *kmspb.RawEncryptRequest) (*kmspb.RawEncryptResponse, error) {
	if err := checkSizes(req.GetPlaintext(), req.GetAdditionalAuthenticatedData()); err != nil {
		return nil, err
	}

	vp, err := verifyCRC(req.GetPlaintextCrc32C(), req.GetPlaintext(), "plaintext_crc32c")
	if err != nil {
		return nil, err
	}

	va, err := verifyCRC(req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData(),
		"additional_authenticated_data_crc32c")
	if err != nil {
		return nil, err
	}

	vi, err := verifyCRC(req.GetInitializationVectorCrc32C(), req.GetInitializationVector(),
		"initialization_vector_crc32c")
	if err != nil {
		return nil, err
	}

	t, err := b.versionTarget(req.GetName(), "RawEncrypt", kmspb.CryptoKey_RAW_ENCRYPT_DECRYPT)
	if err != nil {
		return nil, err
	}

	sealed, err := rawSeal(t.spec, t.material, req.GetPlaintext(), req.GetAdditionalAuthenticatedData(),
		req.GetInitializationVector())
	if err != nil {
		return nil, err
	}

	return &kmspb.RawEncryptResponse{
		Ciphertext: sealed.ct, InitializationVector: sealed.iv, TagLength: sealed.tagLen,
		CiphertextCrc32C: crcValue(sealed.ct), InitializationVectorCrc32C: crcValue(sealed.iv),
		VerifiedPlaintextCrc32C: vp, VerifiedAdditionalAuthenticatedDataCrc32C: va,
		VerifiedInitializationVectorCrc32C: vi, Name: t.name, ProtectionLevel: t.level,
	}, nil
}

// RawDecrypt reverses RawEncrypt.
func (b *InMemoryBackend) RawDecrypt(req *kmspb.RawDecryptRequest) (*kmspb.RawDecryptResponse, error) {
	vc, err := verifyCRC(req.GetCiphertextCrc32C(), req.GetCiphertext(), "ciphertext_crc32c")
	if err != nil {
		return nil, err
	}

	va, err := verifyCRC(req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData(),
		"additional_authenticated_data_crc32c")
	if err != nil {
		return nil, err
	}

	vi, err := verifyCRC(req.GetInitializationVectorCrc32C(), req.GetInitializationVector(),
		"initialization_vector_crc32c")
	if err != nil {
		return nil, err
	}

	t, err := b.versionTarget(req.GetName(), "RawDecrypt", kmspb.CryptoKey_RAW_ENCRYPT_DECRYPT)
	if err != nil {
		return nil, err
	}

	pt, err := rawOpen(t.spec, t.material, req.GetCiphertext(), req.GetAdditionalAuthenticatedData(),
		req.GetInitializationVector(), int(req.GetTagLength()))
	if err != nil {
		return nil, err
	}

	return &kmspb.RawDecryptResponse{
		Plaintext: pt, PlaintextCrc32C: crcValue(pt), ProtectionLevel: t.level, VerifiedCiphertextCrc32C: vc,
		VerifiedAdditionalAuthenticatedDataCrc32C: va, VerifiedInitializationVectorCrc32C: vi,
	}, nil
}

// AsymmetricSign signs a digest (or data) with an ASYMMETRIC_SIGN version.
func (b *InMemoryBackend) AsymmetricSign(req *kmspb.AsymmetricSignRequest) (*kmspb.AsymmetricSignResponse, error) {
	vd, err := verifyCRC(req.GetDigestCrc32C(), digestBytes(req.GetDigest()), "digest_crc32c")
	if err != nil {
		return nil, err
	}

	vdata, err := verifyCRC(req.GetDataCrc32C(), req.GetData(), "data_crc32c")
	if err != nil {
		return nil, err
	}

	t, err := b.versionTarget(req.GetName(), "AsymmetricSign", kmspb.CryptoKey_ASYMMETRIC_SIGN)
	if err != nil {
		return nil, err
	}

	sig, err := signAsymmetric(t.spec, t.material, req)
	if err != nil {
		return nil, err
	}

	return &kmspb.AsymmetricSignResponse{
		Signature: sig, SignatureCrc32C: crcValue(sig), VerifiedDigestCrc32C: vd, VerifiedDataCrc32C: vdata,
		Name: t.name, ProtectionLevel: t.level,
	}, nil
}

func digestBytes(d *kmspb.Digest) []byte {
	_, sum := digestOf(d)

	return sum
}

// AsymmetricDecrypt decrypts RSA-OAEP ciphertext with an ASYMMETRIC_DECRYPT version.
func (b *InMemoryBackend) AsymmetricDecrypt(
	req *kmspb.AsymmetricDecryptRequest,
) (*kmspb.AsymmetricDecryptResponse, error) {
	vc, err := verifyCRC(req.GetCiphertextCrc32C(), req.GetCiphertext(), "ciphertext_crc32c")
	if err != nil {
		return nil, err
	}

	t, err := b.versionTarget(req.GetName(), "AsymmetricDecrypt", kmspb.CryptoKey_ASYMMETRIC_DECRYPT)
	if err != nil {
		return nil, err
	}

	pt, err := decryptAsymmetric(t.spec, t.material, req.GetCiphertext())
	if err != nil {
		return nil, err
	}

	return &kmspb.AsymmetricDecryptResponse{
		Plaintext: pt, PlaintextCrc32C: crcValue(pt), VerifiedCiphertextCrc32C: vc, ProtectionLevel: t.level,
	}, nil
}

// GetPublicKey returns the public half of an asymmetric version.
func (b *InMemoryBackend) GetPublicKey(req *kmspb.GetPublicKeyRequest) (*kmspb.PublicKey, error) {
	if f := req.GetPublicKeyFormat(); f != kmspb.PublicKey_PUBLIC_KEY_FORMAT_UNSPECIFIED &&
		f != kmspb.PublicKey_PEM && f != kmspb.PublicKey_DER {
		return nil, errInvalid("Public key format %s is not supported by this emulator.", f)
	}

	b.mu.Lock("GetPublicKey")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, v, err := b.findVersionLocked(req.GetName())
	if err != nil {
		return nil, err
	}

	if p := k.pb.GetPurpose(); p != kmspb.CryptoKey_ASYMMETRIC_SIGN && p != kmspb.CryptoKey_ASYMMETRIC_DECRYPT {
		return nil, purposeError("GetPublicKey", p, kmspb.CryptoKey_ASYMMETRIC_SIGN)
	}

	if v.pb.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		return nil, errPrecondition("CryptoKeyVersion %s is not enabled, current state is: %s.",
			req.GetName(), v.pb.GetState())
	}

	der, err := publicKeyDER(v.material)
	if err != nil {
		return nil, err
	}

	out := &kmspb.PublicKey{
		Algorithm: v.pb.GetAlgorithm(), Name: req.GetName(), ProtectionLevel: v.pb.GetProtectionLevel(),
		PublicKeyFormat: kmspb.PublicKey_PEM,
	}

	if req.GetPublicKeyFormat() == kmspb.PublicKey_DER {
		out.PublicKeyFormat = kmspb.PublicKey_DER
		out.PublicKey = &kmspb.ChecksummedData{Data: der, Crc32CChecksum: crcValue(der)}

		return out, nil
	}

	out.Pem = pemOf(der)
	out.PemCrc32C = crcValue([]byte(out.GetPem()))

	return out, nil
}

// MacSign computes an HMAC with a MAC version.
func (b *InMemoryBackend) MacSign(req *kmspb.MacSignRequest) (*kmspb.MacSignResponse, error) {
	vd, err := verifyCRC(req.GetDataCrc32C(), req.GetData(), "data_crc32c")
	if err != nil {
		return nil, err
	}

	t, err := b.versionTarget(req.GetName(), "MacSign", kmspb.CryptoKey_MAC)
	if err != nil {
		return nil, err
	}

	mac := hmacOf(t, req.GetData())

	return &kmspb.MacSignResponse{
		Name: t.name, Mac: mac, MacCrc32C: crcValue(mac), VerifiedDataCrc32C: vd, ProtectionLevel: t.level,
	}, nil
}

func hmacOf(t target, data []byte) []byte {
	h := hmac.New(t.spec.hash.New, t.material)
	h.Write(data)

	return h.Sum(nil)
}

// MacVerify checks an HMAC in constant time.
func (b *InMemoryBackend) MacVerify(req *kmspb.MacVerifyRequest) (*kmspb.MacVerifyResponse, error) {
	vd, err := verifyCRC(req.GetDataCrc32C(), req.GetData(), "data_crc32c")
	if err != nil {
		return nil, err
	}

	vm, err := verifyCRC(req.GetMacCrc32C(), req.GetMac(), "mac_crc32c")
	if err != nil {
		return nil, err
	}

	t, err := b.versionTarget(req.GetName(), "MacVerify", kmspb.CryptoKey_MAC)
	if err != nil {
		return nil, err
	}

	return &kmspb.MacVerifyResponse{
		Name: t.name, Success: hmac.Equal(hmacOf(t, req.GetData()), req.GetMac()), VerifiedDataCrc32C: vd,
		VerifiedMacCrc32C: vm, VerifiedSuccessIntegrity: true, ProtectionLevel: t.level,
	}, nil
}

// GenerateRandomBytes returns cryptographically secure random bytes for a location.
func (b *InMemoryBackend) GenerateRandomBytes(
	req *kmspb.GenerateRandomBytesRequest,
) (*kmspb.GenerateRandomBytesResponse, error) {
	if err := parseResource(req.GetLocation(), depthLocation); err != nil {
		return nil, err
	}

	if n := req.GetLengthBytes(); n < minRandomBytes || n > maxRandomBytes {
		return nil, errInvalid("length_bytes must be between %d and %d.", minRandomBytes, maxRandomBytes)
	}

	if l := req.GetProtectionLevel(); l != kmspb.ProtectionLevel_PROTECTION_LEVEL_UNSPECIFIED &&
		l != kmspb.ProtectionLevel_SOFTWARE && l != kmspb.ProtectionLevel_HSM {
		return nil, errInvalid("Protection level %s is not supported by this emulator.", l)
	}

	data, err := randomBytes(int(req.GetLengthBytes()))
	if err != nil {
		return nil, err
	}

	return &kmspb.GenerateRandomBytesResponse{Data: data, DataCrc32C: crcValue(data)}, nil
}
