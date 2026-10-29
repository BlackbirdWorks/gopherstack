package guards_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// allowedWeakHashFiles lists the only files permitted to import a
// cryptographically weak hash. Each entry needs a reason that survives review.
//
// services/ssm/document_hashes.go: DocumentDescription declares a deprecated
// Sha1 member beside Hash and HashType (aws-sdk-go-v2/service/ssm@v1.73.4
// types/enums.go:708-724), so an emulator cannot populate it without computing
// SHA1. The value is a content-integrity checksum echoed on the wire, never a
// credential. See gopherstack-ziv9.
func allowedWeakHashFiles() map[string]string {
	return map[string]string{
		// Azure Blob Content-MD5 headers are the base64 MD5 of the body; integrity checksum, never a credential.
		"services/azureblob/handler_cond.go": "Azure Blob Content-MD5 header parity",
		// npm dist.shasum is the SHA-1 of the tarball; content checksum, never a credential.
		"services/gcpartifactregistry/npm.go": "npm dist.shasum SHA-1 parity",
		// Maven .sha1/.md5 checksum files and twine md5_digest are defined over those digests; never credentials.
		"services/gcpartifactregistry/maven.go":  "Maven repository SHA-1/MD5 checksum parity",
		"services/gcpartifactregistry/python.go": "twine md5_digest upload field verification",
		// Verified this session: DocumentDescription declares a deprecated Sha1
		// member beside Hash/HashType (ssm@v1.73.4 types/enums.go:708-724), so an
		// emulator cannot populate it without computing SHA1. Content-integrity
		// checksum echoed on the wire, never a credential. gopherstack-ziv9.
		// GCS objects carry an md5Hash field and X-Goog-Hash md5= value defined as the
		// base64 MD5 of the content; integrity checksum, never a credential.
		"services/gcpstorage/store.go": "GCS Object.md5Hash parity, verified",
		// OCI Object Storage md5/opc-multipart-md5 headers are base64 MD5 of the content, and API key
		// fingerprints are the MD5 of the DER public key; integrity/identifier values, never credentials.
		"services/ociobjectstorage/store.go": "OCI object md5 and multipart md5 parity",
		"pkgs/ociauth/ociauth.go":            "OCI API key fingerprint is MD5 of the DER public key",
		// Identity Platform batchCreate accepts MD5/SHA1/PBKDF_SHA1/HMAC_* password hashes
		// (UploadAccountRequest.hashAlgorithm); imported digests, never produced for new credentials.
		"services/gcpidentitytoolkit/hashes.go": "Identity Platform legacy password hash import parity",
		// OCI Vault Encrypt/Decrypt expose RSA_OAEP_SHA_1 as a caller-selectable encryption algorithm.
		"services/ocivault/seal.go": "OCI Vault RSA_OAEP_SHA_1 encryption algorithm parity",

		// Workflows hash.compute_checksum and hash.compute_hmac document MD5 and SHA1 among their algorithms;
		// a content hash chosen by the workflow author, never a credential.
		"pkgs/wfinterp/stdlib_text.go": "Workflows hash.compute_checksum MD5/SHA1 parity, verified",

		// GoogleSQL defines MD5() and SHA1() as those digests of the argument; a content hash, never a credential.
		"services/gcpbigquery/bqsql/udf_str.go": "GoogleSQL MD5()/SHA1() function parity, verified",

		// Cloud SQL addresses client certificates by sha1Fingerprint (sslCerts.get/delete path parameter);
		// a certificate identifier, never a credential.
		"services/gcpsql/certs.go": "Cloud SQL sslCert sha1Fingerprint parity, verified",

		// Memorystore TlsCertificate.sha1_fingerprint is defined as the SHA-1 of the CA certificate; a certificate
		// identifier, never a credential.
		"services/gcpredis/engine.go": "Memorystore TlsCertificate.sha1_fingerprint parity, verified",

		// CloudFront Functions' crypto module exposes createHash/createHmac with md5 and sha1;
		// user-script digests, never credential storage.
		"services/cloudfront/function_runtime.go": "CloudFront Functions crypto md5/sha1 parity, verified",

		// Key Vault: RSA-OAEP is defined over SHA-1 (JWA RFC 7518 4.3) and the
		// certificate x5t claim is a SHA-1 thumbprint (RFC 7515 4.1.7).
		"services/azurekeyvault/keycrypto.go":  "JWA RSA-OAEP (SHA-1) key-vault algorithm, verified",
		"services/azurekeyvault/certpolicy.go": "certificate x5t SHA-1 thumbprint, verified",

		// Azure Files Content-MD5 / x-ms-content-md5 headers are defined as the base64 MD5 of the
		// payload; integrity checksum, never a credential.
		"services/azurefile/ops_file.go": "Azure Files Content-MD5 header parity, verified",

		// XML-Enc rsa-oaep-mgf1p defaults to SHA-1 (W3C xmlenc-core1 5.4.2); the digest is OAEP
		// padding for an IdP-encrypted SAML assertion, never a credential hash.
		"services/cognitoidp/saml_decrypt.go": "XML-Enc rsa-oaep-mgf1p default SHA-1 digest, verified",

		"services/neptune/engine_probe.go": "RFC 6455 WebSocket Sec-WebSocket-Accept mandates SHA-1, verified",
		"services/ssm/document_hashes.go":  "DocumentDescription.Sha1 parity, verified",

		// Cloud KMS defines HMAC_SHA1, RSA_DECRYPT_OAEP_*_SHA1 and RSA_OAEP_*_SHA1_AES_256 import
		// (kmspb CryptoKeyVersionAlgorithm / ImportJob_ImportMethod); needs the sha1 registration.
		"services/gcpkms/algs.go": "Cloud KMS HMAC_SHA1 and OAEP SHA1 algorithm parity, verified",

		// Verified 2026-09-19: ChecksumAlgorithm SHA1 is one of the four
		// algorithms the S3 SDK computes over the request body
		// (service/internal/checksum@v1.11.2 algorithms.go), sent as
		// X-Amz-Checksum-Sha1; the emulator must recompute it to verify the
		// header. Content-integrity checksum, never a credential.
		"services/s3/checksum.go": "S3 ChecksumAlgorithm SHA1 verification, verified",

		// Verified 2026-10-03: the IoT SQL reference defines md5() and sha1()
		// as rule functions over payload data, never credentials.
		"services/iot/sql_funcs_message.go": "IoT SQL md5()/sha1() functions, verified",

		// Athena SQL md5()/sha1() are Presto/Trino built-ins, never credentials.
		"services/athena/engine_funcs_str.go": "Athena SQL md5()/sha1() functions, verified",

		// Verified 2026-10-05: kafkaconnect types.CustomPluginFileDescription.FileMd5
		// is defined as the hex-encoded MD5 checksum of the plugin file, so the
		// emulator must compute MD5 over the S3 object to report it. Integrity
		// checksum echoed on the wire, never a credential.
		"services/kafkaconnect/cross_service.go":   "MSK Connect FileMd5 parity, verified",
		"services/cloudfront/dataplane_signing.go": "CloudFront signed URLs and cookies are RSA-SHA1, verified",

		// Pre-existing at the time this guard was added, and NOT individually
		// audited. Each is presumed an AWS-protocol requirement -- S3 ETags are
		// MD5 by specification, TOTP is HMAC-SHA1 by RFC 6238, key-pair
		// fingerprints and Kinesis hash-key ranges are defined by AWS as MD5 --
		// but that is a presumption, not a verification. Seeded so the guard
		// catches NEW weak-hash uses rather than pretending the existing ones
		// were reviewed. Auditing them is worth its own pass.
		"pkgs/httputils/pool.go":                                  "pre-existing, unaudited",
		"services/codeconnections/repository_sync.go":             "pre-existing, unaudited",
		"services/cognitoidp/totp.go":                             "pre-existing, unaudited",
		"services/ec2/key_pairs.go":                               "pre-existing, unaudited",
		"services/kinesis/shards.go":                              "pre-existing, unaudited",
		"services/kinesisanalyticsv2/application_update_apply.go": "pre-existing, unaudited",
		"services/kms/crypto.go":                                  "pre-existing, unaudited",
		"services/s3/multipart.go":                                "pre-existing, unaudited",
		"services/s3/objects.go":                                  "pre-existing, unaudited",
		"services/s3/sse_crypto.go":                               "pre-existing, unaudited",
		"services/s3/utils.go":                                    "pre-existing, unaudited",
		"services/sns/signing.go":                                 "pre-existing, unaudited",
		"services/sqs/checksums.go":                               "pre-existing, unaudited",
		"services/stepfunctions/asl/intrinsics.go":                "pre-existing, unaudited",
		"services/sts/credentials.go":                             "pre-existing, unaudited",
		"services/sts/saml.go":                                    "pre-existing, unaudited",
	}
}

// TestNoUndeclaredWeakHashes replaces the CodeQL coverage that
// .github/codeql/codeql-config.yml switches off.
//
// CodeQL's go/weak-sensitive-data-hashing had to be excluded repo-wide: its
// paths-ignore does not apply to compiled languages under autobuild, so there
// is no way to scope the exception to one file in the config alone. Excluding
// the query without replacing it would go blind to a real weak-hashing bug
// introduced anywhere later, so this guard keeps the invariant in the repo
// where it is reviewable: crypto/sha1 and crypto/md5 may only be imported by
// a file on the allowlist above.
func TestNoUndeclaredWeakHashes(t *testing.T) {
	t.Parallel()

	allowed := allowedWeakHashFiles()

	weak := []string{`"crypto/sha1"`, `"crypto/md5"`}

	var offenders []string

	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() && (d.Name() == ".git" || d.Name() == "node_modules" || d.Name() == "ui") {
			return filepath.SkipDir
		}

		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		for _, w := range weak {
			if strings.Contains(string(src), w) {
				if _, ok := allowed[filepath.ToSlash(path)]; !ok {
					offenders = append(offenders, path+" imports "+w)
				}
			}
		}

		return nil
	})
	require.NoError(t, err)

	require.Empty(t, offenders,
		"weak hash imported outside the allowlist; add a reason to allowedWeakHashFiles() "+
			"or use a strong hash. CodeQL cannot catch this -- see the comment above.")
}
