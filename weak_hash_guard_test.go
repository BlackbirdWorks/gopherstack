package main

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
		// Verified this session: DocumentDescription declares a deprecated Sha1
		// member beside Hash/HashType (ssm@v1.73.4 types/enums.go:708-724), so an
		// emulator cannot populate it without computing SHA1. Content-integrity
		// checksum echoed on the wire, never a credential. gopherstack-ziv9.
		"services/ssm/document_hashes.go": "DocumentDescription.Sha1 parity, verified",

		// Azure Blob's Content-MD5/ETag are MD5 by specification (same as S3's
		// ETag, already allowlisted below) -- a content-integrity fingerprint
		// echoed on the wire so azure-sdk-for-go's blob client can validate
		// upload/download integrity, never a credential or security hash.
		"services/azureblob/store.go": "Content-MD5/ETag generation, Azure Blob wire-protocol requirement, verified",

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
