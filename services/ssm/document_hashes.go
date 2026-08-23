package ssm

import (
	"crypto/sha1" //nolint:gosec // Sha1 is a real DocumentDescription member kept for backward compatibility, not a security boundary
	"crypto/sha256"
	"encoding/hex"
)

const documentHashTypeSha256 = "Sha256"

// documentHashes computes DocumentDescription's Hash/HashType/Sha1 members
// from a document's content. Real AWS computes Hash as the SHA256 (default
// HashType) of the document content, and separately populates the deprecated
// Sha1 field for backward compatibility (aws-sdk-go-v2/service/ssm@v1.73.4
// types/enums.go:708-724, DocumentHashType Sha256|Sha1).
//
// This lives in its own file so .github/codeql/codeql-config.yml can exclude
// exactly these lines from analysis rather than all 982 of documents.go. See
// gopherstack-ziv9.
func documentHashes(content string) (string, string) {
	sum256 := sha256.Sum256([]byte(content))
	sum1 := sha1.Sum([]byte(content)) //nolint:gosec // content-integrity hash, not a security boundary

	return hex.EncodeToString(sum256[:]), hex.EncodeToString(sum1[:])
}
