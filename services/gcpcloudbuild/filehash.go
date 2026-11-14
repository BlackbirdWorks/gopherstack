package gcpcloudbuild

import (
	"crypto/md5" //nolint:gosec // MD5 is a documented Hash_HashType for source provenance, not a security control
	"crypto/sha256"
	"crypto/sha512"
	"slices"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
)

// hashSet computes the digests requested by options.source_provenance_hash.
func (r *runner) hashSet(data []byte) *cloudbuildpb.FileHashes {
	types := r.build.GetOptions().GetSourceProvenanceHash()
	if len(types) == 0 {
		return nil
	}

	fh := &cloudbuildpb.FileHashes{}

	for _, t := range slices.Compact(slices.Sorted(slices.Values(types))) {
		var sum []byte

		switch t {
		case cloudbuildpb.Hash_SHA256:
			s := sha256.Sum256(data)
			sum = s[:]
		case cloudbuildpb.Hash_MD5:
			s := md5.Sum(data) //nolint:gosec // see import
			sum = s[:]
		case cloudbuildpb.Hash_SHA512:
			s := sha512.Sum512(data)
			sum = s[:]
		default:
			continue
		}

		fh.FileHash = append(fh.FileHash, &cloudbuildpb.Hash{Type: t, Value: sum})
	}

	return fh
}
