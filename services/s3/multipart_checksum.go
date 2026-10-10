package s3

import (
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// resolveChecksumType returns the multipart checksum type for algo: the requested
// one, else FULL_OBJECT for CRC64NVME and COMPOSITE for the other algorithms.
func resolveChecksumType(algo types.ChecksumAlgorithm, requested types.ChecksumType) types.ChecksumType {
	switch {
	case algo == "":
		return ""
	case requested != "":
		return requested
	case string(algo) == ChecksumCRC64NVME:
		return types.ChecksumTypeFullObject
	default:
		return types.ChecksumTypeComposite
	}
}

func partChecksumValue(p StoredObjectPart, algo string) string {
	var v *string

	switch algo {
	case ChecksumCRC32:
		v = p.ChecksumCRC32
	case ChecksumCRC32C:
		v = p.ChecksumCRC32C
	case ChecksumCRC64NVME:
		v = p.ChecksumCRC64NVME
	case ChecksumSHA1:
		v = p.ChecksumSHA1
	case ChecksumSHA256:
		v = p.ChecksumSHA256
	case ChecksumMD5:
		v = p.ChecksumMD5
	case ChecksumSHA512:
		v = p.ChecksumSHA512
	}

	return aws.ToString(v)
}

// multipartObjectChecksum computes the object-level checksum of a completed
// multipart upload: FULL_OBJECT hashes the whole body, COMPOSITE hashes the
// concatenated per-part digests and appends "-<part count>".
func multipartObjectChecksum(
	algo types.ChecksumAlgorithm,
	ctype types.ChecksumType,
	chunks [][]byte,
	parts []StoredObjectPart,
) string {
	name := string(algo)

	if ctype == types.ChecksumTypeFullObject {
		h, ok := newHasherForAlgo(name)
		if !ok {
			return ""
		}

		for _, c := range chunks {
			_, _ = h.Write(c)
		}

		return checksumBytesToB64(h)
	}

	outer, ok := newHasherForAlgo(name)
	if !ok {
		return ""
	}

	for i, c := range chunks {
		v := partChecksumValue(parts[i], name)
		if v == "" {
			v = checksumOfBytes(name, c)
		}

		raw, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return ""
		}

		_, _ = outer.Write(raw)
	}

	return checksumBytesToB64(outer) + "-" + strconv.Itoa(len(chunks))
}

func checksumOfBytes(algo string, data []byte) string {
	h, ok := newHasherForAlgo(algo)
	if !ok {
		return ""
	}

	_, _ = h.Write(data)

	return checksumBytesToB64(h)
}

// setVersionChecksum stores value on the checksum field matching algo.
func setVersionChecksum(v *StoredObjectVersion, algo, value string) {
	cs := aws.String(value)

	switch algo {
	case ChecksumCRC32:
		v.ChecksumCRC32 = cs
	case ChecksumCRC32C:
		v.ChecksumCRC32C = cs
	case ChecksumCRC64NVME:
		v.ChecksumCRC64NVME = cs
	case ChecksumSHA1:
		v.ChecksumSHA1 = cs
	case ChecksumSHA256:
		v.ChecksumSHA256 = cs
	case ChecksumMD5:
		v.ChecksumMD5 = cs
	case ChecksumSHA512:
		v.ChecksumSHA512 = cs
	}
}

// verifyCompleteChecksum rejects a CompleteMultipartUpload whose declared
// checksum type or object-level checksum differs from the computed one.
func verifyCompleteChecksum(
	input *s3.CompleteMultipartUploadInput,
	algo types.ChecksumAlgorithm,
	ctype types.ChecksumType,
	computed string,
) error {
	if input.ChecksumType != "" && input.ChecksumType != ctype {
		return ErrBadChecksum
	}

	supplied := map[string]*string{
		ChecksumCRC32:     input.ChecksumCRC32,
		ChecksumCRC32C:    input.ChecksumCRC32C,
		ChecksumCRC64NVME: input.ChecksumCRC64NVME,
		ChecksumSHA1:      input.ChecksumSHA1,
		ChecksumSHA256:    input.ChecksumSHA256,
		ChecksumMD5:       input.ChecksumMD5,
		ChecksumSHA512:    input.ChecksumSHA512,
	}

	for name, v := range supplied {
		if v == nil || *v == "" {
			continue
		}

		if !strings.EqualFold(name, string(algo)) || *v != computed {
			return ErrBadChecksum
		}
	}

	return nil
}

func setCompleteOutputChecksum(out *s3.CompleteMultipartUploadOutput, algo, value string) {
	cs := aws.String(value)

	switch algo {
	case ChecksumCRC32:
		out.ChecksumCRC32 = cs
	case ChecksumCRC32C:
		out.ChecksumCRC32C = cs
	case ChecksumCRC64NVME:
		out.ChecksumCRC64NVME = cs
	case ChecksumSHA1:
		out.ChecksumSHA1 = cs
	case ChecksumSHA256:
		out.ChecksumSHA256 = cs
	case ChecksumMD5:
		out.ChecksumMD5 = cs
	case ChecksumSHA512:
		out.ChecksumSHA512 = cs
	}
}

func (a multipartAssemblyResult) applyChecksumTo(v *StoredObjectVersion) {
	if a.checksum == "" {
		return
	}

	v.ChecksumAlgorithm = a.checksumAlgo
	v.ChecksumType = a.checksumType
	setVersionChecksum(v, string(a.checksumAlgo), a.checksum)
}
