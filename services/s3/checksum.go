package s3

import (
	"encoding/base64"
	"encoding/binary"
	"hash"
	"hash/crc64"
)

// ChecksumCRC64NVME is the algorithm name for CRC64/NVME checksums.
const ChecksumCRC64NVME = "CRC64NVME"

// crc64NVMEPoly is the CRC-64/NVME polynomial (Rocksoft^tm model) reflected
// (reversed) for use with standard little-endian (right-shifting) algorithms.
const crc64NVMEPoly = uint64(0x9a6c9329ac4bc9b5)

// crc64NVMETable is the lookup table for the CRC64/NVME polynomial.
var crc64NVMETable = crc64.MakeTable(crc64NVMEPoly) //nolint:gochecknoglobals // pre-computed lookup table

// NewCRC64NVME returns a new CRC64/NVME hash.
func NewCRC64NVME() hash.Hash {
	return crc64.New(crc64NVMETable)
}

// CalculateCRC64NVME computes the base64-encoded CRC64/NVME checksum of data.
func CalculateCRC64NVME(data []byte) string {
	h := NewCRC64NVME()
	_, _ = h.Write(data)
	sum := h.Sum(nil)

	return base64.StdEncoding.EncodeToString(sum)
}

// checksumBytesToB64 converts a checksum hash's Sum bytes to base64 string,
// handling big-endian conversion for 32-bit hashes.
func checksumBytesToB64(h hash.Hash) string {
	if h32, ok := h.(interface{ Sum32() uint32 }); ok {
		const size = 4
		b := make([]byte, size)
		binary.BigEndian.PutUint32(b, h32.Sum32())

		return base64.StdEncoding.EncodeToString(b)
	}

	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
