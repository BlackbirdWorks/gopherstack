package orcio

// DecodeIntsForTest exposes the integer RLE decoder to the external test package.
func DecodeIntsForTest(b []byte, n int, signed, v2 bool) ([]int64, error) {
	return decodeInts(b, n, signed, v2)
}

// DecodeByteRLEForTest exposes the byte RLE decoder.
func DecodeByteRLEForTest(b []byte, n int) ([]byte, error) { return decodeByteRLE(b, n) }
