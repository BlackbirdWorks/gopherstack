package iot

// md2Sum implements RFC 1319 (not in the Go standard library); IoT SQL exposes md2().
func md2Sum(msg []byte) [md2Size]byte {
	s := md2Table()

	pad := md2Block - len(msg)%md2Block
	buf := make([]byte, 0, len(msg)+pad+md2Block)
	buf = append(buf, msg...)

	for range pad {
		buf = append(buf, byte(pad))
	}

	var checksum [md2Block]byte

	last := byte(0)

	for i := 0; i < len(buf); i += md2Block {
		for j := range md2Block {
			checksum[j] ^= s[buf[i+j]^last]
			last = checksum[j]
		}
	}

	buf = append(buf, checksum[:]...)

	var x [3 * md2Block]byte

	for i := 0; i < len(buf); i += md2Block {
		for j := range md2Block {
			x[md2Block+j] = buf[i+j]
			x[2*md2Block+j] = x[md2Block+j] ^ x[j]
		}

		t := byte(0)

		for round := range md2Rounds {
			for k := range x {
				x[k] ^= s[t]
				t = x[k]
			}

			t += byte(round)
		}
	}

	var out [md2Size]byte

	copy(out[:], x[:md2Size])

	return out
}

const (
	md2Size   = 16
	md2Block  = 16
	md2Rounds = 18
)

func md2Table() *[256]byte {
	return &[256]byte{
		41, 46, 67, 201, 162, 216, 124, 1, 61, 54, 84, 161, 236, 240, 6, 19,
		98, 167, 5, 243, 192, 199, 115, 140, 152, 147, 43, 217, 188, 76, 130, 202,
		30, 155, 87, 60, 253, 212, 224, 22, 103, 66, 111, 24, 138, 23, 229, 18,
		190, 78, 196, 214, 218, 158, 222, 73, 160, 251, 245, 142, 187, 47, 238, 122,
		169, 104, 121, 145, 21, 178, 7, 63, 148, 194, 16, 137, 11, 34, 95, 33,
		128, 127, 93, 154, 90, 144, 50, 39, 53, 62, 204, 231, 191, 247, 151, 3,
		255, 25, 48, 179, 72, 165, 181, 209, 215, 94, 146, 42, 172, 86, 170, 198,
		79, 184, 56, 210, 150, 164, 125, 182, 118, 252, 107, 226, 156, 116, 4, 241,
		69, 157, 112, 89, 100, 113, 135, 32, 134, 91, 207, 101, 230, 45, 168, 2,
		27, 96, 37, 173, 174, 176, 185, 246, 28, 70, 97, 105, 52, 64, 126, 15,
		85, 71, 163, 35, 221, 81, 175, 58, 195, 92, 249, 206, 186, 197, 234, 38,
		44, 83, 13, 110, 133, 40, 132, 9, 211, 223, 205, 244, 65, 129, 77, 82,
		106, 220, 55, 200, 108, 193, 171, 250, 36, 225, 123, 8, 12, 189, 177, 74,
		120, 136, 149, 139, 227, 99, 232, 109, 233, 203, 213, 254, 59, 0, 29, 57,
		242, 239, 183, 14, 102, 88, 208, 228, 166, 119, 114, 248, 235, 117, 75, 10,
		49, 68, 80, 180, 143, 237, 31, 26, 219, 153, 141, 51, 159, 17, 131, 20,
	}
}
