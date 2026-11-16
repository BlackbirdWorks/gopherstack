package bqsql

import "testing"

func TestFarmFingerprint64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want uint64
	}{
		{"", 0x9ae16a3b2f90404f},
		{"1footrue", uint64(0xea9af1c357a4ccc9)},
		{
			"Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley",
			0x5a05644eb66e435e,
		},
		{"It's a tiny change to the code and not completely disgusting. - Bob Manchek", 0x38aa3175b37f305c},
		{
			"There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977",
			0x7e85d7b050ed2967,
		},
		{"You remind me of a TV show, but that's all right: I watch it anyway.", 0xabcdb319fcf2826c},
		{"Give me a rock, paper and scissors and I will move the world.  CCFestoon", 0xa2b8bf3032021993},
		{"It's well we cannot hear the screams/That we create in others' dreams.", 0x1d85702503ac7eb4},
	}

	for _, tt := range tests {
		t.Run(tt.in[:min(len(tt.in), 12)], func(t *testing.T) {
			t.Parallel()

			if got := farmFingerprint64([]byte(tt.in)); got != tt.want {
				t.Errorf("got %#x want %#x", got, tt.want)
			}
		})
	}
}
