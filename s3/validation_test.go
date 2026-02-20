package s3_test

import (
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/s3"

	"github.com/stretchr/testify/assert"
)

func TestIsValidBucketName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want bool
	}{
		{"my-bucket", true},
		{"my.bucket", true},
		{"123-bucket", true},
		{"bucket-123", true},
		{"a.b.c", true},
		{"ab", false},                         // too short
		{strings.Repeat("a", 64), false},      // too long
		{"MyBucket", false},                   // uppercase
		{"my_bucket", false},                  // underscore
		{"-mybucket", false},                  // starts with hyphen
		{"mybucket-", false},                  // ends with hyphen
		{".mybucket", false},                  // starts with dot
		{"mybucket.", false},                  // ends with dot
		{"my..bucket", false},                 // adjacent dots
		{"192.168.1.1", false},                // IP address
		{"xn--bucket", false},                 // reserved prefix
		{"sthree-bucket", false},              // reserved prefix
		{"sthree-configurator-bucket", false}, // reserved prefix
		{"bucket-s3alias", false},             // reserved suffix
		{"bucket--ol-s3", false},              // reserved suffix
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, s3.IsValidBucketName(tt.name), "bucket name: %s", tt.name)
		})
	}
}

func TestIsValidObjectKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want bool
	}{
		{"valid key", "my-key", true},
		{"empty key", "", false},
		{"too long key", strings.Repeat("a", 1025), false},
		{"max length key", strings.Repeat("a", 1024), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, s3.IsValidObjectKey(tt.key), "key: %s", tt.name)
		})
	}
}
