package dashboard

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expected string
		bytes    int64
	}{
		{"B", "500 B", 500},
		{"KB", "1.0 KB", 1024},
		{"MB", "1.0 MB", 1024 * 1024},
		{"GB", "1.0 GB", 1024 * 1024 * 1024},
		{"TB", "1.0 TB", 1024 * 1024 * 1024 * 1024},
		{"PB", "1.0 PB", 1024 * 1024 * 1024 * 1024 * 1024},
		{"EB", "1.0 EB", 1024 * 1024 * 1024 * 1024 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, formatBytes(tt.bytes))
		})
	}
}
