package dashboard

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected string
	}{
		{"B", 500, "500 B"},
		{"KB", 1024, "1.0 KB"},
		{"MB", 1024 * 1024, "1.0 MB"},
		{"GB", 1024 * 1024 * 1024, "1.0 GB"},
		{"TB", 1024 * 1024 * 1024 * 1024, "1.0 TB"},
		{"PB", 1024 * 1024 * 1024 * 1024 * 1024, "1.0 PB"},
		{"EB", 1024 * 1024 * 1024 * 1024 * 1024 * 1024, "1.0 EB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, formatBytes(tt.bytes))
		})
	}
}
