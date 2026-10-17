package ssm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMockJITCredentialsKeyShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{{name: "first"}, {name: "second"}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := mockJITCredentials()
			assert.Regexp(t, `^ASIA[0-9A-Z]{16}$`, c.AccessKeyID)
		})
	}
}
