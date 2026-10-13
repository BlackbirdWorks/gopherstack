package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLinkLocalEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		val  string
		want bool
	}{
		{name: "unset"},
		{name: "true", val: "true", want: true},
		{name: "one", val: "1", want: true},
		{name: "padded", val: " true ", want: true},
		{name: "false", val: "false"},
		{name: "garbage", val: "maybe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, linkLocalEnabled(func(k string) string {
				if k == envLinkLocal {
					return tt.val
				}

				return ""
			}))
		})
	}
}
