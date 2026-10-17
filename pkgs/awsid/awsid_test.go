package awsid_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prefix  string
		re      string
		hexSize int
	}{
		{name: "default", prefix: "i-", re: `^i-[0-9a-f]{17}$`},
		{name: "compound", prefix: "rslvr-in-", re: `^rslvr-in-[0-9a-f]{17}$`},
		{name: "custom", prefix: "x-", re: `^x-[0-9a-f]{8}$`, hexSize: 8},
		{name: "odd", prefix: "x-", re: `^x-[0-9a-f]{5}$`, hexSize: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mint := func() string { return awsid.New(tt.prefix) }
			if tt.hexSize > 0 {
				mint = func() string { return awsid.NewN(tt.prefix, tt.hexSize) }
			}

			first, second := mint(), mint()
			assert.Regexp(t, tt.re, first)
			assert.NotEqual(t, first, second)
		})
	}
}
