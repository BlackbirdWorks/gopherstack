package arn_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func TestCanonicalLocalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		partition string
		in        string
		canonical string
		localized string
	}{
		{"aws", "aws", "arn:aws:s3:::b/*", "arn:aws:s3:::b/*", "arn:aws:s3:::b/*"},
		{"cn", "aws-cn", "arn:aws-cn:s3:::b/*", "arn:aws:s3:::b/*", "arn:aws-cn:s3:::b/*"},
		{"gov", "aws-us-gov", "arn:aws-us-gov:iam::1:role/r", "arn:aws:iam::1:role/r", "arn:aws-us-gov:iam::1:role/r"},
		{"encoded", "aws-cn", "arn%3Aaws-cn%3Aiam%3A%3A1%3Arole%2Fr", "arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fr",
			"arn%3Aaws-cn%3Aiam%3A%3A1%3Arole%2Fr"},
		{"plain text", "aws-cn", "no arns here", "no arns here", "no arns here"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := arn.Canonical(tt.in)
			assert.Equal(t, tt.canonical, got)
			assert.Equal(t, tt.localized, arn.Localize(got, tt.partition))
		})
	}
}

func TestPartitionStrict(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want bool
	}{
		{"default", "", true},
		{"false", "false", false},
		{"false_upper", "FALSE", false},
		{"other", "true", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(arn.EnvPartitionStrict, tt.env)
			assert.Equal(t, tt.want, arn.PartitionStrict())
		})
	}
}
