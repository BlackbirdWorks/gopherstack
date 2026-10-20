package arn_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func TestPartition(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, in, want string }{
		{"aws", "arn:aws:sqs:us-east-1:1:q", "aws"},
		{"cn", "arn:aws-cn:sqs:cn-north-1:1:q", "aws-cn"},
		{"gov", "arn:aws-us-gov:iam::1:role/r", "aws-us-gov"},
		{"unknown", "arn:other:sqs:us-east-1:1:q", ""},
		{"not arn", "queue", ""},
		{"no colon", "arn:aws", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, arn.Partition(tt.in))
		})
	}
}

func TestValidateForRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, region, in string
		wantErr          bool
	}{
		{"match", "us-east-1", "arn:aws:sqs:us-east-1:1:q", false},
		{"cn match", "cn-north-1", "arn:aws-cn:sqs:cn-north-1:1:q", false},
		{"cn arn in aws", "us-east-1", "arn:aws-cn:sqs:cn-north-1:1:q", true},
		{"aws arn in gov", "us-gov-west-1", "arn:aws:sqs:us-east-1:1:q", true},
		{"non arn", "us-east-1", "plain", false},
		{"empty region", "", "arn:aws-cn:sqs:cn-north-1:1:q", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := arn.ValidateForRegion(tt.region, tt.in)
			if tt.wantErr {
				require.ErrorIs(t, err, arn.ErrPartitionMismatch)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFindMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, text, partition, want string
		tolerant                    bool
	}{
		{"none", `{"a":"arn:aws:sqs:us-east-1:1:q"}`, "aws", "", false},
		{"cn in aws", `{"a":"arn:aws-cn:sqs:cn-north-1:1:q","b":1}`, "aws", "arn:aws-cn:sqs:cn-north-1:1:q", false},
		{"aws in cn strict", "x arn:aws:iam::1:role/r y", "aws-cn", "arn:aws:iam::1:role/r", false},
		{"aws in cn tolerant", "x arn:aws:iam::1:role/r y", "aws-cn", "", true},
		{"gov in cn tolerant", "arn:aws-us-gov:iam::1:role/r", "aws-cn", "arn:aws-us-gov:iam::1:role/r", true},
		{"bare arn word", "learn: more", "aws", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, found := arn.FindMismatch(tt.text, tt.partition, tt.tolerant)
			assert.Equal(t, tt.want != "", found)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHasServiceAndHelpers(t *testing.T) {
	t.Parallel()

	assert.True(t, arn.HasService("arn:aws-cn:sqs:cn-north-1:1:q", "sqs"))
	assert.False(t, arn.HasService("arn:aws:sqs:us-east-1:1:q", "sns"))
	assert.False(t, arn.HasService("sqs", "sqs"))
	assert.Equal(t, "arn:aws-cn:s3:::b/k", arn.S3("cn-north-1", "b/k"))
	assert.Equal(t, "arn:aws-us-gov:sqs:us-gov-west-1:1:q",
		arn.WithPartition("us-gov-west-1", "arn:aws:sqs:us-gov-west-1:1:q"))
}
