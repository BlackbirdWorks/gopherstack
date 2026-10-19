package replicator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/internal/replicator"
)

func TestRemapperARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"sqs", "arn:aws:sqs:eu-west-1:111111111111:q", "arn:aws:sqs:us-east-1:222222222222:q"},
		{"iam global", "arn:aws:iam::111111111111:role/r", "arn:aws:iam::222222222222:role/r"},
		{"managed policy", "arn:aws:iam::aws:policy/ReadOnlyAccess", "arn:aws:iam::aws:policy/ReadOnlyAccess"},
		{"foreign account", "arn:aws:sqs:eu-west-1:999999999999:q", "arn:aws:sqs:eu-west-1:999999999999:q"},
		{"s3", "arn:aws:s3:::bucket", "arn:aws:s3:::bucket"},
		{"not an arn", "queue-name", "queue-name"},
		{
			"kms key mapped",
			"arn:aws:kms:eu-west-1:111111111111:key/src-key",
			"arn:aws:kms:us-east-1:222222222222:key/dst-key",
		},
		{"gov partition", "arn:aws-us-gov:sns:eu-west-1:111111111111:t", "arn:aws:sns:us-east-1:222222222222:t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := replicator.NewRemapper("111111111111", "eu-west-1", "222222222222", "us-east-1")
			m.MapKey("src-key", "dst-key")
			assert.Equal(t, tt.want, m.ARN(tt.in))
		})
	}
}

func TestRemapperText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		srcAccount string
		in         string
		want       string
	}{
		{
			"policy", "111111111111",
			`{"Principal":{"AWS":"arn:aws:iam::111111111111:root"},"Resource":"arn:aws:sqs:eu-west-1:111111111111:q"}`,
			`{"Principal":{"AWS":"arn:aws:iam::222222222222:root"},"Resource":"arn:aws:sqs:us-east-1:222222222222:q"}`,
		},
		{"bare account", "111111111111", `{"AWS":"111111111111"}`, `{"AWS":"222222222222"}`},
		{"unknown source account is a no-op", "", `{"AWS":"111111111111"}`, `{"AWS":"111111111111"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := replicator.NewRemapper(tt.srcAccount, "eu-west-1", "222222222222", "us-east-1")
			assert.Equal(t, tt.want, m.Text(tt.in))
		})
	}
}
