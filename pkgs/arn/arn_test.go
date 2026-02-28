package arn_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func TestBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		service   string
		region    string
		accountID string
		resource  string
		want      string
	}{
		{
			name:      "standard service",
			service:   "kinesis",
			region:    "us-east-1",
			accountID: "123456789012",
			resource:  "stream/my-stream",
			want:      "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
		},
		{
			name:      "IAM omits region",
			service:   "iam",
			region:    "",
			accountID: "123456789012",
			resource:  "role/my-role",
			want:      "arn:aws:iam::123456789012:role/my-role",
		},
		{
			name:      "IAM ignores non-empty region",
			service:   "iam",
			region:    "us-west-2",
			accountID: "123456789012",
			resource:  "user/alice",
			want:      "arn:aws:iam::123456789012:user/alice",
		},
		{
			name:      "kms key",
			service:   "kms",
			region:    "eu-west-1",
			accountID: "000000000000",
			resource:  "key/some-uuid",
			want:      "arn:aws:kms:eu-west-1:000000000000:key/some-uuid",
		},
		{
			name:      "lambda function",
			service:   "lambda",
			region:    "ap-southeast-1",
			accountID: "000000000000",
			resource:  "function:my-fn",
			want:      "arn:aws:lambda:ap-southeast-1:000000000000:function:my-fn",
		},
		{
			name:      "empty resource",
			service:   "sqs",
			region:    "us-east-1",
			accountID: "000000000000",
			resource:  "",
			want:      "arn:aws:sqs:us-east-1:000000000000:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := arn.Build(tc.service, tc.region, tc.accountID, tc.resource)
			if got != tc.want {
				t.Errorf("Build(%q, %q, %q, %q) = %q; want %q",
					tc.service, tc.region, tc.accountID, tc.resource, got, tc.want)
			}
		})
	}
}

func TestBuildS3(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
		want     string
	}{
		{
			name:     "bucket ARN",
			resource: "my-bucket",
			want:     "arn:aws:s3:::my-bucket",
		},
		{
			name:     "bucket with prefix",
			resource: "my-bucket/prefix/key",
			want:     "arn:aws:s3:::my-bucket/prefix/key",
		},
		{
			name:     "empty resource",
			resource: "",
			want:     "arn:aws:s3:::",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := arn.BuildS3(tc.resource)
			if got != tc.want {
				t.Errorf("BuildS3(%q) = %q; want %q", tc.resource, got, tc.want)
			}
		})
	}
}
