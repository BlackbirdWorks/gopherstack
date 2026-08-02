package arn_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestBuildGlobal(t *testing.T) {
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
			name:      "directconnect gateway",
			service:   "directconnect",
			region:    "us-east-1",
			accountID: "123456789012",
			resource:  "dx-gateway/11460e6f-c1bc-40a7-b8f9-06f4e6a9d1e1",
			want:      "arn:aws:directconnect::123456789012:dx-gateway/11460e6f-c1bc-40a7-b8f9-06f4e6a9d1e1",
		},
		{
			name:      "govcloud partition still applies",
			service:   "directconnect",
			region:    "us-gov-west-1",
			accountID: "123456789012",
			resource:  "dx-gateway/abc",
			want:      "arn:aws-us-gov:directconnect::123456789012:dx-gateway/abc",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := arn.BuildGlobal(tc.service, tc.region, tc.accountID, tc.resource)
			assert.Equal(t, tc.want, got)
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
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPartitionForRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		want   string
	}{
		{name: "commercial us-east-1", region: "us-east-1", want: "aws"},
		{name: "commercial eu-west-1", region: "eu-west-1", want: "aws"},
		{name: "empty region", region: "", want: "aws"},
		{name: "govcloud west", region: "us-gov-west-1", want: "aws-us-gov"},
		{name: "govcloud east", region: "us-gov-east-1", want: "aws-us-gov"},
		{name: "china north", region: "cn-north-1", want: "aws-cn"},
		{name: "china northwest", region: "cn-northwest-1", want: "aws-cn"},
		{name: "iso east", region: "us-iso-east-1", want: "aws-iso"},
		{name: "isob east", region: "us-isob-east-1", want: "aws-iso-b"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, arn.PartitionForRegion(tc.region))
		})
	}
}

func TestBuildPartitionedRegions(t *testing.T) {
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
			name:      "govcloud kinesis",
			service:   "kinesis",
			region:    "us-gov-west-1",
			accountID: "123456789012",
			resource:  "stream/s1",
			want:      "arn:aws-us-gov:kinesis:us-gov-west-1:123456789012:stream/s1",
		},
		{
			name:      "china s3 control",
			service:   "s3control",
			region:    "cn-north-1",
			accountID: "000000000000",
			resource:  "accesspoint/ap1",
			want:      "arn:aws-cn:s3control:cn-north-1:000000000000:accesspoint/ap1",
		},
		{
			name:      "iso lambda",
			service:   "lambda",
			region:    "us-iso-east-1",
			accountID: "111111111111",
			resource:  "function:fn",
			want:      "arn:aws-iso:lambda:us-iso-east-1:111111111111:function:fn",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := arn.Build(tc.service, tc.region, tc.accountID, tc.resource)
			assert.Equal(t, tc.want, got)
		})
	}
}
