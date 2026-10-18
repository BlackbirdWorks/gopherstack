package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAWSVirtualHostBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		host string
		want string
	}{
		{"bkt.s3.amazonaws.com", "bkt"},
		{"bkt.s3.us-west-2.amazonaws.com:443", "bkt"},
		{"my.dotted.bkt.s3.eu-west-1.amazonaws.com", "my.dotted.bkt"},
		{"bkt.s3-us-west-2.amazonaws.com", "bkt"},
		{"bkt.s3.dualstack.us-east-1.amazonaws.com", "bkt"},
		{"bkt.s3.cn-north-1.amazonaws.com.cn", "bkt"},
		{"bkt.s3.us-east-1.api.aws", "bkt"},
		{"BKT.S3.US-EAST-1.AMAZONAWS.COM", "bkt"},
		{"s3.amazonaws.com", ""},
		{"s3.us-west-2.amazonaws.com", ""},
		{"123456789012.s3-control.us-east-1.amazonaws.com", ""},
		{"bkt.s3-website-us-east-1.amazonaws.com", ""},
		{"sqs.us-east-1.amazonaws.com", ""},
		{"bkt.s3.example.com", ""},
		{"localhost:4566", ""},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, awsVirtualHostBucket(tt.host))
		})
	}
}
