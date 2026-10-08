package main

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func protoTestDescriptorSet(t *testing.T) []byte {
	t.Helper()

	str := descriptorpb.FieldDescriptorProto_TYPE_STRING
	i32 := descriptorpb.FieldDescriptorProto_TYPE_INT32
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL

	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    aws.String("reading.proto"),
		Package: aws.String("telemetry"),
		Syntax:  aws.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: aws.String("Reading"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: aws.String("device_id"), Number: aws.Int32(1), Type: &str, Label: &opt},
				{Name: aws.String("temp"), Number: aws.Int32(2), Type: &i32, Label: &opt},
			},
		}},
	}}}

	raw, err := proto.Marshal(set)
	require.NoError(t, err)

	return raw
}

func TestIoTRuleDecodeProto(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		want    string
		missing bool
	}{
		{
			name: "decoded",
			sql: "SELECT decode(*, 'proto', 'descs', 'reading.desc', 'reading', 'Reading') AS r FROM '" +
				lookupTopic + "'",
			want: `{"r":{"device_id":"d1","temp":21}}`,
		},
		{
			name: "qualified_type_with_extension",
			sql: "SELECT decode(*, 'proto', 'descs', 'reading.desc', 'reading.proto', 'telemetry.Reading').temp AS t" +
				" FROM '" + lookupTopic + "'",
			want: `{"t":21}`,
		},
		{
			name: "unknown_message_fails_rule",
			sql: "SELECT decode(*, 'proto', 'descs', 'reading.desc', 'reading', 'Nope') AS r FROM '" +
				lookupTopic + "'",
			missing: true,
		},
		{
			name: "missing_descriptor_fails_rule",
			sql: "SELECT decode(*, 'proto', 'descs', 'absent.desc', 'reading', 'Reading') AS r FROM '" +
				lookupTopic + "'",
			missing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, false, "*")
			s3c := s3.NewFromConfig(f.fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })

			_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("descs")})
			require.NoError(t, err)

			_, err = s3c.PutObject(t.Context(), &s3.PutObjectInput{
				Bucket: aws.String("descs"), Key: aws.String("reading.desc"),
				Body: bytes.NewReader(protoTestDescriptorSet(t)),
			})
			require.NoError(t, err)

			url := f.lookupRule(t, "pb", tt.sql)
			sentinel := f.lookupRule(t, "sentinel", "SELECT 'ok' AS s FROM '"+lookupTopic+"'")

			f.publish(t, lookupTopic, "\n\x02d1\x10\x15")

			if tt.missing {
				f.wantBody(t, sentinel, `{"s":"ok"}`)
				assert.False(t, authzReceived(t, f.fx, url))

				return
			}

			f.wantBody(t, url, tt.want)
		})
	}
}
