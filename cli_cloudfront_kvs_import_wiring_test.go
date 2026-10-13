package main

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
)

func TestCloudFrontKVSImportSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    map[string]string
		name    string
		body    string
		srcType cftypes.ImportSourceType
		wantErr bool
	}{
		{
			name: "valid", srcType: cftypes.ImportSourceTypeS3,
			body: `{"data":[{"key":"a","value":"1"},{"key":"b","value":"2"}]}`,
			want: map[string]string{"a": "1", "b": "2"},
		},
		{name: "invalid_json", srcType: cftypes.ImportSourceTypeS3, body: `not json`, wantErr: true},
		{name: "missing_value", srcType: cftypes.ImportSourceTypeS3, body: `{"data":[{"key":"a"}]}`, wantErr: true},
		{
			name: "oversized_value", srcType: cftypes.ImportSourceTypeS3, wantErr: true,
			body: `{"data":[{"key":"a","value":"` + strings.Repeat("x", 2000) + `"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			s3c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })

			_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("kvs-src")})
			require.NoError(t, err)

			_, err = s3c.PutObject(t.Context(), &s3.PutObjectInput{
				Bucket: aws.String("kvs-src"), Key: aws.String("data.json"), Body: strings.NewReader(tt.body),
			})
			require.NoError(t, err)

			out, err := cloudfront.NewFromConfig(fx.cfg).CreateKeyValueStore(
				t.Context(), &cloudfront.CreateKeyValueStoreInput{
					Name: aws.String("imported"),
					ImportSource: &cftypes.ImportSource{
						SourceType: tt.srcType, SourceARN: aws.String("arn:aws:s3:::kvs-src/data.json"),
					},
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			cfH, ok := serviceByName(fx.services)["CloudFront"].(*cloudfrontbackend.Handler)
			require.True(t, ok)

			items, _, err := cfH.Backend.ListKVSValues(aws.ToString(out.KeyValueStore.Id))
			require.NoError(t, err)

			got := map[string]string{}
			for _, it := range items {
				got[it.Key] = it.Value
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
