package lightsail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func corsRule(methods ...string) lightsailtypes.BucketCorsRule {
	return lightsailtypes.BucketCorsRule{
		AllowedMethods: methods,
		AllowedOrigins: []string{"https://example.com"},
		AllowedHeaders: []string{"*"},
		Id:             aws.String("rule-1"),
		MaxAgeSeconds:  aws.Int32(300),
	}
}

func TestBucketCORS(t *testing.T) {
	t.Parallel()

	tooMany := make([]lightsailtypes.BucketCorsRule, 21)
	for i := range tooMany {
		tooMany[i] = corsRule("GET")
	}

	noOrigin := corsRule("GET")
	noOrigin.AllowedOrigins = []string{}

	tests := []struct {
		name    string
		wantErr string
		rules   []lightsailtypes.BucketCorsRule
	}{
		{
			name: "unsupported_method", rules: []lightsailtypes.BucketCorsRule{corsRule("PATCH")},
			wantErr: "InvalidInputException",
		},
		{name: "missing_origin", rules: []lightsailtypes.BucketCorsRule{noOrigin}, wantErr: "InvalidInputException"},
		{name: "too_many_rules", rules: tooMany, wantErr: "InvalidInputException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t)
			ctx := t.Context()

			_, err := client.CreateBucket(ctx, &lightsailsdk.CreateBucketInput{
				BucketName: aws.String("cors-bucket"), BundleId: aws.String("small_1_0"),
			})
			require.NoError(t, err)

			upd, err := client.UpdateBucket(ctx, &lightsailsdk.UpdateBucketInput{
				BucketName: aws.String("cors-bucket"),
				Cors:       &lightsailtypes.BucketCorsConfig{Rules: tt.rules},
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, upd.Bucket.Cors)
			assert.Equal(t, tt.rules, upd.Bucket.Cors.Rules)

			plain, err := client.GetBuckets(ctx, &lightsailsdk.GetBucketsInput{BucketName: aws.String("cors-bucket")})
			require.NoError(t, err)
			assert.Nil(t, plain.Buckets[0].Cors)

			withCors, err := client.GetBuckets(ctx, &lightsailsdk.GetBucketsInput{
				BucketName: aws.String("cors-bucket"), IncludeCors: aws.Bool(true),
			})
			require.NoError(t, err)
			require.NotNil(t, withCors.Buckets[0].Cors)
			assert.Equal(t, tt.rules, withCors.Buckets[0].Cors.Rules)

			replaced, err := client.UpdateBucket(ctx, &lightsailsdk.UpdateBucketInput{
				BucketName: aws.String("cors-bucket"),
				Cors:       &lightsailtypes.BucketCorsConfig{Rules: []lightsailtypes.BucketCorsRule{corsRule("HEAD")}},
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"HEAD"}, replaced.Bucket.Cors.Rules[0].AllowedMethods)
		})
	}
}
