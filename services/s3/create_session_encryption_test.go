package s3_test

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sessionEncBucket = "sess-enc--use1-az4--x-s3"

func sessionEncCreateBucket(t *testing.T, client *sdk_s3.Client) {
	t.Helper()

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
		Bucket: aws.String(sessionEncBucket),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			Bucket: &types.BucketInfo{
				Type:           types.BucketTypeDirectory,
				DataRedundancy: types.DataRedundancySingleAvailabilityZone,
			},
			Location: &types.LocationInfo{Type: types.LocationTypeAvailabilityZone, Name: aws.String("use1-az4")},
		},
	})
	require.NoError(t, err)
}

// CreateSessionInput SSE members (s3@v1.111.0 api_op_CreateSession.go) are validated and echoed.
func TestCreateSession_EncryptionHeaders(t *testing.T) {
	t.Parallel()

	arnCtx := base64.StdEncoding.EncodeToString(
		[]byte(`{"aws:s3:arn":"arn:aws:s3express:us-east-1:000000000000:bucket/` + sessionEncBucket + `"}`),
	)
	otherCtx := base64.StdEncoding.EncodeToString([]byte(`{"team":"x"}`))

	tests := []struct {
		wantAlgo types.ServerSideEncryption
		wantKey  string
		wantErr  string
		name     string
		in       sdk_s3.CreateSessionInput
	}{
		{
			name:     "aes256",
			in:       sdk_s3.CreateSessionInput{ServerSideEncryption: types.ServerSideEncryptionAes256},
			wantAlgo: types.ServerSideEncryptionAes256,
		},
		{
			name: "kms", wantAlgo: types.ServerSideEncryptionAwsKms, wantKey: "key-1",
			in: sdk_s3.CreateSessionInput{
				ServerSideEncryption: types.ServerSideEncryptionAwsKms, SSEKMSKeyId: aws.String("key-1"),
				SSEKMSEncryptionContext: aws.String(arnCtx), BucketKeyEnabled: aws.Bool(true),
			},
		},
		{
			name: "kms-without-key", wantErr: "InvalidArgument",
			in: sdk_s3.CreateSessionInput{ServerSideEncryption: types.ServerSideEncryptionAwsKms},
		},
		{
			name: "extra-context", wantErr: "InvalidArgument",
			in: sdk_s3.CreateSessionInput{
				ServerSideEncryption: types.ServerSideEncryptionAwsKms, SSEKMSKeyId: aws.String("k"),
				SSEKMSEncryptionContext: aws.String(otherCtx),
			},
		},
		{
			name: "unsupported-algo", wantErr: "InvalidArgument",
			in: sdk_s3.CreateSessionInput{ServerSideEncryption: types.ServerSideEncryption("aws:kms:dsse")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ExpressClientTest(t)
			sessionEncCreateBucket(t, client)

			in := tt.in
			in.Bucket = aws.String(sessionEncBucket)

			out, err := client.CreateSession(t.Context(), &in)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantAlgo, out.ServerSideEncryption)
			assert.Equal(t, tt.wantKey, aws.ToString(out.SSEKMSKeyId))
			assert.Equal(t, in.BucketKeyEnabled, out.BucketKeyEnabled)
			assert.Equal(t, in.SSEKMSEncryptionContext, out.SSEKMSEncryptionContext)
		})
	}
}

// Objects stored through a session inherit its default SSE when the request names none.
func TestCreateSession_EncryptionAppliedToObjects(t *testing.T) {
	t.Parallel()

	base := newRealS3ExpressClientTest(t)
	sessionEncCreateBucket(t, base)

	injectSSE := func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc("injectSessionSSE",
			func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (
				middleware.BuildOutput, middleware.Metadata, error,
			) {
				if req, ok := in.Request.(*smithyhttp.Request); ok &&
					middleware.GetOperationName(ctx) == "CreateSession" {
					req.Header.Set("X-Amz-Server-Side-Encryption", "AES256")
				}

				return next.HandleBuild(ctx, in)
			}), middleware.After)
	}

	client := sdk_s3.New(base.Options(), func(o *sdk_s3.Options) {
		o.DisableS3ExpressSessionAuth = aws.Bool(false)
		o.APIOptions = append(o.APIOptions, injectSSE)
	})

	_, err := client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(sessionEncBucket), Key: aws.String("k"), Body: strings.NewReader("v"),
	})
	require.NoError(t, err)

	head, err := base.HeadObject(
		t.Context(),
		&sdk_s3.HeadObjectInput{Bucket: aws.String(sessionEncBucket), Key: aws.String("k")},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ServerSideEncryptionAes256, head.ServerSideEncryption)
}
