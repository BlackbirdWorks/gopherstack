package lambda_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

type plainFetcher struct{}

func (plainFetcher) GetObjectBytes(context.Context, string, string) ([]byte, error) {
	return []byte("latest"), nil
}

type versionedFetcher struct{ plainFetcher }

func (versionedFetcher) GetObjectVersionBytes(_ context.Context, _, _, version string) ([]byte, error) {
	return []byte("version-" + version), nil
}

func TestFetchS3Code_ObjectVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fetcher lambda.S3CodeFetcher
		name    string
		version string
		want    string
		wantErr bool
	}{
		{name: "unversioned", fetcher: versionedFetcher{}, want: "latest"},
		{name: "versioned", fetcher: versionedFetcher{}, version: "v7", want: "version-v7"},
		{name: "version_unsupported", fetcher: plainFetcher{}, version: "v7", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, b)
			b.SetS3CodeFetcher(tt.fetcher)

			got, err := lambda.FetchS3CodeForTest(t.Context(), b, "bkt", "key.zip", tt.version)
			if tt.wantErr {
				require.ErrorIs(t, err, lambda.ErrLambdaUnavailable)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

func TestS3ObjectVersion_Roundtrip(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	_, err := client.CreateFunction(ctx, &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String("s3v-fn"),
		PackageType:  lambdatypes.PackageTypeZip,
		Runtime:      lambdatypes.RuntimeNodejs20x,
		Handler:      aws.String("index.handler"),
		Role:         aws.String("arn:aws:iam:::role/r"),
		Code: &lambdatypes.FunctionCode{
			S3Bucket:        aws.String("b"),
			S3Key:           aws.String("k.zip"),
			S3ObjectVersion: aws.String("v1"),
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateFunctionCode(ctx, &lambdasdk.UpdateFunctionCodeInput{
		FunctionName: aws.String("s3v-fn"), S3Bucket: aws.String("b"), S3Key: aws.String("k.zip"),
		S3ObjectVersion: aws.String("v2"), S3ObjectStorageMode: lambdatypes.S3ObjectStorageModeReference,
	})
	require.NoError(t, err)

	got, err := client.GetFunction(ctx, &lambdasdk.GetFunctionInput{FunctionName: aws.String("s3v-fn")})
	require.NoError(t, err)
	require.NotNil(t, got.Code.ResolvedS3Object)
	assert.Equal(t, "v2", aws.ToString(got.Code.ResolvedS3Object.S3ObjectVersion))
}
