package s3vectors_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3vectors"
)

type fakeKeys struct{ err error }

func (f fakeKeys) CheckKey(context.Context, string) error { return f.err }

func TestKMSKeyChecksOnDataOperations(t *testing.T) {
	t.Parallel()

	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/abc"

	tests := []struct {
		err      error
		name     string
		wantCode string
	}{
		{name: "usable"},
		{name: "not found", err: s3vectors.ErrKmsNotFound, wantCode: "KmsNotFoundException"},
		{name: "disabled", err: s3vectors.ErrKmsDisabled, wantCode: "KmsDisabledException"},
		{name: "invalid state", err: s3vectors.ErrKmsInvalidState, wantCode: "KmsInvalidStateException"},
		{name: "invalid usage", err: s3vectors.ErrKmsInvalidKeyUsage, wantCode: "KmsInvalidKeyUsageException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			_, err := f.client.CreateVectorBucket(t.Context(), &sdk.CreateVectorBucketInput{
				VectorBucketName: aws.String("kms-bucket"),
				EncryptionConfiguration: &types.EncryptionConfiguration{
					SseType: types.SseTypeAwsKms, KmsKeyArn: aws.String(keyARN),
				},
			})
			require.NoError(t, err)
			f.index(t, "kms-bucket", "kmsx", "euclidean", 1)
			f.backend.SetKeyChecker(fakeKeys{err: tt.err})

			ops := map[string]func() error{
				"put": func() error {
					_, e := f.client.PutVectors(t.Context(), &sdk.PutVectorsInput{
						VectorBucketName: aws.String("kms-bucket"), IndexName: aws.String("kmsx"),
						Vectors: []types.PutInputVector{vec("a", nil, 1)},
					})

					return e
				},
				"get": func() error {
					_, e := f.client.GetVectors(t.Context(), &sdk.GetVectorsInput{
						VectorBucketName: aws.String("kms-bucket"), IndexName: aws.String("kmsx"), Keys: []string{"a"},
					})

					return e
				},
				"delete": func() error {
					_, e := f.client.DeleteVectors(t.Context(), &sdk.DeleteVectorsInput{
						VectorBucketName: aws.String("kms-bucket"), IndexName: aws.String("kmsx"), Keys: []string{"a"},
					})

					return e
				},
				"query": func() error {
					_, e := query(t, f, "kms-bucket", "kmsx", 1, nil, 1)

					return e
				},
			}

			for name, op := range ops {
				assert.Equal(t, tt.wantCode, apiCode(op()), name)
			}

			_, err = f.client.ListVectors(t.Context(), &sdk.ListVectorsInput{
				VectorBucketName: aws.String("kms-bucket"), IndexName: aws.String("kmsx"),
			})
			assert.NoError(t, err, "ListVectors has no Kms errors")
		})
	}
}
