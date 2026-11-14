package s3files_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3filessdk "github.com/aws/aws-sdk-go-v2/service/s3files"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3files"
)

func TestFileSystemQuotaPerAccount(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	for range 1000 {
		_, _, err := f.backend.CreateFileSystem(t.Context(), s3files.CreateFileSystemInput{
			Bucket: testBucket, RoleARN: testRole,
		})
		require.NoError(t, err)
	}

	_, err := f.client.CreateFileSystem(t.Context(), &s3filessdk.CreateFileSystemInput{
		Bucket: aws.String(testBucket), RoleArn: aws.String(testRole),
	})
	assert.Equal(t, "ServiceQuotaExceededException", apiCode(err))
}

func TestMountTargetQuotas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		subnet   string
		wantCode string
	}{
		{name: "other az same vpc", subnet: "subnet-bbbb"},
		{name: "same az other subnet", subnet: "subnet-cccc", wantCode: "ServiceQuotaExceededException"},
		{name: "other vpc", subnet: "subnet-dddd", wantCode: "ServiceQuotaExceededException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			fsID := f.fileSystem(t)

			_, err := f.client.CreateMountTarget(t.Context(), &s3filessdk.CreateMountTargetInput{
				FileSystemId: aws.String(fsID), SubnetId: aws.String(testSubnet),
			})
			require.NoError(t, err)

			_, err = f.client.CreateMountTarget(t.Context(), &s3filessdk.CreateMountTargetInput{
				FileSystemId: aws.String(fsID), SubnetId: aws.String(tt.subnet),
			})
			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}

func TestPolicySizeLimit(t *testing.T) {
	t.Parallel()

	const head = `{"Statement":[],"Id":"`

	tests := []struct {
		name     string
		wantCode string
		size     int
	}{
		{name: "at limit", size: 20000},
		{name: "over limit", size: 20001, wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			policy := head + strings.Repeat("a", tt.size-len(head)-2) + `"}`

			_, err := f.client.PutFileSystemPolicy(t.Context(), &s3filessdk.PutFileSystemPolicyInput{
				FileSystemId: aws.String(f.fileSystem(t)), Policy: aws.String(policy),
			})
			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}

func TestAccessPointQuotaPerFileSystem(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetAccessPointLimit(3)

	fsID := f.fileSystem(t)

	for range 3 {
		_, _, err := f.backend.CreateAccessPoint(t.Context(), s3files.CreateAccessPointInput{FileSystemID: fsID})
		require.NoError(t, err)
	}

	_, err := f.client.CreateAccessPoint(
		t.Context(), &s3filessdk.CreateAccessPointInput{FileSystemId: aws.String(fsID)},
	)
	assert.Equal(t, "ServiceQuotaExceededException", apiCode(err))
}
