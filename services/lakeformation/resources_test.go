package lakeformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterDeregisterDescribeResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		roleArn     string
		errType     string
		notFound    bool
		wantErr     bool
	}{
		{
			name:        "register_success",
			resourceArn: "arn:aws:s3:::my-bucket",
			roleArn:     "arn:aws:iam::123456789012:role/MyRole",
		},
		{
			name:        "duplicate_register",
			resourceArn: "arn:aws:s3:::duplicate-bucket",
			roleArn:     "arn:aws:iam::123456789012:role/MyRole",
			wantErr:     true,
			errType:     "already exists",
		},
		{
			name:        "deregister_not_found",
			resourceArn: "arn:aws:s3:::nonexistent",
			notFound:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			if tt.notFound {
				// Test not-found path: do NOT register first.
				err := b.DeregisterResource(tt.resourceArn)
				require.Error(t, err)

				_, err = b.DescribeResource(tt.resourceArn)
				require.Error(t, err)

				return
			}

			err := b.RegisterResource(tt.resourceArn, tt.roleArn)
			require.NoError(t, err)

			if tt.wantErr {
				err = b.RegisterResource(tt.resourceArn, tt.roleArn)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "already")

				return
			}

			info, err := b.DescribeResource(tt.resourceArn)
			require.NoError(t, err)
			assert.Equal(t, tt.resourceArn, info.ResourceArn)
			assert.Equal(t, tt.roleArn, info.RoleArn)

			err = b.DeregisterResource(tt.resourceArn)
			require.NoError(t, err)

			_, err = b.DescribeResource(tt.resourceArn)
			require.Error(t, err)
		})
	}
}

func TestDescribeResource_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	_, err := b.DescribeResource("arn:aws:s3:::nonexistent")
	require.Error(t, err)
}

func TestDeregisterResource_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	err := b.DeregisterResource("arn:aws:s3:::nonexistent")
	require.Error(t, err)
}

func TestListResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		arns       []string
		maxResults int
		wantCount  int
		wantToken  bool
	}{
		{
			name:      "empty",
			arns:      []string{},
			wantCount: 0,
		},
		{
			name:      "three_resources",
			arns:      []string{"arn:aws:s3:::a", "arn:aws:s3:::b", "arn:aws:s3:::c"},
			wantCount: 3,
		},
		{
			name:       "paginated",
			arns:       []string{"arn:aws:s3:::a", "arn:aws:s3:::b", "arn:aws:s3:::c"},
			maxResults: 2,
			wantCount:  2,
			wantToken:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			for _, arn := range tt.arns {
				require.NoError(t, b.RegisterResource(arn, "arn:aws:iam::123456789012:role/R"))
			}

			resources, nextToken := b.ListResources(tt.maxResults, "")
			assert.Len(t, resources, tt.wantCount)

			if tt.wantToken {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

func TestDescribeResource_DeepCopy(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddResourceInternal("arn:aws:s3:::bucket1", "role")

	info, err := b.DescribeResource("arn:aws:s3:::bucket1")
	require.NoError(t, err)

	// Mutating the returned LastModified should not affect backend state.
	original := *info.LastModified
	mutated := original.AddDate(0, 0, 1)
	*info.LastModified = mutated

	info2, err := b.DescribeResource("arn:aws:s3:::bucket1")
	require.NoError(t, err)
	// The stored value should still be the original.
	assert.True(t, info2.LastModified.Equal(original) || info2.LastModified.Before(mutated))
}
