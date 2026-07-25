package lakeformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaginate_NextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantCount  int
		wantToken  bool
	}{
		{
			name:       "paginate returns next token when more items exist",
			maxResults: 1,
			wantCount:  1,
			wantToken:  true,
		},
		{
			name:       "paginate returns all items when max is 0",
			maxResults: 0,
			wantCount:  2,
			wantToken:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			require.NoError(
				t,
				b.RegisterResource(
					"arn:aws:s3:::bucket-a",
					"arn:aws:iam::123:role/r",
					lakeformation.RegisterResourceOptions{},
				),
			)
			require.NoError(
				t,
				b.RegisterResource(
					"arn:aws:s3:::bucket-b",
					"arn:aws:iam::123:role/r",
					lakeformation.RegisterResourceOptions{},
				),
			)

			resources, token := b.ListResources(tt.maxResults, "")
			assert.Len(t, resources, tt.wantCount)

			if tt.wantToken {
				assert.NotEmpty(t, token)
			} else {
				assert.Empty(t, token)
			}
		})
	}
}

func TestPaginate_InvalidNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nextToken string
		wantCount int
	}{
		{
			name:      "invalid next token falls back to start",
			nextToken: "not-a-number",
			wantCount: 2,
		},
		{
			name:      "negative next token falls back to start",
			nextToken: "-1",
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			require.NoError(
				t,
				b.RegisterResource("arn:aws:s3:::bucket-x", "arn:role", lakeformation.RegisterResourceOptions{}),
			)
			require.NoError(
				t,
				b.RegisterResource("arn:aws:s3:::bucket-y", "arn:role", lakeformation.RegisterResourceOptions{}),
			)

			resources, _ := b.ListResources(0, tt.nextToken)
			assert.Len(t, resources, tt.wantCount)
		})
	}
}

func TestReset(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddLFTagInternal("123456789012", "env", []string{"dev", "prod"})
	b.AddResourceInternal("arn:aws:s3:::my-bucket", "arn:aws:iam::123456789012:role/MyRole")
	require.Equal(t, 1, b.TagCount())
	require.Equal(t, 1, b.ResourceCount())

	b.Reset()

	assert.Equal(t, 0, b.TagCount())
	assert.Equal(t, 0, b.ResourceCount())
	assert.Equal(t, 0, b.PermissionCount())
}

func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()

	for i := range 3 {
		_ = i
		b.AddLFTagInternal("cat", "key", []string{"v"})
		b.Reset()
		assert.Equal(t, 0, b.TagCount())
	}
}
