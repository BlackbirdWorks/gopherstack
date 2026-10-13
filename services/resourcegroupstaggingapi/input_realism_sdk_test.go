package resourcegroupstaggingapi_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rgtasdk "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func TestGetResources_ResourcesPerPageRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		perPage int32
		wantErr bool
	}{
		{name: "min", perPage: 1},
		{name: "max", perPage: 100},
		{name: "zero", perPage: 0, wantErr: true},
		{name: "negative", perPage: -1, wantErr: true},
		{name: "too_large", perPage: 101, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestRGTAClient(t, resourcegroupstaggingapi.NewInMemoryBackend("000000000000", "us-east-1"))
			_, err := c.GetResources(t.Context(), &rgtasdk.GetResourcesInput{ResourcesPerPage: aws.Int32(tt.perPage)})

			if tt.wantErr {
				require.ErrorContains(t, err, "InvalidParameterException")

				return
			}

			require.NoError(t, err)
		})
	}
}

var errBoom = errors.New("boom")

func TestTagResources_TaggerErrorClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err      error
		name     string
		wantCode string
		wantHTTP int32
	}{
		{
			name:     "not_found",
			err:      awserr.New("NonExistentQueue", awserr.ErrNotFound),
			wantCode: "InvalidParameterException",
			wantHTTP: 400,
		},
		{
			name:     "invalid",
			err:      awserr.New("bad", awserr.ErrInvalidParameter),
			wantCode: "InvalidParameterException",
			wantHTTP: 400,
		},
		{name: "internal", err: errBoom, wantCode: "InternalServiceException", wantHTTP: 500},
	}

	const arn = "arn:aws:sqs:us-east-1:000000000000:q"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := resourcegroupstaggingapi.NewInMemoryBackend("000000000000", "us-east-1")
			b.RegisterARNTagger(
				func(_ context.Context, _ string, _ map[string]string) (bool, error) { return true, tt.err },
			)
			b.RegisterARNUntagger(func(_ context.Context, _ string, _ []string) (bool, error) { return true, tt.err })
			c := newTestRGTAClient(t, b)

			tagOut, err := c.TagResources(t.Context(), &rgtasdk.TagResourcesInput{
				ResourceARNList: []string{arn}, Tags: map[string]string{"k": "v"},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, string(tagOut.FailedResourcesMap[arn].ErrorCode))
			assert.Equal(t, tt.wantHTTP, tagOut.FailedResourcesMap[arn].StatusCode)

			untagOut, err := c.UntagResources(t.Context(), &rgtasdk.UntagResourcesInput{
				ResourceARNList: []string{arn}, TagKeys: []string{"k"},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, string(untagOut.FailedResourcesMap[arn].ErrorCode))
			assert.Equal(t, tt.wantHTTP, untagOut.FailedResourcesMap[arn].StatusCode)
		})
	}
}
