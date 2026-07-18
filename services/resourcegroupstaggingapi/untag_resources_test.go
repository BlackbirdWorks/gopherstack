package resourcegroupstaggingapi_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func TestUntagResources(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	untaggedARNs := make(map[string][]string)

	b.RegisterARNUntagger(func(_ context.Context, arn string, keys []string) (bool, error) {
		if !isARN(arn, "sqs") {
			return false, nil
		}

		untaggedARNs[arn] = keys

		return true, nil
	})

	out, err := b.UntagResources(context.Background(), &resourcegroupstaggingapi.UntagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:123:q1"},
		TagKeys:         []string{"env"},
	})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.FailedResourcesMap)
	assert.Equal(t, []string{"env"}, untaggedARNs["arn:aws:sqs:us-east-1:123:q1"])
}

// TestUntagResources_Validation covers UntagResourcesInput parameter validation: ARN list
// and tag-key list emptiness/size limits.
func TestUntagResources_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arns    []string
		keys    []string
		wantErr bool
	}{
		{
			name:    "empty_arn_list",
			arns:    []string{},
			keys:    []string{"env"},
			wantErr: true,
		},
		{
			name: "too_many_arns",
			arns: func() []string {
				arns := make([]string, 21)
				for i := range arns {
					arns[i] = "arn:aws:sqs:us-east-1:000000000000:q"
				}

				return arns
			}(),
			keys:    []string{"env"},
			wantErr: true,
		},
		{
			name:    "empty_tag_keys_list",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
			keys:    []string{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.UntagResources(context.Background(), &resourcegroupstaggingapi.UntagResourcesInput{
				ResourceARNList: tt.arns,
				TagKeys:         tt.keys,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUntagResources_ARN_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arns    []string
		wantErr bool
	}{
		{
			name:    "empty_arn",
			arns:    []string{""},
			wantErr: true,
		},
		{
			name:    "invalid_arn",
			arns:    []string{"bogus"},
			wantErr: true,
		},
		{
			name:    "valid_arn",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.UntagResources(context.Background(), &resourcegroupstaggingapi.UntagResourcesInput{
				ResourceARNList: tt.arns,
				TagKeys:         []string{"env"},
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUntagResources_TagKeys_Validation(t *testing.T) {
	t.Parallel()

	validARN := "arn:aws:sqs:us-east-1:000000000000:q1"

	tests := []struct {
		name    string
		keys    []string
		wantErr bool
	}{
		{
			name:    "empty_key_in_list",
			keys:    []string{""},
			wantErr: true,
		},
		{
			name:    "key_too_long",
			keys:    []string{strings.Repeat("k", 129)},
			wantErr: true,
		},
		{
			name:    "key_exactly_max_length",
			keys:    []string{strings.Repeat("k", 128)},
			wantErr: false,
		},
		{
			name:    "too_many_keys_51",
			keys:    makeKeys(51),
			wantErr: true,
		},
		{
			name:    "exactly_50_keys",
			keys:    makeKeys(50),
			wantErr: false,
		},
		{
			name:    "single_valid_key",
			keys:    []string{"env"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.UntagResources(context.Background(), &resourcegroupstaggingapi.UntagResourcesInput{
				ResourceARNList: []string{validARN},
				TagKeys:         tt.keys,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUntagResources_Batch(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	untaggedState := make(map[string][]string)

	b.RegisterARNUntagger(func(_ context.Context, arn string, keys []string) (bool, error) {
		if !strings.Contains(arn, "sqs") {
			return false, nil
		}

		untaggedState[arn] = append(untaggedState[arn], keys...)

		return true, nil
	})

	arns := []string{
		"arn:aws:sqs:us-east-1:000000000000:q1",
		"arn:aws:sqs:us-east-1:000000000000:q2",
	}

	out, err := b.UntagResources(context.Background(), &resourcegroupstaggingapi.UntagResourcesInput{
		ResourceARNList: arns,
		TagKeys:         []string{"env", "owner"},
	})

	require.NoError(t, err)
	assert.Empty(t, out.FailedResourcesMap)

	for _, arn := range arns {
		require.Contains(t, untaggedState, arn)
		assert.ElementsMatch(t, []string{"env", "owner"}, untaggedState[arn])
	}
}

func TestUntagResources_UnhandledARN_Returns400InMap(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out, err := b.UntagResources(context.Background(), &resourcegroupstaggingapi.UntagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:unregistered-queue"},
		TagKeys:         []string{"env"},
	})

	require.NoError(t, err)
	require.NotNil(t, out.FailedResourcesMap)
	entry := out.FailedResourcesMap["arn:aws:sqs:us-east-1:000000000000:unregistered-queue"]
	assert.Equal(t, http.StatusBadRequest, entry.StatusCode)
}
