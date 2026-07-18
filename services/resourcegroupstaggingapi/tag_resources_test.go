package resourcegroupstaggingapi_test

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// isARN is a helper used in test code to check service prefix in an ARN. Shared with
// untag_resources_test.go.
func isARN(arn, service string) bool {
	// arn:aws:<service>:...
	parts := splitARN(arn)

	return len(parts) >= 3 && parts[2] == service
}

func splitARN(arn string) []string {
	out := make([]string, 0)
	start := 0

	for i := range len(arn) {
		if arn[i] == ':' {
			out = append(out, arn[start:i])
			start = i + 1
		}
	}

	out = append(out, arn[start:])

	return out
}

func TestTagResources_Handled(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	taggedARNs := make(map[string]map[string]string)

	b.RegisterARNTagger(func(_ context.Context, arn string, tags map[string]string) (bool, error) {
		if !isARN(arn, "sqs") {
			return false, nil
		}

		taggedARNs[arn] = tags

		return true, nil
	})

	out, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:123:q1"},
		Tags:            map[string]string{"env": "test"},
	})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.FailedResourcesMap)
	assert.Equal(t, map[string]string{"env": "test"}, taggedARNs["arn:aws:sqs:us-east-1:123:q1"])
}

func TestTagResources_Unhandled(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:123:q1"},
		Tags:            map[string]string{"env": "test"},
	})

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Len(t, out.FailedResourcesMap, 1)
	assert.Contains(t, out.FailedResourcesMap, "arn:aws:sqs:us-east-1:123:q1")
}

// TestTagResources_Validation covers TagResourcesInput parameter validation: ARN list and
// tag-map size limits plus per-tag key/value constraints.
func TestTagResources_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags    map[string]string
		name    string
		arns    []string
		wantErr bool
	}{
		{
			name:    "empty_arn_list",
			arns:    []string{},
			tags:    map[string]string{"env": "test"},
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
			tags:    map[string]string{"env": "test"},
			wantErr: true,
		},
		{
			name:    "empty_tags",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
			tags:    map[string]string{},
			wantErr: true,
		},
		{
			name: "too_many_tags",
			arns: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
			tags: func() map[string]string {
				tags := make(map[string]string, 51)
				for i := range 51 {
					tags[fmt.Sprintf("key%d", i)] = "v"
				}

				return tags
			}(),
			wantErr: true,
		},
		{
			name:    "empty_tag_key",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
			tags:    map[string]string{"": "value"},
			wantErr: true,
		},
		{
			name:    "tag_key_too_long",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
			tags:    map[string]string{strings.Repeat("k", 129): "v"},
			wantErr: true,
		},
		{
			name:    "tag_value_too_long",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
			tags:    map[string]string{"key": strings.Repeat("v", 257)},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
				ResourceARNList: tt.arns,
				Tags:            tt.tags,
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

func TestTagResources_ExactlyMaxARNs(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	arns := make([]string, 20)

	for i := range arns {
		arns[i] = "arn:aws:sqs:us-east-1:000000000000:q" + strings.Repeat("x", i+1)
	}

	// No taggers registered → all 20 fail, but no validation error.
	out, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: arns,
		Tags:            map[string]string{"env": "test"},
	})

	require.NoError(t, err)
	assert.Len(t, out.FailedResourcesMap, 20)
}

func TestTagResources_ARN_Validation(t *testing.T) {
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
			name:    "invalid_arn_no_colons",
			arns:    []string{"not-an-arn"},
			wantErr: true,
		},
		{
			name:    "valid_sqs_arn",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:my-queue"},
			wantErr: false,
		},
		{
			name:    "valid_ec2_arn",
			arns:    []string{"arn:aws:ec2:us-east-1:000000000000:instance/i-1234"},
			wantErr: false,
		},
		{
			name:    "mixed_valid_and_invalid",
			arns:    []string{"arn:aws:sqs:us-east-1:000000000000:q1", "bad"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
				ResourceARNList: tt.arns,
				Tags:            map[string]string{"env": "test"},
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

func TestTagResources_AwsReservedPrefix_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{name: "aws_colon_prefix", tags: map[string]string{"aws:reserved": "value"}},
		{name: "aws_colon_prefix_long", tags: map[string]string{"aws:ec2:autoscaling": "yes"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
				ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
				Tags:            tt.tags,
			})

			require.Error(t, err)
			require.ErrorIs(t, err, resourcegroupstaggingapi.ErrValidation)
			assert.Contains(t, err.Error(), "reserved prefix")
		})
	}
}

func TestTagResources_NormalTagKey_OK(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	arn := "arn:aws:sqs:us-east-1:000000000000:q1"
	handled := false
	b.RegisterARNTagger(func(_ context.Context, a string, _ map[string]string) (bool, error) {
		if a == arn {
			handled = true

			return true, nil
		}

		return false, nil
	})

	_, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{arn},
		Tags:            map[string]string{"env": "prod", "team": "platform"},
	})

	require.NoError(t, err)
	assert.True(t, handled)
}

func TestTagResources_Batch(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	taggedState := make(map[string]map[string]string)

	b.RegisterARNTagger(func(_ context.Context, arn string, tags map[string]string) (bool, error) {
		if !strings.Contains(arn, "sqs") {
			return false, nil
		}

		taggedState[arn] = maps.Clone(tags)

		return true, nil
	})

	arns := []string{
		"arn:aws:sqs:us-east-1:000000000000:q1",
		"arn:aws:sqs:us-east-1:000000000000:q2",
		"arn:aws:sqs:us-east-1:000000000000:q3",
	}

	out, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: arns,
		Tags:            map[string]string{"env": "prod", "owner": "team-a"},
	})

	require.NoError(t, err)
	assert.Empty(t, out.FailedResourcesMap)

	for _, arn := range arns {
		require.Contains(t, taggedState, arn)
		assert.Equal(t, "prod", taggedState[arn]["env"])
		assert.Equal(t, "team-a", taggedState[arn]["owner"])
	}
}

func TestTagResources_UnhandledARN_Returns400InMap(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	out, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:unregistered-queue"},
		Tags:            map[string]string{"key": "val"},
	})

	require.NoError(t, err)
	require.NotNil(t, out.FailedResourcesMap)
	entry := out.FailedResourcesMap["arn:aws:sqs:us-east-1:000000000000:unregistered-queue"]
	assert.Equal(t, http.StatusBadRequest, entry.StatusCode)
	assert.Equal(t, "InvalidParameterException", entry.ErrorCode)
}

func TestTagResources_TaggerInternalError_Returns500InMap(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	arn := "arn:aws:sqs:us-east-1:000000000000:q1"
	b.RegisterARNTagger(func(_ context.Context, a string, _ map[string]string) (bool, error) {
		if a == arn {
			return true, assert.AnError
		}

		return false, nil
	})

	out, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{arn},
		Tags:            map[string]string{"key": "val"},
	})

	require.NoError(t, err)
	require.NotNil(t, out.FailedResourcesMap)
	entry := out.FailedResourcesMap[arn]
	assert.Equal(t, http.StatusInternalServerError, entry.StatusCode)
	assert.Equal(t, "InternalServiceException", entry.ErrorCode)
}

// TestTagResources_DeepCopyTags verifies that TagResources deep-copies the caller's tag
// map before handing it to registered taggers: mutations flow in neither direction after
// the call -- mutating the caller's original map must not affect what the tagger
// received, and mutating what the tagger received must not affect the caller's original.
func TestTagResources_DeepCopyTags(t *testing.T) {
	t.Parallel()

	t.Run("original_mutation_after_call_does_not_affect_tagger_copy", func(t *testing.T) {
		t.Parallel()

		b := newBackend(t)

		var receivedTags map[string]string

		b.RegisterARNTagger(func(_ context.Context, _ string, tags map[string]string) (bool, error) {
			receivedTags = tags

			return true, nil
		})

		originalTags := map[string]string{"env": "prod"}
		_, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
			ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
			Tags:            originalTags,
		})
		require.NoError(t, err)

		originalTags["env"] = "mutated"

		require.NotNil(t, receivedTags)
		assert.Equal(t, "prod", receivedTags["env"])
	})

	t.Run("tagger_copy_mutation_after_call_does_not_affect_original", func(t *testing.T) {
		t.Parallel()

		b := newBackend(t)
		var received map[string]string

		b.RegisterARNTagger(func(_ context.Context, _ string, tags map[string]string) (bool, error) {
			received = tags

			return true, nil
		})

		original := map[string]string{"env": "test"}

		out, err := b.TagResources(context.Background(), &resourcegroupstaggingapi.TagResourcesInput{
			ResourceARNList: []string{"arn:aws:sqs:us-east-1:000000000000:q1"},
			Tags:            original,
		})

		require.NoError(t, err)
		require.NotNil(t, out)

		received["extra"] = "modified"
		assert.NotContains(t, original, "extra", "original tags map must not be mutated by tagger")
	})
}
