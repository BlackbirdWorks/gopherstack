package kinesisanalytics_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

func TestTagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*kinesisanalytics.InMemoryBackend) string
		tags     map[string]string
		wantTags map[string]string
		name     string
		op       string
		tagKeys  []string
		wantErr  bool
	}{
		{
			name: "list tags returns all tags",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := kinesisanalytics.CreateApp(
					b, testRegion, testAccountID, "tagged-app", "", "",
					map[string]string{"key": "val"},
				)

				return app.ApplicationARN
			},
			op:       "list",
			wantTags: map[string]string{"key": "val"},
		},
		{
			name: "tag resource adds tags",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "tag-add-app", "", "", nil)

				return app.ApplicationARN
			},
			op:       "tag",
			tags:     map[string]string{"new": "tag"},
			wantTags: map[string]string{"new": "tag"},
		},
		{
			name: "untag resource removes tags",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := kinesisanalytics.CreateApp(
					b, testRegion, testAccountID, "untag-app", "", "",
					map[string]string{"remove": "me", "keep": "this"},
				)

				return app.ApplicationARN
			},
			op:       "untag",
			tagKeys:  []string{"remove"},
			wantTags: map[string]string{"keep": "this"},
		},
		{
			name: "list tags not found",
			setup: func(_ *kinesisanalytics.InMemoryBackend) string {
				return "arn:aws:kinesisanalytics:us-east-1:000000000000:application/nonexistent"
			},
			op:      "list",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			resourceARN := tt.setup(b)

			var err error

			switch tt.op {
			case "list":
				tags, listErr := b.ListTagsForResource(context.Background(), resourceARN)
				err = listErr

				if !tt.wantErr {
					require.NoError(t, listErr)
					assert.Equal(t, tt.wantTags, tags)
				}
			case "tag":
				err = b.TagResource(context.Background(), resourceARN, tt.tags)

				if !tt.wantErr {
					require.NoError(t, err)
					tags, _ := b.ListTagsForResource(context.Background(), resourceARN)
					assert.Equal(t, tt.wantTags, tags)
				}
			case "untag":
				err = b.UntagResource(context.Background(), resourceARN, tt.tagKeys)

				if !tt.wantErr {
					require.NoError(t, err)
					tags, _ := b.ListTagsForResource(context.Background(), resourceARN)
					assert.Equal(t, tt.wantTags, tags)
				}
			}

			if tt.wantErr {
				require.Error(t, err)
			}
		})
	}
}

func TestTagResource_InitNil(t *testing.T) {
	t.Parallel()

	b := newBackend()
	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "nil-tag-app", "", "", nil)
	require.NoError(t, err)

	err = b.TagResource(context.Background(), app.ApplicationARN, map[string]string{"key": "val"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), app.ApplicationARN)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestARNValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arn     string
		wantErr bool
	}{
		{
			name: "valid ARN",
			arn:  "arn:aws:kinesisanalytics:us-east-1:000000000000:application/my-app",
		},
		{
			name:    "wrong service",
			arn:     "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			wantErr: true,
		},
		{
			name:    "wrong region",
			arn:     "arn:aws:kinesisanalytics:eu-west-1:000000000000:application/my-app",
			wantErr: true,
		},
		{
			name:    "wrong account",
			arn:     "arn:aws:kinesisanalytics:us-east-1:111111111111:application/my-app",
			wantErr: true,
		},
		{
			name:    "not an application resource",
			arn:     "arn:aws:kinesisanalytics:us-east-1:000000000000:stream/my-app",
			wantErr: true,
		},
		{
			name:    "malformed ARN",
			arn:     "not-an-arn",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.ListTagsForResource(context.Background(), tt.arn)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			// Valid ARN shape but no app → ErrNotFound, not ErrInvalidParameter
			require.Error(t, err)
			assert.ErrorIs(t, err, awserr.ErrNotFound)
		})
	}
}

func TestTagKeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags    map[string]string
		name    string
		wantErr bool
	}{
		{
			name: "valid tags",
			tags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name:    "empty tag key",
			tags:    map[string]string{"": "value"},
			wantErr: true,
		},
		{
			name:    "aws: prefixed key",
			tags:    map[string]string{"aws:reserved": "value"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "tag-valid-app", "", "", nil)
			require.NoError(t, err)

			err = b.TagResource(context.Background(), app.ApplicationARN, tt.tags)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestTagLimit covers the KDA-specific 50-user-tag cap (not the generic
// 200 used by many other services) and the dedicated TooManyTagsException error AWS models
// for CreateApplication/TagResource, distinct from the generic LimitExceededException.
func TestTagLimit(t *testing.T) {
	t.Parallel()

	manyTags := func(n int) map[string]string {
		tags := make(map[string]string, n)
		for i := range n {
			tags[fmt.Sprintf("key%d", i)] = "value"
		}

		return tags
	}

	t.Run("CreateApplication accepts exactly the 50-tag cap", func(t *testing.T) {
		t.Parallel()

		b := newBackend()
		_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "tag-cap-app", "", "", manyTags(50))
		require.NoError(t, err)
	})

	t.Run("CreateApplication rejects more than 50 tags", func(t *testing.T) {
		t.Parallel()

		b := newBackend()
		_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "tag-over-app", "", "", manyTags(51))
		require.ErrorIs(t, err, kinesisanalytics.ErrTooManyTags)
		assert.NotErrorIs(t, err, awserr.ErrConflict, "must not also match the generic LimitExceededException sentinel")
	})

	t.Run("CreateApplication validates tag keys (previously skipped entirely)", func(t *testing.T) {
		t.Parallel()

		b := newBackend()
		_, err := kinesisanalytics.CreateApp(
			b, testRegion, testAccountID, "tag-invalid-app", "", "", map[string]string{"aws:reserved": "v"},
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, awserr.ErrInvalidParameter)
	})

	t.Run("TagResource rejects exceeding the cap", func(t *testing.T) {
		t.Parallel()

		b := newBackend()
		app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "tag-add-over-app", "", "", nil)
		require.NoError(t, err)

		err = b.TagResource(context.Background(), app.ApplicationARN, manyTags(51))
		require.Error(t, err)
		assert.ErrorIs(t, err, kinesisanalytics.ErrTooManyTags)
	})
}

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*kinesisanalytics.InMemoryBackend) string
		tags       []map[string]string
		tagKeys    []string
		wantStatus int
	}{
		{
			name: "list tags returns tags",
			op:   "ListTagsForResource",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := kinesisanalytics.CreateApp(
					b,
					testRegion,
					testAccountID,
					"tag-app",
					"",
					"",
					map[string]string{"env": "test"},
				)

				return app.ApplicationARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "tag resource succeeds",
			op:   "TagResource",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "tag2-app", "", "", nil)

				return app.ApplicationARN
			},
			tags:       []map[string]string{{"Key": "new", "Value": "val"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "untag resource succeeds",
			op:   "UntagResource",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := kinesisanalytics.CreateApp(
					b,
					testRegion,
					testAccountID,
					"untag-app",
					"",
					"",
					map[string]string{"remove": "me"},
				)

				return app.ApplicationARN
			},
			tagKeys:    []string{"remove"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			resourceARN := tt.setup(b)

			var input map[string]any

			switch tt.op {
			case "ListTagsForResource":
				input = map[string]any{"ResourceARN": resourceARN}
			case "TagResource":
				input = map[string]any{"ResourceARN": resourceARN, "Tags": tt.tags}
			case "UntagResource":
				input = map[string]any{"ResourceARN": resourceARN, "TagKeys": tt.tagKeys}
			}

			rec := doRequest(t, h, tt.op, input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_MissingResourceARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "list tags missing ARN",
			op:         "ListTagsForResource",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag resource missing ARN",
			op:         "TagResource",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "untag resource missing ARN",
			op:         "UntagResource",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestSortedListTagsForResource verifies tags are returned in alphabetical key order.
func TestSortedListTagsForResource(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "sorted-tags", "", "", map[string]string{
		"z": "last",
		"a": "first",
		"m": "mid",
	})

	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": app.ApplicationARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	require.Len(t, resp.Tags, 3)
	assert.Equal(t, "a", resp.Tags[0].Key)
	assert.Equal(t, "m", resp.Tags[1].Key)
	assert.Equal(t, "z", resp.Tags[2].Key)
}

// TestTagResource_NotFound returns 404 for unknown ARN.
func TestTagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": "arn:aws:kinesisanalytics:us-east-1:000000000000:application/nope",
		"Tags":        []map[string]any{{"Key": "k", "Value": "v"}},
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestUntagResource_NotFound returns 404 for unknown ARN.
func TestUntagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": "arn:aws:kinesisanalytics:us-east-1:000000000000:application/nope",
		"TagKeys":     []string{"k"},
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}
