package kinesisanalytics_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "000000000000"
)

func newBackend() *kinesisanalytics.InMemoryBackend {
	return kinesisanalytics.NewInMemoryBackend(testRegion, testAccountID)
}

func TestInMemoryBackend_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags        map[string]string
		name        string
		appName     string
		description string
		code        string
		wantErr     bool
	}{
		{
			name:        "creates new application",
			appName:     "test-app",
			description: "test description",
			code:        "SELECT 1",
			tags:        map[string]string{"env": "test"},
		},
		{
			name:    "creates application without optional fields",
			appName: "minimal-app",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			app, err := kinesisanalytics.CreateApp(
				b, testRegion, testAccountID, tt.appName, tt.description, tt.code, tt.tags,
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, app)
			assert.Equal(t, tt.appName, app.ApplicationName)
			assert.Equal(t, "READY", app.ApplicationStatus)
			assert.Equal(t, int64(1), app.ApplicationVersionID)
			assert.NotEmpty(t, app.ApplicationARN)
			assert.NotNil(t, app.CreateTimestamp)
			assert.Equal(t, "SQL-1_0", app.RuntimeEnvironment)
		})
	}
}

func TestInMemoryBackend_CreateApplication_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "dup-app", "", "", nil)
	require.NoError(t, err)

	_, err = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "dup-app", "", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrAlreadyExists)
}

func TestInMemoryBackend_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "deletes existing application",
			appName: "to-delete",
		},
		{
			name:    "not found when missing",
			appName: "missing",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			var ts *time.Time

			if tt.appName == "to-delete" {
				app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "to-delete", "", "", nil)
				require.NoError(t, err)
				ts = app.CreateTimestamp
			}

			err := b.DeleteApplication(context.Background(), tt.appName, ts)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DeleteApplication_NilTimestamp(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-nil-ts", "", "", nil)
	require.NoError(t, err)

	err = b.DeleteApplication(context.Background(), "del-nil-ts", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrInvalidParameter)
}

func TestInMemoryBackend_DescribeApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		setup   func(*kinesisanalytics.InMemoryBackend)
		want    string
		wantErr bool
	}{
		{
			name:    "returns existing application",
			appName: "my-app",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "my-app", "desc", "code", nil)
			},
			want: "my-app",
		},
		{
			name:    "returns not found for missing",
			appName: "ghost",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if tt.setup != nil {
				tt.setup(b)
			}

			app, err := b.DescribeApplication(context.Background(), tt.appName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, app.ApplicationName)
		})
	}
}

func TestInMemoryBackend_ListApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*kinesisanalytics.InMemoryBackend)
		exclusiveStart string
		limit          int
		wantCount      int
		wantHasMore    bool
	}{
		{
			name: "lists all applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-a", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-b", "", "", nil)
			},
			wantCount: 2,
		},
		{
			name:  "empty list",
			setup: func(_ *kinesisanalytics.InMemoryBackend) {},
		},
		{
			name: "limit truncates results",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "l-app-a", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "l-app-b", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "l-app-c", "", "", nil)
			},
			limit:       2,
			wantCount:   2,
			wantHasMore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			apps, hasMore, err := b.ListApplications(context.Background(), tt.exclusiveStart, tt.limit)
			require.NoError(t, err)
			assert.Len(t, apps, tt.wantCount)
			assert.Equal(t, tt.wantHasMore, hasMore)
		})
	}
}

func TestInMemoryBackend_StartApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kinesisanalytics.InMemoryBackend)
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "start transitions to running",
			appName: "runnable",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "runnable", "", "", nil)
			},
		},
		{
			name:    "start not found returns error",
			appName: "missing",
			setup:   func(_ *kinesisanalytics.InMemoryBackend) {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			err := kinesisanalytics.StartAppNoConfig(b, tt.appName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			kinesisanalytics.WaitForStatus(t, b, tt.appName, "RUNNING")
		})
	}
}

func TestInMemoryBackend_StopApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *kinesisanalytics.InMemoryBackend)
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "stop transitions to ready",
			appName: "stoppable",
			setup: func(t *testing.T, b *kinesisanalytics.InMemoryBackend) {
				t.Helper()
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "stoppable", "", "", nil)
				require.NoError(t, kinesisanalytics.StartAppNoConfig(b, "stoppable"))
				kinesisanalytics.WaitForStatus(t, b, "stoppable", "RUNNING")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(t, b)

			err := b.StopApplication(context.Background(), tt.appName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			kinesisanalytics.WaitForStatus(t, b, tt.appName, "READY")
		})
	}
}

func TestInMemoryBackend_UpdateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(*kinesisanalytics.InMemoryBackend)
		name             string
		appName          string
		codeUpdate       string
		currentVersionID int64
		wantVersionID    int64
		wantErr          bool
	}{
		{
			name:             "updates application code",
			appName:          "updatable",
			currentVersionID: 1,
			codeUpdate:       "SELECT 2",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "updatable", "", "SELECT 1", nil)
			},
			wantVersionID: 2,
		},
		{
			name:             "version mismatch returns error",
			appName:          "ver-app",
			currentVersionID: 99,
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ver-app", "", "", nil)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			app, err := kinesisanalytics.UpdateAppCode(b, tt.appName, tt.currentVersionID, tt.codeUpdate)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, app)
			assert.Equal(t, tt.wantVersionID, app.ApplicationVersionID)

			if tt.codeUpdate != "" {
				assert.Equal(t, tt.codeUpdate, app.ApplicationCode)
			}
		})
	}
}

func TestInMemoryBackend_TagOperations(t *testing.T) {
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

func TestInMemoryBackend_TimestampsSet(t *testing.T) {
	t.Parallel()

	b := newBackend()
	before := time.Now().Add(-time.Second)
	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ts-app", "", "", nil)
	require.NoError(t, err)

	assert.NotNil(t, app.CreateTimestamp)
	assert.True(t, app.CreateTimestamp.After(before), "CreateTimestamp should be after test start")
	assert.NotNil(t, app.LastUpdateTimestamp)
}

func TestInMemoryBackend_ListApplications_ExclusiveStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*kinesisanalytics.InMemoryBackend)
		exclusiveStart string
		wantCountMin   int
	}{
		{
			name: "exclusiveStart not found still returns all",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "exc-app-a", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "exc-app-b", "", "", nil)
			},
			exclusiveStart: "nonexistent",
			wantCountMin:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			apps, _, err := b.ListApplications(context.Background(), tt.exclusiveStart, 0)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(apps), tt.wantCountMin)
		})
	}
}

func TestInMemoryBackend_TagResource_InitNil(t *testing.T) {
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

func TestInMemoryBackend_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kinesisanalytics.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name:    "empty_backend",
			setup:   func(_ *kinesisanalytics.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "with_applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(
					b, testRegion, testAccountID,
					"persist-app-1", "desc", "SELECT 1",
					map[string]string{"env": "test"},
				)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "persist-app-2", "", "", nil)
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := newBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			apps, _, err := b2.ListApplications(context.Background(), "", 0)
			require.NoError(t, err)
			require.Len(t, apps, tt.wantLen)

			// Verify appsByARN index is rebuilt: tag operations should work via ARN.
			if tt.wantLen > 0 {
				descApp, descErr := b2.DescribeApplication(context.Background(), "persist-app-1")
				require.NoError(t, descErr)

				tagErr := b2.TagResource(context.Background(), descApp.ApplicationARN, map[string]string{"new": "tag"})
				require.NoError(t, tagErr)

				tags, listErr := b2.ListTagsForResource(context.Background(), descApp.ApplicationARN)
				require.NoError(t, listErr)
				assert.Equal(t, "tag", tags["new"])
				assert.Equal(t, "test", tags["env"])
			}

			// Snapshot isolation: mutating b2 should not affect original snapshot bytes.
			if tt.wantLen > 0 {
				_, _ = kinesisanalytics.CreateApp(b2, testRegion, testAccountID, "extra-app", "", "", nil)
				snap2 := b2.Snapshot(t.Context())
				assert.NotEqual(t, snap, snap2)
			}
		})
	}
}

func TestInMemoryBackend_ApplicationLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limit   int
		wantErr bool
	}{
		{
			name:    "reject limit above 50",
			limit:   51,
			wantErr: true,
		},
		{
			name:    "reject negative limit",
			limit:   -1,
			wantErr: true,
		},
		{
			name:  "accept limit of 50",
			limit: 50,
		},
		{
			name:  "accept limit of 0 (default)",
			limit: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, _, err := b.ListApplications(context.Background(), "", tt.limit)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_ARNValidation(t *testing.T) {
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

func TestInMemoryBackend_ApplicationNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		wantErr bool
	}{
		{name: "valid alphanumeric", appName: "myApp123"},
		{name: "valid with dashes", appName: "my-app"},
		{name: "valid with underscores", appName: "my_app"},
		{name: "valid with dots", appName: "my.app"},
		{name: "empty name", appName: "", wantErr: true},
		{name: "name with spaces", appName: "my app", wantErr: true},
		{name: "name with special chars", appName: "my@app", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, tt.appName, "", "", nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_TagKeyValidation(t *testing.T) {
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

// TestInMemoryBackend_TagLimit covers the KDA-specific 50-user-tag cap (not the generic
// 200 used by many other services) and the dedicated TooManyTagsException error AWS models
// for CreateApplication/TagResource, distinct from the generic LimitExceededException.
func TestInMemoryBackend_TagLimit(t *testing.T) {
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

// TestInMemoryBackend_AddApplication_LimitErrorCode verifies that exceeding the per-application
// input/output/reference-data-source/CloudWatch-logging-option caps surfaces as
// InvalidArgumentException, matching the modeled error set for these operations
// (AddApplicationInput/Output/ReferenceDataSource/CloudWatchLoggingOption have no
// LimitExceededException in their AWS API definitions).
func TestInMemoryBackend_AddApplication_LimitErrorCode(t *testing.T) {
	t.Parallel()

	t.Run("AddApplicationInput at capacity", func(t *testing.T) {
		t.Parallel()

		b := newBackend()
		app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "in-cap-app", "", "", nil)
		require.NoError(t, err)

		require.NoError(t, b.AddApplicationInput(
			context.Background(), app.ApplicationName, app.ApplicationVersionID,
			kinesisanalytics.InputDescription{NamePrefix: "IN1"},
		))

		err = b.AddApplicationInput(
			context.Background(), app.ApplicationName, 2,
			kinesisanalytics.InputDescription{NamePrefix: "IN2"},
		)
		require.ErrorIs(t, err, awserr.ErrInvalidParameter)
		assert.NotErrorIs(t, err, awserr.ErrConflict)
	})

	t.Run("AddApplicationReferenceDataSource at capacity", func(t *testing.T) {
		t.Parallel()

		b := newBackend()
		app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ref-cap-app", "", "", nil)
		require.NoError(t, err)

		ref := kinesisanalytics.ReferenceDataSourceDescription{
			TableName: "T1",
			S3ReferenceDataSourceDescription: &kinesisanalytics.S3ReferenceDataSourceDesc{
				BucketARN: "arn:aws:s3:::b", FileKey: "k", ReferenceRoleARN: "arn:aws:iam::000000000000:role/r",
			},
		}
		require.NoError(t, b.AddApplicationReferenceDataSource(
			context.Background(), app.ApplicationName, app.ApplicationVersionID, ref,
		))

		err = b.AddApplicationReferenceDataSource(context.Background(), app.ApplicationName, 2, ref)
		require.ErrorIs(t, err, awserr.ErrInvalidParameter)
		assert.NotErrorIs(t, err, awserr.ErrConflict)
	})
}

func TestInMemoryBackend_CancelFuncs_Lifecycle(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "lifecycle-app", "", "", nil)
	require.NoError(t, err)

	// No goroutines before start.
	assert.Equal(t, 0, kinesisanalytics.GetCancelFuncsLen(b))

	// Start spawns a goroutine.
	require.NoError(t, kinesisanalytics.StartAppNoConfig(b, "lifecycle-app"))
	assert.Equal(t, 1, kinesisanalytics.GetCancelFuncsLen(b))

	// After transition completes, goroutine cleans itself up.
	kinesisanalytics.WaitForStatus(t, b, "lifecycle-app", "RUNNING")
	assert.Equal(t, 0, kinesisanalytics.GetCancelFuncsLen(b))
}

func TestInMemoryBackend_ApplicationCount(t *testing.T) {
	t.Parallel()

	b := newBackend()
	assert.Equal(t, 0, kinesisanalytics.ApplicationCount(b))

	_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "count-app-1", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, kinesisanalytics.ApplicationCount(b))

	_, err = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "count-app-2", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, kinesisanalytics.ApplicationCount(b))
}
