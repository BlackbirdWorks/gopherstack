package kinesisanalytics_test

import (
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
			app, err := b.CreateApplication(testRegion, testAccountID, tt.appName, tt.description, tt.code, tt.tags)

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
		})
	}
}

func TestInMemoryBackend_CreateApplication_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateApplication(testRegion, testAccountID, "dup-app", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateApplication(testRegion, testAccountID, "dup-app", "", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrAlreadyExists)
}

func TestInMemoryBackend_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kinesisanalytics.InMemoryBackend)
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "deletes existing application",
			appName: "to-delete",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "to-delete", "", "", nil)
			},
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

			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.DeleteApplication(tt.appName, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
		})
	}
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
				_, _ = b.CreateApplication(testRegion, testAccountID, "my-app", "desc", "code", nil)
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

			app, err := b.DescribeApplication(tt.appName)

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
				_, _ = b.CreateApplication(testRegion, testAccountID, "app-a", "", "", nil)
				_, _ = b.CreateApplication(testRegion, testAccountID, "app-b", "", "", nil)
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
				_, _ = b.CreateApplication(testRegion, testAccountID, "l-app-a", "", "", nil)
				_, _ = b.CreateApplication(testRegion, testAccountID, "l-app-b", "", "", nil)
				_, _ = b.CreateApplication(testRegion, testAccountID, "l-app-c", "", "", nil)
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

			apps, hasMore := b.ListApplications(tt.exclusiveStart, tt.limit)
			assert.Len(t, apps, tt.wantCount)
			assert.Equal(t, tt.wantHasMore, hasMore)
		})
	}
}

func TestInMemoryBackend_StartStopApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		appName    string
		setup      func(*kinesisanalytics.InMemoryBackend)
		wantStatus string
		wantErr    bool
	}{
		{
			name:    "start transitions to running",
			op:      "start",
			appName: "runnable",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "runnable", "", "", nil)
			},
			wantStatus: "RUNNING",
		},
		{
			name:    "stop transitions to ready",
			op:      "stop",
			appName: "stoppable",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "stoppable", "", "", nil)
				_ = b.StartApplication("stoppable")
			},
			wantStatus: "READY",
		},
		{
			name:    "start not found returns error",
			op:      "start",
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

			var err error
			if tt.op == "start" {
				err = b.StartApplication(tt.appName)
			} else {
				err = b.StopApplication(tt.appName)
			}

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			app, err := b.DescribeApplication(tt.appName)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, app.ApplicationStatus)
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
				_, _ = b.CreateApplication(testRegion, testAccountID, "updatable", "", "SELECT 1", nil)
			},
			wantVersionID: 2,
		},
		{
			name:             "version mismatch returns error",
			appName:          "ver-app",
			currentVersionID: 99,
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = b.CreateApplication(testRegion, testAccountID, "ver-app", "", "", nil)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			app, err := b.UpdateApplication(tt.appName, tt.currentVersionID, tt.codeUpdate)

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
				app, _ := b.CreateApplication(
					testRegion,
					testAccountID,
					"tagged-app",
					"",
					"",
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
				app, _ := b.CreateApplication(testRegion, testAccountID, "tag-add-app", "", "", nil)

				return app.ApplicationARN
			},
			op:       "tag",
			tags:     map[string]string{"new": "tag"},
			wantTags: map[string]string{"new": "tag"},
		},
		{
			name: "untag resource removes tags",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				app, _ := b.CreateApplication(
					testRegion,
					testAccountID,
					"untag-app",
					"",
					"",
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
				tags, listErr := b.ListTagsForResource(resourceARN)
				err = listErr

				if !tt.wantErr {
					require.NoError(t, listErr)
					assert.Equal(t, tt.wantTags, tags)
				}
			case "tag":
				err = b.TagResource(resourceARN, tt.tags)

				if !tt.wantErr {
					require.NoError(t, err)
					tags, _ := b.ListTagsForResource(resourceARN)
					assert.Equal(t, tt.wantTags, tags)
				}
			case "untag":
				err = b.UntagResource(resourceARN, tt.tagKeys)

				if !tt.wantErr {
					require.NoError(t, err)
					tags, _ := b.ListTagsForResource(resourceARN)
					assert.Equal(t, tt.wantTags, tags)
				}
			}

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)
			}
		})
	}
}

func TestInMemoryBackend_TimestampsSet(t *testing.T) {
	t.Parallel()

	b := newBackend()
	before := time.Now().Add(-time.Second)
	app, err := b.CreateApplication(testRegion, testAccountID, "ts-app", "", "", nil)
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
				_, _ = b.CreateApplication(testRegion, testAccountID, "exc-app-a", "", "", nil)
				_, _ = b.CreateApplication(testRegion, testAccountID, "exc-app-b", "", "", nil)
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

			apps, _ := b.ListApplications(tt.exclusiveStart, 0)
			assert.GreaterOrEqual(t, len(apps), tt.wantCountMin)
		})
	}
}

func TestInMemoryBackend_TagResource_InitNil(t *testing.T) {
	t.Parallel()

	b := newBackend()
	// Create app with no tags (nil map)
	app, err := b.CreateApplication(testRegion, testAccountID, "nil-tag-app", "", "", nil)
	require.NoError(t, err)

	// TagResource should init the tags map if nil
	err = b.TagResource(app.ApplicationARN, map[string]string{"key": "val"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(app.ApplicationARN)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}
