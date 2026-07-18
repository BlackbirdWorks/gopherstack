package cloudwatchlogs_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsBackend_CreateLogGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setup           func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		createGroup     string
		wantErr         error
		wantName        string
		wantArnContains string
	}{
		{
			name:            "success",
			createGroup:     "/my/group",
			wantName:        "/my/group",
			wantArnContains: "/my/group",
		},
		{
			name: "already_exists",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "dup-group", "", "")
				require.NoError(t, err)
			},
			createGroup: "dup-group",
			wantErr:     cloudwatchlogs.ErrLogGroupAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			g, err := b.CreateLogGroup(context.Background(), tt.createGroup, "", "")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, g.LogGroupName)
			}

			if tt.wantArnContains != "" {
				assert.Contains(t, g.Arn, tt.wantArnContains)
			}
		})
	}
}

func TestCloudWatchLogsBackend_DeleteLogGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name    string
		group   string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "to-delete", "", "")
				require.NoError(t, err)
			},
			group: "to-delete",
		},
		{
			name:    "not_found",
			group:   "nonexistent",
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteLogGroup(context.Background(), tt.group)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			groups, _, err := b.DescribeLogGroups(context.Background(), "", "", 0)
			require.NoError(t, err)
			assert.Empty(t, groups)
		})
	}
}

func TestCloudWatchLogsBackend_DescribeLogGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name          string
		prefix        string
		token         string
		wantFirstName string
		limit         int
		wantCount     int
	}{
		{
			name: "prefix",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "/prod/app", "", "")
				_, _ = b.CreateLogGroup(context.Background(), "/dev/app", "", "")
			},
			prefix:        "/prod",
			wantCount:     1,
			wantFirstName: "/prod/app",
		},
		{
			name: "beyond_end",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateLogGroup(context.Background(), "/group/a", "", "")
			},
			token:     "999",
			limit:     10,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			groups, next, err := b.DescribeLogGroups(
				context.Background(),
				tt.prefix,
				tt.token,
				tt.limit,
			)
			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, groups, tt.wantCount)

			if tt.wantFirstName != "" && tt.wantCount > 0 {
				assert.Equal(t, tt.wantFirstName, groups[0].LogGroupName)
			}
		})
	}
}

func TestCloudWatchLogsBackend_DescribeLogGroups_Pagination(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	for i := range 5 {
		_, _ = b.CreateLogGroup(context.Background(), "/group/"+string(rune('a'+i)), "", "")
	}

	page1, token, err := b.DescribeLogGroups(context.Background(), "", "", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, token)

	page2, token2, err := b.DescribeLogGroups(context.Background(), "", token, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, token2)

	page3, token3, err := b.DescribeLogGroups(context.Background(), "", token2, 2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, token3)
}

func TestCloudWatchLogsBackend_PaginationToken_Opaque(t *testing.T) {
	t.Parallel()

	// Verify that emitted nextTokens are not plain decimal integers (opaque encoding).
	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	for i := range 5 {
		_, _ = b.CreateLogGroup(context.Background(), fmt.Sprintf("/grp-%d", i), "", "")
	}

	_, token, err := b.DescribeLogGroups(context.Background(), "", "", 2)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	// Token must not be a bare integer string.
	_, parseErr := fmt.Sscanf(token, "%d", new(int))
	require.Error(t, parseErr, "nextToken should be opaque (not a plain integer), got %q", token)

	// Token must be valid base64.
	_, decodeErr := base64.StdEncoding.DecodeString(token)
	assert.NoError(t, decodeErr, "nextToken should be base64-encoded, got %q", token)
}

func TestCloudWatchLogsBackend_SetRetentionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		days    *int32
		name    string
		group   string
	}{
		{
			name: "set_retention",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
				require.NoError(t, err)
			},
			group: "grp",
			days:  ptr32(30),
		},
		{
			name: "clear_retention",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
				require.NoError(t, err)
				require.NoError(t, b.SetRetentionPolicy(context.Background(), "grp", ptr32(30)))
			},
			group: "grp",
			days:  nil,
		},
		{
			name:    "group_not_found",
			group:   "nonexistent",
			days:    ptr32(7),
			wantErr: cloudwatchlogs.ErrLogGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.SetRetentionPolicy(context.Background(), tt.group, tt.days)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			// Verify the retention is reflected in DescribeLogGroups.
			groups, _, gErr := b.DescribeLogGroups(context.Background(), "", "", 100)
			require.NoError(t, gErr)
			require.Len(t, groups, 1)

			if tt.days == nil {
				assert.Nil(t, groups[0].RetentionInDays)
			} else {
				require.NotNil(t, groups[0].RetentionInDays)
				assert.Equal(t, *tt.days, *groups[0].RetentionInDays)
			}
		})
	}
}

func ptr32(v int32) *int32 {
	r := v

	return &r
}

func TestCloudWatchLogsBackend_DisassociateKmsKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr            error
		name               string
		logGroupName       string
		resourceIdentifier string
	}{
		{
			name:         "by_log_group",
			logGroupName: "my-group",
		},
		{
			name:               "by_resource_id",
			resourceIdentifier: "arn:aws:logs:::query-results",
		},
		{
			name:    "both_empty_fails",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.logGroupName != "" {
				err := b.AssociateKmsKey(tt.logGroupName, "", "arn:aws:kms:::key/1")
				require.NoError(t, err)
			}
			if tt.resourceIdentifier != "" {
				err := b.AssociateKmsKey("", tt.resourceIdentifier, "arn:aws:kms:::key/1")
				require.NoError(t, err)
			}

			err := b.DisassociateKmsKey(tt.logGroupName, tt.resourceIdentifier)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_GetLogGroupFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		setup        func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name         string
		logGroupName string
		wantFields   int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "my-group", "", "")
				require.NoError(t, err)
			},
			logGroupName: "my-group",
			wantFields:   4,
		},
		{
			name:         "not_found",
			logGroupName: "nonexistent",
			wantErr:      cloudwatchlogs.ErrLogGroupNotFound,
		},
		{
			name:    "empty_name",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			fields, err := b.GetLogGroupFields(context.Background(), tt.logGroupName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, fields, tt.wantFields)
		})
	}
}

func TestCloudWatchLogsBackend_ListLogGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		prefix  string
		wantLen int
	}{
		{
			name: "all_groups",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "grp-a", "", "")
				require.NoError(t, err)
				_, err = b.CreateLogGroup(context.Background(), "grp-b", "", "")
				require.NoError(t, err)
			},
			wantLen: 2,
		},
		{
			name: "prefix_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateLogGroup(context.Background(), "grp-a", "", "")
				require.NoError(t, err)
				_, err = b.CreateLogGroup(context.Background(), "other-b", "", "")
				require.NoError(t, err)
			},
			prefix:  "grp",
			wantLen: 1,
		},
		{
			name:    "empty",
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			groups, _, err := b.ListLogGroups(context.Background(), tt.prefix, "", 50)

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantLen)
		})
	}
}

func TestCloudWatchLogsBackend_ValidLogGroupName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		logGroupName string
	}{
		{
			name:         "valid_name",
			logGroupName: "/aws/lambda/my-function",
		},
		{
			name:         "valid_with_dots_dashes",
			logGroupName: "my.log-group_1",
		},
		{
			name:         "valid_hash",
			logGroupName: "group#1",
		},
		{
			name:         "invalid_space",
			logGroupName: "group name with spaces",
			wantErr:      cloudwatchlogs.ErrValidation,
		},
		{
			name:         "invalid_empty",
			logGroupName: "",
			wantErr:      cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), tt.logGroupName, "", "")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_ValidRetentionDays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		days    int32
	}{
		{name: "valid_7_days", days: 7},
		{name: "valid_30_days", days: 30},
		{name: "valid_365_days", days: 365},
		{name: "invalid_10_days", days: 10, wantErr: cloudwatchlogs.ErrValidation},
		{name: "invalid_999_days", days: 999, wantErr: cloudwatchlogs.ErrValidation},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "g", "", "")
			require.NoError(t, err)

			days := tt.days
			err = b.SetRetentionPolicy(context.Background(), "g", &days)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_CreateLogGroup_WithClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		name          string
		logGroupClass string
		kmsKeyID      string
		wantClass     string
		wantKmsKeyID  string
	}{
		{
			name:      "default_class_is_standard",
			wantClass: cloudwatchlogs.LogGroupClassStandard,
		},
		{
			name:          "explicit_standard",
			logGroupClass: cloudwatchlogs.LogGroupClassStandard,
			wantClass:     cloudwatchlogs.LogGroupClassStandard,
		},
		{
			name:          "infrequent_access",
			logGroupClass: cloudwatchlogs.LogGroupClassInfrequentAccess,
			wantClass:     cloudwatchlogs.LogGroupClassInfrequentAccess,
		},
		{
			name:          "invalid_class",
			logGroupClass: "NONEXISTENT",
			wantErr:       cloudwatchlogs.ErrValidation,
		},
		{
			name:         "with_kms_key",
			kmsKeyID:     "arn:aws:kms:us-east-1:123:key/abc",
			wantClass:    cloudwatchlogs.LogGroupClassStandard,
			wantKmsKeyID: "arn:aws:kms:us-east-1:123:key/abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			g, err := b.CreateLogGroup(
				context.Background(),
				"/test/group",
				tt.logGroupClass,
				tt.kmsKeyID,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantClass, g.LogGroupClass)
			assert.Equal(t, tt.wantKmsKeyID, g.KmsKeyID)
		})
	}
}

func TestCloudWatchLogsBackend_DescribeLogGroups_ReturnsClass(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(
		context.Background(),
		"/ia",
		cloudwatchlogs.LogGroupClassInfrequentAccess,
		"",
	)
	require.NoError(t, err)
	_, err = b.CreateLogGroup(
		context.Background(),
		"/std",
		cloudwatchlogs.LogGroupClassStandard,
		"",
	)
	require.NoError(t, err)

	groups, _, err := b.DescribeLogGroups(context.Background(), "", "", 50)
	require.NoError(t, err)
	require.Len(t, groups, 2)

	classMap := make(map[string]string)
	for _, g := range groups {
		classMap[g.LogGroupName] = g.LogGroupClass
	}

	assert.Equal(t, cloudwatchlogs.LogGroupClassInfrequentAccess, classMap["/ia"])
	assert.Equal(t, cloudwatchlogs.LogGroupClassStandard, classMap["/std"])
}

func TestLogGroupDeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "default_is_false",
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.False(t, b.IsLogGroupDeletionProtected("/aws/lambda/fn"))
			},
		},
		{
			name: "enable_protection",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.SetLogGroupDeletionProtection("/aws/lambda/fn", true)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.True(t, b.IsLogGroupDeletionProtected("/aws/lambda/fn"))
			},
		},
		{
			name: "disable_protection",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.SetLogGroupDeletionProtection("/aws/lambda/fn", true)
				require.NoError(t, err)
				err = b.SetLogGroupDeletionProtection("/aws/lambda/fn", false)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.False(t, b.IsLogGroupDeletionProtected("/aws/lambda/fn"))
			},
		},
		{
			name: "groups_are_independent",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.SetLogGroupDeletionProtection("/grp-a", true)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.True(t, b.IsLogGroupDeletionProtected("/grp-a"))
				assert.False(t, b.IsLogGroupDeletionProtected("/grp-b"))
			},
		},
		{
			name: "empty_identifier_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.SetLogGroupDeletionProtection("", true)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

func TestSetRetentionPolicy_ZeroDays_BackendReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		days int32
	}{
		{name: "zero_is_invalid", days: 0},
		{name: "negative_is_invalid", days: -5},
		{name: "arbitrary_non_allowed_value", days: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
			require.NoError(t, err)

			days := tt.days
			err = b.SetRetentionPolicy(context.Background(), "grp", &days)
			require.ErrorIs(t, err, cloudwatchlogs.ErrValidation,
				"days=%d must be rejected with ErrValidation", tt.days)
		})
	}
}
