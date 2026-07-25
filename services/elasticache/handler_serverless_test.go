package elasticache_test

import (
	"context"
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DescribeServerlessCache_UserGroupId(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	opts := elasticache.ServerlessCreateOpts{
		Name:        "sc-ug",
		Engine:      "redis",
		UserGroupID: "grp-xyz",
	}
	_, err := b.CreateServerlessCacheFull(context.Background(), opts)
	require.NoError(t, err)

	p, err := b.DescribeServerlessCaches(context.Background(), "sc-ug", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "grp-xyz", p.Data[0].UserGroupID)
}

// TestHandler_ServerlessCache_WireShapeFieldsSurfaced is a regression test
// for a wire-shape bug: serverlessCacheXML (handler_serverless.go) only
// mapped ARN/ServerlessCacheName/Description/Status/Engine/Endpoint/
// ReaderEndpoint, silently dropping CreateTime/DailySnapshotTime/KmsKeyId/
// MajorEngineVersion/SecurityGroupIds/SnapshotRetentionLimit/SubnetIds/
// UserGroupId from every response despite the domain model already storing
// all of them -- confirmed against
// aws-sdk-go-v2/service/elasticache@v1.51.11's
// awsAwsquery_deserializeDocumentServerlessCache. Unlike
// TestHandler_DescribeServerlessCache_UserGroupId (which only asserts
// against the Go-level backend struct and would not have caught this), this
// test drives a real generated SDK client against the httptest server, so it
// exercises the actual XML wire encoding/decoding round trip end to end
// (parity-principles.md rule 3: unit tests against gopherstack's own structs
// are not parity proof).
func TestHandler_ServerlessCache_WireShapeFieldsSurfaced(t *testing.T) {
	t.Parallel()

	backend, client := newTestStackWithBackend(t)

	_, err := backend.CreateServerlessCacheFull(context.Background(), elasticache.ServerlessCreateOpts{
		Name:                   "sc-wire-shape",
		Engine:                 "redis",
		KmsKeyID:               "arn:aws:kms:us-east-1:000000000000:key/abc",
		UserGroupID:            "grp-wire",
		DailySnapshotTime:      "05:00",
		MajorEngineVersion:     "7",
		SecurityGroupIDs:       []string{"sg-1", "sg-2"},
		SubnetIDs:              []string{"subnet-1", "subnet-2"},
		SnapshotRetentionLimit: 5,
	})
	require.NoError(t, err)

	out, err := client.DescribeServerlessCaches(context.Background(), &elasticachesdk.DescribeServerlessCachesInput{
		ServerlessCacheName: aws.String("sc-wire-shape"),
	})
	require.NoError(t, err)
	require.Len(t, out.ServerlessCaches, 1)

	sc := out.ServerlessCaches[0]
	assert.Equal(t, "grp-wire", aws.ToString(sc.UserGroupId))
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/abc", aws.ToString(sc.KmsKeyId))
	assert.Equal(t, "05:00", aws.ToString(sc.DailySnapshotTime))
	assert.Equal(t, "7", aws.ToString(sc.MajorEngineVersion))
	assert.ElementsMatch(t, []string{"sg-1", "sg-2"}, sc.SecurityGroupIds)
	assert.ElementsMatch(t, []string{"subnet-1", "subnet-2"}, sc.SubnetIds)
	assert.Equal(t, int32(5), aws.ToInt32(sc.SnapshotRetentionLimit))
	assert.NotNil(t, sc.CreateTime, "CreateTime must be present on the wire response")
}

// TestHandler_ServerlessCacheSnapshot_CreateTimeSurfaced is a regression
// test for the same bug class as above, on ServerlessCacheSnapshot's
// CreateTime field (serverlessCacheSnapshotXML previously had no CreateTime
// mapping at all, despite the domain model storing it as CreatedAt).
func TestHandler_ServerlessCacheSnapshot_CreateTimeSurfaced(t *testing.T) {
	t.Parallel()

	backend, client := newTestStackWithBackend(t)

	_, err := backend.CreateServerlessCacheFull(context.Background(), elasticache.ServerlessCreateOpts{
		Name: "sc-for-snap", Engine: "redis",
	})
	require.NoError(t, err)

	_, err = backend.CreateServerlessCacheSnapshot(context.Background(), "snap-wire-shape", "sc-for-snap")
	require.NoError(t, err)

	in := &elasticachesdk.DescribeServerlessCacheSnapshotsInput{
		ServerlessCacheSnapshotName: aws.String("snap-wire-shape"),
	}
	out, err := client.DescribeServerlessCacheSnapshots(context.Background(), in)
	require.NoError(t, err)
	require.Len(t, out.ServerlessCacheSnapshots, 1)
	assert.NotNil(t, out.ServerlessCacheSnapshots[0].CreateTime, "CreateTime must be present on the wire response")
}

func TestHandler_ServerlessCache_FullLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create.
	createOut, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
		ServerlessCacheName: aws.String("lifecycle-sc"),
		Engine:              aws.String("redis"),
		Description:         aws.String("lifecycle test"),
	})
	require.NoError(t, err)
	assert.Equal(t, "lifecycle-sc", aws.ToString(createOut.ServerlessCache.ServerlessCacheName))
	arn := aws.ToString(createOut.ServerlessCache.ARN)
	assert.NotEmpty(t, arn)

	// Create snapshot.
	snapOut, err := client.CreateServerlessCacheSnapshot(
		t.Context(),
		&elasticachesdk.CreateServerlessCacheSnapshotInput{
			ServerlessCacheSnapshotName: aws.String("lifecycle-sc-snap"),
			ServerlessCacheName:         aws.String("lifecycle-sc"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "lifecycle-sc-snap", aws.ToString(snapOut.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))

	// Copy snapshot.
	copyOut, err := client.CopyServerlessCacheSnapshot(
		t.Context(),
		&elasticachesdk.CopyServerlessCacheSnapshotInput{
			SourceServerlessCacheSnapshotName: aws.String("lifecycle-sc-snap"),
			TargetServerlessCacheSnapshotName: aws.String("lifecycle-sc-snap-copy"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "lifecycle-sc-snap-copy", aws.ToString(copyOut.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))

	// Describe cache.
	descOut, err := client.DescribeServerlessCaches(t.Context(), &elasticachesdk.DescribeServerlessCachesInput{
		ServerlessCacheName: aws.String("lifecycle-sc"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.ServerlessCaches, 1)

	// Modify.
	_, err = client.ModifyServerlessCache(t.Context(), &elasticachesdk.ModifyServerlessCacheInput{
		ServerlessCacheName: aws.String("lifecycle-sc"),
		Description:         aws.String("modified description"),
	})
	require.NoError(t, err)

	// Delete snapshot.
	_, err = client.DeleteServerlessCacheSnapshot(
		t.Context(),
		&elasticachesdk.DeleteServerlessCacheSnapshotInput{
			ServerlessCacheSnapshotName: aws.String("lifecycle-sc-snap"),
		},
	)
	require.NoError(t, err)

	// Delete cache.
	_, err = client.DeleteServerlessCache(t.Context(), &elasticachesdk.DeleteServerlessCacheInput{
		ServerlessCacheName: aws.String("lifecycle-sc"),
	})
	require.NoError(t, err)
}

// ----------------------------------------
// TriggerAutoSnapshot — engine and ARN
// ----------------------------------------

func TestHandler_Tags_OnServerlessCache(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
		ServerlessCacheName: aws.String("tag-cache"),
		Engine:              aws.String("redis"),
	})
	require.NoError(t, err)

	arn := aws.ToString(out.ServerlessCache.ARN)
	require.NotEmpty(t, arn)

	// Add tags to the serverless cache.
	_, err = client.AddTagsToResource(t.Context(), &elasticachesdk.AddTagsToResourceInput{
		ResourceName: aws.String(arn),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)

	// List tags.
	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.TagList[0].Key))
	assert.Equal(t, "prod", aws.ToString(tagsOut.TagList[0].Value))

	// Remove tags.
	_, err = client.RemoveTagsFromResource(t.Context(), &elasticachesdk.RemoveTagsFromResourceInput{
		ResourceName: aws.String(arn),
		TagKeys:      []string{"env"},
	})
	require.NoError(t, err)

	tagsOut2, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Empty(t, tagsOut2.TagList)
}

func TestHandler_Tags_OnServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
		ServerlessCacheName: aws.String("snap-tag-cache"),
		Engine:              aws.String("redis"),
	})
	require.NoError(t, err)

	snapOut, err := client.CreateServerlessCacheSnapshot(
		t.Context(),
		&elasticachesdk.CreateServerlessCacheSnapshotInput{
			ServerlessCacheSnapshotName: aws.String("snap-tag-snap"),
			ServerlessCacheName:         aws.String("snap-tag-cache"),
		},
	)
	require.NoError(t, err)

	arn := aws.ToString(snapOut.ServerlessCacheSnapshot.ARN)
	require.NotEmpty(t, arn)

	_, err = client.AddTagsToResource(t.Context(), &elasticachesdk.AddTagsToResourceInput{
		ResourceName: aws.String(arn),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("type"), Value: aws.String("backup")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Len(t, tagsOut.TagList, 1)
}

func TestDeleteServerlessCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		scName  string
		wantErr bool
	}{
		{
			name:   "success",
			scName: "sc-del-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc-del-1"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			scName:  "no-such-sc",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.DeleteServerlessCache(t.Context(), &elasticachesdk.DeleteServerlessCacheInput{
				ServerlessCacheName: aws.String(tt.scName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.scName, aws.ToString(out.ServerlessCache.ServerlessCacheName))
		})
	}
}

// ----------------------------------------
// DeleteServerlessCacheSnapshot
// ----------------------------------------

func TestDeleteServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, client *elasticachesdk.Client)
		name     string
		snapName string
		wantErr  bool
	}{
		{
			name:     "success",
			snapName: "snap-del-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc-snap-del"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("snap-del-1"),
						ServerlessCacheName:         aws.String("sc-snap-del"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:     "not_found",
			snapName: "no-such-snap",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.DeleteServerlessCacheSnapshot(
				t.Context(),
				&elasticachesdk.DeleteServerlessCacheSnapshotInput{
					ServerlessCacheSnapshotName: aws.String(tt.snapName),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.snapName, aws.ToString(out.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))
		})
	}
}

// ----------------------------------------
// DescribeServerlessCaches
// ----------------------------------------

func TestDescribeServerlessCaches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		scName    string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "all_caches",
			wantCount: 2,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, n := range []string{"sc1", "sc2"} {
					_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
						ServerlessCacheName: aws.String(n),
						Engine:              aws.String("redis"),
					})
					require.NoError(t, err)
				}
			},
		},
		{
			name:      "filter_by_name",
			scName:    "sc3",
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc3"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			scName:  "no-such-sc",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			input := &elasticachesdk.DescribeServerlessCachesInput{}
			if tt.scName != "" {
				input.ServerlessCacheName = aws.String(tt.scName)
			}

			out, err := client.DescribeServerlessCaches(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.ServerlessCaches, tt.wantCount)
		})
	}
}

// ----------------------------------------
// DescribeServerlessCacheSnapshots
// ----------------------------------------

func TestDescribeServerlessCacheSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		snapName  string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "all_snapshots",
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc-dss"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("snap-dss"),
						ServerlessCacheName:         aws.String("sc-dss"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:     "not_found",
			snapName: "no-such-snap",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			input := &elasticachesdk.DescribeServerlessCacheSnapshotsInput{}
			if tt.snapName != "" {
				input.ServerlessCacheSnapshotName = aws.String(tt.snapName)
			}

			out, err := client.DescribeServerlessCacheSnapshots(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.ServerlessCacheSnapshots, tt.wantCount)
		})
	}
}

// ----------------------------------------
// ExportServerlessCacheSnapshot
// ----------------------------------------

func TestExportServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, client *elasticachesdk.Client)
		name     string
		snapName string
		wantErr  bool
	}{
		{
			name:     "success",
			snapName: "snap-exp-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc-exp"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("snap-exp-1"),
						ServerlessCacheName:         aws.String("sc-exp"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:     "not_found",
			snapName: "no-such-snap",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.ExportServerlessCacheSnapshot(
				t.Context(),
				&elasticachesdk.ExportServerlessCacheSnapshotInput{
					ServerlessCacheSnapshotName: aws.String(tt.snapName),
					S3BucketName:                aws.String("my-bucket"),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.snapName, aws.ToString(out.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))
		})
	}
}

// ----------------------------------------
// ModifyServerlessCache
// ----------------------------------------

func TestModifyServerlessCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		scName  string
		wantErr bool
	}{
		{
			name:   "success",
			scName: "sc-mod-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc-mod-1"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			scName:  "no-such-sc",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.ModifyServerlessCache(t.Context(), &elasticachesdk.ModifyServerlessCacheInput{
				ServerlessCacheName: aws.String(tt.scName),
				Description:         aws.String("updated description"),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.scName, aws.ToString(out.ServerlessCache.ServerlessCacheName))
		})
	}
}

// ----------------------------------------
// StartMigration
// ----------------------------------------

func TestCreateServerlessCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		cacheName   string
		description string
		engine      string
		wantErr     bool
	}{
		{
			name:        "success_redis",
			cacheName:   "my-serverless",
			description: "My serverless cache",
			engine:      "redis",
		},
		{
			name:      "success_valkey",
			cacheName: "my-valkey",
			engine:    "valkey",
		},
		{
			name:      "already_exists",
			cacheName: "dup-serverless",
			engine:    "redis",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("dup-serverless"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
				ServerlessCacheName: aws.String(tt.cacheName),
				Description:         aws.String(tt.description),
				Engine:              aws.String(tt.engine),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ServerlessCache)
			assert.Equal(t, tt.cacheName, aws.ToString(out.ServerlessCache.ServerlessCacheName))
			assert.NotEmpty(t, aws.ToString(out.ServerlessCache.ARN))
		})
	}
}

// ----------------------------------------
// CreateServerlessCacheSnapshot
// ----------------------------------------

func TestCreateServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup               func(t *testing.T, client *elasticachesdk.Client)
		name                string
		snapshotName        string
		serverlessCacheName string
		wantErr             bool
	}{
		{
			name:                "success",
			snapshotName:        "my-snap",
			serverlessCacheName: "my-serverless",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("my-serverless"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:                "cache_not_found",
			snapshotName:        "snap-fail",
			serverlessCacheName: "nonexistent-cache",
			wantErr:             true,
		},
		{
			name:                "snapshot_already_exists",
			snapshotName:        "dup-snap",
			serverlessCacheName: "my-serverless2",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("my-serverless2"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("dup-snap"),
						ServerlessCacheName:         aws.String("my-serverless2"),
					},
				)
				require.NoError(t, err)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.CreateServerlessCacheSnapshot(
				t.Context(),
				&elasticachesdk.CreateServerlessCacheSnapshotInput{
					ServerlessCacheSnapshotName: aws.String(tt.snapshotName),
					ServerlessCacheName:         aws.String(tt.serverlessCacheName),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ServerlessCacheSnapshot)
			assert.Equal(t, tt.snapshotName, aws.ToString(out.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))
			assert.NotEmpty(t, aws.ToString(out.ServerlessCacheSnapshot.ARN))
		})
	}
}

// ----------------------------------------
// CopyServerlessCacheSnapshot
// ----------------------------------------

func TestCopyServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(t *testing.T, client *elasticachesdk.Client)
		name           string
		sourceSnapshot string
		targetSnapshot string
		wantErr        bool
	}{
		{
			name:           "success",
			sourceSnapshot: "source-snap",
			targetSnapshot: "target-snap",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("src-cache"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("source-snap"),
						ServerlessCacheName:         aws.String("src-cache"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:           "source_not_found",
			sourceSnapshot: "nonexistent-snap",
			targetSnapshot: "target",
			wantErr:        true,
		},
		{
			name:           "target_already_exists",
			sourceSnapshot: "src-snap",
			targetSnapshot: "existing-snap",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("cache-for-copy"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("src-snap"),
						ServerlessCacheName:         aws.String("cache-for-copy"),
					},
				)
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("existing-snap"),
						ServerlessCacheName:         aws.String("cache-for-copy"),
					},
				)
				require.NoError(t, err)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.CopyServerlessCacheSnapshot(
				t.Context(),
				&elasticachesdk.CopyServerlessCacheSnapshotInput{
					SourceServerlessCacheSnapshotName: aws.String(tt.sourceSnapshot),
					TargetServerlessCacheSnapshotName: aws.String(tt.targetSnapshot),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ServerlessCacheSnapshot)
			assert.Equal(t, tt.targetSnapshot, aws.ToString(out.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))
		})
	}
}
