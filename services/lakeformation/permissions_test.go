package lakeformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGrantRevokeListPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		principal   string
		resourceArn string
		perms       []string
		wantCount   int
	}{
		{
			name:        "grant_and_list",
			principal:   "arn:aws:iam::123456789012:user/alice",
			resourceArn: "arn:aws:s3:::my-bucket",
			perms:       []string{"DATA_LOCATION_ACCESS"},
			wantCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			entry := &lakeformation.PermissionEntry{
				Principal: &lakeformation.DataLakePrincipal{
					DataLakePrincipalIdentifier: tt.principal,
				},
				Resource: &lakeformation.Resource{
					DataLocation: &lakeformation.DataLocationResource{ResourceArn: tt.resourceArn},
				},
				Permissions: tt.perms,
			}

			require.NoError(t, b.GrantPermissions(entry))

			entries, _ := b.ListPermissions(tt.resourceArn, 0, "", nil, "")
			assert.Len(t, entries, tt.wantCount)

			require.NoError(t, b.RevokePermissions(entry))

			entries, _ = b.ListPermissions(tt.resourceArn, 0, "", nil, "")
			assert.Empty(t, entries)
		})
	}
}

func TestBatchGrantRevokePermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		entries      []*lakeformation.PermissionEntry
		wantFailures int
	}{
		{
			name: "batch_grant_no_failures",
			entries: []*lakeformation.PermissionEntry{
				{
					Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/a"},
					Resource: &lakeformation.Resource{
						DataLocation: &lakeformation.DataLocationResource{ResourceArn: "arn:aws:s3:::bucket-a"},
					},
					Permissions: []string{"DATA_LOCATION_ACCESS"},
				},
			},
			wantFailures: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			failures := b.BatchGrantPermissions(tt.entries)
			assert.Len(t, failures, tt.wantFailures)

			failures = b.BatchRevokePermissions(tt.entries)
			assert.Len(t, failures, tt.wantFailures)
		})
	}
}

func TestRevokePermissions_NoDanglingPointers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "revoke allocates new slice without dangling pointers"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			p1 := &lakeformation.PermissionEntry{
				Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/a"},
				Resource: &lakeformation.Resource{
					DataLocation: &lakeformation.DataLocationResource{ResourceArn: "arn:aws:s3:::bucket-a"},
				},
				Permissions: []string{"DATA_LOCATION_ACCESS"},
			}
			p2 := &lakeformation.PermissionEntry{
				Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/b"},
				Resource: &lakeformation.Resource{
					DataLocation: &lakeformation.DataLocationResource{ResourceArn: "arn:aws:s3:::bucket-b"},
				},
				Permissions: []string{"DATA_LOCATION_ACCESS"},
			}

			require.NoError(t, b.GrantPermissions(p1))
			require.NoError(t, b.GrantPermissions(p2))

			// Revoke first entry.
			require.NoError(t, b.RevokePermissions(p1))

			entries, _ := b.ListPermissions("", 0, "", nil, "")
			assert.Len(t, entries, 1)
			assert.Equal(t, "arn:aws:iam::123:user/b", entries[0].Principal.DataLakePrincipalIdentifier)
		})
	}
}

func TestPermissionMatches_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		principal   string
		resourceArn string
		revokeArn   string
		perms       []string
		wantRemain  int
	}{
		{
			name:        "revoke with database resource",
			principal:   "arn:aws:iam::123:user/db-user",
			resourceArn: "db-arn",
			perms:       []string{"CREATE_TABLE"},
			revokeArn:   "db-arn",
			wantRemain:  0,
		},
		{
			name:        "revoke with table resource",
			principal:   "arn:aws:iam::123:user/tbl-user",
			resourceArn: "tbl-arn",
			perms:       []string{"SELECT"},
			revokeArn:   "tbl-arn",
			wantRemain:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			var entry *lakeformation.PermissionEntry

			if tt.name == "revoke with database resource" {
				entry = &lakeformation.PermissionEntry{
					Principal: &lakeformation.DataLakePrincipal{
						DataLakePrincipalIdentifier: tt.principal,
					},
					Resource: &lakeformation.Resource{
						Database: &lakeformation.DatabaseResource{Name: tt.resourceArn},
					},
					Permissions: tt.perms,
				}
			} else {
				entry = &lakeformation.PermissionEntry{
					Principal: &lakeformation.DataLakePrincipal{
						DataLakePrincipalIdentifier: tt.principal,
					},
					Resource: &lakeformation.Resource{
						Table: &lakeformation.TableResource{DatabaseName: "mydb", Name: tt.resourceArn},
					},
					Permissions: tt.perms,
				}
			}

			require.NoError(t, b.GrantPermissions(entry))
			require.NoError(t, b.RevokePermissions(entry))

			entries, _ := b.ListPermissions("", 0, "", nil, "")
			assert.Len(t, entries, tt.wantRemain)
		})
	}
}

func TestPermissionMatches_NilHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		wantCount int
	}{
		{
			name:      "nil resource for permissionMatchesARN returns no match",
			principal: "arn:aws:iam::123:user/x",
			wantCount: 1, // entry with nil DataLocation should not be matched by ARN filter
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			// Entry with Catalog resource (no DataLocation) - won't match ARN filter.
			entry := &lakeformation.PermissionEntry{
				Principal: &lakeformation.DataLakePrincipal{
					DataLakePrincipalIdentifier: tt.principal,
				},
				Resource:    &lakeformation.Resource{Catalog: &lakeformation.CatalogResource{}},
				Permissions: []string{"ALL"},
			}

			require.NoError(t, b.GrantPermissions(entry))

			// Filter by a specific ARN - should not match the catalog resource.
			filtered, _ := b.ListPermissions("arn:aws:s3:::no-match", 0, "", nil, "")
			assert.Empty(t, filtered)

			// Filter by empty ARN - should return the catalog entry.
			all, _ := b.ListPermissions("", 0, "", nil, "")
			assert.Len(t, all, tt.wantCount)
		})
	}
}

func TestRevokePermissions_NilEntry(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	err := b.RevokePermissions(nil)
	assert.Error(t, err)
}

func TestListPermissions_SortedDeterministic(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddPermissionInternal(&lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{
			DataLakePrincipalIdentifier: "arn:aws:iam::000000000000:user/bob",
		},
		Resource:    &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "db1"}},
		Permissions: []string{"CREATE_TABLE"},
	})
	b.AddPermissionInternal(&lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{
			DataLakePrincipalIdentifier: "arn:aws:iam::000000000000:user/alice",
		},
		Resource:    &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "db1"}},
		Permissions: []string{"SELECT"},
	})

	perms, _ := b.ListPermissions("", 0, "", nil, "")
	require.Len(t, perms, 2)
	// alice sorts before bob
	assert.Equal(t, "arn:aws:iam::000000000000:user/alice", perms[0].Principal.DataLakePrincipalIdentifier)
	assert.Equal(t, "arn:aws:iam::000000000000:user/bob", perms[1].Principal.DataLakePrincipalIdentifier)
}
