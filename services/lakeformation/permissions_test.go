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

			resource := &lakeformation.Resource{
				DataLocation: &lakeformation.DataLocationResource{ResourceArn: tt.resourceArn},
			}
			entry := &lakeformation.PermissionEntry{
				Principal: &lakeformation.DataLakePrincipal{
					DataLakePrincipalIdentifier: tt.principal,
				},
				Resource:    resource,
				Permissions: tt.perms,
			}

			require.NoError(t, b.GrantPermissions(entry))

			entries, _ := b.ListPermissions(resource, 0, "", nil, "")
			assert.Len(t, entries, tt.wantCount)
			require.Len(t, entries, tt.wantCount)

			if tt.wantCount > 0 {
				assert.NotNil(t, entries[0].LastUpdated, "GrantPermissions must stamp LastUpdated")
			}

			require.NoError(t, b.RevokePermissions(entry))

			entries, _ = b.ListPermissions(resource, 0, "", nil, "")
			assert.Empty(t, entries)
		})
	}
}

func TestGrantPermissions_ConditionRoundTrips(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()

	resource := &lakeformation.Resource{
		Database: &lakeformation.DatabaseResource{Name: "conditional-db"},
	}
	entry := &lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{
			DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/alice",
		},
		Resource:    resource,
		Permissions: []string{"CREATE_TABLE"},
		Condition:   &lakeformation.Condition{Expression: "principal.department == \"eng\""},
	}

	require.NoError(t, b.GrantPermissions(entry))

	entries, _ := b.ListPermissions(resource, 0, "", nil, "")
	require.Len(t, entries, 1)
	require.NotNil(t, entries[0].Condition)
	assert.Equal(t, "principal.department == \"eng\"", entries[0].Condition.Expression)
}

func TestBatchGrantRevokePermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		entries      []*lakeformation.BatchPermissionsRequestEntry
		wantFailures int
	}{
		{
			name: "batch_grant_no_failures",
			entries: []*lakeformation.BatchPermissionsRequestEntry{
				{
					ID:        "entry-1",
					Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/a"},
					Resource: &lakeformation.Resource{
						DataLocation: &lakeformation.DataLocationResource{ResourceArn: "arn:aws:s3:::bucket-a"},
					},
					Permissions: []string{"DATA_LOCATION_ACCESS"},
				},
			},
			wantFailures: 0,
		},
		{
			name: "batch_grant_invalid_permission_fails",
			entries: []*lakeformation.BatchPermissionsRequestEntry{
				{
					ID:        "entry-2",
					Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/b"},
					Resource: &lakeformation.Resource{
						DataLocation: &lakeformation.DataLocationResource{ResourceArn: "arn:aws:s3:::bucket-b"},
					},
					Permissions: []string{"NOT_A_REAL_PERMISSION"},
				},
			},
			wantFailures: 1,
		},
		{
			// BatchGrantPermissions must apply the same "PermissionsWithGrantOption
			// must be a subset of Permissions" validation GrantPermissions enforces
			// -- as a per-entry Failures[] item, not a whole-request rejection.
			name: "batch_grant_option_not_subset_fails",
			entries: []*lakeformation.BatchPermissionsRequestEntry{
				{
					ID: "entry-3",
					Principal: &lakeformation.DataLakePrincipal{
						DataLakePrincipalIdentifier: "arn:aws:iam::123:user/c",
					},
					Resource: &lakeformation.Resource{
						Database: &lakeformation.DatabaseResource{Name: "db1"},
					},
					Permissions:                []string{"SELECT"},
					PermissionsWithGrantOption: []string{"INSERT"},
				},
			},
			wantFailures: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			failures := b.BatchGrantPermissions(tt.entries)
			assert.Len(t, failures, tt.wantFailures)

			if tt.wantFailures > 0 {
				require.NotNil(t, failures[0].RequestEntry)
				assert.Equal(t, tt.entries[0].ID, failures[0].RequestEntry.ID,
					"failure must echo back the caller-supplied Id for correlation")
			}
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

			entries, _ := b.ListPermissions(nil, 0, "", nil, "")
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

			entries, _ := b.ListPermissions(nil, 0, "", nil, "")
			assert.Len(t, entries, tt.wantRemain)
		})
	}
}

func TestListPermissions_ResourceFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter    *lakeformation.Resource
		name      string
		wantCount int
	}{
		{
			name:      "filter by non-matching database returns nothing",
			filter:    &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "no-match"}},
			wantCount: 0,
		},
		{
			name:      "nil filter returns everything",
			filter:    nil,
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			// Entry with Catalog resource (no Database) - won't match the database filter.
			entry := &lakeformation.PermissionEntry{
				Principal: &lakeformation.DataLakePrincipal{
					DataLakePrincipalIdentifier: "arn:aws:iam::123:user/x",
				},
				Resource:    &lakeformation.Resource{Catalog: &lakeformation.CatalogResource{}},
				Permissions: []string{"ALL"},
			}

			require.NoError(t, b.GrantPermissions(entry))

			filtered, _ := b.ListPermissions(tt.filter, 0, "", nil, "")
			assert.Len(t, filtered, tt.wantCount)
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

	perms, _ := b.ListPermissions(nil, 0, "", nil, "")
	require.Len(t, perms, 2)
	// alice sorts before bob
	assert.Equal(t, "arn:aws:iam::000000000000:user/alice", perms[0].Principal.DataLakePrincipalIdentifier)
	assert.Equal(t, "arn:aws:iam::000000000000:user/bob", perms[1].Principal.DataLakePrincipalIdentifier)
}
