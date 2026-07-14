package iam_test

import (
	"context"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIAM_BackendReset(t *testing.T) {
	t.Parallel()

	_, be := newTestHandler(t)

	// Create some resources
	_, err := be.CreateUser("reset-user", "/", "")
	require.NoError(t, err)

	// Reset exercises collectAndDeleteFunctions and cleanup paths
	be.Reset()

	// Verify reset worked
	usersPage, err := be.ListUsers("", 100)
	require.NoError(t, err)
	assert.Empty(t, usersPage.Data)
}

// TestBackendResetAndPurge covers Reset and Purge as Go methods (not IAM actions).
func TestBackendResetAndPurge(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateUser("reset-test-user", "/", "")
	require.NoError(t, err)

	// Reset clears backend state.
	h.Reset()

	// Purge just calls through without panicking.
	h.Purge(context.Background(), timeNow())
}

// newBackend returns a fresh InMemoryBackend for testing.
func newBackend(t *testing.T) *iam.InMemoryBackend {
	t.Helper()

	return iam.NewInMemoryBackend()
}

func TestNormPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantPath string
	}{
		{
			name:     "empty_path_defaults_to_root",
			input:    "",
			wantPath: "/",
		},
		{
			name:     "path_without_trailing_slash_gets_one",
			input:    "/engineering",
			wantPath: "/engineering/",
		},
		{
			name:     "path_with_trailing_slash_unchanged",
			input:    "/engineering/",
			wantPath: "/engineering/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			u, err := b.CreateUser("normpath-user-"+tt.name, tt.input, "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantPath, u.Path)
		})
	}
}

// TestListPagination_SortedOrder verifies that List operations return
// results in lexicographic (sorted) order and correctly paginate using the
// returned marker token.
func TestListPagination_SortedOrder(t *testing.T) {
	t.Parallel()

	type listFunc func(b *iam.InMemoryBackend, marker string, pageLimit int) (names []string, next string, err error)

	tests := []struct {
		listFn    listFunc
		name      string
		itemNames []string // names to create, must include more items than pageSize
		pageSize  int
	}{
		{
			name:      "list_users_sorted_paginated",
			pageSize:  3,
			itemNames: []string{"zara", "alice", "mike", "bob", "carol", "dave"},
			listFn: func(b *iam.InMemoryBackend, marker string, pageLimit int) ([]string, string, error) {
				pg, err := b.ListUsers(marker, pageLimit)
				if err != nil {
					return nil, "", err
				}

				names := make([]string, len(pg.Data))
				for i, u := range pg.Data {
					names[i] = u.UserName
				}

				return names, pg.Next, nil
			},
		},
		{
			name:      "list_roles_sorted_paginated",
			pageSize:  2,
			itemNames: []string{"zeta-role", "alpha-role", "beta-role", "gamma-role", "delta-role"},
			listFn: func(b *iam.InMemoryBackend, marker string, pageLimit int) ([]string, string, error) {
				pg, err := b.ListRoles(marker, pageLimit)
				if err != nil {
					return nil, "", err
				}

				names := make([]string, len(pg.Data))
				for i, r := range pg.Data {
					names[i] = r.RoleName
				}

				return names, pg.Next, nil
			},
		},
		{
			name:      "list_groups_sorted_paginated",
			pageSize:  2,
			itemNames: []string{"ops", "dev", "qa", "sre", "mgmt"},
			listFn: func(b *iam.InMemoryBackend, marker string, pageLimit int) ([]string, string, error) {
				pg, err := b.ListGroups(marker, pageLimit)
				if err != nil {
					return nil, "", err
				}

				names := make([]string, len(pg.Data))
				for i, g := range pg.Data {
					names[i] = g.GroupName
				}

				return names, pg.Next, nil
			},
		},
		{
			name:      "list_policies_sorted_paginated",
			pageSize:  3,
			itemNames: []string{"ZPolicy", "APolicy", "MPolicy", "BPolicy", "CPolicy", "DPolicy"},
			listFn: func(b *iam.InMemoryBackend, marker string, pageLimit int) ([]string, string, error) {
				pg, err := b.ListPolicies(marker, pageLimit)
				if err != nil {
					return nil, "", err
				}

				names := make([]string, len(pg.Data))
				for i, p := range pg.Data {
					names[i] = p.PolicyName
				}

				return names, pg.Next, nil
			},
		},
	}

	const validTrustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	const validPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*"}]}`

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := iam.NewInMemoryBackend()

			// Create resources in the order given (intentionally unsorted).
			for _, name := range tt.itemNames {
				switch tt.name {
				case "list_users_sorted_paginated":
					_, err := b.CreateUser(name, "/", "")
					require.NoError(t, err)
				case "list_roles_sorted_paginated":
					_, err := b.CreateRole(name, "/", validTrustPolicy, "")
					require.NoError(t, err)
				case "list_groups_sorted_paginated":
					_, err := b.CreateGroup(name, "/")
					require.NoError(t, err)
				case "list_policies_sorted_paginated":
					_, err := b.CreatePolicy(name, "/", validPolicy)
					require.NoError(t, err)
				}
			}

			// Collect all items by paginating.
			var allNames []string
			marker := ""

			for {
				names, next, err := tt.listFn(b, marker, tt.pageSize)
				require.NoError(t, err)

				if len(names) == 0 && next == "" {
					break
				}

				// Each page must have at most pageSize items.
				assert.LessOrEqual(t, len(names), tt.pageSize,
					"page must not exceed pageSize=%d", tt.pageSize)

				allNames = append(allNames, names...)

				if next == "" {
					break
				}

				marker = next
			}

			assert.Len(t, allNames, len(tt.itemNames),
				"paginated result must contain all %d items", len(tt.itemNames))

			// Verify lexicographic sort order across all pages.
			for i := 1; i < len(allNames); i++ {
				assert.Less(t, allNames[i-1], allNames[i],
					"items must be in sorted order: allNames[%d]=%q must be < allNames[%d]=%q",
					i-1, allNames[i-1], i, allNames[i])
			}
		})
	}
}

// TestSortedIndex_MaintainedAfterDelete verifies that sorted name indexes
// are correctly updated when resources are deleted, and that subsequent List
// operations return the correct remaining items in sorted order.
func TestSortedIndex_MaintainedAfterDelete(t *testing.T) {
	t.Parallel()

	type deleteTestCase struct {
		create     []string
		toDelete   []string
		name       string
		wantRemain []string
	}

	tests := []deleteTestCase{
		{
			name:       "user_delete_updates_index",
			create:     []string{"zara", "alice", "bob", "carol"},
			toDelete:   []string{"alice", "carol"},
			wantRemain: []string{"bob", "zara"},
		},
		{
			name:       "group_delete_updates_index",
			create:     []string{"ops", "dev", "qa"},
			toDelete:   []string{"dev"},
			wantRemain: []string{"ops", "qa"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := iam.NewInMemoryBackend()
			isUser := strings.Contains(tt.name, "user")

			for _, name := range tt.create {
				if isUser {
					_, err := b.CreateUser(name, "/", "")
					require.NoError(t, err)
				} else {
					_, err := b.CreateGroup(name, "/")
					require.NoError(t, err)
				}
			}

			for _, name := range tt.toDelete {
				if isUser {
					require.NoError(t, b.DeleteUser(name))
				} else {
					require.NoError(t, b.DeleteGroup(name))
				}
			}

			// List all remaining items.
			var gotNames []string

			if isUser {
				pg, err := b.ListUsers("", 100)
				require.NoError(t, err)

				for _, u := range pg.Data {
					gotNames = append(gotNames, u.UserName)
				}
			} else {
				pg, err := b.ListGroups("", 100)
				require.NoError(t, err)

				for _, g := range pg.Data {
					gotNames = append(gotNames, g.GroupName)
				}
			}

			assert.Equal(t, tt.wantRemain, gotNames,
				"remaining items after delete must match expected sorted list")
		})
	}
}
