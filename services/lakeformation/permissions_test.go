package lakeformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/lakeformation"
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

			require.NoError(t, b.GrantPermissions(t.Context(), entry))

			entries, _ := b.ListPermissions(resource, 0, "", nil, "")
			assert.Len(t, entries, tt.wantCount)
			require.Len(t, entries, tt.wantCount)

			if tt.wantCount > 0 {
				assert.NotNil(t, entries[0].LastUpdated, "GrantPermissions must stamp LastUpdated")
			}

			require.NoError(t, b.RevokePermissions(t.Context(), entry))

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

	require.NoError(t, b.GrantPermissions(t.Context(), entry))

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

			failures := b.BatchGrantPermissions(t.Context(), tt.entries)
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

			require.NoError(t, b.GrantPermissions(t.Context(), p1))
			require.NoError(t, b.GrantPermissions(t.Context(), p2))

			// Revoke first entry.
			require.NoError(t, b.RevokePermissions(t.Context(), p1))

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

			require.NoError(t, b.GrantPermissions(t.Context(), entry))
			require.NoError(t, b.RevokePermissions(t.Context(), entry))

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

			require.NoError(t, b.GrantPermissions(t.Context(), entry))

			filtered, _ := b.ListPermissions(tt.filter, 0, "", nil, "")
			assert.Len(t, filtered, tt.wantCount)
		})
	}
}

func TestRevokePermissions_NilEntry(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	err := b.RevokePermissions(t.Context(), nil)
	assert.Error(t, err)
}

// TestGrantPermissions_StampsLastUpdatedBy covers
// PermissionEntry.LastUpdatedBy, "the user who updated the record"
// (types.PrincipalResourcePermissions.LastUpdatedBy, types/types.go:652,
// aws-sdk-go-v2/service/lakeformation@v1.50.4). gopherstack has no real IAM
// principal, so it stamps the same synthetic caller ARN GetDataLakePrincipal
// derives from the request's account context.
func TestGrantPermissions_StampsLastUpdatedBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		account string
		want    string
	}{
		{
			name:    "account in context",
			account: "111122223333",
			want:    "arn:aws:iam::111122223333:user/gopherstack-user",
		},
		{
			name:    "no account falls back to default",
			account: "",
			want:    "arn:aws:iam::" + awsmeta.DefaultAccount + ":user/gopherstack-user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Account: tt.account})

			resource := &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "db1"}}
			entry := &lakeformation.PermissionEntry{
				Principal: &lakeformation.DataLakePrincipal{
					DataLakePrincipalIdentifier: "arn:aws:iam::123:user/alice",
				},
				Resource:    resource,
				Permissions: []string{"DESCRIBE"},
			}

			require.NoError(t, b.GrantPermissions(ctx, entry))

			entries, _ := b.ListPermissions(resource, 0, "", nil, "")
			require.Len(t, entries, 1)
			assert.Equal(t, tt.want, entries[0].LastUpdatedBy)
		})
	}
}

// TestRevokePermissions_PartialRevoke_StampsLastUpdatedBy covers the partial
// revoke path (some permissions remain): the surviving entry's
// LastUpdatedBy must reflect the revoking caller, not the original granter.
func TestRevokePermissions_PartialRevoke_StampsLastUpdatedBy(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	resource := &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "db1"}}
	entry := &lakeformation.PermissionEntry{
		Principal:   &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/alice"},
		Resource:    resource,
		Permissions: []string{"DESCRIBE", "ALTER"},
	}

	granterCtx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Account: "111111111111"})
	require.NoError(t, b.GrantPermissions(granterCtx, entry))

	revokerCtx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Account: "222222222222"})
	require.NoError(t, b.RevokePermissions(revokerCtx, &lakeformation.PermissionEntry{
		Principal:   entry.Principal,
		Resource:    resource,
		Permissions: []string{"ALTER"},
	}))

	entries, _ := b.ListPermissions(resource, 0, "", nil, "")
	require.Len(t, entries, 1)
	assert.Equal(t, "arn:aws:iam::222222222222:user/gopherstack-user", entries[0].LastUpdatedBy)
	assert.Equal(t, []string{"DESCRIBE"}, entries[0].Permissions)
}

// TestGetEffectivePermissionsForPath_ExpandsLFTagPolicyGrants covers the
// documented purpose of GetEffectivePermissionsForPath ("Returns the Lake
// Formation permissions for a specified table or database resource located
// at a path", api_op_GetEffectivePermissionsForPath.go:14-15,
// aws-sdk-go-v2/service/lakeformation@v1.50.4) versus ListPermissions'
// literal-grant-only semantics. A grant made against an LFTagPolicy resource
// has no Database/Table field to match a concrete resource's ARN, so it must
// be resolved against the resource's actual assigned LF-tags (AWS's
// documented AND-across-keys/OR-within-a-key expression evaluation:
// https://docs.aws.amazon.com/lake-formation/latest/dg/managing-tag-expressions.html)
// to be visible here at all.
func TestGetEffectivePermissionsForPath_ExpandsLFTagPolicyGrants(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	require.NoError(t, b.CreateLFTag("", "confidentiality", []string{"public", "private"}))

	dbResource := &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "salesdb"}}
	failures := b.AddLFTagsToResource("", dbResource, []lakeformation.LFTagPair{
		{TagKey: "confidentiality", TagValues: []string{"public"}},
	})
	require.Empty(t, failures)

	tagPolicyEntry := &lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/alice"},
		Resource: &lakeformation.Resource{
			LFTagPolicy: &lakeformation.LFTagPolicyResource{
				ResourceType: "DATABASE",
				Expression:   []lakeformation.LFTag{{TagKey: "confidentiality", TagValues: []string{"public"}}},
			},
		},
		Permissions: []string{"DESCRIBE"},
	}
	require.NoError(t, b.GrantPermissions(t.Context(), tagPolicyEntry))

	entries, _ := b.GetEffectivePermissionsForPath(
		"arn:aws:glue:us-east-1:123456789012:database/salesdb", 0, "",
	)
	require.Len(t, entries, 1)
	assert.Equal(t, "arn:aws:iam::123:user/alice", entries[0].Principal.DataLakePrincipalIdentifier)
	assert.Equal(t, []string{"DESCRIBE"}, entries[0].Permissions)
}

// TestGetEffectivePermissionsForPath_LFTagPolicyNonMatchExcluded covers the
// negative case: a resource whose LF-tags don't satisfy the grant's
// expression must not appear.
func TestGetEffectivePermissionsForPath_LFTagPolicyNonMatchExcluded(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	require.NoError(t, b.CreateLFTag("", "confidentiality", []string{"public", "private"}))

	dbResource := &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "salesdb"}}
	failures := b.AddLFTagsToResource("", dbResource, []lakeformation.LFTagPair{
		{TagKey: "confidentiality", TagValues: []string{"private"}},
	})
	require.Empty(t, failures)

	tagPolicyEntry := &lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/alice"},
		Resource: &lakeformation.Resource{
			LFTagPolicy: &lakeformation.LFTagPolicyResource{
				ResourceType: "DATABASE",
				Expression:   []lakeformation.LFTag{{TagKey: "confidentiality", TagValues: []string{"public"}}},
			},
		},
		Permissions: []string{"DESCRIBE"},
	}
	require.NoError(t, b.GrantPermissions(t.Context(), tagPolicyEntry))

	entries, _ := b.GetEffectivePermissionsForPath(
		"arn:aws:glue:us-east-1:123456789012:database/salesdb", 0, "",
	)
	assert.Empty(t, entries)
}

// TestGetEffectivePermissionsForPath_LFTagPolicyViaExpressionName covers the
// ExpressionName variant of LFTagPolicyResource ("permissions are granted to
// the Data Catalog resources whose assigned LF-Tags match the expression
// body of the saved expression under the provided ExpressionName",
// types/types.go:583-585, aws-sdk-go-v2/service/lakeformation@v1.50.4).
func TestGetEffectivePermissionsForPath_LFTagPolicyViaExpressionName(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	require.NoError(t, b.CreateLFTag("", "confidentiality", []string{"public", "private"}))
	require.NoError(t, b.CreateLFTagExpression("public-dbs", "", "",
		[]lakeformation.LFTag{{TagKey: "confidentiality", TagValues: []string{"public"}}}))

	dbResource := &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "salesdb"}}
	failures := b.AddLFTagsToResource("", dbResource, []lakeformation.LFTagPair{
		{TagKey: "confidentiality", TagValues: []string{"public"}},
	})
	require.Empty(t, failures)

	entry := &lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/alice"},
		Resource: &lakeformation.Resource{
			LFTagPolicy: &lakeformation.LFTagPolicyResource{
				ResourceType:   "DATABASE",
				ExpressionName: "public-dbs",
			},
		},
		Permissions: []string{"DESCRIBE"},
	}
	require.NoError(t, b.GrantPermissions(t.Context(), entry))

	entries, _ := b.GetEffectivePermissionsForPath(
		"arn:aws:glue:us-east-1:123456789012:database/salesdb", 0, "",
	)
	require.Len(t, entries, 1)
	assert.Equal(t, "arn:aws:iam::123:user/alice", entries[0].Principal.DataLakePrincipalIdentifier)
}

// TestListPermissions_DoesNotExpandLFTagPolicyGrants documents an intentional
// scope boundary: unlike GetEffectivePermissionsForPath, ListPermissions
// filtered by a concrete resource does NOT expand LFTagPolicy grants. A
// tag-based grant is queried via its own LFTagPolicy/LF_TAG_POLICY_* resource
// type, not by listing the concrete resource it happens to cover.
func TestListPermissions_DoesNotExpandLFTagPolicyGrants(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	require.NoError(t, b.CreateLFTag("", "confidentiality", []string{"public"}))

	dbResource := &lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "salesdb"}}
	failures := b.AddLFTagsToResource("", dbResource, []lakeformation.LFTagPair{
		{TagKey: "confidentiality", TagValues: []string{"public"}},
	})
	require.Empty(t, failures)

	entry := &lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/alice"},
		Resource: &lakeformation.Resource{
			LFTagPolicy: &lakeformation.LFTagPolicyResource{
				ResourceType: "DATABASE",
				Expression:   []lakeformation.LFTag{{TagKey: "confidentiality", TagValues: []string{"public"}}},
			},
		},
		Permissions: []string{"DESCRIBE"},
	}
	require.NoError(t, b.GrantPermissions(t.Context(), entry))

	entries, _ := b.ListPermissions(dbResource, 0, "", nil, "")
	assert.Empty(t, entries)
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
