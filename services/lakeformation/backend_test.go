package lakeformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestGetPutDataLakeSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		settings *lakeformation.DataLakeSettings
		name     string
	}{
		{
			name: "default_empty",
		},
		{
			name: "with_admins",
			settings: &lakeformation.DataLakeSettings{
				DataLakeAdmins: []lakeformation.DataLakePrincipal{
					{DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/admin"},
				},
			},
		},
		{
			name: "with_trusted_owners",
			settings: &lakeformation.DataLakeSettings{
				TrustedResourceOwners: []string{"123456789012"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			got := b.GetDataLakeSettings()
			require.NotNil(t, got)

			if tt.settings != nil {
				b.PutDataLakeSettings(tt.settings)

				got = b.GetDataLakeSettings()
				assert.Equal(t, tt.settings, got)
			}
		})
	}
}

func TestRegisterDeregisterDescribeResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		roleArn     string
		errType     string
		notFound    bool
		wantErr     bool
	}{
		{
			name:        "register_success",
			resourceArn: "arn:aws:s3:::my-bucket",
			roleArn:     "arn:aws:iam::123456789012:role/MyRole",
		},
		{
			name:        "duplicate_register",
			resourceArn: "arn:aws:s3:::duplicate-bucket",
			roleArn:     "arn:aws:iam::123456789012:role/MyRole",
			wantErr:     true,
			errType:     "already exists",
		},
		{
			name:        "deregister_not_found",
			resourceArn: "arn:aws:s3:::nonexistent",
			notFound:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			if tt.notFound {
				// Test not-found path: do NOT register first.
				err := b.DeregisterResource(tt.resourceArn)
				require.Error(t, err)

				_, err = b.DescribeResource(tt.resourceArn)
				require.Error(t, err)

				return
			}

			err := b.RegisterResource(tt.resourceArn, tt.roleArn)
			require.NoError(t, err)

			if tt.wantErr {
				err = b.RegisterResource(tt.resourceArn, tt.roleArn)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "already")

				return
			}

			info, err := b.DescribeResource(tt.resourceArn)
			require.NoError(t, err)
			assert.Equal(t, tt.resourceArn, info.ResourceArn)
			assert.Equal(t, tt.roleArn, info.RoleArn)

			err = b.DeregisterResource(tt.resourceArn)
			require.NoError(t, err)

			_, err = b.DescribeResource(tt.resourceArn)
			require.Error(t, err)
		})
	}
}

func TestDescribeResource_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	_, err := b.DescribeResource("arn:aws:s3:::nonexistent")
	require.Error(t, err)
}

func TestDeregisterResource_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	err := b.DeregisterResource("arn:aws:s3:::nonexistent")
	require.Error(t, err)
}

func TestListResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		arns       []string
		maxResults int
		wantCount  int
		wantToken  bool
	}{
		{
			name:      "empty",
			arns:      []string{},
			wantCount: 0,
		},
		{
			name:      "three_resources",
			arns:      []string{"arn:aws:s3:::a", "arn:aws:s3:::b", "arn:aws:s3:::c"},
			wantCount: 3,
		},
		{
			name:       "paginated",
			arns:       []string{"arn:aws:s3:::a", "arn:aws:s3:::b", "arn:aws:s3:::c"},
			maxResults: 2,
			wantCount:  2,
			wantToken:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			for _, arn := range tt.arns {
				require.NoError(t, b.RegisterResource(arn, "arn:aws:iam::123456789012:role/R"))
			}

			resources, nextToken := b.ListResources(tt.maxResults, "")
			assert.Len(t, resources, tt.wantCount)

			if tt.wantToken {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

func TestCreateGetDeleteLFTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		catalogID string
		tagKey    string
		tagValues []string
		wantErr   bool
	}{
		{
			name:      "success",
			catalogID: "123456789012",
			tagKey:    "env",
			tagValues: []string{"dev", "prod"},
		},
		{
			name:      "duplicate",
			catalogID: "123456789012",
			tagKey:    "env",
			tagValues: []string{"dev"},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			err := b.CreateLFTag(tt.catalogID, tt.tagKey, tt.tagValues)
			require.NoError(t, err)

			if tt.wantErr {
				err = b.CreateLFTag(tt.catalogID, tt.tagKey, tt.tagValues)
				require.Error(t, err)

				return
			}

			tag, err := b.GetLFTag(tt.catalogID, tt.tagKey)
			require.NoError(t, err)
			assert.Equal(t, tt.tagKey, tag.TagKey)
			assert.ElementsMatch(t, tt.tagValues, tag.TagValues)

			err = b.DeleteLFTag(tt.catalogID, tt.tagKey)
			require.NoError(t, err)

			_, err = b.GetLFTag(tt.catalogID, tt.tagKey)
			require.Error(t, err)
		})
	}
}

func TestUpdateLFTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		initial      []string
		toAdd        []string
		toDelete     []string
		wantValues   []string
		wantNotFound bool
	}{
		{
			name:       "add_value",
			initial:    []string{"dev"},
			toAdd:      []string{"prod"},
			wantValues: []string{"dev", "prod"},
		},
		{
			name:       "delete_value",
			initial:    []string{"dev", "prod"},
			toDelete:   []string{"prod"},
			wantValues: []string{"dev"},
		},
		{
			name:         "not_found",
			wantNotFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			if tt.wantNotFound {
				err := b.UpdateLFTag("cat", "missing-key", nil, nil)
				require.Error(t, err)

				return
			}

			require.NoError(t, b.CreateLFTag("cat", "env", tt.initial))
			require.NoError(t, b.UpdateLFTag("cat", "env", tt.toAdd, tt.toDelete))

			tag, err := b.GetLFTag("cat", "env")
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantValues, tag.TagValues)
		})
	}
}

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

			entries, _ := b.ListPermissions(tt.resourceArn, 0, "")
			assert.Len(t, entries, tt.wantCount)

			require.NoError(t, b.RevokePermissions(entry))

			entries, _ = b.ListPermissions(tt.resourceArn, 0, "")
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

func TestGetDataLakeSettings_ReturnsCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating returned settings does not affect backend state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			b.PutDataLakeSettings(&lakeformation.DataLakeSettings{
				DataLakeAdmins: []lakeformation.DataLakePrincipal{
					{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/admin"},
				},
			})

			s := b.GetDataLakeSettings()
			require.NotNil(t, s)

			// Mutate the returned copy.
			s.DataLakeAdmins = append(s.DataLakeAdmins, lakeformation.DataLakePrincipal{
				DataLakePrincipalIdentifier: "arn:aws:iam::123:user/evil",
			})

			// Backend state must be unchanged.
			s2 := b.GetDataLakeSettings()
			assert.Len(t, s2.DataLakeAdmins, 1, "mutating returned settings must not affect backend state")
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

			entries, _ := b.ListPermissions("", 0, "")
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

			entries, _ := b.ListPermissions("", 0, "")
			assert.Len(t, entries, tt.wantRemain)
		})
	}
}

func TestListLFTags_AllCatalogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCount int
	}{
		{name: "empty catalog ID returns all tags", wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			require.NoError(t, b.CreateLFTag("cat1", "env", []string{"prod", "dev"}))
			require.NoError(t, b.CreateLFTag("cat2", "tier", []string{"gold", "silver"}))

			tags, _ := b.ListLFTags("", 0, "")
			assert.Len(t, tags, tt.wantCount)
		})
	}
}

func TestDeleteLFTag_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tagKey  string
		wantErr bool
	}{
		{name: "delete non-existent tag returns error", tagKey: "nonexistent", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			err := b.DeleteLFTag("cat1", tt.tagKey)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCopyDataLakeSettings_NilFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil settings fields are preserved as nil in copy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			// Settings with all nil slices.
			b.PutDataLakeSettings(&lakeformation.DataLakeSettings{})

			s := b.GetDataLakeSettings()
			assert.Nil(t, s.DataLakeAdmins)
			assert.Nil(t, s.TrustedResourceOwners)
		})
	}
}

func TestPaginate_NextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantCount  int
		wantToken  bool
	}{
		{
			name:       "paginate returns next token when more items exist",
			maxResults: 1,
			wantCount:  1,
			wantToken:  true,
		},
		{
			name:       "paginate returns all items when max is 0",
			maxResults: 0,
			wantCount:  2,
			wantToken:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			require.NoError(t, b.RegisterResource("arn:aws:s3:::bucket-a", "arn:aws:iam::123:role/r"))
			require.NoError(t, b.RegisterResource("arn:aws:s3:::bucket-b", "arn:aws:iam::123:role/r"))

			resources, token := b.ListResources(tt.maxResults, "")
			assert.Len(t, resources, tt.wantCount)

			if tt.wantToken {
				assert.NotEmpty(t, token)
			} else {
				assert.Empty(t, token)
			}
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
			filtered, _ := b.ListPermissions("arn:aws:s3:::no-match", 0, "")
			assert.Empty(t, filtered)

			// Filter by empty ARN - should return the catalog entry.
			all, _ := b.ListPermissions("", 0, "")
			assert.Len(t, all, tt.wantCount)
		})
	}
}

func TestGetDataLakeSettings_NilBackingStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil settings stored returns empty DataLakeSettings"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			// Put nil settings to test nil case handling in GetDataLakeSettings.
			b.PutDataLakeSettings(nil)

			s := b.GetDataLakeSettings()
			assert.NotNil(t, s)
		})
	}
}

func TestCopyDataLakeSettings_WithAllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		admins     []lakeformation.DataLakePrincipal
		trusted    []string
		wantCopied bool
	}{
		{
			name: "deep copy preserves all fields",
			admins: []lakeformation.DataLakePrincipal{
				{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/admin"},
			},
			trusted:    []string{"123456789012"},
			wantCopied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			b.PutDataLakeSettings(&lakeformation.DataLakeSettings{
				DataLakeAdmins:        tt.admins,
				TrustedResourceOwners: tt.trusted,
				CreateDatabaseDefaultPermissions: []lakeformation.PrincipalPermissions{
					{Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:iam::role/r"}},
				},
				CreateTableDefaultPermissions: []lakeformation.PrincipalPermissions{
					{Principal: &lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:iam::role/t"}},
				},
			})

			s := b.GetDataLakeSettings()
			assert.Len(t, s.DataLakeAdmins, len(tt.admins))
			assert.Len(t, s.TrustedResourceOwners, len(tt.trusted))
		})
	}
}

func TestPaginate_InvalidNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nextToken string
		wantCount int
	}{
		{
			name:      "invalid next token falls back to start",
			nextToken: "not-a-number",
			wantCount: 2,
		},
		{
			name:      "negative next token falls back to start",
			nextToken: "-1",
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()

			require.NoError(t, b.RegisterResource("arn:aws:s3:::bucket-x", "arn:role"))
			require.NoError(t, b.RegisterResource("arn:aws:s3:::bucket-y", "arn:role"))

			resources, _ := b.ListResources(0, tt.nextToken)
			assert.Len(t, resources, tt.wantCount)
		})
	}
}
