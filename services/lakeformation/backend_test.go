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
