package lakeformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
)

func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()

	b.AddLFTagInternal("cat", "k1", []string{"v1"})
	b.AddLFTagInternal("cat", "k2", []string{"v2"})
	assert.Equal(t, 2, b.TagCount())

	b.AddResourceInternal("arn:aws:s3:::bucket1", "role")
	assert.Equal(t, 1, b.ResourceCount())

	b.AddPermissionInternal(&lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{
			DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/alice",
		},
		Resource: &lakeformation.Resource{Catalog: &lakeformation.CatalogResource{}},
	})
	assert.Equal(t, 1, b.PermissionCount())

	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "cat",
		DatabaseName:   "db",
		TableName:      "tbl",
		Name:           "filter1",
	})
	assert.Equal(t, 1, b.DataCellsFilterCount())

	b.AddLFTagExpressionInternal(&lakeformation.LFTagExpression{
		Name:      "expr1",
		CatalogID: "cat",
	})
	assert.Equal(t, 1, b.LFTagExpressionCount())
}

func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	assert.Equal(t, 0, b.ResourceCount())
	assert.Equal(t, 0, b.TagCount())
	assert.Equal(t, 0, b.PermissionCount())
	assert.Equal(t, 0, b.DataCellsFilterCount())
	assert.Equal(t, 0, b.LFTagExpressionCount())
	assert.Equal(t, 0, b.OptInCount())
	assert.Equal(t, 0, b.TransactionCount())
}

func TestExportHelpers(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	assert.Equal(t, 0, b.ResourceLFTagCount())
	assert.Equal(t, 0, b.IdentityCenterConfigCount())

	b.AddLFTagInternal("", "k", []string{"v"})
	_, _ = b.CreateLakeFormationIdentityCenterConfiguration("123", "arn:aws:sso:::instance/ssoins-abc", nil, nil)
	assert.Equal(t, 1, b.IdentityCenterConfigCount())
}
