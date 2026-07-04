package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// newFpgaImageFixture creates a fresh backend with a single FPGA image for
// use by a single subtest.
func newFpgaImageFixture(t *testing.T, name, description string) (*ec2.InMemoryBackend, string) {
	t.Helper()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	img, createErr := b.CreateFpgaImage(name, description)
	require.NoError(t, createErr)

	return b, img.FpgaImageID
}

func TestBackend_FpgaImage_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	t.Run("create returns available image owned by account", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "my-afi", "test description")

		imgs := b.DescribeFpgaImages([]string{afiID})
		require.Len(t, imgs, 1)
		assert.Contains(t, imgs[0].FpgaImageID, "afi-")
		assert.Contains(t, imgs[0].FpgaImageGlobalID, "agfi-")
		assert.Equal(t, "available", imgs[0].State)
		assert.Equal(t, "123456789012", imgs[0].OwnerID)
	})

	t.Run("describe returns created image", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "my-afi", "")

		imgs := b.DescribeFpgaImages([]string{afiID})
		require.Len(t, imgs, 1)
		assert.Equal(t, "my-afi", imgs[0].Name)
	})

	t.Run("describe with no filter returns all", func(t *testing.T) {
		t.Parallel()

		b, _ := newFpgaImageFixture(t, "my-afi", "")

		imgs := b.DescribeFpgaImages(nil)
		assert.NotEmpty(t, imgs)
	})

	t.Run("describe unknown id returns empty", func(t *testing.T) {
		t.Parallel()

		b, _ := newFpgaImageFixture(t, "my-afi", "")

		imgs := b.DescribeFpgaImages([]string{"afi-doesnotexist"})
		assert.Empty(t, imgs)
	})

	t.Run("delete then describe returns nothing", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "delete-me", "")

		require.NoError(t, b.DeleteFpgaImage(afiID))

		imgs := b.DescribeFpgaImages([]string{afiID})
		assert.Empty(t, imgs)
	})

	t.Run("delete unknown id returns not found", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		deleteErr := b.DeleteFpgaImage("afi-doesnotexist")
		require.ErrorIs(t, deleteErr, ec2.ErrFpgaImageNotFound)
	})
}

func TestBackend_FpgaImage_CopyFpgaImage(t *testing.T) {
	t.Parallel()

	t.Run("copy within same region inherits fields", func(t *testing.T) {
		t.Parallel()

		b, srcID := newFpgaImageFixture(t, "source-afi", "source description")

		copied, copyErr := b.CopyFpgaImage(srcID, "us-east-1", "", "")
		require.NoError(t, copyErr)
		assert.NotEqual(t, srcID, copied.FpgaImageID)
		assert.Equal(t, "source-afi", copied.Name)
		assert.Equal(t, "source description", copied.Description)
	})

	t.Run("copy overrides name and description when given", func(t *testing.T) {
		t.Parallel()

		b, srcID := newFpgaImageFixture(t, "source-afi", "source description")

		copied, copyErr := b.CopyFpgaImage(srcID, "us-east-1", "new-name", "new description")
		require.NoError(t, copyErr)
		assert.Equal(t, "new-name", copied.Name)
		assert.Equal(t, "new description", copied.Description)
	})

	t.Run("copy from unknown source in same region returns not found", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		_, copyErr := b.CopyFpgaImage("afi-doesnotexist", "us-east-1", "", "")
		require.ErrorIs(t, copyErr, ec2.ErrFpgaImageNotFound)
	})

	t.Run("copy from a different region synthesises a new image", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		copied, copyErr := b.CopyFpgaImage("afi-remote1234567", "us-west-2", "cross-region", "cross region copy")
		require.NoError(t, copyErr)
		assert.Contains(t, copied.FpgaImageID, "afi-")
		assert.Equal(t, "cross-region", copied.Name)
	})

	t.Run("missing SourceFpgaImageId returns invalid parameter", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		_, copyErr := b.CopyFpgaImage("", "us-east-1", "", "")
		require.ErrorIs(t, copyErr, ec2.ErrInvalidParameter)
	})
}

func TestBackend_FpgaImage_AttributeLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe description attribute", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "attr-afi", "initial description")

		attr, descErr := b.DescribeFpgaImageAttribute(afiID, "description")
		require.NoError(t, descErr)
		assert.Equal(t, "initial description", attr.Description)
	})

	t.Run("modify description attribute", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "attr-afi", "initial description")

		newDesc := "updated description"
		updated, modErr := b.ModifyFpgaImageAttribute(afiID, ec2.FpgaImageAttributeModification{
			Attribute:   "description",
			Description: &newDesc,
		})
		require.NoError(t, modErr)
		assert.Equal(t, "updated description", updated.Description)
	})

	t.Run("modify name attribute", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "attr-afi", "")

		newName := "renamed-afi"
		updated, modErr := b.ModifyFpgaImageAttribute(afiID, ec2.FpgaImageAttributeModification{
			Attribute: "name",
			Name:      &newName,
		})
		require.NoError(t, modErr)
		assert.Equal(t, "renamed-afi", updated.Name)
	})

	t.Run("add load permission makes public and reset clears it", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "load-perm-afi", "")

		updated, modErr := b.ModifyFpgaImageAttribute(afiID, ec2.FpgaImageAttributeModification{
			Attribute:         "loadPermission",
			LoadPermissionAdd: []ec2.FpgaImageLoadPermission{{Group: "all"}},
		})
		require.NoError(t, modErr)
		assert.True(t, updated.Public)
		require.Len(t, updated.LoadPermissions, 1)
		assert.Equal(t, "all", updated.LoadPermissions[0].Group)

		attr, descErr := b.DescribeFpgaImageAttribute(afiID, "loadPermission")
		require.NoError(t, descErr)
		require.Len(t, attr.LoadPermissions, 1)

		require.NoError(t, b.ResetFpgaImageAttribute(afiID, "loadPermission"))

		imgs := b.DescribeFpgaImages([]string{afiID})
		require.Len(t, imgs, 1)
		assert.False(t, imgs[0].Public)
		assert.Empty(t, imgs[0].LoadPermissions)
	})

	t.Run("add and remove specific user load permission", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "user-perm-afi", "")

		_, addErr := b.ModifyFpgaImageAttribute(afiID, ec2.FpgaImageAttributeModification{
			Attribute:         "loadPermission",
			LoadPermissionAdd: []ec2.FpgaImageLoadPermission{{UserID: "111122223333"}},
		})
		require.NoError(t, addErr)

		updated, removeErr := b.ModifyFpgaImageAttribute(afiID, ec2.FpgaImageAttributeModification{
			Attribute:         "loadPermission",
			LoadPermissionDel: []ec2.FpgaImageLoadPermission{{UserID: "111122223333"}},
		})
		require.NoError(t, removeErr)
		assert.Empty(t, updated.LoadPermissions)
	})

	t.Run("add and remove product codes", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "product-code-afi", "")

		updated, addErr := b.ModifyFpgaImageAttribute(afiID, ec2.FpgaImageAttributeModification{
			Attribute:     "productCodes",
			ProductCodes:  []string{"abc123"},
			OperationType: "add",
		})
		require.NoError(t, addErr)
		assert.Equal(t, []string{"abc123"}, updated.ProductCodes)

		updated2, removeErr := b.ModifyFpgaImageAttribute(afiID, ec2.FpgaImageAttributeModification{
			Attribute:     "productCodes",
			ProductCodes:  []string{"abc123"},
			OperationType: "remove",
		})
		require.NoError(t, removeErr)
		assert.Empty(t, updated2.ProductCodes)
	})

	t.Run("describe attribute with unsupported name errors", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "attr-afi", "")

		_, descErr := b.DescribeFpgaImageAttribute(afiID, "bogus")
		require.ErrorIs(t, descErr, ec2.ErrInvalidParameter)
	})

	t.Run("describe attribute for unknown image errors not found", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		_, descErr := b.DescribeFpgaImageAttribute("afi-doesnotexist", "description")
		require.ErrorIs(t, descErr, ec2.ErrFpgaImageNotFound)
	})

	t.Run("reset unsupported attribute errors", func(t *testing.T) {
		t.Parallel()

		b, afiID := newFpgaImageFixture(t, "attr-afi", "")

		resetErr := b.ResetFpgaImageAttribute(afiID, "description")
		require.ErrorIs(t, resetErr, ec2.ErrInvalidParameter)
	})

	t.Run("reset unknown image errors not found", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		resetErr := b.ResetFpgaImageAttribute("afi-doesnotexist", "loadPermission")
		require.ErrorIs(t, resetErr, ec2.ErrFpgaImageNotFound)
	})
}
